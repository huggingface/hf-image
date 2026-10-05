//! Push an image already built: layers are stored as their uncompressed tar (digest = diff_id), so
//! Xet deduplicates them chunk by chunk. A layer the registry already has (probed by diff_id) is
//! never read; a new one is decompressed once and only its new chunks are uploaded. With
//! `preserve_digests`, blobs and manifests are pushed byte for byte instead.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, mpsc};

use crate::containerd::{Containerd, Store};
use crate::oci::{self, Compression, Descriptor, Index, Manifest, diff_ids, is_index, sha256};
use crate::reference::ImageRef;
use crate::registry::{Registration, Registry};
use crate::util;
use crate::xet::{FileUpload, Uploader, Xet};

const DECODE_CHUNK: usize = 4 << 20;

pub struct PushOpts {
    pub image: ImageRef,
    pub token: Option<String>,
    pub from: Origin,
    /// Pushes blobs and manifests as stored (same digests as `docker push`), without tar conversion.
    pub preserve_digests: bool,
}

/// The image to push: in a daemon's store, or an OCI layout.
pub enum Origin {
    Store { store: Store, name: String },
    Layout(PathBuf),
}

/// Where the image to push is read from.
#[async_trait]
pub trait Source: Send + Sync {
    async fn top(&self) -> anyhow::Result<(String, String)>;
    async fn has(&self, digest: &str) -> anyhow::Result<bool>;
    async fn read(&self, digest: &str) -> anyhow::Result<BoxStream<'static, anyhow::Result<Bytes>>>;
    async fn read_all(&self, digest: &str) -> anyhow::Result<Bytes> {
        let mut s = self.read(digest).await?;
        let mut out = Vec::new();
        while let Some(b) = s.next().await {
            out.extend_from_slice(&b?);
        }
        Ok(out.into())
    }
}

struct LocalImage {
    cd: Containerd,
    name: String,
}

#[async_trait]
impl Source for LocalImage {
    async fn top(&self) -> anyhow::Result<(String, String)> {
        let d = self.cd.image(&self.name).await?.with_context(|| format!("no local image {}", self.name))?;
        Ok((d.media_type, d.digest))
    }
    async fn has(&self, digest: &str) -> anyhow::Result<bool> {
        Ok(self.cd.info(digest).await?.is_some())
    }
    async fn read(&self, digest: &str) -> anyhow::Result<BoxStream<'static, anyhow::Result<Bytes>>> {
        Ok(self.cd.read(digest).await?.boxed())
    }
}

struct Layout {
    dir: PathBuf,
}

impl Layout {
    fn path(&self, digest: &str) -> PathBuf {
        self.dir.join("blobs").join("sha256").join(digest.trim_start_matches("sha256:"))
    }
}

#[async_trait]
impl Source for Layout {
    async fn top(&self) -> anyhow::Result<(String, String)> {
        let index: Index = serde_json::from_slice(&tokio::fs::read(self.dir.join("index.json")).await?)?;
        let d = index.manifests.first().context("empty OCI layout")?;
        Ok((d.media_type.clone(), d.digest.clone()))
    }
    async fn has(&self, digest: &str) -> anyhow::Result<bool> {
        Ok(tokio::fs::try_exists(self.path(digest)).await?)
    }
    async fn read(&self, digest: &str) -> anyhow::Result<BoxStream<'static, anyhow::Result<Bytes>>> {
        let f = tokio::fs::File::open(self.path(digest)).await.with_context(|| format!("blob {digest} is missing"))?;
        let s = tokio_util_stream(f);
        Ok(s.boxed())
    }
}

fn tokio_util_stream(f: tokio::fs::File) -> impl futures::Stream<Item = anyhow::Result<Bytes>> {
    futures::stream::unfold(Some(f), |state| async move {
        use tokio::io::AsyncReadExt;
        let mut f = state?;
        let mut buf = vec![0u8; DECODE_CHUNK];
        match f.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some((Ok(Bytes::from(buf)), Some(f)))
            }
            Err(e) => Some((Err(e.into()), None)),
        }
    })
}

#[derive(Default)]
struct Totals {
    layers: usize,
    new_layers: usize,
    read: u64,
    new_bytes: u64,
    registrations: Vec<Registration>,
}

pub async fn push(opts: &PushOpts) -> anyhow::Result<String> {
    let start = Instant::now();
    let source: Box<dyn Source> = match &opts.from {
        Origin::Layout(dir) => Box::new(Layout { dir: dir.clone() }),
        Origin::Store { store, name } => Box::new(LocalImage { cd: store.connect().await?, name: name.clone() }),
    };
    let registry = Registry::new(&opts.image, opts.token.clone(), true)?;
    let xet = Xet::new(registry.clone())?;
    let uploader = xet.uploader().await?;
    let totals = Mutex::new(Totals::default());
    let ctx = Ctx { source: source.as_ref(), registry: &registry, uploader: &uploader, totals: &totals };
    let plan = if opts.preserve_digests { ctx.preserve().await? } else { ctx.convert().await? };

    uploader.finalize().await?;
    let totals = totals.into_inner();
    registry.register(&totals.registrations).await?;
    for (digest, media_type, bytes) in &plan.children {
        registry.put_manifest(digest, bytes, media_type).await?;
    }
    let digest = registry.put_manifest(&opts.image.tag_or_latest(), &plan.top, &plan.top_type).await?;
    eprintln!(
        "pushed {} ({}) in {}: {} layers, {} new; {} read, {} new after dedup",
        opts.image.tagged(),
        &digest[..19],
        util::secs(start.elapsed()),
        totals.layers,
        totals.new_layers,
        util::bytes(totals.read),
        util::bytes(totals.new_bytes),
    );
    Ok(digest)
}

fn attestation(d: &Descriptor) -> bool {
    d.annotations.as_ref().is_some_and(|a| a.get("vnd.docker.reference.type").is_some())
}

/// Manifests to put once blobs are registered: children by digest, then the top one by tag.
struct Plan {
    /// (digest, media type, bytes)
    children: Vec<(String, String, Bytes)>,
    top_type: String,
    top: Bytes,
}

impl Plan {
    /// The image's manifests as stored, image manifests before attestations, and the blobs they
    /// reference, each once.
    async fn verbatim(source: &dyn Source) -> anyhow::Result<(Self, Vec<Descriptor>)> {
        let (top_type, top_digest) = source.top().await?;
        let top = Self::read(source, &top_digest).await?;
        let mut children = Vec::new();
        if is_index(&top_type) {
            let index: Index = serde_json::from_slice(&top).context("invalid image index")?;
            let mut descs = index.manifests;
            descs.sort_by_key(attestation);
            for d in descs {
                if is_index(&d.media_type) {
                    bail!("manifest {} is an index: nested indexes are not supported", d.digest);
                }
                if !source.has(&d.digest).await? {
                    let platform = d.platform.as_ref().map(|p| format!(" ({}/{})", p.os, p.architecture));
                    bail!(
                        "cannot preserve digests: manifest {}{} of the index is missing locally; pull every platform \
                         first (`docker pull --platform <os>/<arch>` for each), or push without --preserve-digests",
                        d.digest,
                        platform.unwrap_or_default(),
                    );
                }
                let bytes = Self::read(source, &d.digest).await?;
                children.push((d.digest, d.media_type, bytes));
            }
        }
        let docs = if is_index(&top_type) { children.iter().map(|(_, _, b)| b).collect() } else { vec![&top] };
        let (mut seen, mut blobs) = (HashSet::new(), Vec::new());
        for bytes in docs {
            let m: Manifest = serde_json::from_slice(bytes).context("invalid manifest")?;
            for d in std::iter::once(m.config).chain(m.layers) {
                if seen.insert(d.digest.clone()) {
                    blobs.push(d);
                }
            }
        }
        Ok((Self { children, top_type, top }, blobs))
    }

    /// `original` when `converted` is the same JSON document, so a no-op conversion keeps the digest.
    fn unchanged(original: &Bytes, converted: Vec<u8>) -> Bytes {
        let parse = |b: &[u8]| serde_json::from_slice::<serde_json::Value>(b).ok();
        match (parse(original), parse(&converted)) {
            (Some(a), Some(b)) if a == b => original.clone(),
            _ => converted.into(),
        }
    }

    /// A manifest's bytes, checked against its digest.
    async fn read(source: &dyn Source, digest: &str) -> anyhow::Result<Bytes> {
        let bytes = source.read_all(digest).await?;
        if sha256(&bytes) != digest {
            bail!("manifest {digest} does not match its digest");
        }
        Ok(bytes)
    }
}

struct Ctx<'a> {
    source: &'a dyn Source,
    registry: &'a Arc<Registry>,
    uploader: &'a Uploader,
    totals: &'a Mutex<Totals>,
}

impl Ctx<'_> {
    /// The image with tar layers; manifests the conversion leaves as they were keep their bytes.
    async fn convert(&self) -> anyhow::Result<Plan> {
        let (top_type, top_digest) = self.source.top().await?;
        let top = self.source.read_all(&top_digest).await?;
        if !is_index(&top_type) {
            let (top_type, top) = self.image(&top).await?;
            return Ok(Plan { children: vec![], top_type, top });
        }
        let index: Index = serde_json::from_slice(&top)?;
        let mut children = Vec::new();
        for (i, d) in index.manifests.iter().enumerate() {
            if self.source.has(&d.digest).await? {
                children.push((i, d.clone()));
            }
        }
        if children.is_empty() {
            bail!("none of the image's manifests are available locally");
        }
        // Image manifests first: attestations point at their new digests.
        children.sort_by_key(|(_, d)| attestation(d));
        let mut renamed = HashMap::new();
        let mut plan = Vec::new();
        let mut manifests = Vec::new();
        for (i, d) in children {
            let bytes = self.source.read_all(&d.digest).await?;
            let (new_type, new_bytes) =
                if attestation(&d) { self.verbatim(&bytes, &renamed).await? } else { self.image(&bytes).await? };
            let new_digest = sha256(&new_bytes);
            renamed.insert(d.digest.clone(), new_digest.clone());
            let mut nd = d.clone();
            nd.media_type = new_type.clone();
            nd.digest = new_digest.clone();
            nd.size = new_bytes.len() as u64;
            if let Some(a) = nd.annotations.as_mut()
                && let Some(target) = a.get("vnd.docker.reference.digest").and_then(|t| renamed.get(t))
            {
                a.insert("vnd.docker.reference.digest".into(), target.clone());
            }
            plan.push((new_digest, new_type, new_bytes));
            manifests.push((i, nd));
        }
        manifests.sort_by_key(|(i, _)| *i);
        let mut out = index;
        out.media_type = Some(oci::OCI_INDEX.into());
        out.manifests = manifests.into_iter().map(|(_, d)| d).collect();
        let top = Plan::unchanged(&top, serde_json::to_vec(&out)?);
        Ok(Plan { children: plan, top_type: oci::OCI_INDEX.into(), top })
    }

    /// The image as stored: every blob and manifest byte for byte.
    async fn preserve(&self) -> anyhow::Result<Plan> {
        let (plan, blobs) = Plan::verbatim(self.source).await?;
        futures::stream::iter(blobs.iter().map(anyhow::Ok))
            .try_for_each_concurrent(util::concurrency(), |d| self.raw_blob(d))
            .await?;
        Ok(plan)
    }

    /// Uploads an image manifest's config and layers; returns the manifest with tar layers.
    async fn image(&self, bytes: &Bytes) -> anyhow::Result<(String, Bytes)> {
        let mut m: Manifest = serde_json::from_slice(bytes).context("invalid image manifest")?;
        let config = self.source.read_all(&m.config.digest).await?;
        let diffs = diff_ids(&config)?;
        if diffs.len() != m.layers.len() {
            bail!("the config lists {} layers, the manifest {}", diffs.len(), m.layers.len());
        }
        self.small_blob(&m.config.digest, config).await?;
        let sizes: Vec<_> = futures::stream::iter(m.layers.iter().zip(diffs.iter()).map(|(l, d)| self.layer(l, d)))
            .buffered(util::concurrency())
            .collect()
            .await;
        let sizes = sizes.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
        m.to_tar(&diffs, sizes);
        Ok((oci::OCI_MANIFEST.into(), Plan::unchanged(bytes, serde_json::to_vec(&m)?)))
    }

    /// An attestation or artifact manifest: its blobs as is, references to renamed manifests fixed.
    async fn verbatim(&self, bytes: &Bytes, renamed: &HashMap<String, String>) -> anyhow::Result<(String, Bytes)> {
        let mut m: Manifest = serde_json::from_slice(bytes).context("invalid manifest")?;
        for d in std::iter::once(&m.config).chain(m.layers.iter()) {
            let blob = self.source.read_all(&d.digest).await?;
            self.small_blob(&d.digest, blob).await?;
        }
        if let Some(serde_json::Value::Object(s)) = m.extra.get_mut("subject")
            && let Some(new) = s.get("digest").and_then(|d| d.as_str()).and_then(|d| renamed.get(d)).cloned()
        {
            s.insert("digest".into(), new.into());
        }
        let media_type = m.media_type.clone().unwrap_or_else(|| oci::OCI_MANIFEST.into());
        Ok((media_type, Plan::unchanged(bytes, serde_json::to_vec(&m)?)))
    }

    /// Uploads a blob as stored unless the registry has it, checking its digest and size.
    async fn raw_blob(&self, d: &Descriptor) -> anyhow::Result<()> {
        let layer = oci::is_layer(&d.media_type);
        if layer {
            self.totals.lock().await.layers += 1;
        }
        if self.registry.blob(&d.digest).await?.is_some() {
            return Ok(());
        }
        let mut stream = self.source.read(&d.digest).await?;
        let mut up = self.uploader.file(&d.digest, Some(d.size))?;
        let (mut hasher, mut size) = (Sha256::new(), 0u64);
        while let Some(b) = stream.next().await {
            let b = b?;
            hasher.update(&b);
            size += b.len() as u64;
            up.write(b).await?;
        }
        let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
        if digest != d.digest || size != d.size {
            bail!("blob {} is {digest} ({size} bytes) locally, the manifest says {} bytes", d.digest, d.size);
        }
        let done = up.finish().await?;
        let mut t = self.totals.lock().await;
        t.new_layers += usize::from(layer);
        t.read += size;
        t.new_bytes += done.new_bytes;
        t.registrations.push(Registration { digest, xet_hash: done.hash, size });
        Ok(())
    }

    async fn small_blob(&self, digest: &str, bytes: Bytes) -> anyhow::Result<()> {
        if self.registry.blob(digest).await?.is_some() {
            return Ok(());
        }
        let size = bytes.len() as u64;
        let mut up = self.uploader.file(digest, Some(size))?;
        up.write(bytes).await?;
        let done = up.finish().await?;
        let mut t = self.totals.lock().await;
        t.new_bytes += done.new_bytes;
        t.registrations.push(Registration { digest: digest.into(), xet_hash: done.hash, size });
        Ok(())
    }

    /// Returns the uploaded (or already present) tar size of a layer.
    async fn layer(&self, layer: &Descriptor, diff_id: &str) -> anyhow::Result<u64> {
        self.totals.lock().await.layers += 1;
        if !oci::is_layer(&layer.media_type) {
            let blob = self.source.read_all(&layer.digest).await?;
            let size = blob.len() as u64;
            self.small_blob(&layer.digest, blob).await?;
            return Ok(size);
        }
        if let Some(existing) = self.registry.blob(diff_id).await? {
            return existing.size.with_context(|| format!("the registry gave no size for layer {diff_id}"));
        }
        let stream = self.source.read(&layer.digest).await?;
        let upload = self.uploader.file(diff_id, None)?;
        let (digest, size, read, done) = decode_into(stream, upload).await?;
        if digest != diff_id {
            bail!("layer {} decompresses to {digest}, the config says {diff_id}", layer.digest);
        }
        let mut t = self.totals.lock().await;
        t.new_layers += 1;
        t.read += read;
        t.new_bytes += done.new_bytes;
        t.registrations.push(Registration { digest, xet_hash: done.hash, size });
        Ok(size)
    }
}

/// Decompresses a layer (gzip, zstd or plain tar, by magic) into an upload, hashing the tar.
/// Returns (tar digest, tar size, bytes read, upload).
async fn decode_into(
    mut stream: BoxStream<'static, anyhow::Result<Bytes>>,
    mut upload: FileUpload,
) -> anyhow::Result<(String, u64, u64, crate::xet::Uploaded)> {
    let (in_tx, in_rx) = mpsc::channel::<Bytes>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<Bytes>(4);
    let decoder = tokio::task::spawn_blocking(move || -> anyhow::Result<(String, u64)> {
        let mut reader = ChannelReader { rx: in_rx, cur: Bytes::new() };
        let mut head = [0u8; 4];
        let n = read_up_to(&mut reader, &mut head)?;
        let head = &head[..n];
        let chained = std::io::Cursor::new(head.to_vec()).chain(reader);
        let mut decoded: Box<dyn Read> = match Compression::sniff(head) {
            Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(chained)),
            Compression::Zstd => Box::new(zstd::Decoder::new(chained)?),
            Compression::None => Box::new(chained),
        };
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        loop {
            let mut buf = vec![0u8; DECODE_CHUNK];
            let n = read_up_to(&mut decoded, &mut buf)?;
            if n == 0 {
                break;
            }
            buf.truncate(n);
            hasher.update(&buf);
            size += n as u64;
            if out_tx.blocking_send(Bytes::from(buf)).is_err() {
                bail!("upload stopped");
            }
        }
        Ok((format!("sha256:{}", hex::encode(hasher.finalize())), size))
    });
    let feed = async {
        let mut read = 0u64;
        while let Some(b) = stream.next().await {
            let b = b?;
            read += b.len() as u64;
            if in_tx.send(b).await.is_err() {
                break;
            }
        }
        drop(in_tx);
        anyhow::Ok(read)
    };
    // Owns `out_rx`: a failed upload closes the channel, which stops the decoder, then `feed`.
    let up = &mut upload;
    let drain = async move {
        while let Some(b) = out_rx.recv().await {
            up.write(b).await?;
        }
        anyhow::Ok(())
    };
    let (read, drained) = tokio::join!(feed, drain);
    let read = read?;
    drained?;
    let (digest, size) = decoder.await??;
    let done = upload.finish().await?;
    Ok((digest, size, read, done))
}

fn read_up_to(r: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

struct ChannelReader {
    rx: mpsc::Receiver<Bytes>,
    cur: Bytes,
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            match self.rx.blocking_recv() {
                Some(b) => self.cur = b,
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.cur.len());
        buf[..n].copy_from_slice(&self.cur[..n]);
        self.cur = self.cur.slice(n..);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// An in-memory image store.
    #[derive(Default)]
    struct Fake {
        top: (String, String),
        blobs: HashMap<String, Bytes>,
    }

    impl Fake {
        fn add(&mut self, bytes: impl Into<Bytes>) -> String {
            let bytes = bytes.into();
            let digest = sha256(&bytes);
            self.blobs.insert(digest.clone(), bytes);
            digest
        }
    }

    #[async_trait]
    impl Source for Fake {
        async fn top(&self) -> anyhow::Result<(String, String)> {
            Ok(self.top.clone())
        }
        async fn has(&self, digest: &str) -> anyhow::Result<bool> {
            Ok(self.blobs.contains_key(digest))
        }
        async fn read(&self, digest: &str) -> anyhow::Result<BoxStream<'static, anyhow::Result<Bytes>>> {
            let b = self.blobs.get(digest).cloned().with_context(|| format!("no blob {digest}"))?;
            Ok(futures::stream::once(async { Ok(b) }).boxed())
        }
    }

    fn desc(media_type: &str, digest: &str, size: usize) -> serde_json::Value {
        json!({ "mediaType": media_type, "digest": digest, "size": size })
    }

    #[test]
    fn a_no_op_conversion_keeps_the_bytes() {
        let diff = sha256(b"tar");
        let raw = Bytes::from(format!(
            r#"{{ "layers": [{{"size": 3, "digest": "{diff}", "mediaType": "{}"}}],
                "config": {{"digest": "sha256:c", "size": 2, "mediaType": "{}"}},
                "mediaType": "{}", "schemaVersion": 2 }}"#,
            oci::OCI_LAYER_TAR,
            oci::OCI_CONFIG,
            oci::OCI_MANIFEST,
        ));
        let mut m: Manifest = serde_json::from_slice(&raw).unwrap();
        m.to_tar(&[diff], vec![3]);
        assert_eq!(Plan::unchanged(&raw, serde_json::to_vec(&m).unwrap()), raw);
    }

    #[test]
    fn a_compressed_layer_is_rewritten_as_tar() {
        let (gz, diff) = (sha256(b"gz"), sha256(b"tar"));
        let raw = Bytes::from(
            serde_json::to_vec(&json!({
                "schemaVersion": 2,
                "mediaType": oci::DOCKER_MANIFEST,
                "config": desc(oci::DOCKER_CONFIG, "sha256:c", 2),
                "layers": [desc("application/vnd.docker.image.rootfs.diff.tar.gzip", &gz, 2)],
            }))
            .unwrap(),
        );
        let mut m: Manifest = serde_json::from_slice(&raw).unwrap();
        m.to_tar(std::slice::from_ref(&diff), vec![3]);
        let out = Plan::unchanged(&raw, serde_json::to_vec(&m).unwrap());
        assert_ne!(out, raw);
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["layers"][0], desc(oci::OCI_LAYER_TAR, &diff, 3));
        assert_eq!(v["config"]["mediaType"], oci::OCI_CONFIG);
    }

    #[tokio::test]
    async fn preserve_digests_pushes_every_manifest_as_stored() {
        let mut s = Fake::default();
        let config = s.add(&b"{}"[..]);
        let layer = s.add(&b"gz"[..]);
        let statement = s.add(&b"intoto"[..]);
        let image = json!({ "schemaVersion": 2, "config": desc(oci::DOCKER_CONFIG, &config, 2),
            "layers": [desc("application/vnd.docker.image.rootfs.diff.tar.gzip", &layer, 2)] });
        let image = Bytes::from(format!("{image:#}"));
        let image_digest = s.add(image.clone());
        let att = json!({ "schemaVersion": 2, "config": desc(oci::OCI_CONFIG, &config, 2),
            "layers": [desc("application/vnd.in-toto+json", &statement, 6)] });
        let att_digest = s.add(serde_json::to_vec(&att).unwrap());
        let mut att_desc = desc(oci::OCI_MANIFEST, &att_digest, 1);
        att_desc["annotations"] = json!({ "vnd.docker.reference.type": "attestation-manifest" });
        let index =
            json!({ "schemaVersion": 2, "manifests": [att_desc, desc(oci::DOCKER_MANIFEST, &image_digest, 1)] });
        let index = Bytes::from(serde_json::to_vec(&index).unwrap());
        s.top = (oci::OCI_INDEX.into(), s.add(index.clone()));

        let (plan, blobs) = Plan::verbatim(&s).await.unwrap();
        let children: Vec<_> = plan.children.iter().map(|(d, t, b)| (d.as_str(), t.as_str(), b.clone())).collect();
        assert_eq!(
            children,
            [
                (image_digest.as_str(), oci::DOCKER_MANIFEST, image),
                (att_digest.as_str(), oci::OCI_MANIFEST, Bytes::from(serde_json::to_vec(&att).unwrap()))
            ]
        );
        assert_eq!((plan.top_type.as_str(), plan.top), (oci::OCI_INDEX, index));
        let blobs: Vec<_> = blobs.iter().map(|d| d.digest.as_str()).collect();
        assert_eq!(blobs, [config.as_str(), layer.as_str(), statement.as_str()]);
    }

    #[tokio::test]
    async fn preserve_digests_refuses_an_index_with_a_missing_manifest() {
        let mut s = Fake::default();
        let index = json!({ "schemaVersion": 2, "manifests": [desc(oci::OCI_MANIFEST, &sha256(b"elsewhere"), 1)] });
        s.top = (oci::OCI_INDEX.into(), s.add(serde_json::to_vec(&index).unwrap()));
        let err = Plan::verbatim(&s).await.err().unwrap();
        assert!(err.to_string().contains("missing locally"), "{err}");
    }
}
