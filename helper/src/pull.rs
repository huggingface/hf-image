//! Pull: layers come straight from Xet (only chunks not already cached), into containerd's content
//! store where Docker reads images, and are unpacked in order as they land (plain tar layers need
//! no decompression). `Target::Layout` writes an OCI layout instead.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, bail};
use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncReadExt;

use crate::containerd::Containerd;
use crate::oci::{self, Descriptor, Index, Manifest, Platform, chain_ids, diff_ids, is_index};
use crate::reference::ImageRef;
use crate::registry::{BlobInfo, Registry};
use crate::util::{self, xet_stream};
use crate::xet::{Downloader, Xet};

pub struct PullOpts {
    pub image: ImageRef,
    pub token: Option<String>,
    pub platform: Platform,
    pub target: Target,
    /// Keeps a local image that is current, that the registry does not have, or that it cannot
    /// be asked about.
    pub if_stale: bool,
}

/// Where a pull writes.
pub enum Target {
    Store(Containerd),
    Layout(PathBuf),
}

struct Resolved {
    top: oci::Descriptor,
    top_bytes: Bytes,
    manifest: Manifest,
    manifest_desc: oci::Descriptor,
    manifest_bytes: Bytes,
    config: Bytes,
}

/// Returns the image's digest.
pub async fn pull(opts: PullOpts) -> anyhow::Result<String> {
    let start = Instant::now();
    let registry = Registry::new(&opts.image, opts.token.clone(), false)?;
    let name = local_name(&opts.image);
    if opts.if_stale
        && let Target::Store(cd) = &opts.target
    {
        let reference = opts.image.reference();
        let (local, remote) = tokio::join!(cd.image(&name), registry.manifest(&reference));
        if let Some(local) = local? {
            let current = match remote {
                Ok(Some(r)) => r.digest == local.digest,
                Ok(None) => true,
                Err(e) => {
                    tracing::warn!("{e:#}: running the local image");
                    true
                }
            };
            if current {
                return Ok(local.digest);
            }
        }
    }
    let xet = Xet::new(registry.clone())?;
    // Independent round trips, overlapped.
    let (resolved, downloader) = tokio::try_join!(resolve(&registry, &opts.image, &opts.platform), xet.downloader())?;
    let downloader = Arc::new(downloader);
    let (layers, fetched) = match opts.target {
        Target::Layout(dir) => into_layout(&dir, &registry, &downloader, &opts.image, &resolved).await?,
        Target::Store(cd) => into_containerd(cd, &registry, &downloader, &opts.image, &resolved).await?,
    };
    let layer_bytes = resolved.manifest.layers.iter().map(|l| l.size).sum();
    eprintln!(
        "pulled {name} ({}) in {}: {layers} layers ({}), {fetched} fetched",
        short(&resolved.top.digest),
        util::secs(start.elapsed()),
        util::bytes(layer_bytes),
    );
    Ok(resolved.top.digest)
}

pub fn local_name(image: &ImageRef) -> String {
    match &image.digest {
        Some(d) if image.tag.is_none() => format!("{}/{}@{d}", image.registry, image.repo),
        _ => image.tagged(),
    }
}

fn short(digest: &str) -> &str {
    &digest[..digest.len().min(19)]
}

async fn resolve(registry: &Registry, image: &ImageRef, platform: &Platform) -> anyhow::Result<Resolved> {
    let top = registry.manifest(&image.reference()).await?.with_context(|| format!("{image} not found"))?;
    let top_type = media_type(&top.media_type, &top.bytes);
    let top_desc = Descriptor::new(&top_type, &top.digest, top.bytes.len() as u64);
    let (manifest_desc, manifest_bytes) = if is_index(&top_type) {
        let index: Index = serde_json::from_slice(&top.bytes).context("invalid image index")?;
        let child = index.select(platform).with_context(|| format!("{image} has no {platform} image"))?;
        let m =
            registry.manifest(&child.digest).await?.with_context(|| format!("manifest {} not found", child.digest))?;
        (child.clone(), m.bytes)
    } else {
        (top_desc.clone(), top.bytes.clone())
    };
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).context("invalid image manifest")?;
    let config = registry.fetch_blob(&manifest.config.digest).await?;
    Ok(Resolved { top: top_desc, top_bytes: top.bytes, manifest, manifest_desc, manifest_bytes, config })
}

/// The header's media type, else the document's own `mediaType` (or its shape).
fn media_type(header: &str, bytes: &[u8]) -> String {
    if !header.is_empty() && header != "application/octet-stream" {
        return header.to_string();
    }
    let v: serde_json::Value = serde_json::from_slice(bytes).unwrap_or_default();
    match v["mediaType"].as_str() {
        Some(m) => m.to_string(),
        None if v.get("manifests").is_some() => oci::OCI_INDEX.into(),
        None => oci::OCI_MANIFEST.into(),
    }
}

/// The registry's `HEAD` of a layer.
async fn layer_info(registry: &Registry, layer: &Descriptor) -> anyhow::Result<BlobInfo> {
    registry.blob(&layer.digest).await?.with_context(|| format!("layer {} is missing", layer.digest))
}

/// A layer's bytes: from Xet when the registry stores it there, else from the registry.
async fn layer_stream(
    registry: &Registry,
    downloader: &Downloader,
    layer: &Descriptor,
    info: BlobInfo,
) -> anyhow::Result<futures::stream::BoxStream<'static, anyhow::Result<Bytes>>> {
    match info.xet_hash {
        Some(h) => Ok(xet_stream(downloader.stream(&h, layer.size).await?).boxed()),
        None => {
            let resp = registry.download(&layer.digest).await?;
            Ok(resp.bytes_stream().map(|r| r.map_err(anyhow::Error::from)).boxed())
        }
    }
}

async fn into_containerd(
    cd: Containerd,
    registry: &Arc<Registry>,
    downloader: &Arc<Downloader>,
    image: &ImageRef,
    r: &Resolved,
) -> anyhow::Result<(usize, usize)> {
    let cd = Arc::new(cd.leased().await?);
    let result = write_image(&cd, registry, downloader, image, r).await;
    cd.release().await;
    result
}

async fn write_image(
    cd: &Arc<Containerd>,
    registry: &Arc<Registry>,
    downloader: &Arc<Downloader>,
    image: &ImageRef,
    r: &Resolved,
) -> anyhow::Result<(usize, usize)> {
    let diffs = diff_ids(&r.config)?;
    if diffs.len() != r.manifest.layers.len() {
        bail!("the config lists {} layers, the manifest {}", diffs.len(), r.manifest.layers.len());
    }
    let chains = chain_ids(&diffs);
    let source = (format!("containerd.io/distribution.source.{}", image.registry), image.repo.clone());
    let permits = Arc::new(tokio::sync::Semaphore::new(util::concurrency()));

    // Layers download concurrently; each resolves to whether it was fetched.
    let mut fetches = Vec::new();
    for (layer, diff) in r.manifest.layers.iter().zip(&diffs) {
        let (cd, registry, downloader, permits) = (cd.clone(), registry.clone(), downloader.clone(), permits.clone());
        let (layer, diff, source) = (layer.clone(), diff.clone(), source.clone());
        fetches.push(tokio::spawn(async move {
            if cd.info(&layer.digest).await?.is_some() {
                return anyhow::Ok(false);
            }
            let _permit = permits.acquire_owned().await?;
            let mut labels = HashMap::from([source]);
            if layer.digest != diff {
                labels.insert("containerd.io/uncompressed".into(), diff.clone());
            }
            let info = layer_info(&registry, &layer).await?;
            let stream = layer_stream(&registry, &downloader, &layer, info).await?;
            cd.write(&layer.digest, layer.size, labels, stream).await?;
            Ok(true)
        }));
    }
    // Unpack in chain order as layers land.
    let mut fetched = 0;
    for (i, (fetch, layer)) in fetches.into_iter().zip(&r.manifest.layers).enumerate() {
        fetched += usize::from(fetch.await??);
        let parent = i.checked_sub(1).map(|p| chains[p].as_str());
        cd.unpack(&layer.into(), &diffs[i], &chains[i], parent).await?;
    }

    let sn_label = format!("containerd.io/gc.ref.snapshot.{}", cd.snapshotter);
    let mut labels = HashMap::from([source.clone()]);
    if let Some(top) = chains.last() {
        labels.insert(sn_label, top.clone());
    }
    cd.put(&r.manifest.config.digest, r.config.clone(), labels).await?;
    let mut labels = HashMap::from([
        ("containerd.io/gc.ref.content.config".to_string(), r.manifest.config.digest.clone()),
        source.clone(),
    ]);
    for (i, l) in r.manifest.layers.iter().enumerate() {
        labels.insert(format!("containerd.io/gc.ref.content.l.{i}"), l.digest.clone());
    }
    cd.put(&r.manifest_desc.digest, r.manifest_bytes.clone(), labels).await?;
    if r.top.digest != r.manifest_desc.digest {
        let labels = HashMap::from([("containerd.io/gc.ref.content.m.0".to_string(), r.manifest_desc.digest.clone())]);
        cd.put(&r.top.digest, r.top_bytes.clone(), labels).await?;
    }
    cd.put_image(&local_name(image), (&r.top).into()).await?;
    Ok((r.manifest.layers.len(), fetched))
}

async fn into_layout(
    dir: &Path,
    registry: &Arc<Registry>,
    downloader: &Arc<Downloader>,
    image: &ImageRef,
    r: &Resolved,
) -> anyhow::Result<(usize, usize)> {
    let blobs = dir.join("blobs").join("sha256");
    tokio::fs::create_dir_all(&blobs).await?;
    let path = |digest: &str| blobs.join(digest.trim_start_matches("sha256:"));
    let mut fetched = 0;
    // A blob listed twice (e.g. the empty layer) is written once: concurrent writers race on the rename.
    let mut seen = HashSet::new();
    let layers: Vec<_> = r.manifest.layers.iter().filter(|l| seen.insert(l.digest.as_str())).cloned().collect();
    let mut tasks = futures::stream::iter(layers.into_iter().map(|layer| {
        let (registry, downloader, target) = (registry.clone(), downloader.clone(), path(&layer.digest));
        async move {
            if tokio::fs::try_exists(&target).await? {
                return anyhow::Ok(false);
            }
            let info = layer_info(&registry, &layer).await?;
            let partial = target.with_extension("partial");
            match info.xet_hash {
                Some(h) => downloader.to_file(&h, layer.size, &partial).await?,
                None => {
                    let mut out = tokio::fs::File::create(&partial).await?;
                    let mut s = registry.download(&layer.digest).await?.bytes_stream();
                    while let Some(b) = s.next().await {
                        tokio::io::AsyncWriteExt::write_all(&mut out, &b?).await?;
                    }
                }
            }
            if file_digest(&partial).await? != layer.digest {
                bail!("layer {} does not match its digest", layer.digest);
            }
            tokio::fs::rename(&partial, &target).await?;
            Ok(true)
        }
    }))
    .buffer_unordered(util::concurrency());
    while let Some(done) = tasks.next().await {
        fetched += usize::from(done?);
    }
    tokio::fs::write(path(&r.manifest.config.digest), &r.config).await?;
    tokio::fs::write(path(&r.manifest_desc.digest), &r.manifest_bytes).await?;
    tokio::fs::write(path(&r.top.digest), &r.top_bytes).await?;
    let mut top = r.top.clone();
    top.annotations = Some([("org.opencontainers.image.ref.name".to_string(), image.tag_or_latest())].into());
    let index = serde_json::json!({ "schemaVersion": 2, "mediaType": oci::OCI_INDEX, "manifests": [top] });
    tokio::fs::write(dir.join("index.json"), serde_json::to_vec_pretty(&index)?).await?;
    tokio::fs::write(dir.join("oci-layout"), br#"{"imageLayoutVersion":"1.0.0"}"#).await?;
    Ok((r.manifest.layers.len(), fetched))
}

async fn file_digest(path: &Path) -> anyhow::Result<String> {
    use sha2::{Digest as _, Sha256};
    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 4 << 20];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}
