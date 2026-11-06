//! A containerd proxy snapshotter with overlayfs' layout. Layers of images on Xet registries are
//! pulled and unpacked here, every layer of an image at once, while containerd waits in `Prepare`
//! for each in turn and then skips its download (`ALREADY_EXISTS`); other layers unpack as usual.
//! `hf-image-apply` writes each layer: containerd's own applier, in its own process. Snapshot
//! records live in memory: a restart forgets them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use anyhow::{Context, bail};
use containerd_snapshots::api::types::Mount;
use containerd_snapshots::{Info, Kind, Usage};
use futures::future::{BoxFuture, FutureExt, Shared};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tonic::Status;

use crate::oci::{Manifest, chain_ids, diff_ids};
use crate::reference::ImageRef;
use crate::registry::Registry;
use crate::untar::{self, Hashing};
use crate::util::{self, xet_stream};
use crate::xet::{Downloader, Xet};

const SNAPSHOT_REF: &str = "containerd.io/snapshot.ref";
const IMAGE_REF: &str = "containerd.io/snapshot/cri.image-ref";
const MANIFEST_DIGEST: &str = "containerd.io/snapshot/cri.manifest-digest";
const LAYER_DIGEST: &str = "containerd.io/snapshot/cri.layer-digest";

#[derive(Clone)]
struct Record {
    id: u64,
    kind: Kind,
    parent: String,
    labels: HashMap<String, String>,
    created: SystemTime,
    updated: SystemTime,
}

#[derive(Default)]
struct Records {
    next_id: u64,
    by_name: HashMap<String, Record>,
}

/// One unpacked layer: its snapshot id and the directories its tar does not list, or why it failed.
type Unpacked = Result<(u64, Arc<Vec<PathBuf>>), String>;
type Layer = Shared<BoxFuture<'static, Unpacked>>;

/// An image whose layers are being pulled: by chain id.
struct ImagePull {
    layers: HashMap<String, Layer>,
}

pub struct Snapshotter {
    root: PathBuf,
    token: Option<String>,
    /// Registries whose layers come from Xet.
    registries: Vec<String>,
    /// `hf-image-apply`, which unpacks layers.
    applier: Arc<PathBuf>,
    records: Mutex<Records>,
    pulls: tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<ImagePull>>>>>,
}

impl Snapshotter {
    pub fn new(
        root: PathBuf,
        token: Option<String>,
        registries: Vec<String>,
        applier: PathBuf,
    ) -> anyhow::Result<Arc<Self>> {
        anyhow::ensure!(applier.is_file(), "{} not found", applier.display());
        std::fs::create_dir_all(root.join("snapshots"))?;
        let applier = Arc::new(applier);
        Ok(Arc::new(Self { root, token, registries, applier, records: Default::default(), pulls: Default::default() }))
    }

    pub async fn serve(self: Arc<Self>, socket: &Path) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        let _ = std::fs::remove_file(socket);
        let listener = tokio::net::UnixListener::bind(socket).with_context(|| format!("bind {}", socket.display()))?;
        let incoming = tokio_stream::wrappers::UnixListenerStream::new(listener);
        let service = containerd_snapshots::server(self);
        Ok(tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder().add_service(service).serve_with_incoming(incoming).await
            {
                tracing::error!("snapshotter stopped: {e}");
            }
        }))
    }

    fn dir(&self, id: u64) -> PathBuf {
        self.root.join("snapshots").join(id.to_string())
    }

    fn info(name: &str, r: &Record) -> Info {
        Info {
            kind: r.kind,
            name: name.to_string(),
            parent: r.parent.clone(),
            labels: r.labels.clone(),
            created_at: r.created,
            updated_at: r.updated,
        }
    }

    fn new_id(&self) -> u64 {
        let mut records = self.records.lock().unwrap();
        records.next_id += 1;
        records.next_id
    }

    /// Ids of `parent` and its ancestors, nearest first.
    fn chain(records: &Records, parent: &str) -> Result<Vec<u64>, Status> {
        let mut ids = Vec::new();
        let mut name = parent.to_string();
        while !name.is_empty() {
            let r = records.by_name.get(&name).ok_or_else(|| Status::not_found(format!("parent {name}")))?;
            if r.kind != Kind::Committed {
                return Err(Status::failed_precondition(format!("parent {name} is not committed")));
            }
            ids.push(r.id);
            name = r.parent.clone();
        }
        Ok(ids)
    }

    fn mounts_of(&self, r: &Record, parents: &[u64]) -> Vec<Mount> {
        let fs = |id: u64| self.dir(id).join("fs").to_string_lossy().into_owned();
        let lower = || format!("lowerdir={}", parents.iter().map(|p| fs(*p)).collect::<Vec<_>>().join(":"));
        let mount = |t: &str, source: String, options: Vec<String>| Mount {
            r#type: t.into(),
            source,
            target: String::new(),
            options,
        };
        match (r.kind, parents.len()) {
            (Kind::View, 0) => vec![mount("bind", fs(r.id), vec!["ro".into(), "rbind".into()])],
            (Kind::View, 1) => vec![mount("bind", fs(parents[0]), vec!["ro".into(), "rbind".into()])],
            (Kind::View, _) => vec![mount("overlay", "overlay".into(), vec![lower()])],
            (_, 0) => vec![mount("bind", fs(r.id), vec!["rw".into(), "rbind".into()])],
            _ => {
                let work = self.dir(r.id).join("work").to_string_lossy().into_owned();
                vec![mount(
                    "overlay",
                    "overlay".into(),
                    vec![format!("workdir={work}"), format!("upperdir={}", fs(r.id)), lower()],
                )]
            }
        }
    }

    async fn create(
        &self,
        kind: Kind,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<Mount>, Status> {
        let id = self.new_id();
        let dir = self.dir(id);
        std::fs::create_dir_all(dir.join("fs")).map_err(|e| Status::internal(e.to_string()))?;
        std::fs::create_dir_all(dir.join("work")).map_err(|e| Status::internal(e.to_string()))?;
        let mut records = self.records.lock().unwrap();
        if records.by_name.contains_key(&key) {
            return Err(Status::already_exists(format!("snapshot {key}")));
        }
        let parents = Self::chain(&records, &parent)?;
        let now = SystemTime::now();
        let r = Record { id, kind, parent, labels, created: now, updated: now };
        let mounts = self.mounts_of(&r, &parents);
        records.by_name.insert(key, r);
        Ok(mounts)
    }

    /// The committed snapshot of `target` on `parent`, unpacked by an earlier pull.
    fn existing(&self, target: &str, parent: &str) -> bool {
        let records = self.records.lock().unwrap();
        records.by_name.values().any(|r| {
            r.kind == Kind::Committed && r.parent == parent && r.labels.get(SNAPSHOT_REF).is_some_and(|t| t == target)
        })
    }

    /// Pulls the layer `labels` name from Xet (with the rest of its image), then commits it as `key`.
    async fn remote(&self, key: &str, parent: &str, labels: &HashMap<String, String>) -> anyhow::Result<bool> {
        let (Some(target), Some(image), Some(manifest), Some(_)) =
            (labels.get(SNAPSHOT_REF), labels.get(IMAGE_REF), labels.get(MANIFEST_DIGEST), labels.get(LAYER_DIGEST))
        else {
            return Ok(false);
        };
        let image = ImageRef::parse(image)?;
        if !self.registries.contains(&image.registry) {
            return Ok(false);
        }
        if self.existing(target, parent) {
            return Ok(true);
        }
        let pull = self.pull(&image, manifest).await?;
        let layer = pull.layers.get(target).with_context(|| format!("{target} is not a layer of {manifest}"))?;
        let (id, implicit) = layer.clone().await.map_err(anyhow::Error::msg)?;
        let lowers: Vec<_> = {
            let records = self.records.lock().unwrap();
            Self::chain(&records, parent)?.into_iter().map(|p| self.dir(p).join("fs")).collect()
        };
        let fs = self.dir(id).join("fs");
        tokio::task::spawn_blocking(move || untar::inherit(&fs, &lowers, &implicit)).await??;
        let mut records = self.records.lock().unwrap();
        let now = SystemTime::now();
        let r = Record {
            id,
            kind: Kind::Committed,
            parent: parent.into(),
            labels: labels.clone(),
            created: now,
            updated: now,
        };
        records.by_name.insert(key.into(), r);
        Ok(true)
    }

    /// The pull of `manifest`, started by the first layer that asks.
    async fn pull(&self, image: &ImageRef, manifest: &str) -> anyhow::Result<Arc<ImagePull>> {
        let cell = self.pulls.lock().await.entry(manifest.to_string()).or_default().clone();
        let pull = cell.get_or_try_init(|| self.start(image, manifest)).await?;
        Ok(pull.clone())
    }

    async fn start(&self, image: &ImageRef, manifest: &str) -> anyhow::Result<Arc<ImagePull>> {
        let started = Instant::now();
        let registry = Registry::new(image, self.token.clone(), false)?;
        let bytes = registry.manifest(manifest).await?.with_context(|| format!("manifest {manifest} not found"))?.bytes;
        let m: Manifest = serde_json::from_slice(&bytes).context("invalid image manifest")?;
        let (config, downloader) = tokio::try_join!(registry.fetch_blob(&m.config.digest), async {
            Xet::new(registry.clone())?.downloader().await
        })?;
        let diffs = diff_ids(&config)?;
        if diffs.len() != m.layers.len() {
            bail!("the config lists {} layers, the manifest {}", diffs.len(), m.layers.len());
        }
        let chains = chain_ids(&diffs);
        let downloader = Arc::new(downloader);
        let permits = Arc::new(tokio::sync::Semaphore::new(util::concurrency()));
        let mut layers = HashMap::new();
        for ((layer, diff), chain) in m.layers.iter().zip(diffs).zip(&chains) {
            let id = self.new_id();
            let dir = self.dir(id);
            let (registry, downloader, permits, layer, applier) =
                (registry.clone(), downloader.clone(), permits.clone(), layer.clone(), self.applier.clone());
            let task = tokio::spawn(async move {
                let _permit = permits.acquire_owned().await.map_err(|e| e.to_string())?;
                let t = Instant::now();
                let implicit = Self::unpack_layer(&registry, &downloader, &layer, &diff, &dir, applier)
                    .await
                    .map_err(|e| format!("{e:#}"))?;
                tracing::debug!(layer = %layer.digest, size = layer.size, secs = t.elapsed().as_secs_f64(), "unpacked");
                Ok((id, Arc::new(implicit)))
            });
            let fut: BoxFuture<'static, Unpacked> = async move { task.await.map_err(|e| e.to_string())? }.boxed();
            layers.insert(chain.clone(), fut.shared());
        }
        tracing::info!(%manifest, layers = m.layers.len(), secs = started.elapsed().as_secs_f64(), "pull started");
        Ok(Arc::new(ImagePull { layers }))
    }

    /// Streams a layer from Xet into `<dir>/fs`, checking its digest and `diff_id`.
    async fn unpack_layer(
        registry: &Registry,
        downloader: &Arc<Downloader>,
        layer: &crate::oci::Descriptor,
        diff: &str,
        dir: &Path,
        applier: Arc<PathBuf>,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let info = registry.blob(&layer.digest).await?.with_context(|| format!("layer {} is missing", layer.digest))?;
        let hash = info.xet_hash.with_context(|| format!("layer {} is not in Xet", layer.digest))?;
        let stream = xet_stream(downloader.stream(&hash, layer.size).await?).boxed();
        // Large reads: each one crosses from the blocking untar into the async stream. Fused: the
        // digest check reads again after EOF.
        let bridge = SyncIoBridge::new(StreamReader::new(stream.map_err(std::io::Error::other).fuse()));
        let reader = std::io::BufReader::with_capacity(8 << 20, bridge);
        let (fs, media, digest) = (dir.join("fs"), layer.media_type.clone(), layer.digest.clone());
        std::fs::create_dir_all(&fs)?;
        std::fs::create_dir_all(dir.join("work"))?;
        let (got_digest, unpacked) =
            tokio::task::spawn_blocking(move || -> anyhow::Result<(String, untar::Unpacked)> {
                if media.ends_with("+gzip") || media.ends_with(".gzip") {
                    let mut outer = Hashing::new(reader);
                    let unpacked = Self::untar(flate2::read::MultiGzDecoder::new(&mut outer), &fs, &applier)?;
                    Ok((outer.finish()?, unpacked))
                } else if media.ends_with("+zstd") {
                    let mut outer = Hashing::new(reader);
                    let unpacked = Self::untar(zstd::Decoder::new(&mut outer)?, &fs, &applier)?;
                    Ok((outer.finish()?, unpacked))
                } else {
                    let unpacked = Self::untar(reader, &fs, &applier)?;
                    Ok((unpacked.diff_id.clone(), unpacked))
                }
            })
            .await??;
        if got_digest != digest || unpacked.diff_id != diff {
            bail!("layer {digest} unpacked to {got_digest} ({}), expected diff_id {diff}", unpacked.diff_id);
        }
        Ok(unpacked.implicit)
    }

    /// Unpacks with `applier`, or with the Rust untar under `HF_IMAGE_RUST_UNTAR` (benchmarks only).
    fn untar(reader: impl std::io::Read, fs: &Path, applier: &Path) -> anyhow::Result<untar::Unpacked> {
        match std::env::var_os("HF_IMAGE_RUST_UNTAR") {
            Some(_) => untar::unpack(reader, fs),
            None => untar::apply(reader, fs, applier),
        }
    }

    fn rm(path: PathBuf) {
        tokio::task::spawn_blocking(move || {
            if let Err(e) = std::fs::remove_dir_all(&path) {
                tracing::warn!("failed to remove {}: {e}", path.display());
            }
        });
    }
}

#[tonic::async_trait]
impl containerd_snapshots::Snapshotter for Snapshotter {
    type Error = Status;

    async fn stat(&self, key: String) -> Result<Info, Status> {
        let records = self.records.lock().unwrap();
        let r = records.by_name.get(&key).ok_or_else(|| Status::not_found(format!("snapshot {key}")))?;
        Ok(Self::info(&key, r))
    }

    async fn update(&self, info: Info, fieldpaths: Option<Vec<String>>) -> Result<Info, Status> {
        let mut records = self.records.lock().unwrap();
        let r =
            records.by_name.get_mut(&info.name).ok_or_else(|| Status::not_found(format!("snapshot {}", info.name)))?;
        let paths = fieldpaths.unwrap_or_default();
        if paths.is_empty() || paths.iter().any(|p| p == "labels") {
            r.labels = info.labels;
        } else {
            for p in paths {
                if let Some(k) = p.strip_prefix("labels.") {
                    match info.labels.get(k) {
                        Some(v) => r.labels.insert(k.into(), v.clone()),
                        None => r.labels.remove(k),
                    };
                }
            }
        }
        r.updated = SystemTime::now();
        Ok(Self::info(&info.name, r))
    }

    async fn usage(&self, key: String) -> Result<Usage, Status> {
        self.stat(key).await?;
        Ok(Usage::default())
    }

    async fn mounts(&self, key: String) -> Result<Vec<Mount>, Status> {
        let records = self.records.lock().unwrap();
        let r = records.by_name.get(&key).ok_or_else(|| Status::not_found(format!("snapshot {key}")))?;
        let parents = Self::chain(&records, &r.parent)?;
        Ok(self.mounts_of(r, &parents))
    }

    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<Mount>, Status> {
        match self.remote(&key, &parent, &labels).await {
            Ok(true) => {
                let target = labels.get(SNAPSHOT_REF).cloned().unwrap_or_default();
                return Err(Status::already_exists(format!("target snapshot {target:?}: already exists")));
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(%key, "{e:#}: containerd unpacks it"),
        }
        self.create(Kind::Active, key, parent, labels).await
    }

    async fn view(&self, key: String, parent: String, labels: HashMap<String, String>) -> Result<Vec<Mount>, Status> {
        self.create(Kind::View, key, parent, labels).await
    }

    async fn commit(&self, name: String, key: String, labels: HashMap<String, String>) -> Result<(), Status> {
        let mut records = self.records.lock().unwrap();
        if records.by_name.contains_key(&name) {
            return Err(Status::already_exists(format!("snapshot {name}")));
        }
        let mut r = records.by_name.remove(&key).ok_or_else(|| Status::not_found(format!("snapshot {key}")))?;
        if r.kind != Kind::Active {
            records.by_name.insert(key.clone(), r);
            return Err(Status::failed_precondition(format!("snapshot {key} is not active")));
        }
        r.kind = Kind::Committed;
        r.labels.extend(labels);
        r.updated = SystemTime::now();
        records.by_name.insert(name, r);
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<(), Status> {
        let mut records = self.records.lock().unwrap();
        if records.by_name.values().any(|r| r.parent == key) {
            return Err(Status::failed_precondition(format!("snapshot {key} has children")));
        }
        let r = records.by_name.remove(&key).ok_or_else(|| Status::not_found(format!("snapshot {key}")))?;
        if !records.by_name.values().any(|o| o.id == r.id) {
            Self::rm(self.dir(r.id));
        }
        Ok(())
    }

    type InfoStream = futures::stream::Iter<std::vec::IntoIter<Result<Info, Status>>>;

    async fn list(&self, _snapshotter: String, _filters: Vec<String>) -> Result<Self::InfoStream, Status> {
        let records = self.records.lock().unwrap();
        let all: Vec<_> = records.by_name.iter().map(|(k, r)| Ok(Self::info(k, r))).collect();
        Ok(futures::stream::iter(all))
    }
}
