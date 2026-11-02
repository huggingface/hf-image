//! containerd's content, image, lease, snapshot and diff services: where Docker's containerd image
//! store keeps images (namespace `moby`). Everything written runs under one lease, so content is
//! never collected before the image record and its GC labels reference it.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};
use bytes::Bytes;
use containerd_client::services::v1::content_client::ContentClient;
use containerd_client::services::v1::diff_client::DiffClient;
use containerd_client::services::v1::images_client::ImagesClient;
use containerd_client::services::v1::introspection_client::IntrospectionClient;
use containerd_client::services::v1::leases_client::LeasesClient;
use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, PrepareSnapshotRequest, RemoveSnapshotRequest, StatSnapshotRequest,
};
use containerd_client::services::v1::{
    ApplyRequest, CreateImageRequest, CreateRequest, DeleteRequest, GetImageRequest, Image, Info, InfoRequest,
    PluginsRequest, ReadContentRequest, UpdateImageRequest, WriteAction, WriteContentRequest,
};
use containerd_client::types::{Descriptor, Mount};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::oci;

const MAX_MESSAGE: usize = 1 << 20;
const LEASE_TTL: Duration = Duration::from_secs(2 * 3600);

/// A Docker daemon's image store: containerd's socket, namespace and snapshotter.
#[derive(Debug, Clone)]
pub struct Store {
    pub address: String,
    pub namespace: String,
    pub snapshotter: String,
}

impl Store {
    pub async fn connect(&self) -> anyhow::Result<Containerd> {
        Containerd::connect(&self.address, &self.namespace, &self.snapshotter).await
    }
}

pub struct Containerd {
    channel: Channel,
    namespace: String,
    pub snapshotter: String,
    lease: Option<String>,
}

impl Containerd {
    pub async fn connect(address: &str, namespace: &str, snapshotter: &str) -> anyhow::Result<Self> {
        let channel = containerd_client::connect(address).await.with_context(|| {
            format!("cannot reach containerd at {address} (is Docker using the containerd image store?)")
        })?;
        Ok(Self { channel, namespace: namespace.into(), snapshotter: snapshotter.into(), lease: None })
    }

    fn req<T>(&self, msg: T) -> Request<T> {
        let mut r = Request::new(msg);
        let md = r.metadata_mut();
        md.insert("containerd-namespace", self.namespace.parse().expect("valid namespace"));
        if let Some(l) = &self.lease {
            md.insert("containerd-lease", l.parse().expect("valid lease"));
        }
        r
    }

    /// Protects everything written through the returned handle until [`Containerd::release`].
    pub async fn leased(mut self) -> anyhow::Result<Self> {
        let id = format!("hf-image-{}", uuid::Uuid::new_v4());
        let expire = rfc3339(SystemTime::now() + LEASE_TTL);
        let labels = HashMap::from([("containerd.io/gc.expire".to_string(), expire)]);
        LeasesClient::new(self.channel.clone()).create(self.req(CreateRequest { id: id.clone(), labels })).await?;
        self.lease = Some(id);
        Ok(self)
    }

    pub async fn release(&self) {
        if let Some(id) = self.lease.clone() {
            let _ = LeasesClient::new(self.channel.clone()).delete(self.req(DeleteRequest { id, sync: false })).await;
        }
    }

    pub async fn image(&self, name: &str) -> anyhow::Result<Option<Descriptor>> {
        match ImagesClient::new(self.channel.clone()).get(self.req(GetImageRequest { name: name.into() })).await {
            Ok(r) => Ok(r.into_inner().image.and_then(|i| i.target)),
            Err(s) if s.code() == Code::NotFound => Ok(None),
            Err(s) => Err(s.into()),
        }
    }

    /// Creates or points the image record `name` at `target`.
    pub async fn put_image(&self, name: &str, target: Descriptor) -> anyhow::Result<()> {
        let image = Image { name: name.into(), target: Some(target), ..Default::default() };
        let mut client = ImagesClient::new(self.channel.clone());
        match client.create(self.req(CreateImageRequest { image: Some(image.clone()), source_date_epoch: None })).await
        {
            Ok(_) => Ok(()),
            Err(s) if s.code() == Code::AlreadyExists => {
                let update = UpdateImageRequest { image: Some(image), update_mask: None, source_date_epoch: None };
                client.update(self.req(update)).await?;
                Ok(())
            }
            Err(s) => Err(s.into()),
        }
    }

    pub async fn info(&self, digest: &str) -> anyhow::Result<Option<Info>> {
        match ContentClient::new(self.channel.clone()).info(self.req(InfoRequest { digest: digest.into() })).await {
            Ok(r) => Ok(r.into_inner().info),
            Err(s) if s.code() == Code::NotFound => Ok(None),
            Err(s) => Err(s.into()),
        }
    }

    pub async fn read(&self, digest: &str) -> anyhow::Result<impl Stream<Item = anyhow::Result<Bytes>> + use<>> {
        let msg = ReadContentRequest { digest: digest.into(), offset: 0, size: 0 };
        let stream = ContentClient::new(self.channel.clone()).read(self.req(msg)).await?.into_inner();
        Ok(stream.map(|r| r.map(|m| Bytes::from(m.data)).map_err(anyhow::Error::from)))
    }

    /// Writes content from a stream; containerd verifies digest and size on commit. Content that
    /// already exists is left as is.
    pub async fn write(
        &self,
        digest: &str,
        size: u64,
        labels: HashMap<String, String>,
        data: impl Stream<Item = anyhow::Result<Bytes>>,
    ) -> anyhow::Result<()> {
        let reference = format!("hf-image-{}-{}", digest.trim_start_matches("sha256:"), uuid::Uuid::new_v4());
        let (tx, rx) = mpsc::channel::<WriteContentRequest>(4);
        let request = |action: WriteAction, offset: u64| WriteContentRequest {
            action: action as i32,
            r#ref: reference.clone(),
            offset: offset as i64,
            ..Default::default()
        };
        tx.send(request(WriteAction::Stat, 0)).await?;
        let outbound = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|m| (m, rx)) });
        let mut inbound = ContentClient::new(self.channel.clone()).write(self.req(outbound)).await?.into_inner();
        inbound.message().await?.context("containerd closed the write")?;
        // Every request is acknowledged: drain the acks while writing.
        let acks = tokio::spawn(async move {
            let mut last = None;
            loop {
                match inbound.message().await {
                    Ok(Some(m)) => last = Some(m),
                    Ok(None) => return Ok(last),
                    Err(s) => return Err(s),
                }
            }
        });
        let mut offset = 0u64;
        let mut data = std::pin::pin!(data);
        'send: while let Some(chunk) = data.next().await {
            let chunk = chunk?;
            for piece in chunk.chunks(MAX_MESSAGE) {
                let mut m = request(WriteAction::Write, offset);
                m.data = piece.to_vec();
                offset += piece.len() as u64;
                if tx.send(m).await.is_err() {
                    break 'send;
                }
            }
        }
        let mut commit = request(WriteAction::Commit, offset);
        commit.total = size as i64;
        commit.expected = digest.into();
        commit.labels = labels;
        let _ = tx.send(commit).await;
        drop(tx);
        match acks.await? {
            Ok(_) => Ok(()),
            Err(s) if s.code() == Code::AlreadyExists => Ok(()),
            Err(s) => bail!("containerd refused {digest}: {}", s.message()),
        }
    }

    pub async fn put(&self, digest: &str, bytes: Bytes, labels: HashMap<String, String>) -> anyhow::Result<()> {
        let size = bytes.len() as u64;
        self.write(digest, size, labels, futures::stream::iter([Ok(bytes)])).await
    }

    pub async fn snapshot_exists(&self, key: &str) -> anyhow::Result<bool> {
        let req = StatSnapshotRequest { snapshotter: self.snapshotter.clone(), key: key.into() };
        match SnapshotsClient::new(self.channel.clone()).stat(self.req(req)).await {
            Ok(_) => Ok(true),
            Err(s) if s.code() == Code::NotFound => Ok(false),
            Err(s) => Err(s.into()),
        }
    }

    /// Whether the snapshotter commits a snapshot onto a parent it was not prepared on (containerd 2.2+),
    /// so layers can be unpacked at once and committed in chain order.
    pub async fn rebases(&self) -> anyhow::Result<bool> {
        let filters = vec![format!("type==io.containerd.snapshotter.v1,id=={}", self.snapshotter)];
        let plugins =
            IntrospectionClient::new(self.channel.clone()).plugins(self.req(PluginsRequest { filters })).await?;
        Ok(plugins.into_inner().plugins.iter().any(|p| p.capabilities.iter().any(|c| c == "rebase")))
    }

    /// Applies `layer` on top of `parent` into the snapshot `chain_id` (a no-op when it exists);
    /// returns whether it unpacked anything.
    pub async fn unpack(
        &self,
        layer: &Descriptor,
        diff_id: &str,
        chain_id: &str,
        parent: Option<&str>,
    ) -> anyhow::Result<bool> {
        if self.snapshot_exists(chain_id).await? {
            return Ok(false);
        }
        let key = self.apply(layer, diff_id, chain_id, parent, false).await?;
        self.commit(&key, chain_id, None).await
    }

    /// Applies `layer` into a new active snapshot on `parent`; returns its key, for [`Containerd::commit`].
    /// `unmounted_lower`: layers below it exist but are not its parent yet, so its whiteouts must be kept.
    pub async fn apply(
        &self,
        layer: &Descriptor,
        diff_id: &str,
        chain_id: &str,
        parent: Option<&str>,
        unmounted_lower: bool,
    ) -> anyhow::Result<String> {
        let key = format!("extract-{} {chain_id}", uuid::Uuid::new_v4());
        let prepare = PrepareSnapshotRequest {
            snapshotter: self.snapshotter.clone(),
            key: key.clone(),
            parent: parent.unwrap_or_default().into(),
            labels: HashMap::new(),
        };
        let mut mounts =
            SnapshotsClient::new(self.channel.clone()).prepare(self.req(prepare)).await?.into_inner().mounts;
        if unmounted_lower && self.snapshotter == "overlayfs" {
            mounts = Self::upper_only(mounts);
        }
        let applied = async {
            let apply = ApplyRequest { diff: Some(layer.clone()), mounts, payloads: HashMap::new(), sync_fs: false };
            let applied = DiffClient::new(self.channel.clone()).apply(self.req(apply)).await?.into_inner().applied;
            let got = applied.map(|d| d.digest).unwrap_or_default();
            if got != diff_id {
                bail!("layer {} unpacked to {got}, expected {diff_id}", layer.digest);
            }
            Ok(())
        }
        .await;
        match applied {
            Ok(()) => Ok(key),
            Err(e) => {
                self.remove_snapshot(&key).await;
                Err(e)
            }
        }
    }

    /// Commits the active snapshot `key` as `chain_id`, onto `parent` when given (the snapshotter must
    /// rebase); returns whether it was new.
    pub async fn commit(&self, key: &str, chain_id: &str, parent: Option<&str>) -> anyhow::Result<bool> {
        let commit = CommitSnapshotRequest {
            snapshotter: self.snapshotter.clone(),
            name: chain_id.into(),
            key: key.into(),
            labels: HashMap::new(),
            parent: parent.unwrap_or_default().into(),
        };
        let result = match SnapshotsClient::new(self.channel.clone()).commit(self.req(commit)).await {
            Ok(_) => return Ok(true),
            Err(s) if s.code() == Code::AlreadyExists => Ok(false),
            Err(s) => Err(s.into()),
        };
        self.remove_snapshot(key).await;
        result
    }

    /// overlayfs mounts a snapshot without parent as a bind mount, which the applier unpacks deleting what
    /// whiteouts name; as an overlay mount of its upper directory alone, they become overlay whiteouts.
    fn upper_only(mounts: Vec<Mount>) -> Vec<Mount> {
        match <[Mount; 1]>::try_from(mounts) {
            Ok([m]) if m.r#type == "bind" => {
                let mut options: Vec<String> = m.options.into_iter().filter(|o| o != "rbind").collect();
                options.push(format!("upperdir={}", m.source));
                vec![Mount { r#type: "overlay".into(), source: "overlay".into(), options, ..Default::default() }]
            }
            Ok(m) => m.into(),
            Err(mounts) => mounts,
        }
    }

    /// Removes an active snapshot, best effort: the lease's expiry collects what this misses.
    pub async fn remove_snapshot(&self, key: &str) {
        let rm = RemoveSnapshotRequest { snapshotter: self.snapshotter.clone(), key: key.into() };
        let _ = SnapshotsClient::new(self.channel.clone()).remove(self.req(rm)).await;
    }
}

impl From<&oci::Descriptor> for Descriptor {
    fn from(d: &oci::Descriptor) -> Self {
        Self {
            media_type: d.media_type.clone(),
            digest: d.digest.clone(),
            size: d.size as i64,
            annotations: HashMap::new(),
        }
    }
}

/// RFC 3339 to the second, as `containerd.io/gc.expire` wants it.
fn rfc3339(t: SystemTime) -> String {
    let mut ts = prost_types::Timestamp::from(t);
    ts.nanos = 0;
    ts.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parentless_overlay_snapshots_apply_to_their_upper_directory() {
        let mount = |t: &str, source: &str, options: &[&str]| Mount {
            r#type: t.into(),
            source: source.into(),
            options: options.iter().map(|o| o.to_string()).collect(),
            ..Default::default()
        };
        let got = Containerd::upper_only(vec![mount("bind", "/s/1/fs", &["rw", "rbind"])]);
        assert_eq!(got, vec![mount("overlay", "overlay", &["rw", "upperdir=/s/1/fs"])]);
        let layered = vec![mount("overlay", "overlay", &["upperdir=/s/2/fs", "lowerdir=/s/1/fs"])];
        assert_eq!(Containerd::upper_only(layered.clone()), layered);
    }

    #[test]
    fn timestamps() {
        assert_eq!(rfc3339(SystemTime::UNIX_EPOCH + Duration::from_secs(951_868_800)), "2000-03-01T00:00:00Z");
    }
}
