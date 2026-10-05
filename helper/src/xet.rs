//! Xet transfers straight to and from CAS, with tokens the registry mints for the repo.
//!
//! Each file upload runs in a detached task fed through a channel, so a dropped request (the local
//! gateway serves BuildKit) never cancels a xet-core future: its shard-cache lock is not
//! cancel-safe. Downloads use the chunk cache, which makes re-pulls of rebuilt layers cheap.

use std::sync::Arc;

use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use xet_client::cas_client::auth::{AuthError, TokenInfo, TokenRefresher};
use xet_data::processing::configurations::TranslatorConfig;
use xet_data::processing::data_client::default_config;
use xet_data::processing::{
    CacheConfig, DownloadStream, FileDownloadSession, FileUploadSession, Sha256Policy, XetFileInfo, get_cache,
};
use xet_runtime::config::XetConfig;
use xet_runtime::core::{XetContext, xet_cache_root};

use crate::registry::Registry;

/// Default chunk cache size (`HF_IMAGE_CHUNK_CACHE_BYTES` overrides, 0 disables).
const CHUNK_CACHE_BYTES: u64 = 20_000_000_000;
/// Concurrent xorb uploads.
const UPLOAD_CONCURRENCY: usize = 8;
/// Dedup's target chunks per file range (xet-core: 8): fewer, larger fetches on pulls.
const MIN_CHUNKS_PER_RANGE: usize = 64;

pub struct Xet {
    ctx: XetContext,
    registry: Arc<Registry>,
}

impl Xet {
    pub fn new(registry: Arc<Registry>) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self { ctx: Self::context()?, registry }))
    }

    /// Upload concurrency is bounded: xet-core buffers up to two copies of a 64 MiB xorb per
    /// in-flight upload. Dedup keeps ranges of [`MIN_CHUNKS_PER_RANGE`]. `HF_XET_*` variables override.
    fn context() -> anyhow::Result<XetContext> {
        let mut config = XetConfig::new();
        let tuned = ["HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY", "HF_XET_HIGH_PERFORMANCE", "HF_XET_HP"]
            .iter()
            .any(|v| std::env::var_os(v).is_some());
        if !tuned {
            config = config.with_config("client.ac_max_upload_concurrency", UPLOAD_CONCURRENCY)?;
        }
        if std::env::var_os("HF_XET_DEDUPLICATION_MIN_N_CHUNKS_PER_RANGE").is_none() {
            config = config.with_config("deduplication.min_n_chunks_per_range", MIN_CHUNKS_PER_RANGE)?;
        }
        XetContext::with_config(config).context("failed to build the xet context")
    }

    async fn config(&self, write: bool) -> anyhow::Result<Arc<TranslatorConfig>> {
        let t = self.registry.xet_token(write).await?;
        let refresher: Arc<dyn TokenRefresher> = Arc::new(Refresher { registry: self.registry.clone(), write });
        Ok(Arc::new(
            default_config(&self.ctx, t.cas_url, Some((t.access_token, t.exp)), Some(refresher), None)
                .context("failed to build the CAS config")?,
        ))
    }

    pub async fn uploader(&self) -> anyhow::Result<Uploader> {
        let session =
            FileUploadSession::new(self.config(true).await?).await.context("failed to open an upload session")?;
        Ok(Uploader { session })
    }

    pub async fn downloader(&self) -> anyhow::Result<Downloader> {
        let size =
            std::env::var("HF_IMAGE_CHUNK_CACHE_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(CHUNK_CACHE_BYTES);
        let cache = if size > 0 {
            let cfg = CacheConfig { cache_directory: xet_cache_root().join("chunk-cache"), cache_size: size };
            Some(get_cache(&self.ctx.config, &cfg).context("failed to open the chunk cache")?)
        } else {
            None
        };
        let session = FileDownloadSession::new(self.config(false).await?, cache)
            .await
            .context("failed to open a download session")?;
        Ok(Downloader { session })
    }
}

pub struct Uploader {
    session: Arc<FileUploadSession>,
}

impl Uploader {
    /// Starts uploading one file; bytes go through [`FileUpload::write`].
    pub fn file(&self, name: &str, size: Option<u64>) -> anyhow::Result<FileUpload> {
        let (_, mut cleaner) = self
            .session
            .start_clean(Some(name.into()), size, Sha256Policy::Skip)
            .context("failed to start an upload")?;
        let (tx, mut rx) = mpsc::channel::<Msg>(8);
        let join = tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(Msg::Data(b)) => cleaner.add_data_from_bytes(b).await.context("upload failed")?,
                    Some(Msg::Done) => break,
                    None => bail!("upload abandoned"),
                }
            }
            let (info, metrics) = cleaner.finish().await.context("upload failed")?;
            Ok(Uploaded { hash: info.hash, new_bytes: metrics.new_bytes })
        });
        Ok(FileUpload { tx, join })
    }

    /// Uploads the remaining data and file metadata; files are readable (and registrable) after.
    pub async fn finalize(self) -> anyhow::Result<()> {
        self.session.finalize().await.context("upload commit failed")?;
        Ok(())
    }
}

enum Msg {
    Data(Bytes),
    Done,
}

pub struct Uploaded {
    pub hash: String,
    /// Bytes of the file not found in Xet already (before compression).
    pub new_bytes: u64,
}

pub struct FileUpload {
    tx: mpsc::Sender<Msg>,
    join: JoinHandle<anyhow::Result<Uploaded>>,
}

impl FileUpload {
    pub async fn write(&mut self, data: Bytes) -> anyhow::Result<()> {
        if !data.is_empty() && self.tx.send(Msg::Data(data)).await.is_err() {
            bail!("upload stopped");
        }
        Ok(())
    }

    pub async fn finish(self) -> anyhow::Result<Uploaded> {
        let _ = self.tx.send(Msg::Done).await;
        self.join.await.context("upload task panicked")?
    }
}

pub struct Downloader {
    session: Arc<FileDownloadSession>,
}

impl Downloader {
    pub async fn stream(&self, hash: &str, size: u64) -> anyhow::Result<DownloadStream> {
        let info = XetFileInfo::new(hash.to_string(), size);
        let (_, stream) = self.session.download_stream(&info, None).await.context("failed to start a download")?;
        Ok(stream)
    }

    /// Downloads to a file (parallel range writes).
    pub async fn to_file(&self, hash: &str, size: u64, path: &std::path::Path) -> anyhow::Result<()> {
        let info = XetFileInfo::new(hash.to_string(), size);
        self.session.download_file(&info, path).await.context("download failed")?;
        Ok(())
    }
}

struct Refresher {
    registry: Arc<Registry>,
    write: bool,
}

#[async_trait]
impl TokenRefresher for Refresher {
    async fn refresh(&self) -> Result<TokenInfo, AuthError> {
        let t =
            self.registry.xet_token(self.write).await.map_err(|e| AuthError::TokenRefreshFailure(format!("{e:#}")))?;
        Ok((t.access_token, t.exp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_keeps_dedup_ranges_long() {
        let ctx = Xet::context().unwrap();
        assert_eq!(ctx.config.deduplication.min_n_chunks_per_range, MIN_CHUNKS_PER_RANGE as f32);
    }
}
