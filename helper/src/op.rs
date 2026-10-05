//! What the helper runs for the `hf image` CLI, next to the Docker daemon (in its VM on macOS, in
//! rootlesskit's namespace for rootless Docker), where containerd's socket and BuildKit's network
//! are local. The HF token is the first line of stdin; the next line, or EOF, stops it. Results are
//! JSON lines on stdout.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use anyhow::{Context, anyhow, bail};
use clap::Subcommand;
use tokio::io::{AsyncBufReadExt, BufReader, Lines};

use crate::containerd::Store;
use crate::gateway::Gateway;
use crate::pull::{PullOpts, Target, pull};
use crate::push::{Origin, PushOpts, push};
use crate::reference::ImageRef;
use crate::registry::Registry;
use crate::xet::Xet;

#[derive(Debug, Clone, Subcommand)]
pub enum Op {
    /// Pulls into the daemon's store, or an OCI layout with `--output`; prints the digest.
    Pull {
        image: String,
        #[arg(long)]
        platform: String,
        #[arg(long)]
        if_stale: bool,
        #[arg(long)]
        output: Option<PathBuf>,
        /// `<uid>:<gid>` that gets what the pull writes to `--output`.
        #[arg(long, requires = "output")]
        owner: Option<String>,
    },
    /// Pushes an image of the daemon's store, or an OCI layout (`oci:<dir>`); prints the digest.
    Push {
        image: String,
        #[arg(long)]
        from: String,
        #[arg(long)]
        preserve_digests: bool,
    },
    /// Serves the build endpoint on loopback; prints its `Endpoint`, then its `Summary` once stopped.
    Serve { image: String },
}

impl Op {
    /// Runs against the daemon's `store` (none for OCI layouts).
    pub async fn execute(self, store: Option<Store>) -> anyhow::Result<()> {
        let mut host = HostLink::attach();
        let token = host.token().await?;
        match self {
            Op::Pull { image, platform, if_stale, output, owner } => {
                let target = match &output {
                    Some(dir) => Target::Layout(dir.clone()),
                    None => Target::Store(Self::store(store)?),
                };
                let opts =
                    PullOpts { image: ImageRef::parse(&image)?, token, platform: Some(platform), target, if_stale };
                let pulled = tokio::select! {
                    digest = pull(&opts) => digest,
                    () = host.stopped() => Err(anyhow!("stopped")),
                };
                // Partial pulls too: they leave blobs a later pull reuses.
                if let (Some(dir), Some(owner)) = (output, owner) {
                    Self::chown(&dir, &owner)?;
                }
                Self::print_digest(&pulled?);
            }
            Op::Push { image, from, preserve_digests } => {
                let from = match from.strip_prefix("oci:") {
                    Some(dir) => Origin::Layout(dir.into()),
                    None => Origin::Store { store: Self::store(store)?, name: from },
                };
                let opts = PushOpts { image: ImageRef::parse(&image)?, token, from, preserve_digests };
                let digest = tokio::select! {
                    digest = push(&opts) => digest?,
                    () = host.stopped() => bail!("stopped"),
                };
                Self::print_digest(&digest);
            }
            Op::Serve { image } => {
                let registry = Registry::new(&ImageRef::parse(&image)?, token, true)?;
                let gw = Gateway::new(registry.clone(), Xet::new(registry)?);
                let (endpoint, serving) = gw.serve(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
                println!("{}", serde_json::to_string(&endpoint)?);
                host.stopped().await;
                serving.abort();
                println!("{}", serde_json::to_string(&gw.summary().await)?);
            }
        }
        Ok(())
    }

    fn store(store: Option<Store>) -> anyhow::Result<Store> {
        store.context("this op needs --containerd, --namespace and --snapshotter")
    }

    fn print_digest(digest: &str) {
        println!("{}", serde_json::json!({ "digest": digest }));
    }

    /// Gives `dir` and everything under it to `<uid>:<gid>`.
    fn chown(dir: &Path, owner: &str) -> anyhow::Result<()> {
        let (uid, gid) = owner.split_once(':').context("--owner is <uid>:<gid>")?;
        let (uid, gid) = (uid.parse().context("invalid uid")?, gid.parse().context("invalid gid")?);
        let mut pending = vec![dir.to_path_buf()];
        while let Some(path) = pending.pop() {
            std::os::unix::fs::lchown(&path, Some(uid), Some(gid))
                .with_context(|| format!("failed to chown {}", path.display()))?;
            if std::fs::symlink_metadata(&path)?.is_dir() {
                for entry in std::fs::read_dir(&path)? {
                    pending.push(entry?.path());
                }
            }
        }
        Ok(())
    }
}

/// stdin from the host.
struct HostLink {
    lines: Lines<BufReader<tokio::io::Stdin>>,
}

impl HostLink {
    fn attach() -> Self {
        Self { lines: BufReader::new(tokio::io::stdin()).lines() }
    }

    async fn token(&mut self) -> anyhow::Result<Option<String>> {
        let line = self.lines.next_line().await?.context("no token line on stdin")?;
        Ok(Some(line.trim().to_string()).filter(|t| !t.is_empty()))
    }

    /// Resolves when the host asks to stop (a line, or EOF).
    async fn stopped(mut self) {
        let _ = self.lines.next_line().await;
    }
}
