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
use crate::oci::Platform;
use crate::pull::{PullOpts, Target, pull};
use crate::push::{Origin, PushOpts, push};
use crate::reference::ImageRef;
use crate::registry::{Refused, Registry};
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
    /// Deletes an image by digest; the registry refuses while a tag or an index references it.
    Delete { image: String },
    /// Deletes a tag; prints the digest it pointed to.
    Untag { image: String },
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
                    None => Target::Store(Self::store(store)?.connect().await?),
                };
                let (image, platform) = (ImageRef::parse(&image)?, Platform::parse(&platform)?);
                let opts = PullOpts { image, token, platform, target, if_stale };
                let pulled = tokio::select! {
                    digest = pull(opts) => digest,
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
                // Fails before the build when the repo is missing or not writable.
                registry.xet_token(true).await?;
                let gw = Gateway::new(registry.clone(), Xet::new(registry)?);
                let (endpoint, serving) = gw.serve(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
                println!("{}", serde_json::to_string(&endpoint)?);
                host.stopped().await;
                serving.abort();
                println!("{}", serde_json::to_string(&gw.summary().await)?);
            }
            Op::Delete { image } => {
                let image = ImageRef::parse(&image)?;
                let digest = image.digest.clone().context("delete takes <registry>/<namespace>/<name>@<digest>")?;
                let deleted = Registry::new(&image, token, true)?.delete(&digest).await;
                deleted.map_err(|e| Self::refused(e, "delete", &image))?;
                Self::print_digest(&digest);
            }
            Op::Untag { image } => {
                let image = ImageRef::parse(&image)?;
                let tag = image.tag.clone().filter(|_| image.digest.is_none());
                let tag = tag.context("untag takes <registry>/<namespace>/<name>:<tag>")?;
                let registry = Registry::new(&image, token, true)?;
                let manifest = registry.manifest(&tag).await?.with_context(|| format!("{image} not found"))?;
                registry.delete(&tag).await.map_err(|e| Self::refused(e, "untag", &image))?;
                Self::print_digest(&manifest.digest);
            }
        }
        Ok(())
    }

    /// A refused delete of `image`, explained: what still references it and how to remove that.
    fn refused(e: anyhow::Error, verb: &str, image: &ImageRef) -> anyhow::Error {
        let Some(refused) = e.downcast_ref::<Refused>() else { return e };
        let Some(refs) = &refused.references else {
            return anyhow!("cannot {verb} {image}: {}", refused.message);
        };
        let name = format!("{}/{}", image.registry, image.repo);
        let mut lines = vec![format!("cannot {verb} {image}: still referenced by")];
        lines.extend(refs.tags.iter().map(|t| format!("  tag {t}")));
        lines.extend(refs.indexes.iter().map(|i| match i.tags.is_empty() {
            true => format!("  index {}", i.digest),
            false => format!("  index {} (tagged {})", i.digest, i.tags.join(", ")),
        }));
        lines.push("remove them first:".into());
        lines.extend(refs.tags.iter().map(|t| format!("  hf image untag {name}:{t}")));
        for i in &refs.indexes {
            lines.extend(i.tags.iter().map(|t| format!("  hf image untag {name}:{t}")));
            lines.push(format!("  hf image delete {name}@{}", i.digest));
        }
        anyhow!(lines.join("\n"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{IndexRef, References};

    #[test]
    fn refused_deletes_list_the_references_and_the_commands_that_remove_them() {
        let (m, i) = (format!("sha256:{}", "a".repeat(64)), format!("sha256:{}", "b".repeat(64)));
        let image = ImageRef::parse(&format!("cr.hf.co/acme/app@{m}")).unwrap();
        let refs = References {
            tags: vec!["arm64".into()],
            indexes: vec![IndexRef { digest: i.clone(), tags: vec!["multi".into()] }],
        };
        let refused = Refused { message: "manifest … is tagged arm64".into(), references: Some(refs) };
        let err = Op::refused(refused.into(), "delete", &image);
        assert_eq!(
            err.to_string(),
            [
                format!("cannot delete cr.hf.co/acme/app@{m}: still referenced by"),
                "  tag arm64".into(),
                format!("  index {i} (tagged multi)"),
                "remove them first:".into(),
                "  hf image untag cr.hf.co/acme/app:arm64".into(),
                "  hf image untag cr.hf.co/acme/app:multi".into(),
                format!("  hf image delete cr.hf.co/acme/app@{i}"),
            ]
            .join("\n")
        );

        let refused = Refused { message: format!("manifest {m} does not exist"), references: None };
        let err = Op::refused(refused.into(), "delete", &image);
        assert_eq!(err.to_string(), format!("cannot delete cr.hf.co/acme/app@{m}: manifest {m} does not exist"));
        assert_eq!(Op::refused(anyhow!("timeout"), "delete", &image).to_string(), "timeout");
    }
}
