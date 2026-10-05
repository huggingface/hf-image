use clap::Parser;
use hf_image_helper::containerd::Store;
use hf_image_helper::op::Op;
use tracing_subscriber::EnvFilter;

/// The data plane of `hf image`, run by the CLI as a container next to the Docker daemon.
#[derive(Parser)]
#[command(name = "hf-image-helper", version)]
struct Cli {
    /// containerd's socket, for ops on the daemon's image store (OCI layout ops run without it).
    #[arg(long, requires_all = ["namespace", "snapshotter"])]
    containerd: Option<String>,
    #[arg(long, requires = "containerd")]
    namespace: Option<String>,
    #[arg(long, requires = "containerd")]
    snapshotter: Option<String>,
    #[command(subcommand)]
    op: Op,
}

impl Cli {
    fn store(&self) -> Option<Store> {
        Some(Store {
            address: self.containerd.clone()?,
            namespace: self.namespace.clone()?,
            snapshotter: self.snapshotter.clone()?,
        })
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_env("HF_IMAGE_LOG").unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();
    let cli = Cli::parse();
    let store = cli.store();
    let code = match cli.op.execute(store).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    };
    // Without waiting for the runtime: a blocking stdin read never returns while the host waits.
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("hf-image-helper").chain(args.iter().copied())).unwrap()
    }

    /// The command lines the CLI sends (see tests/test_helper.py).
    #[test]
    fn cli_command_lines() {
        let store =
            ["--containerd", "/run/hf-image/containerd.sock", "--namespace", "moby", "--snapshotter", "overlayfs"];
        let cli =
            parse(&[&store[..], &["pull", "cr.hf.co/a/b:v1", "--platform", "linux/arm64", "--if-stale"]].concat());
        assert_eq!(cli.store().unwrap().namespace, "moby");
        assert!(
            matches!(cli.op, Op::Pull { if_stale: true, output: None, ref platform, .. } if platform == "linux/arm64")
        );

        let cli = parse(&[
            "pull",
            "cr.hf.co/a/b:v1",
            "--platform",
            "linux/amd64",
            "--output",
            "/layout",
            "--owner",
            "1000:1000",
        ]);
        assert!(cli.store().is_none());
        assert!(matches!(cli.op, Op::Pull { output: Some(_), owner: Some(ref o), .. } if o == "1000:1000"));

        let cli = parse(&["push", "cr.hf.co/a/b:v1", "--from", "oci:/layout", "--preserve-digests"]);
        assert!(matches!(cli.op, Op::Push { ref from, preserve_digests: true, .. } if from == "oci:/layout"));

        let cli = parse(&[&store[..], &["serve", "cr.hf.co/a/b"]].concat());
        assert!(matches!(cli.op, Op::Serve { .. }));

        assert!(Cli::try_parse_from(["hf-image-helper", "pull", "x", "--platform", "p", "--owner", "0:0"]).is_err());
    }
}
