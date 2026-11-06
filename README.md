# hf image

Fast build, push, pull and run of container images on the Hugging Face registry (`cr.hf.co`), as
an [`hf` CLI extension](https://huggingface.co/docs/huggingface_hub/guides/cli-extensions).

```console
$ hf image build -t cr.hf.co/acme/app:v2 .       # your Dockerfile, your buildx
$ hf image push cr.hf.co/acme/app:v2              # an image already in your Docker store
$ hf image pull cr.hf.co/acme/app:v2 [-o dir]     # into Docker, or an OCI layout
$ hf image run --rm cr.hf.co/acme/app:v2          # pulls first if needed, then docker run
$ hf image untag cr.hf.co/acme/app:v2             # the tag; the image stays, by digest
$ hf image delete cr.hf.co/acme/app@sha256:…      # an image no tag points to
```

Images stay plain OCI: anything pushed with `hf image` pulls with `docker pull`, and `hf image pull`
reads images pushed with `docker push`.

## Install

```console
$ hf extensions install hf-image
```

## Why it is fast

- **Layers are stored as uncompressed tar** (digest = the layer's `diff_id`), so
  [Xet](https://huggingface.co/docs/hub/xet) deduplicates them chunk by chunk across versions,
  images and users. Rebuilding a 2 GB layer that changed by 100 MB uploads ~100 MB; pulling it over
  the previous version downloads ~100 MB.
- **No gzip.** BuildKit exports uncompressed layers, push and pull never compress, and unpacking a
  plain tar runs at disk speed.
- **Bytes go straight between your machine and Xet**, over many connections. The registry only sees
  manifests and small registration calls.
- **Builds push into a local endpoint**: layers the registry already has are never read, new ones
  stream to Xet as BuildKit exports them.
- **Pulls write into containerd**, Docker's image store, and unpack layers as they land.

Because layers are stored as tar, pushing an image with compressed layers gives it a new digest.
`push --preserve-digests` pushes blobs and manifests as stored, still straight to Xet: same digests
as `docker push`, so signatures and pins by digest keep working, but compressed layers don't
deduplicate chunk by chunk.

## How it runs

`hf image` is a small Python CLI that drives `docker`. The work runs in a helper container next to
the Docker daemon: [`hf-image-helper`](helper) (Rust), with containerd's socket and a named volume
for the Xet caches. `build` runs it in BuildKit's network and points `docker buildx build` at it.
The same path covers rootful and rootless Docker on Linux, and Docker in a macOS VM (tested with
Docker Desktop and Lima).

OCI layouts (`pull -o`, `push --from oci:<dir>`) are bind-mounted into the helper: on macOS the
directory must be shared with the VM, and writable for `pull -o`.

The Hugging Face token goes on the helper's stdin, never on a command line, in its environment or in
its logs.

## containerd snapshotter (experimental)

For hosts where containerd pulls images itself (Kubernetes nodes): `hf-image-helper snapshotter` is
a proxy snapshotter that streams every layer of a `cr.hf.co` image from Xet at once and unpacks it
straight into its snapshot. Layers skip containerd's content store, so each byte is written once.
CRI pulls of other registries, and pulls without CRI's snapshot labels, unpack as usual.

Layers are unpacked by [`hf-image-apply`](applier) (Go), found next to the helper binary:
containerd's own applier, with the options of its overlayfs snapshotter, in its own process.

```toml
# /etc/containerd/config.toml
[proxy_plugins.hf]
  type = "snapshot"
  address = "/run/hf-image/snapshotter.sock"

[plugins."io.containerd.cri.v1.images"]
  snapshotter = "hf"
  disable_snapshot_annotations = false
```

```console
# (printf '%s\n' "$HF_TOKEN"; sleep infinity) |
    hf-image-helper snapshotter --socket /run/hf-image/snapshotter.sock --root /var/lib/hf-image
```

Snapshot records live in memory: a restart of the snapshotter forgets the images it unpacked.

## Requirements

- Docker with the [containerd image store](https://docs.docker.com/engine/storage/containerd/)
  (the default for new Docker 29 installs), rootful or rootless, on Linux or in a macOS VM. The
  classic graphdriver store and Windows are not supported.
- For `build`: the `docker` builder driver, or a `docker-container` builder.
- A Hugging Face token with access to the image's repository: `hf auth login`, `HF_TOKEN`, or
  `--token`.
- The helper image, pulled on first use without registry credentials:
  `cr.hf.co/infra-workloads/hf-image-helper` (public, linux/amd64 and linux/arm64), pinned by digest
  for each version.

## Configuration

| Variable | Default | |
|---|---|---|
| `HF_IMAGE_CONCURRENCY` | 6 | layers transferred at once |
| `HF_IMAGE_CHUNK_CACHE_BYTES` | 0 (off) | Xet chunk cache for pulls, in bytes: re-pulls of rebuilt layers get cheaper, cold pulls slower |
| `HF_IMAGE_CACHE_VOLUME` | `hf-image-cache` | named volume for the helper's Xet caches |
| `HF_IMAGE_HELPER_IMAGE` | pinned for each version | the helper image |
| `HF_IMAGE_CONTAINERD_ADDRESS` | from `docker info` | containerd's socket, on the daemon's host |
| `HF_IMAGE_INSECURE_REGISTRIES` | | comma-separated `host:port` served over plain HTTP (loopback always is) |
| `HF_IMAGE_LOG` | `warn` | the helper's log filter (`RUST_LOG` syntax) |
| `HF_XET_*` | | xet-core settings, passed to the helper |

## Development

```console
$ uv run ruff check && uv run pytest                                     # the CLI
$ cd helper && cargo clippy --all-targets -- -D warnings && cargo test   # the helper (needs protoc)
$ cd applier && go vet ./... && go test -c -o applier.test . && sudo ./applier.test   # hf-image-apply (Linux, root)
$ DOCKER_BUILD=1 scripts/helper-image.sh                                 # hf-image-helper:dev, built in your daemon
$ export HF_IMAGE_HELPER_IMAGE=hf-image-helper:dev
$ scripts/dev-install.sh                                                 # `hf image` runs this checkout
```

`scripts/release.sh X.Y.Z` bumps the version, publishes its helper image (the Release workflow),
pins it by digest and opens the PR: merging that PR releases the version.

## License

[Apache 2.0](LICENSE)
