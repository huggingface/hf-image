#!/usr/bin/env bash
# Builds the helper image (the Linux hf-image-helper that `hf image` runs next to the Docker daemon)
# into the current daemon, from a static musl build for the daemon's architecture.
#
#   scripts/helper-image.sh [musl binary] [image]
#
# Default binary: `cargo build --release --target <arch>-unknown-linux-musl` (Linux with musl-tools),
# or, with DOCKER_BUILD=1, the same build in a rust:alpine container of the daemon (any host).
# Default image: hf-image-helper:dev. Use it with HF_IMAGE_HELPER_IMAGE.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
bin="${1:-}"
image="${2:-hf-image-helper:dev}"
case "$(docker info --format '{{.Architecture}}')" in
  x86_64 | amd64) arch=amd64 target=x86_64-unknown-linux-musl ;;
  aarch64 | arm64) arch=arm64 target=aarch64-unknown-linux-musl ;;
  *) echo "no helper build for this daemon's architecture" >&2; exit 1 ;;
esac
ctx="$(mktemp -d)"
trap 'rm -rf "$ctx"' EXIT
if [[ -z "$bin" && "${DOCKER_BUILD:-0}" == 1 ]]; then
  # The sources go in through stdin, the binary comes out through stdout: no bind mounts.
  COPYFILE_DISABLE=1 tar -C "$root/helper" --exclude target -cf - . |
    docker run --rm -i -v hf-image-helper-cargo:/usr/local/cargo/registry -v hf-image-helper-target:/src/target \
      rust:1-alpine sh -c \
      'apk add -q musl-dev protoc protobuf-dev >&2 && tar -C /src -xf - && cd /src &&
       cargo build -q --release --locked >&2 && cat target/release/hf-image-helper' >"$ctx/hf-image-helper-$arch"
elif [[ -z "$bin" ]]; then
  cargo build --release --locked --target "$target" --manifest-path "$root/helper/Cargo.toml"
  bin="${CARGO_TARGET_DIR:-$root/helper/target}/$target/release/hf-image-helper"
fi
[[ -z "$bin" ]] || cp "$bin" "$ctx/hf-image-helper-$arch"
chmod +x "$ctx/hf-image-helper-$arch"
docker build -q --platform "linux/$arch" -t "$image" -f "$root/helper/Dockerfile" "$ctx" >/dev/null
echo "built $image: export HF_IMAGE_HELPER_IMAGE=$image"
