#!/usr/bin/env bash
# Prepares release X.Y.Z: bumps the version on release/X.Y.Z, publishes its helper image (the Release
# workflow), pins it by digest and opens the PR. Merging that PR is the release.
#
#   scripts/release.sh X.Y.Z
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "usage: scripts/release.sh X.Y.Z" >&2
  exit 1
fi
cd "$(dirname "$0")/.."
branch="release/$version"
pin=src/hf_image/_pin.py
if ! git diff --quiet HEAD; then
  echo "commit or stash your changes first" >&2
  exit 1
fi

git fetch -q origin main
git switch -q -c "$branch" origin/main
for f in pyproject.toml helper/Cargo.toml; do
  sed -i.bak "s/^version = \".*\"$/version = \"$version\"/" "$f" && rm "$f.bak"
done
cargo update -q --workspace --manifest-path helper/Cargo.toml
# The lock resolves against PyPI, whatever the local uv config says.
uv lock -q --no-config
git commit -q -am "chore: release $version"
git push -q -u origin "$branch"

sha="$(git rev-parse HEAD)"
gh workflow run release.yml --ref "$branch" >/dev/null
run=""
for _ in $(seq 30); do
  run="$(gh run list --workflow release.yml --commit "$sha" --json databaseId --jq '.[0].databaseId // empty')"
  [[ -n "$run" ]] && break
  sleep 2
done
if [[ -z "$run" ]]; then
  echo "no Release run started for $branch" >&2
  exit 1
fi
url="$(gh run view "$run" --json url --jq .url)"
echo "building the helper image: $url" >&2
gh run watch "$run" --exit-status --compact >&2

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
gh run download "$run" --name pin --dir "$tmp"
image="$(cat "$tmp/pin")"
sed -i.bak "s|^HELPER_IMAGE = .*|HELPER_IMAGE = \"$image\"|" "$pin" && rm "$pin.bak"
git commit -q -am "chore: pin the helper image of $version"
git push -q

gh pr create --base main --head "$branch" --title "chore: release $version" \
  --body "Pins \`$image\`, built by $url. Merging this PR releases $version."
