#!/usr/bin/env bash
# Installs this checkout as the `hf image` extension of the hf CLI: an editable venv, and the
# manifest `hf` reads. Replaces any installed `hf image`.
#
#   scripts/dev-install.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dir="$HOME/.local/share/hf/extensions/hf-image"
rm -rf "$dir"
uv venv -q "$dir/venv"
uv pip install -q --python "$dir/venv/bin/python" -e "$root"
printf '%s\n' '{' \
  '  "owner": "local",' \
  '  "repo": "hf-image",' \
  '  "repo_id": "local/hf-image",' \
  '  "short_name": "image",' \
  "  \"executable_path\": \"$dir/venv/bin/hf-image\"," \
  '  "type": "python",' \
  "  \"installed_at\": \"$(date -u +%Y-%m-%dT%H:%M:%S+00:00)\"," \
  '  "description": "Fast build, push, pull and run on the Hugging Face registry (local checkout)"' \
  '}' >"$dir/manifest.json"
echo "installed: hf image ($root, editable)"
