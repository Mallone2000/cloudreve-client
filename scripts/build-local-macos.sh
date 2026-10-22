#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This script builds a local macOS app." >&2
  exit 1
fi

if [[ ! -x ui/node_modules/.bin/tauri ]]; then
  echo "Install frontend dependencies first: cd ui && yarn install --frozen-lockfile" >&2
  exit 1
fi

ui/node_modules/.bin/tauri build --bundles app \
  --config '{"bundle":{"createUpdaterArtifacts":false,"macOS":{"signingIdentity":"-"}}}' \
  --ci

app_path="target/release/bundle/macos/Cloudreve.app"
codesign --verify --deep --strict --verbose=2 "$app_path"
echo "Local app ready: $PWD/$app_path"
