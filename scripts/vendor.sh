#!/usr/bin/env bash
# Clone the exact upstream sources this book walks through.
# Pinned by tag: every code snippet in the book is extracted from these trees,
# so the pins must match SOURCES.lock or the build will disagree with the text.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENDOR="$ROOT/vendor"
mkdir -p "$VENDOR"

clone_at_tag() {
  local name="$1" url="$2" tag="$3" dest="$VENDOR/$1"
  if [ -d "$dest/.git" ]; then
    local have
    have="$(git -C "$dest" describe --tags --exact-match 2>/dev/null || echo '')"
    if [ "$have" = "$tag" ]; then
      echo "==> $name already at $tag"
      return 0
    fi
    echo "==> $name is at '${have:-unknown}', want $tag -- refetching"
    rm -rf "$dest"
  fi
  echo "==> cloning $name @ $tag"
  git clone --depth 1 --branch "$tag" --single-branch "$url" "$dest"
}

clone_at_tag iceberg https://github.com/apache/iceberg.git       "${ICEBERG_TAG:-apache-iceberg-1.11.0}"
clone_at_tag nessie  https://github.com/projectnessie/nessie.git "${NESSIE_TAG:-nessie-0.108.4}"

echo
echo "vendor/ ready:"
du -sh "$VENDOR"/* 2>/dev/null || true
