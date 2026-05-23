#!/usr/bin/env bash

set -euo pipefail

WORKSPACE_VERSION="$(
  awk '
    $1 == "[workspace.package]" { in_ws = 1; next }
    /^\[/ { if ($0 != "[workspace.package]") in_ws = 0 }
    in_ws && $1 == "version" && $2 == "=" {
      gsub(/\"/, "", $3)
      print $3
      exit
    }
  ' Cargo.toml
)"

if [[ -z "$WORKSPACE_VERSION" ]]; then
  echo "Cargo.toml is missing [workspace.package] version."
  exit 1
fi

TAG="v$WORKSPACE_VERSION"

git fetch origin --tags

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "Tag $TAG already exists; skipping."
  if [[ -n "${TRACEN_RELEASE_TAG_RESULT:-}" ]]; then
    {
      echo "tag=$TAG"
      echo "created=false"
    } > "$TRACEN_RELEASE_TAG_RESULT"
  fi
  exit 0
fi

git tag -a "$TAG" -m "Release $TAG" HEAD
git push origin "$TAG"

if [[ -n "${TRACEN_RELEASE_TAG_RESULT:-}" ]]; then
  {
    echo "tag=$TAG"
    echo "created=true"
  } > "$TRACEN_RELEASE_TAG_RESULT"
fi
