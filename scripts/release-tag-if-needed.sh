#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release-tag-if-needed.sh --version <version> --confirm yes

This helper is deprecated and intentionally refuses to perform auto-tagging.
Use the release-tag workflow instead.
USAGE
}

VERSION=""
CONFIRM="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:?--version requires a version}"
      shift 2
      ;;
    --confirm)
      CONFIRM="${2:?--confirm requires yes/no}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "Usage requires --version <version>."
  usage
  exit 1
fi

if [[ "$CONFIRM" != "yes" ]]; then
  echo "Auto tagging is disabled. Set --confirm yes to acknowledge this helper is deprecated."
  exit 1
fi

echo "Refusing to tag from this helper. Run workflow release-tag.yml with version $VERSION."
exit 1
