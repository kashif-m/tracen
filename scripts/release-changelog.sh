#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: release-changelog.sh <tag-or-version> [--at <commit-ish>] [--push] [--branch <branch>]

Examples:
  release-changelog.sh v0.1.7 --at HEAD
  release-changelog.sh 0.1.7 --at HEAD --push --branch main
USAGE
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

TAG_OR_VERSION="$1"
shift

PUSH=false
BRANCH="main"
AT="HEAD"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --at)
      AT="${2:?--at requires a commit-ish}"
      shift 2
      ;;
    --push)
      PUSH=true
      shift
      ;;
    --branch)
      BRANCH="${2:?--branch requires a branch name}"
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

if "$PUSH"; then
  bash scripts/append-changelog-from-tag.sh "${TAG_OR_VERSION}" --at "${AT}" --push --branch "${BRANCH}"
else
  bash scripts/append-changelog-from-tag.sh "${TAG_OR_VERSION}" --at "${AT}"
fi
