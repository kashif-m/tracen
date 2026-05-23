#!/usr/bin/env bash

set -euo pipefail

TAG="${RELEASE_TAG:-${GITHUB_REF_NAME:-}}"

if [[ -z "$TAG" ]]; then
  echo "RELEASE_TAG or GITHUB_REF_NAME is required."
  exit 1
fi

if [[ ! "$TAG" == v[0-9]* ]]; then
  echo "Release tag must match v[0-9]*, got: $TAG"
  exit 1
fi

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "GITHUB_TOKEN is required."
  exit 1
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git remote set-url origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"

git fetch origin refs/heads/main:refs/remotes/origin/main --tags

if ! git rev-parse -q --verify "${TAG}^{commit}" >/dev/null; then
  echo "Tag ${TAG} does not resolve to a commit."
  exit 1
fi

if ! git merge-base --is-ancestor "${TAG}^{commit}" origin/main; then
  echo "Tag ${TAG} does not point to a commit reachable from origin/main."
  exit 1
fi

bash scripts/release-changelog.sh "$TAG" --at "$TAG" --push --branch main
