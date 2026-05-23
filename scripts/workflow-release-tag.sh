#!/usr/bin/env bash

set -euo pipefail

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "GITHUB_TOKEN is required."
  exit 1
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git remote set-url origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"

RESULT_FILE="$(mktemp)"
trap 'rm -f "$RESULT_FILE"' EXIT

TRACEN_RELEASE_TAG_RESULT="$RESULT_FILE" bash scripts/release-tag-if-needed.sh

source "$RESULT_FILE"

if [[ "${created:-false}" != "true" ]]; then
  exit 0
fi

curl -fsSL \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "https://api.github.com/repos/${GITHUB_REPOSITORY}/dispatches" \
  -d "{\"event_type\":\"release-tagged\",\"client_payload\":{\"tag\":\"${tag}\"}}"
