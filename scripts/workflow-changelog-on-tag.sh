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

TARGET_BRANCH="${CHANGELOG_TARGET_BRANCH:-main}"
CHANGELOG_BRANCH="automation/changelog-${TAG}"

git fetch origin "refs/heads/${TARGET_BRANCH}:refs/remotes/origin/${TARGET_BRANCH}" --tags

if ! git rev-parse -q --verify "${TAG}^{commit}" >/dev/null; then
  echo "Tag ${TAG} does not resolve to a commit."
  exit 1
fi

if ! git merge-base --is-ancestor "${TAG}^{commit}" "origin/${TARGET_BRANCH}"; then
  echo "Tag ${TAG} does not point to a commit reachable from origin/${TARGET_BRANCH}."
  exit 1
fi

git checkout -B "$CHANGELOG_BRANCH" "origin/${TARGET_BRANCH}"

bash scripts/release-changelog.sh "$TAG" --at "$TAG"

if [[ -z "$(git status --porcelain -- CHANGELOG.md)" ]]; then
  echo "No changelog changes were produced."
  exit 0
fi

VERSION="${TAG#v}"
git add CHANGELOG.md
git commit -m "chore(changelog): append release notes for ${VERSION}"
git push --force-with-lease origin "HEAD:refs/heads/${CHANGELOG_BRANCH}"

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is required to open the changelog pull request."
  exit 1
fi

export GH_TOKEN="$GITHUB_TOKEN"

EXISTING_PR_URL="$(
  gh pr list \
    --repo "$GITHUB_REPOSITORY" \
    --head "$CHANGELOG_BRANCH" \
    --base "$TARGET_BRANCH" \
    --state open \
    --json url \
    --jq '.[0].url // empty'
)"

if [[ -n "$EXISTING_PR_URL" ]]; then
  echo "Changelog pull request already exists: $EXISTING_PR_URL"
  exit 0
fi

gh pr create \
  --repo "$GITHUB_REPOSITORY" \
  --head "$CHANGELOG_BRANCH" \
  --base "$TARGET_BRANCH" \
  --title "chore(changelog): append release notes for ${VERSION}" \
  --body "Generated changelog entry for ${TAG}."
