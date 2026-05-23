#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: append-changelog-from-tag.sh <tag-or-version> [--at <commit-ish>] [--push] [--branch <branch>] [--changelog <path>]

Examples:
  append-changelog-from-tag.sh v0.1.7 --at v0.1.7
  append-changelog-from-tag.sh 0.1.7 --at HEAD --push --branch main
USAGE
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

TAG="$1"
shift

PUSH_TO_BRANCH=""
CHANGELOG_PATH="CHANGELOG.md"
AT="HEAD"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --at)
      AT="${2:?--at requires a commit-ish}"
      shift 2
      ;;
    --push)
      PUSH_TO_BRANCH="main"
      shift
      ;;
    --branch)
      PUSH_TO_BRANCH="${2:?--branch requires a branch name}"
      shift 2
      ;;
    --changelog)
      CHANGELOG_PATH="${2:?--changelog requires a file path}"
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

if [[ ! "$TAG" == v* ]]; then
  TAG="v$TAG"
fi

if ! git rev-parse -q --verify "${AT}^{commit}" >/dev/null; then
  echo "Commit '$AT' not found."
  exit 1
fi

if [[ ! -f "$CHANGELOG_PATH" ]]; then
  mkdir -p "$(dirname "$CHANGELOG_PATH")"
  : > "$CHANGELOG_PATH"
  echo "Created missing changelog at $CHANGELOG_PATH."
fi

if [[ -n "$PUSH_TO_BRANCH" ]]; then
  git fetch origin "refs/heads/$PUSH_TO_BRANCH:refs/remotes/origin/$PUSH_TO_BRANCH"
  git checkout -B "$PUSH_TO_BRANCH" "origin/$PUSH_TO_BRANCH"
fi

VERSION="${TAG#v}"
if [[ -z "$VERSION" ]]; then
  echo "Could not parse a version from '$TAG'."
  exit 1
fi

if grep -Fq "## [$VERSION]" "$CHANGELOG_PATH"; then
  echo "Changelog already contains section $VERSION, nothing to do."
  exit 0
fi

RELEASE_DATE="$(git log -1 --format=%cs "${AT}^{commit}")"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

RELEASE_SECTION_FILE="$TEMP_DIR/release.md"
UPDATED_CHANGELOG_FILE="$TEMP_DIR/changelog.md"

PREVIOUS_TAG="$(git describe --tags --match 'v[0-9]*' --abbrev=0 "${AT}^{commit}^" 2>/dev/null || true)"

if [[ -n "$PREVIOUS_TAG" ]]; then
  LOG_RANGE="${PREVIOUS_TAG}..${AT}"
else
  LOG_RANGE="${AT}"
fi

mapfile -t CHANGE_LINES < <(git log --pretty='- %s' "$LOG_RANGE")

{
  echo "## [$VERSION] - $RELEASE_DATE"
  echo
  echo "### Changes"
  echo
  if [[ ${#CHANGE_LINES[@]} -eq 0 ]]; then
    echo "- No recorded changes for this tag."
  else
    for line in "${CHANGE_LINES[@]}"; do
      echo "$line"
    done
  fi
  echo
} > "$RELEASE_SECTION_FILE"

awk -v release_file="$RELEASE_SECTION_FILE" '
BEGIN {
  inserted = 0
  while ((getline line < release_file) > 0) {
    release_section = release_section line "\n"
  }
  close(release_file)
}
{
  if (!inserted && $0 ~ /^## \[[0-9]+\./) {
    printf "%s", release_section
    inserted = 1
  }
  print
}
END {
  if (!inserted) {
    printf "%s", release_section
  }
}' "$CHANGELOG_PATH" > "$UPDATED_CHANGELOG_FILE"

mv "$UPDATED_CHANGELOG_FILE" "$CHANGELOG_PATH"

if [[ -z "$(git status --porcelain -- "$CHANGELOG_PATH")" ]]; then
  echo "No changelog changes were produced."
  exit 0
fi

if [[ -z "$PUSH_TO_BRANCH" ]]; then
  echo "Changelog updated locally at $CHANGELOG_PATH."
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add "$CHANGELOG_PATH"
git commit -m "chore(changelog): append release notes for $VERSION"
git push origin "HEAD:$PUSH_TO_BRANCH"
