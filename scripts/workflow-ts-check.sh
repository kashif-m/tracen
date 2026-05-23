#!/usr/bin/env bash

set -euo pipefail

if [[ ! -f package.json ]]; then
  echo "No root package.json found; generated TypeScript is covered by Rust pack-codegen snapshots."
  exit 0
fi

if [[ -f pnpm-lock.yaml ]]; then
  corepack enable
  pnpm install --frozen-lockfile
  pnpm run typecheck --if-present
  pnpm test --if-present
elif [[ -f package-lock.json ]]; then
  npm ci
  npm run typecheck --if-present
  npm test --if-present
else
  npm install
  npm run typecheck --if-present
  npm test --if-present
fi
