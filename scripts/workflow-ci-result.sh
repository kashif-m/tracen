#!/usr/bin/env bash

set -euo pipefail

check_result() {
  local name="$1"
  local result="$2"

  echo "$name => $result"

  if [[ "$result" != "success" && "$result" != "skipped" ]]; then
    echo "$name failed with result: $result"
    exit 1
  fi
}

check_result "changes" "${1:?changes result is required}"
check_result "rust" "${2:?rust result is required}"
check_result "scripts" "${3:?scripts result is required}"
check_result "ts" "${4:?ts result is required}"
check_result "security-audit" "${5:?security-audit result is required}"
