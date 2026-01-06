#!/usr/bin/env bash
set -euo pipefail

readonly ADMIN_HEALTH_URL="${ADMIN_HEALTH_URL:-http://localhost:3903/health}"

main() {
  wget -qO- "${ADMIN_HEALTH_URL}" >/dev/null 2>&1
}

main "$@"
