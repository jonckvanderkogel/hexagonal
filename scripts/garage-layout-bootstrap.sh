#!/bin/sh
set -eu

GARAGE_CONFIG_PATH="${GARAGE_CONFIG_PATH:-/etc/garage.toml}"
GARAGE_BIN="${GARAGE_BIN:-garage}"

GARAGE_ZONE="${GARAGE_ZONE:-dc1}"
GARAGE_CAPACITY="${GARAGE_CAPACITY:-1G}"
TARGET_VERSION="${GARAGE_LAYOUT_TARGET_VERSION:-1}"

wait_for_garage_cli_ready() {
  attempts=0
  max_attempts=120

  while :; do
    if "$GARAGE_BIN" -c "$GARAGE_CONFIG_PATH" status >/dev/null 2>&1; then
      return 0
    fi

    attempts=$((attempts + 1))
    if [ "$attempts" -ge "$max_attempts" ]; then
      echo "Garage CLI not ready after ${max_attempts}s (garage status failing)" >&2
      return 1
    fi

    echo "Waiting for Garage to be ready (garage status)..." >&2
    sleep 1
  done
}

current_layout_version() {
  v="$("$GARAGE_BIN" -c "$GARAGE_CONFIG_PATH" layout show 2>/dev/null \
    | sed -n 's/^Current cluster layout version: \([0-9][0-9]*\)$/\1/p' \
    | tail -n 1)"
  [ -n "${v:-}" ] && echo "$v" || echo "0"
}

first_node_id() {
  "$GARAGE_BIN" -c "$GARAGE_CONFIG_PATH" status 2>/dev/null \
    | sed -n 's/^\([0-9a-f][0-9a-f]*\)[[:space:]].*/\1/p' \
    | head -n 1
}

assign_layout() {
  node_id="$1"
  "$GARAGE_BIN" -c "$GARAGE_CONFIG_PATH" layout assign -z "$GARAGE_ZONE" -c "$GARAGE_CAPACITY" "$node_id"
}

apply_layout() {
  version="$1"
  "$GARAGE_BIN" -c "$GARAGE_CONFIG_PATH" layout apply --version "$version"
}

main() {
  wait_for_garage_cli_ready

  v="$(current_layout_version)"
  if [ "$v" -ge "$TARGET_VERSION" ]; then
    echo "Garage layout already applied (version $v), skipping."
    return 0
  fi

  node_id="$(first_node_id)"
  if [ -z "${node_id:-}" ]; then
    echo "Could not determine Garage node id from 'garage status'." >&2
    return 1
  fi

  assign_layout "$node_id"
  apply_layout "$TARGET_VERSION"

  echo "Garage layout bootstrapped (version $TARGET_VERSION)."
}

main "$@"