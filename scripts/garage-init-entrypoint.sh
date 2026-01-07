#!/bin/sh
set -eu

/opt/garage/garage-layout-bootstrap.sh
echo "garage-init completed"

# Make PID1 stop fast when Docker sends SIGTERM/SIGINT.
trap 'echo "garage-init stopping"; exit 0' INT TERM

# Keep the container alive *without* spawning a child that won't receive signals.
while :; do
  sleep 3600
done