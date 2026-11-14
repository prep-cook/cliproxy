#!/usr/bin/env bash
# Run from the repository root; OUT must be a new external output directory.
# Acceptance by default: exits nonzero unless every scenario, flow metric, frame
# position and FLOW_BUILD_PROVENANCE is verified. --diagnostic never passes.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd)
exec python3 -I "$HERE/flow/driver.py" "$@"
