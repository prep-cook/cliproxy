#!/usr/bin/env bash
set -u -o pipefail
if [[ $# != 2 ]]; then
    printf 'usage: %s TMPFS OUT\n' "$0" >&2
    exit 2
fi
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tmpfs=$(realpath -- "$1") || exit 2
out=$(realpath -m -- "$2") || exit 2
if [[ $out/ == "$root"/* ]]; then
    printf 'OUT must be outside the repository\n' >&2
    exit 2
fi
mkdir -p -- "$out" || exit 2
if [[ -n $(ls -A -- "$out") ]]; then
    printf 'OUT must be empty; raw evidence is never overwritten\n' >&2
    exit 2
fi
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$root/target/stream"}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
failed=()
if ! cargo build --release --locked --offline --manifest-path "$root/bench/stream/Cargo.toml" >"$out/build.log" 2>&1; then
    failed=(--build-failed)
fi
exec python3 -I "$root/bench/stream/gate.py" "$tmpfs" "$out" \
    --binary "$CARGO_TARGET_DIR/release/ck-stream-bench" "${failed[@]}"
