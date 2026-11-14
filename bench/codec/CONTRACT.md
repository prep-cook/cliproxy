# External codec workload contract v1

`bench/codec-gate.sh OLD_CODEC NEW_CODEC OUT` compares two executable workload harnesses, not gateway binaries. `build-baseline.sh` builds a real instrumented harness against the pinned old pair implementation. `build-new.sh` builds the new boundary-codec harness; it covers Chat and Anthropic only, so the full gate exits 2 until the other protocols merge.

## Run the gate

From the repository root on Linux, install Python 3, Bash, util-linux (`findmnt`, `flock`, `taskset`) and both release harness executables. Use a fresh external output directory. Run on an otherwise quiet host:

```sh
flock /tmp/cooker-bench.lock taskset -c 0-7,16-23 \
  bench/codec-gate.sh --calibrate "$OLD_CODEC" "$BAND_OUT"
CODEC_NOISE_BAND="$BAND_OUT/noise-band.json" flock /tmp/cooker-bench.lock taskset -c 0-7,16-23 \
  bench/codec-gate.sh "$OLD_CODEC" "$NEW_CODEC" "$OUT"
```

Calibrate once per baseline build, host and kernel, and again after 30 days; see "Calibrate the memory noise band".

The driver runs old-1, new-1, old-2, new-2, old-3, new-3, old-4, new-4 as separate processes and discards round 1 of each harness, whose first run is cold. The enclosing invocation owns serialization; the driver does not acquire another lock. Both harnesses inherit that CPU affinity. Do not run builds during measurements. The baseline builder defaults to `BUILD_CPUS=8-15,24-31`, `CARGO_BUILD_JOBS=4` and `CARGO_TARGET_DIR=/scratch/ssd/bench-target`. Override any of these environment variables for another build host; provenance records all three effective values.

Exit 0 means all required records passed. Exit 1 means at least one median regressed. Exit 2 means a prerequisite, workload, schema or measurement failed. No missing row is skipped. Existing output is refused rather than overwritten. Failures retain available raw stdout/stderr and `summary.json`; a nonexistent executable is an error, not a baseline.

Identical executable SHA-256 values, or an identical pair of source revision and source checksum, return exit 2 before measurement. A new harness that reports the baseline revision also returns exit 2, whatever its source checksum: a harness built from that revision compares the old code with itself. Distinct wrapper paths are not distinct implementations.

## Implement the harness CLI

The driver invokes each executable directly, without shell argument evaluation:

```text
HARNESS --describe
HARNESS --run /absolute/path/workloads.json --round 1
```

`--describe` must finish within 30 seconds and print one JSON object to stdout:

```json
{
  "schema_version": 1,
  "revision": "b518060ea1e43ef4b266591986ca92321e6289b5",
  "source_checksum": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "compiler": "rustc -Vv output from the build",
  "target": "x86_64-unknown-linux-gnu",
  "profile": "release",
  "features": [],
  "build_flags": ["--locked", "--release"],
  "config": {"retention": "release-at-completion", "workers": 1},
  "storage_directory": "/absolute/existing/private/directory"
}
```

The checksum above illustrates the format; it is not baseline evidence. `source_checksum` identifies the exact source bundle, including benchmark instrumentation. Record compiler, profile, features and flags from the actual build. `config` must be nonempty and identical between old and new; it contains comparable external settings such as workers, allocator, instrumentation and retention. Record backend-specific pool settings and limits in a separate `backend_config` object, not in the equality-checked external configuration. `storage_directory` identifies the actual storage filesystem even when the old harness has no pool. Source revision, source checksum and executable checksum are separate provenance fields.

`--run` must finish within 600 seconds and print JSONL only to stdout. Diagnostics go to stderr. Emit exactly one record per required workload, in any order. The record keys are exactly:

```text
schema_version: integer 1
workload_id: exact workloads[].id
manifest_sha256: SHA-256 of the unchanged workloads.json file bytes
round: integer supplied by --round
metrics: object containing every key in workloads.json.metrics
```

Each metric is exactly `{"value": NUMBER, "unit": "UNIT"}`. Use the units supplied in the manifest. Numbers must be finite, nonnegative JSON numbers, never strings, booleans, null, NaN or Infinity. Duplicate JSON keys, duplicate/unknown workload IDs, extra/missing metrics, wrong units, stale rounds, changed manifest bytes and nonzero harness exits fail. Unsupported instrumentation must exit nonzero; it must not report an invented zero.

Each latency distribution must satisfy `p50 <= p95 <= p99`. Schema versions and round numbers must be JSON integers, not booleans.

The gate validates records, not the truthfulness of instrumentation or semantic equivalence. A real harness needs source review and independent semantic verification before its measurements are acceptance evidence. Synthetic controls deliberately supply invented measurements and are not usable baseline evidence.

## Consume driver-owned immutable inputs

`fixtures.json` pins all 30 recorded pair fixture files by SHA-256. These files contain 12,143 cases at the frozen corpus revision `b518060ea1e43ef4b266591986ca92321e6289b5`. The driver verifies every file before constructing the manifest. It never asks a harness which workloads it supports.

The manifest groups every case by fixture file, mode and external event kind. A request row measures `client_decode+upstream_encode` together; a response/count row measures `upstream_decode+client_encode` together. These two external transformations cover all four required boundary roles without inventing an old canonical-SSE stage. Each row names both protocols, direction, combined role, mode, one event kind, source path/checksum and original case indexes. Native Chat, Gemini and Interactions and all 30 recorded cross-format arms remain mandatory. The workload ID hashes that identity; no harness-generated matrix can replace it.

For streams, event kinds are the original `event:` names and JSON `type`/`object` values. `[DONE]`, JSON without those fields, incomplete/framing input and empty streams have explicit kinds. A fixture with `finalize=true` also requires a `finalize` kind row, measuring the EOF tool-input reconciliation call and its output framing. A case containing several kinds appears in each applicable kind workload with its complete context. Other modes use their fixture path as the kind (`request`, `request_compat`, `request_envelope`, `non_stream`, `token_count`). These are external wire kinds and terminal operations, not the future internal grammar's event names.

Selection is line-based and identical in the driver and baseline harness. An `event:` line contributes only its trimmed event name (`unnamed` when empty); it is never `framing-or-fragment`. A `data:` line uses the trimmed text after that prefix; any other line uses the original text. Recognize an exact `[DONE]` payload before parsing JSON. A JSON object contributes its `type`, then `object`, otherwise `json`; other JSON values contribute `json`. Failed JSON parsing contributes `framing-or-fragment`, including blank lines and partial payloads. Event headers are not retried as JSON payloads.

`cases[].input` preserves external inputs and context: name, path, model, input, lines, original/translated request context, stream, bytes, finalize and count where present. `count` is the input to `translate_token_count`, not an expected result. The per-case `bytes` flag converts each Unicode scalar to its low byte, matching the baseline golden reader. Old expected outputs, alternate expected outputs, dynamic assertions and expected errors are excluded. A harness must execute malformed/refused inputs too. Do not regenerate fixtures or use old expected output as the new codec's output.

This corpus is the required v1 comparison floor, not proof of complete S1 coverage. Native Messages/Responses, compact, legacy/media/duplex and provider envelopes outside these 30 files still need their independent semantic and flow acceptance inputs from `docs/STREAM-GOLDEN-CHANGES.md`. This driver cannot declare those surfaces passed. Extend the driver-owned corpus with reviewed immutable inputs before using it to claim full S1 codec acceptance; never shrink it to match harness availability.

## Measure equivalent boundary work

The manifest fixes 10 warmup and 100 measured iterations per case, deterministic file/case order, and release-at-completion retention. Prepare model/request context and external bytes outside timers, then measure the entire request or response transformation. The old harness invokes its pair transform; the new harness must execute both named boundary stages through its internal stream. Do not apportion old pair cost among invented stages or duplicate it into two role records. Old pair APIs remain private to the baseline harness, not part of the new codec interface.

For a stream kind workload, replay the full case context on every iteration and attribute event costs to the selected external kind. Keep stream-local state and framing fragmentation intact. Report latency distributions from that round only; do not combine percentiles across rounds or substitute a whole-stream average. Whole-workload resource peaks and throughput include the context needed to produce the selected kind. For a nonstream request, buffered body, count result or explicit rejection, one complete boundary operation is the event/completion unit. Empty input and refusal cases measure the boundary's validation/terminal operation, not zero executed work.

Use fresh request-local state per operation and exclude corpus loading, setup, warmup and metric serialization from CPU/allocation timing. Both harnesses must time CPU with `CLOCK_PROCESS_CPUTIME_ID`, which includes any deep-stack worker thread, around the same regions: each stream line, the finalize call, stream construction and drop, and each single operation. That clock is a system call, not vDSO: on the 2026-11-14 build host one pair cost a median 260 ns, 17% of the median baseline event, and over 5% in 334 of 371 workloads. At process start each harness takes the median of 1,001 empty clock pairs and subtracts it from every timed interval, never below zero; evidence records it as `cpu_clock_overhead_ns`. A harness on another clock, or without that subtraction, is not comparable. Its global System allocator wrapper counts successful allocation/reallocation calls and requested bytes, tracks live bytes and peak live bytes, and observes retained bytes after outputs/state drop. Output bytes pass through `black_box`. Stream replay feeds original lines to `go_stream`, applies `stream::frame` to output chunks and honors `finalize_tool_input`; request, compat, envelope, buffered and count modes follow the pinned golden reader without consulting its output assertions.

The manifest specifies every required metric and unit:

- CPU ns/event, allocations/event, allocated B/event, encoded/decoded B/event and CPU ns/completion.
- Throughput in events/s; first-deliverable, inter-event and completion latency p50/p95/p99 in ns.
- Heap peak/retained, RSS, PSS, anonymous/shmem/mapped residency, metadata, tmpfs/cgroup/total charge in B.
- Reserved/used pool frames and minor/major page faults.
- Output chunks and completions without deliverable output, as totals over the round's measured operations.

Measure memory with equivalent retention, after operation completion and release of its leases/cache references, as well as peak residency. Do not compare new retained-cache lifetime with old disconnect cleanup. Use isolated processes/cgroups or explicit baselines so unrelated host charges do not become workload charges. `total_charged` must account for anonymous memory, mapped/tmpfs residency and OS page cache without double counting shared pages; keep its component measurements. Zero is permitted only when measured or structurally absent (for example, old pool frames); unavailable PSS/cgroup/allocator instrumentation is an error.

For each workload, the baseline launcher creates a fresh process and transient private cgroup under `/sys/fs/cgroup/cooker-codec-UID-PID-INDEX`. The launcher, prior results and other workloads remain outside that group. It moves only the disposable worker into the group and removes the group after exit. Passwordless `sudo` performs mkdir, ownership of `memory.peak`, migration and rmdir; the launcher never enables controllers or changes existing groups. Missing controllers, permissions, smaps fields or unrelated members fail loudly. `total_charged` is private `memory.peak`, reset and read through one held descriptor; `cgroup_charge` is post-release private `memory.current`. These are absolute private-group charges, not host-wide values or deltas. They include charged anonymous/file/shmem/kernel memory without summing overlapping components. File pages already charged elsewhere remain outside private charge; RSS/PSS independently gate mapped residency, so this metric alone is not a complete process-memory claim.

If that exact cgroup already exists, the launcher exits 2 and reports its absolute path for inspection. It never reuses accounting or removes a leftover group's members.

RSS/PSS report the maximum observed setup/post-operation/post-release smaps sample, not a continuously sampled RSS peak, minus a snapshot taken after the workload's inputs are prepared and before its warmup. The subtraction keeps each harness's own binary, libraries, startup and inputs out of the 0% comparison, so a larger harness executable does not fail every workload; codec code and data touched during warmup and measurement still count. In three 2026-11-14 baseline rounds that snapshot was a median 3,004 KiB, 72% of the absolute RSS.

These OS counters are not repeatable at the 0% boundary. Between two warm baseline rounds of identical code, per-workload RSS/PSS moved a median 80 KiB (up to 508 KiB) and `cgroup_charge`/`total_charged` a median 168 KiB (up to 1,272 KiB), up and down about equally; a cold first round ran about 380 KiB lower RSS than warm rounds. Old against old would fail about half the workloads on each. Heap peak/retained were identical across rounds. The gate therefore compares warm rounds only, and the calibrated noise band below covers these four counters only. Anonymous, shmem and mapped components use `Anonymous`, `Pss_Shmem` and `Pss_File + Pss_Shmem`; the latter two are proportional-residency diagnostics. Page faults come from `getrusage`. The old backend has no pool, pool metadata or tmpfs stream files, so those structurally absent diagnostics are zero. Its heap peak/retained values subtract pre-workload input/instrumentation allocation. Cgroup totals count only charges after the worker joined its private group.

Latency samples use monotonic clocks at output-batch availability and operation completion. Chunks returned together have zero inter-output delay. Operations producing no deliverable output record a structural zero for first-deliverable latency and increment the `no_output_completions` metric. A workload with no output pairs has structural zero inter-event percentiles. These are absent output observations, not invented delivery timestamps or unavailable instrumentation.


## Apply median thresholds

Compare each workload and metric independently using the median of its three warm old values (rounds 2-4) and median of its three warm new values. Never pool protocols, roles, modes, event kinds or percentiles.

- CPU, allocations and all latency percentiles: `new * 100 <= old * 105`.
- Throughput: `new * 100 >= old * 95`.
- Heap and allocation metrics (`allocated_bytes_per_event`, `heap_peak`, `heap_retained`): `new <= old`, without a noise allowance.
- OS memory counters (`rss`, `pss`, `cgroup_charge`, `total_charged`): `new - old <= allowance`, where `allowance` is that workload's calibrated noise band in bytes; beyond it the 0% rule holds.
- Output presence: `encoded_bytes_per_event` and `output_chunks` fail when old is above zero and new is zero, and `no_output_completions` must satisfy `new <= old`. A harness that emits nothing has structural zero latencies, so without these rules it would pass every latency metric. They check presence, not equal output; semantic equivalence stays with the semantic suites.
- An old zero requires a new zero for lower-is-better metrics; positive throughput from an old zero remains an improvement.
- Decoded bytes, anonymous/shmem/mapped splits, metadata, tmpfs components, pool frames and page faults are report-only diagnostics. Backend components are not comparable aggregates: old zero mmap frames cannot gate a new shared pool. Their cost still contributes to the strictly gated process/cgroup totals. This reporting classification does not waive aggregate resource regressions or change G-storage. Semantic byte preservation is checked separately, not inferred from equal lengths.

The exact boundary passes; a value beyond it fails. Decimal arithmetic avoids float rounding at 5%. These rules are the INTERNAL-STREAM §9.2 "Codec and pipeline tolerances" table, which records the owner's 2026-11-14 option-B ruling: at most 5% for codec/pipeline cost and latency, 0% for memory and storage, with allocated bytes/event in the memory class. They do not relax the independent G-storage rules.

## Calibrate the memory noise band

Owner ruling 2026-11-14 (「都可以捏」, item 2): RSS, PSS, `cgroup_charge` and `total_charged` may exceed old by a noise band measured from old-against-old runs; heap and allocation metrics stay exact.

`bench/codec-gate.sh --calibrate OLD_CODEC OUT` runs the baseline harness over the full manifest 11 times and writes `OUT/noise-band.json`. Round 1 is discarded, as in the gate. The band file stores each workload's ten warm byte values for each counter, not an allowance.

The gate recomputes each allowance from those samples with the statistic it tests. For each workload and counter it takes every ordered pair of disjoint triples among the ten warm rounds (4,200 pairs) and computes `median(second triple) - median(first triple)`, the old-against-old version of `new_median - old_median`. Both orders of every pair are present, so these differences have mean zero. The allowance is the largest of:

- one page (`page_size`, 4,096 bytes on x86-64), the counters' resolution;
- `ceil(3 × σ)`, with σ the root mean square of the differences;
- the largest difference observed.

Each workload is calibrated from its own rounds only: a noisy workload never widens another workload's band. The page floor cannot exceed one page. A median of three ignores one outlier round, so a workload with nine equal values and one outlier gets the one-page floor. Ten warm rounds take about ten minutes under the benchmark lock.

The band file records the rule, unit (`B`), multiplier, page size, round counts, baseline revision, source and executable checksums, manifest checksum, hostname, kernel, affinity, load before and after, creation time and the samples. It is host evidence, not a versioned repository file: keep it with external evidence and pass its path in `CODEC_NOISE_BAND`. The gate returns exit 2 before measuring when:

- that variable is unset;
- the file's fields are not exactly those above;
- any recorded value differs from the current run, including the old executable checksum and the CPU affinity;
- the file is older than 30 days or has no UTC offset;
- a workload in the manifest is missing or an extra workload is present;
- a sample list is not ten nonnegative integers.

Calibrate again for each baseline build, host, kernel and affinity.

Open limitation: ten samples per workload under-estimate the old-against-old spread. On 2026-11-14 two independent calibrations ran 20 minutes apart. Each band was checked against the other session's rounds split like a gate run, old rounds 2, 4, 6 against new rounds 3, 5, 7. Across four such simulated runs per direction, 17-18 RSS/PSS and 38-40 `cgroup_charge`/`total_charged` comparisons per counter, out of 1,484 (four runs of 371 workloads), exceeded their allowances. An unchanged harness would therefore fail about 29 of the 1,484 banded comparisons per run. The multiplier, round count and any floor beyond one page await an owner decision.

## Read evidence

`workloads.json` contains the full required workload and metric matrix. `old-describe.json` and `new-describe.json` preserve build metadata verbatim. Eight `SIDE-ROUND.jsonl` files preserve measurements, including the two discarded warm-up rounds marked `discarded_warmup` in `summary.json`; matching `.stderr` files preserve diagnostics. `summary.json` contains harness paths/checksums, source revisions, configuration, manifest/driver/pin checksums, every raw-round filename/checksum, exit codes, host/page size/kernel/architecture/CPU information, actual affinity, filesystem source/type/options and before/after load/time snapshots. Comparisons include units, both medians, direction, tolerance and pass/fail. Exact medians are serialized as decimal strings; raw measurement values remain JSON numbers.

## Locate the old baseline

Retained provenance in `/home/zakk/code/jev-router/p4/s1-baseline/` identifies post-W1 `b518060ea1e43ef4b266591986ca92321e6289b5`. `fix-baseline-targets.json` records library/test targets for `ck-translate`, not a codec benchmark. The retained `cooker-b518060e` and `claude_latency-b518060e` executables are gateway/pipeline artifacts and cannot implement this contract.

Build from a fresh detached checkout at that exact revision. From the repository root, with Rust and the baseline's dependencies cached, run:

```sh
bench/codec/build-baseline.sh "$BASELINE_SOURCE" "$ARTIFACTS/baseline-build"
"$ARTIFACTS/baseline-build/baseline-harness" --describe
```

The builder reads the detached HEAD file without Git writes and refuses another revision. It creates a standalone crate only in the empty external output directory. The crate depends on the baseline's `ck-core`, `ck-wire` and `ck-translate` paths and patches gjson to baseline `vendor/gjson`. It copies the original Cargo.lock, updates the external lock's root/workspace membership offline, then builds `--offline --locked --release`. It does not edit baseline production sources or workspace manifests. Retain original/resolved locks, source-file checksums, compiler/profile/features/flags, build logs, binary checksum and `provenance.json` beside `old-codec-harness` and its `baseline-harness` launcher.

`--run` requires private cgroup v2 accounting and writes detailed per-workload counters/smaps/charges into `CODEC_EVIDENCE_DIR`; the driver supplies a separate directory per side/round. The launcher extracts each workload into a temporary execution manifest, records both original and execution manifest checksums, and returns records bound to the unchanged original manifest. Direct invocations default to a unique evidence directory beside the built binary. On 2026-11-14 a round over all 371 workloads exited 0 with a valid record for each, so the baseline's error paths do not abort the corpus. A 550-second launcher deadline leaves cleanup time before the driver's 600-second timeout. Keep built artifacts separate from the captured baseline. No old/new performance acceptance is possible until the new harness implements the same external work and accounting contract.

## Build the new harness

From a checkout whose harness inputs are committed, run:

```sh
bench/codec/build-new.sh "$SOURCE" "$ARTIFACTS/new-build"
"$ARTIFACTS/new-build/new-harness" --describe
```

The builder refuses uncommitted changes to its inputs (`crates/ck-stream`, the S1 codec modules, `Cargo.toml`, `Cargo.lock` and the harness files) because the revision would not name the measured code. It refuses the baseline revision. It generates a standalone crate in the empty external output directory that includes the codec modules by path, as the codec integration tests do, and enables `ck-stream`'s `bench-hooks` feature for the metadata metric. Build variables and defaults match `build-baseline.sh`, with `CARGO_TARGET_DIR=/scratch/ssd/codecbench-target`. `CODEC_POOL_DIRECTORY` (default `/scratch/ram/codecbench-pool`) must be on tmpfs; it is the reported `storage_directory`.

The new harness runs each request row through the client protocol's request ingress and the upstream protocol's request egress, and each response row through the upstream response ingress and the client response egress, over one ck-stream store per worker process. Each operation uses a fresh writer and ends it with `Terminal::Gone`, the release-at-completion retention. Its launcher reuses `baseline_launcher.worker`, so cgroup accounting is identical. The corpus keeps Go-era input conventions, and the harness adapts only these:

- A request object without `model` gets the fixture's `model` as its first member; the old pair received it in `RequestCtx`.
- Each recorded SSE line gets its line terminator, and every line except `event:`, `id:` and `retry:` also ends its event. A `non_stream` body recorded as SSE lines is read by streaming upstream ingress and buffered client egress.
- A refused request emits `REFUSED:<error>`, as the baseline does; a buffered response or count that failed without output emits `REFUSED:boundary`.

A workload naming a protocol without a merged codec exits nonzero. To add one, extend `Protocol` and its role constructors in `new/protocols.rs` and `CODEC_MODULES` in `build_new.py`.

Until every protocol is covered, `partial.py --protocols openai,claude OLD NEW OUT` runs the gate's own manifest, describe checks, noise band, rounds and thresholds over the workloads whose client and upstream are both listed. Its summary is marked `acceptance_evidence: false`; it never replaces the gate. `report.py OUT` renders any gate or partial summary as Markdown tables of every metric.

## Run synthetic controls

Choose a nonexistent external evidence directory, then run from the repository root:

```sh
CODEC_TEST_ARTIFACTS="$ARTIFACTS/selftest" \
  flock /tmp/cooker-bench.lock taskset -c 0-7,16-23 \
  python3 -I -B bench/codec/test_gate.py
shellcheck bench/codec-gate.sh bench/codec/build-baseline.sh
```

The controls exercise the actual shell entrypoint and complete required matrix: exact-boundary pass with outliers, CPU/throughput/memory regression, a new harness that emits no output, a missing, stale, other-build, other-host or malformed noise band (including a stored allowance, extra or missing workloads and float samples), allowances recomputed from median-of-three differences, a constant workload beside a noisy one, a 1,000,000-byte regression on a zero-variance workload, a calibration with the synthetic harness, missing output, absent old executable, identical executable content, identical source identity and a new harness at the baseline revision. Focused checks cover stream-kind selection, leftover-cgroup diagnostics, each metric's direction, 0% memory/storage limits including allocated bytes, counters inside and beyond the noise band, heap metrics that ignore the band, zero-old values, invalid numbers, missing metrics, wrong units, wrong manifest/round and duplicate/unknown workload IDs. `fake_harness.py` is a synthetic control only, not a production or baseline harness.
