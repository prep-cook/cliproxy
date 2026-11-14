# Production stream storage gate

This standalone benchmark compares the production `ck-stream` public API with benchmark-only heap storage. It implements every G-storage row from `docs/INTERNAL-STREAM.md`, §9.3 and the S1 plan, §7. It does not contain a pool allocator.

## Run the gate

From the repository root on an otherwise idle Linux host:

```sh
bench/stream-gate.sh "$TMPFS" "$OUT"
```

`TMPFS` must be an existing private tmpfs directory. `OUT` must be an empty external evidence directory; the runner refuses a repository path or existing evidence before building. The runner creates disposable private storage directories beneath `TMPFS` and removes them after each sample. The ENOSPC row mounts a private 64 MiB tmpfs with `unshare -Urm`; user and mount namespaces and the `mount` command must work. It creates the capped production pool before filling a second file on that same mount, then requires an actual growth error with errno 28 and a visible storage-exhausted reader terminal. The process must execute the post-error checks and emit its survivor PID.

The script builds offline with the checked-in lockfile in release mode, respects `CARGO_TARGET_DIR` (default `target/stream`) and defaults to two build jobs. Cached dependencies are required. No production quotas are increased. The total-cap row requires the derived default to be 85,899 frames; a smaller startup cap fails the row rather than being overridden. A tmpfs of at least 8 GiB and sufficient free tmpfs/cgroup memory are needed to reach that default.

The gate returns nonzero for build failures, missing samples, nonzero workload exits, missing metrics, timeouts, namespace failures and regressions. It attempts all rows after individual workload failures. A build/preflight failure writes every row as failed without claiming a measurement.

Output files:

- `raw.jsonl`: each measured subprocess result, errors, exit status, pair order, command and host load. Preflight failures are explicitly unmeasured.
- `medians.json`: metric minima, P25, median, P75 and maxima, with each row's failure reasons. Comparison rows also report `minor_faults` per backend as a diagnostic.
- `provenance.json`: revision, source/binary hashes, compiler, build flags, kernel, architecture, page size, affinity, filesystem and initial host load.
- `RESULTS.md`: row verdicts. `build.log` and per-sample stderr files retain diagnostics.

## Workload and comparison contracts

Every general comparison uses seven pool/heap pairs, reversing the first backend on successive pairs. Each sample is a fresh process. Before the measured pass of a small, concurrent, throughput or latency row, both backends run the identical workload once untimed, expire it and receive one decay tick. The measured pass therefore starts with warm code, runtime and allocator metadata and with idle storage on both sides: freed heap memory is whatever the system allocator keeps after trimming, and freed pool frames are punched except each free list's warm frames. Neither backend has storage prefaulted for it outside the timer beyond its own idle policy. A sample without `"warmup": true` fails its row. `minor_faults` counts process-wide minor page faults during the measured pass. Memory, total-cap and ENOSPC rows and the startup-reserve diagnostics run cold. The 4-reader, 64-byte, 1-MiB throughput row uses 101 pairs. Its pool median must reach heap P25; both IQRs and the median difference are reported. General lower-is-better metrics fail above heap's maximum; higher-is-better metrics fail below heap's minimum.

Small requests run 20,000 create/write/read/drop operations at 1/64 tasks. Responses remain cached after the client drops. To avoid treating 4,096 header slots as an unlimited request count, batches of at most 2,048 requests expire through `ManualClock` outside the timed region. The heap comparator uses the same batch lifecycle. Concurrent rows run 256/1,024 streams of 256 KiB, yielding every 16 appends. Throughput yields every 256 KiB. The 49,152-byte throughput and memory appends start on frame boundaries in both backends; 64/256-byte appends are packed.

Latency rows publish 20,000 timestamped 64-byte records, yielding between arrivals until 20 microseconds have elapsed. Readers verify record boundaries and report append-to-observe p50/p99 and TTFB p50/p99. Throughput, small and concurrent payload byte `i` of stream `id` is `(i + 97 × id) mod 251`; every reader compares every byte at its logical offset, so a stale, shifted or cross-stream frame fails. Both backends use 50,000-byte frames, complete-record publication and notification-before-recheck waiting.

The heap comparator follows the S0 heap that produced the §9.3 "Measured" column. Frames are uninitialised 50,000-byte allocations from the system allocator. The writer copies without a lock and publishes each record with a Release store of the record count; readers load it with Acquire. Locks cover only frame-table growth, the run-index update per append and the frame-pointer lookup per visit, never a payload copy or a reader's visitor. Records stay retained until explicit expiry. It is not an implementation of the production allocator, cache-pressure policy or metadata ceiling.

Memory rows use an eight-worker runtime and eight producer tasks. They pin all records while holding 100 × 1 MiB or 10 × 64 MiB, advance `ManualClock` to expire the pool, release leases and sample after one full decay interval. A sample later than two intervals fails. Decay eligibility counts ticks, not wall time: a frame is punched by the first tick after one that already saw it free. The row ticks explicitly right after the release and again after one second, so the sample always follows an eligible pass; the background one-second tick can only punch earlier. A 2026-11-14 check (three pool samples) measured 2.2 MiB `VmRSS` and 1.2 MiB tmpfs over baseline against the 7.05 MiB allowance. The pool stays open during the sample. Each pool sample must meet the baseline + 4 MiB + 3,200,000-byte RSS/tmpfs allowance; the pool's post-decay RSS median must not exceed heap's. Raw and median reports include RssAnon, RssShmem, RssFile and cgroup charges. Shared-host tmpfs/cgroup measurements include other processes.

The total-cap row acquires an active lease after every frame append and retains all leases until all 17 streams have been attempted. Streams 1–16 must reach exactly 5,368 frames; stream 17 must reach exactly the remaining 11 (85,899 − 16 × 5,368). Fewer would mean a leaked or miscounted frame. Every refusal must expose a storage-exhausted terminal.

## Default metadata constraint

The production default metadata limit remains 1 MiB. Consecutive records with identical length, stride, type and correlation share a bounded run index; heterogeneous records still consume individual entries. The heap comparator keeps the same kind of run index: consecutive records of one length at one stride share an entry. Both backends retain identical 50,000-byte payload frames and complete-record semantics. If either workload fails, the benchmark records the byte offset and counters, returns nonzero and still attempts subsequent rows. It never raises `max_metadata_bytes`, releases response prefixes or substitutes another pool to make a row run.

## Startup reserve diagnostics

The release driver accepts `ck-stream-bench pool|heap ROW DIRECTORY [RESERVE_BYTES [IDLE_SECONDS]]`. The optional byte value configures the production store's deploy-only startup reserve; the heap comparator ignores it. Omitting it uses the production default. No frame, stream or metadata quota is raised.

With an explicit reserve, the driver records pool-open time and pre-open, initial-idle and post-expiry/decay RSS, anonymous/shmem and tmpfs/cgroup snapshots. It runs the first burst, expires retained streams, releases their frames through normal ownership and samples decay. With a positive idle interval it then repeats the same workload on the same pool; `metrics` describes that post-decay burst and `startup.first_burst` preserves the cold result. Use two idle seconds to cover a full eligibility interval. Whole-process timing and peak RSS belong in the parent runner and include diagnostic sampling and any idle wait; they are not startup-only measurements.

These diagnostics keep the ordinary memory row's pre-open baseline and report the full RSS/tmpfs charge, including the idle reserve. Under the maintainer's 2026-11-14 interpretation of startup option B, explicit-reserve comparisons add the configured reserve to the baseline allowance and heap RSS comparator (§9.3). The ordinary reserve-zero gate is unchanged.

## Cache admission contention

The cache index uses the same `parking_lot` mutex as stream metadata. Stream admission and insertion into the lookup map and last-read queue remain serialized. Retention, publication, quotas and the default startup reserve are unchanged.

In seven interleaved pairs on 2026-11-14, with a 256 MiB reserve and eight workers pinned to one CCD, replacing the cache mutex reduced concurrent-256/1024 TTFB p99 medians from 21.890/19.470 to 17.820/15.620 microseconds. These are affected-row measurements, not complete storage-gate acceptance. Moving stream construction outside the lock reduced reserved-pool tails further but worsened the zero-reserve concurrent-256 tail by 36% in a separate seven-pair comparison, so that experiment was rejected. The small-1 and periodic-latency regressions remain open; the default reserve remains zero.

## Pool against heap

The gate still fails after the pool work on `perf/s1-pool-vs-heap`. Seven rows fail: small-1, small-64, concurrent-256, concurrent-1024, throughput-4-49152-1, latency-1 and latency-4. Medians come from one full gate on each tree on 2026-11-14, pinned to cores 0-7 and 16-23. Host load was 7 for the base run and 10-11 for the branch run. "Base" is `feat/s1-stream-rewrite` at 67219e95; "pool" is the branch head; "heap" is the branch run's comparator median and range.

| Row | Metric | Base | Pool | Heap | Pool status |
| --- | --- | --- | --- | --- | --- |
| small-1 | req/s | 243,900 | 379,900 | 416,500 [412,200..436,700] | fail |
| small-1 | p50 / p99 ns | 3,300 / 11,010 | 2,440 / 4,410 | 2,220 / 3,930 | fail |
| small-64 | req/s | 651,300 | 1,797,000 | 2,445,000 [2,269,000..2,479,000] | fail |
| small-64 | p50 / p99 ns | 4,620 / 84,090 | 3,640 / 8,380 | 2,660 / 5,140 | fail |
| concurrent-256 | wall ms / TTFB p99 us | 15.2 / 137.3 | 6.60 / 8.7 | 5.04 / 3.0 | fail |
| concurrent-1024 | wall ms / TTFB p99 us | 65.0 / 138.2 | 25.5 / 12.0 | 21.3 / 3.1 | fail |
| throughput-1-49152-1 | MB/s | 9,062 | 27,220 | 27,730 [19,670..32,480] | pass |
| throughput-4-49152-1 | MB/s | 9,154 | 14,650 | 21,500 [19,030..23,310] | fail |
| throughput-4-64-1 | MB/s | 62.5 | 109.7 | 63.8 [61.4..84.8] | pass |
| other throughput rows | MB/s | | 1.16-1.80x heap | | pass |
| latency-1 | p50 ns / TTFB p50 ns | 1,840 / 4,450 | 1,850 / 4,140 | 1,800 [1,790..1,820] / 3,550 | fail |
| latency-4 | TTFB p50 ns | 6,910 | 7,719 | 5,330 [4,100..7,270] | fail |
| memory, totalcap, enospc | | pass | pass | | pass |

The remaining gaps and their measured causes:

- Concurrent rows and the first batch of each small pass refault punched frames. After warmup, expiry and two decay ticks the pool keeps 8 warm frames per free list and punches the rest; glibc keeps the freed heap memory in its arenas. concurrent-256 takes about 15,800 minor faults against the heap's 2,100. Each refault is a shared-file tmpfs fault on one inode, where the heap takes a private anonymous fault. A larger warm budget would break the post-decay memory rows.
- small-1 costs the same user-space cycles on both backends: per request, pool 12.8k and heap 12.9k cycles; pool 33.2k and heap 20.1k instructions. The remaining 9% lies outside user space. Each pass takes 3,024 minor faults on the pool and 400-4,000 on the heap. The kernel cause was not isolated.
- small-64 still loses about 27% after per-frame counters stopped sharing cache lines. Its cause was not isolated.
- latency-1 misses by 30-50 ns at p50, within the run-to-run spread of a 7-pair A/B on the same host. In a 7-pair A/B at load 8-10, latency-4 TTFB p50 stays inside the heap range (6,409 against 5,160..7,690). The branch gate ran at load 10-11.

A decay budget counted in populated bytes rather than frames was measured and rejected: in a 7-pair A/B it left the faults (3,024) and req/s unchanged.

## Narrow verification

```sh
bash -n bench/stream-gate.sh
python3 -I bench/stream/gate.py --self-test
CARGO_TARGET_DIR=/scratch/ssd/s1b-target cargo build --release --locked --offline --manifest-path bench/stream/Cargo.toml
/scratch/ssd/s1b-target/release/ck-stream-bench pool smoke "$TMPFS"
/scratch/ssd/s1b-target/release/ck-stream-bench heap smoke "$TMPFS"
```

The self-test plants a regression, the stricter P25 failure, failed subprocess evidence, missing samples/metrics, samples without warmup, a short stream 17 in the total-cap row and repository or nonempty output directories. `cargo test --release --manifest-path bench/stream/Cargo.toml` proves the payload check rejects stale, shifted and cross-stream bytes and that the heap comparator delivers every record byte, including to four readers polling while the writer publishes and grows the frame table. These checks do not establish production performance. Run the full gate only after integration and without competing compilation.
