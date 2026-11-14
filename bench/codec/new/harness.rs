//! New boundary-codec workload harness under bench/codec/CONTRACT.md. Instrumentation
//! matches `baseline.rs`; the work runs through the S1 codecs and one ck-stream pool.
use crate::ops::{Chunk, Prepared, Run, Samples, operation};
use crate::protocols::Protocol;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::{alloc::{GlobalAlloc, Layout, System}, collections::BTreeMap, fs,
    path::{Path, PathBuf}, sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed}};

struct Allocator;
pub static LIVE: AtomicU64 = AtomicU64::new(0);
pub static PEAK: AtomicU64 = AtomicU64::new(0);
pub static ALLOCS: AtomicU64 = AtomicU64::new(0);
pub static BYTES: AtomicU64 = AtomicU64::new(0);
pub static COUNT: AtomicBool = AtomicBool::new(false);
pub static TRACK: AtomicBool = AtomicBool::new(false);
static CLOCK_OVERHEAD: AtomicU64 = AtomicU64::new(0);
fn allocated(size: usize) {
    let live = LIVE.fetch_add(size as u64, Relaxed) + size as u64;
    if TRACK.load(Relaxed) { PEAK.fetch_max(live, Relaxed); }
    if COUNT.load(Relaxed) { ALLOCS.fetch_add(1, Relaxed); BYTES.fetch_add(size as u64, Relaxed); }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() { allocated(layout.size()); } p
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() { allocated(layout.size()); } p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Relaxed);
        unsafe { System.dealloc(p, layout) };
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(p, layout, size) };
        if !out.is_null() { LIVE.fetch_sub(layout.size() as u64, Relaxed); allocated(size); } out
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

pub type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;
/// Process CPU clock: includes the pool's maintenance thread, like any deep-stack worker.
pub fn cpu() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    assert_eq!(unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) }, 0);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
/// CPU time since `start`, less the calibrated cost of one empty clock pair.
pub fn cpu_since(start: u64) -> u64 { (cpu() - start).saturating_sub(CLOCK_OVERHEAD.load(Relaxed)) }
/// Median of 1,001 empty `cpu()` pairs; the clock is a syscall, not vDSO.
fn calibrate_clock() -> u64 {
    let mut samples: Vec<u64> = (0..1001).map(|_| { let start = cpu(); cpu() - start }).collect();
    samples.sort_unstable(); samples[500]
}
fn faults() -> Result<(u64, u64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let usage = unsafe { usage.assume_init() };
    Ok((usage.ru_minflt as u64, usage.ru_majflt as u64))
}
fn smaps() -> Result<BTreeMap<String, u64>> {
    let text = fs::read_to_string("/proc/self/smaps_rollup")?;
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let key = fields.next().unwrap_or("").trim_end_matches(':');
        if let (Some(number), Some("kB")) = (fields.next(), fields.next()) {
            values.insert(key.to_owned(), number.parse::<u64>()? * 1024);
        }
    }
    for key in ["Rss", "Pss", "Anonymous", "Pss_Anon", "Pss_File", "Pss_Shmem"] {
        if !values.contains_key(key) { return Err(format!("smaps_rollup missing {key}").into()); }
    }
    Ok(values)
}
fn cg_value(path: &Path, name: &str) -> Result<u64> {
    Ok(fs::read_to_string(path.join(name))?.trim().parse()?)
}
fn cgroup() -> Result<PathBuf> {
    let path = PathBuf::from(std::env::var("CODEC_PRIVATE_CGROUP")?);
    if path.parent() != Some(Path::new("/sys/fs/cgroup"))
        || !path.file_name().and_then(|s| s.to_str()).is_some_and(|s| s.starts_with("cooker-codec-")) {
        return Err("private cooker-codec cgroup required; invoke new-harness launcher".into());
    }
    let membership = fs::read_to_string("/proc/self/cgroup")?;
    let actual = Path::new("/sys/fs/cgroup").join(membership.trim().strip_prefix("0::/")
        .ok_or("unified cgroup membership required")?);
    if actual != path { return Err("process is not in the declared private cgroup".into()); }
    let members = fs::read_to_string(path.join("cgroup.procs"))?;
    if members.lines().any(|pid| pid.parse::<u32>().ok() != Some(std::process::id())) {
        return Err("private cgroup contains unrelated processes".into());
    }
    cg_value(&path, "memory.current")?;
    cg_value(&path, "memory.peak")?;
    Ok(path)
}
fn percentile(values: &mut [u64], percent: usize) -> u64 {
    if values.is_empty() { return 0; }
    values.sort_unstable(); values[(values.len() * percent).div_ceil(100).saturating_sub(1)]
}
fn protocol(row: &Value, field: &str) -> Result<Protocol> {
    let name = row[field].as_str().ok_or("missing protocol")?;
    Protocol::parse(name).ok_or_else(|| format!(
        "{field} {name} has no merged S1 codec (supported: {:?}); refusing to invent a measurement",
        Protocol::SUPPORTED).into())
}

/// Warm each case, then size the latency vectors so measured pushes never reallocate.
fn warm(cases: &[Prepared], run: &Run, warmup: u64, iterations: u64, outputs: &mut Vec<Chunk>) -> Result<Samples> {
    let mut output_capacity = 0;
    for case in cases {
        let mut observed = 0;
        for _ in 0..warmup.max(1) {
            let mut sample = Samples::default();
            operation(case, run, false, &mut sample, outputs)?;
            observed = observed.max(sample.output_chunks);
        }
        output_capacity += observed as usize * iterations as usize;
    }
    let mut stats = Samples::default();
    let capacity = cases.len() * iterations as usize;
    stats.first.reserve(capacity); stats.completion.reserve(capacity);
    stats.inter.reserve(output_capacity);
    Ok(stats)
}

struct Observed { start: BTreeMap<String, u64>, before: BTreeMap<String, u64>, after: BTreeMap<String, u64>,
    rss: u64, pss: u64, faults: (u64, u64), heap: (u64, u64, u64), charge: (u64, u64, u64) }

fn run_workload(cases: &[Prepared], run: &Run, manifest: &Value, cg: &Path) -> Result<(Samples, Observed)> {
    let warmup = manifest["warmup_iterations"].as_u64().ok_or("missing warmup iterations")?;
    let iterations = manifest["measured_iterations"].as_u64().filter(|n| *n > 0).ok_or("invalid measured iterations")?;
    let mut outputs = Vec::with_capacity(1024);
    // Process baseline before any codec work: RSS/PSS exclude harness startup, pool open and inputs.
    let start_os = smaps()?;
    let mut stats = warm(cases, run, warmup, iterations, &mut outputs)?;
    let before_os = smaps()?; let before_faults = faults()?;
    let before_charge = cg_value(cg, "memory.current")?;
    let mut charge_peak_file = fs::OpenOptions::new().read(true).write(true).open(cg.join("memory.peak"))?;
    charge_peak_file.write_all(b"0")?;
    let before_heap = LIVE.load(Relaxed);
    let mut peak_heap = 0; let mut rss = before_os["Rss"]; let mut pss = before_os["Pss"];
    for case in cases {
        for iteration in 0..iterations {
            operation(case, run, true, &mut stats, &mut outputs)?;
            peak_heap = peak_heap.max(PEAK.load(Relaxed).saturating_sub(before_heap));
            if iteration == 0 {
                let sample = smaps()?; rss = rss.max(sample["Rss"]); pss = pss.max(sample["Pss"]);
            }
        }
    }
    if stats.events == 0 { return Err("selected event kind executed no events".into()); }
    let after_heap = LIVE.load(Relaxed);
    let faults = faults()?; let after_os = smaps()?;
    let charge = cg_value(cg, "memory.current")?;
    charge_peak_file.seek(SeekFrom::Start(0))?;
    let mut peak_text = String::new(); charge_peak_file.read_to_string(&mut peak_text)?;
    Ok((stats, Observed { start: start_os, before: before_os, after: after_os, rss, pss,
        faults: (faults.0 - before_faults.0, faults.1 - before_faults.1),
        heap: (before_heap, after_heap, peak_heap), charge: (before_charge, charge, peak_text.trim().parse()?) }))
}

fn values(s: &mut Samples, o: &Observed, store: &ck_stream::Store) -> BTreeMap<String, f64> {
    let events = s.events as f64;
    let backed = store.backed_frames();
    let mut values = BTreeMap::<String, f64>::new();
    for (key, value) in [
        ("cpu_ns_per_event", s.event_cpu as f64 / events),
        ("allocations_per_event", s.allocations as f64 / events),
        ("allocated_bytes_per_event", s.allocated as f64 / events),
        ("encoded_bytes_per_event", s.encoded as f64 / events),
        ("decoded_bytes_per_event", s.decoded as f64 / events),
        ("cpu_ns_per_completion", s.completion_cpu as f64 / s.completions as f64),
        ("throughput", events * 1e9 / s.wall.max(1) as f64),
        ("heap_peak", o.heap.2 as f64), ("heap_retained", o.heap.1.saturating_sub(o.heap.0) as f64),
        ("rss", o.rss.max(o.after["Rss"]).saturating_sub(o.start["Rss"]) as f64),
        ("pss", o.pss.max(o.after["Pss"]).saturating_sub(o.start["Pss"]) as f64),
        ("output_chunks", s.output_chunks as f64), ("no_output_completions", s.no_output as f64),
        ("rss_anon", o.after["Anonymous"] as f64), ("rss_shmem", o.after["Pss_Shmem"] as f64),
        ("mapped_resident", (o.after["Pss_File"] + o.after["Pss_Shmem"]) as f64),
        // Largest per-operation stream metadata charge and used frames, sampled before release;
        // backed frames are the pool's tmpfs allocation after the workload.
        ("metadata", s.metadata as f64), ("tmpfs_charge", (backed as usize * ck_stream::FRAME_BYTES) as f64),
        ("pool_reserved", backed as f64), ("pool_used", s.pool_used as f64),
        ("cgroup_charge", o.charge.1 as f64), ("total_charged", o.charge.2 as f64),
        ("minor_faults", o.faults.0 as f64), ("major_faults", o.faults.1 as f64),
    ] { values.insert(key.into(), value); }
    for (name, samples) in [("first_deliverable", &mut s.first), ("inter_event", &mut s.inter),
                            ("completion", &mut s.completion)] {
        for p in [50, 95, 99] { values.insert(format!("{name}_p{p}"), percentile(samples, p) as f64); }
    }
    values
}

fn measure(row: &Value, manifest: &Value, cg: &Path, store: &ck_stream::Store) -> Result<(Value, Value)> {
    let (client, upstream) = (protocol(row, "client_protocol")?, protocol(row, "upstream_protocol")?);
    let selected = row["event_kinds"][0].as_str().ok_or("missing selected kind")?;
    let cases: Vec<Prepared> = row["cases"].as_array().ok_or("missing cases")?.iter()
        .map(|case| Prepared::new(&case["input"], selected)).collect::<Result<_>>()?;
    if cases.is_empty() { return Err("empty workload".into()); }
    let limit = store.limits().max_event_bytes as u64;
    let run = Run { store, client, upstream, selected, limit };
    let (mut stats, observed) = run_workload(&cases, &run, manifest, cg)?;
    let mut values = values(&mut stats, &observed, store);
    let mut metrics = serde_json::Map::new();
    for (name, spec) in manifest["metrics"].as_object().ok_or("missing metric definitions")? {
        let value = values.remove(name).ok_or_else(|| format!("unsupported metric {name}"))?;
        if !value.is_finite() || value < 0.0 { return Err(format!("invalid measurement {name}").into()); }
        metrics.insert(name.clone(), json!({"unit": spec["unit"], "value": value}));
    }
    let o = &observed;
    let evidence = json!({"workload_id": row["id"], "events": stats.events, "completions": stats.completions,
        "output_chunks": stats.output_chunks, "consumed_bytes": stats.consumed_bytes,
        "cpu_ns": stats.event_cpu, "allocations": stats.allocations, "allocated_bytes": stats.allocated,
        "encoded_bytes": stats.encoded, "decoded_bytes": stats.decoded, "wall_ns": stats.wall,
        "refused_operations": stats.refused, "ingress_failures": stats.ingress_errors,
        "egress_failures": stats.egress_errors,
        "heap_before": o.heap.0, "heap_after": o.heap.1, "smaps_start": o.start, "smaps_before": o.before,
        "smaps_after": o.after, "private_cgroup": cg, "charge_before": o.charge.0,
        "charge_after": o.charge.1, "charge_peak": o.charge.2, "pool": {
            "capacity_frames": store.capacity_frames(), "backed_frames": store.backed_frames(),
            "used_frames_after": store.used_frames()},
        "no_output_completions": stats.no_output, "cpu_clock_overhead_ns": CLOCK_OVERHEAD.load(Relaxed)});
    Ok((Value::Object(metrics), evidence))
}

fn open_store() -> Result<ck_stream::Store> {
    let mut config = ck_stream::Config::new(PathBuf::from(std::env::var("CODEC_POOL_DIRECTORY")?));
    config.workers = 1;
    Ok(ck_stream::Store::open(config)?)
}

pub fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let provenance = fs::read_to_string(std::env::var("CODEC_NEW_PROVENANCE")?)?;
    if args.get(1).map(String::as_str) == Some("--describe") { println!("{provenance}"); return Ok(()); }
    if args.len() != 5 || args[1] != "--run" || args[3] != "--round" {
        return Err("usage: new-harness --describe | --run MANIFEST --round N".into());
    }
    let cg = cgroup()?;
    CLOCK_OVERHEAD.store(calibrate_clock(), Relaxed);
    let raw = fs::read(&args[2])?;
    let execution_checksum = format!("{:x}", Sha256::digest(&raw));
    let checksum = std::env::var("CODEC_MANIFEST_SHA256").unwrap_or(execution_checksum.clone());
    let manifest: Value = serde_json::from_slice(&raw)?;
    let round: u64 = args[4].parse()?;
    let evidence = PathBuf::from(std::env::var("CODEC_EVIDENCE_DIR")?);
    fs::create_dir_all(&evidence)?;
    let mut store = open_store()?;
    for row in manifest["workloads"].as_array().ok_or("missing workload matrix")? {
        let (metrics, mut detail) = measure(row, &manifest, &cg, &store)?;
        detail["execution_manifest_sha256"] = execution_checksum.clone().into();
        detail["original_manifest_sha256"] = checksum.clone().into();
        fs::write(evidence.join(format!("{}.json", row["id"].as_str().ok_or("missing id")?)), serde_json::to_vec_pretty(&detail)?)?;
        println!("{}", json!({"schema_version": 1, "workload_id": row["id"], "manifest_sha256": checksum,
                            "round": round, "metrics": metrics}));
    }
    store.shutdown()?;
    Ok(())
}
