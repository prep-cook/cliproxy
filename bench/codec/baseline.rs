//! Baseline-only external pair replay; no new codec dependencies or output oracles.
use ck_core::format::Format;
use ck_translate::{RequestCtx, ResponseCtx, go_stream, pair, translate_request, translate_token_count};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::{alloc::{GlobalAlloc, Layout, System}, collections::BTreeMap, fs, hint::black_box,
    path::{Path, PathBuf}, sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed}, time::Instant};

struct Allocator;
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static COUNT: AtomicBool = AtomicBool::new(false);
static TRACK: AtomicBool = AtomicBool::new(false);
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

type Result<T, E = Box<dyn std::error::Error>> = std::result::Result<T, E>;
fn cpu() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    assert_eq!(unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) }, 0);
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}
/// CPU time since `start`, less the calibrated cost of one empty clock pair.
fn cpu_since(start: u64) -> u64 { (cpu() - start).saturating_sub(CLOCK_OVERHEAD.load(Relaxed)) }
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
        return Err("private cooker-codec cgroup required; invoke baseline-harness launcher".into());
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
fn bytes(v: &Value, raw: bool) -> Vec<u8> {
    let s = v.as_str().unwrap_or_default();
    if raw { s.chars().map(|c| c as u32 as u8).collect() } else { s.as_bytes().to_vec() }
}
fn kind(line: &str, selected: &str) -> bool {
    if let Some(name) = line.strip_prefix("event:") {
        return name.trim() == selected || (name.trim().is_empty() && selected == "unnamed");
    }
    let payload = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
    if payload == "[DONE]" { return selected == "[DONE]"; }
    match serde_json::from_str::<Value>(payload) {
        Ok(value) => value.get("type").or_else(|| value.get("object"))
            .and_then(Value::as_str).unwrap_or("json") == selected,
        Err(_) => selected == "framing-or-fragment",
    }
}

#[cfg(test)]
mod selection_tests {
    use super::kind;

    #[test]
    fn headers_are_not_payload_fragments() {
        assert!(kind("event: message", "message"));
        assert!(kind("event:", "unnamed"));
        assert!(!kind("event: message", "framing-or-fragment"));
        assert!(!kind("event:", "framing-or-fragment"));
        assert!(kind("data: {", "framing-or-fragment"));
        assert!(kind("data: [DONE]", "[DONE]"));
        assert!(kind(r#"data: {"type":"message"}"#, "message"));
    }
}
struct Prepared {
    model: String, mode: String, input: Vec<u8>, original: Vec<u8>, translated: Vec<u8>,
    lines: Vec<Vec<u8>>, selected: Vec<bool>, stream: bool, finalize: bool, count: i64,
    info: Option<ck_core::registry::ModelInfo>,
}
impl Prepared {
    fn new(f: &Value, selected_kind: &str) -> Result<Self> {
        let raw = f["bytes"].as_bool().unwrap_or(false);
        let model = f["model"].as_str().ok_or("missing model")?.to_owned();
        let mode = f["path"].as_str().ok_or("missing mode")?.to_owned();
        let mut lines = Vec::new(); let mut selected = Vec::new();
        if mode == "stream" {
            for line in f["lines"].as_array().ok_or("missing stream lines")? {
                selected.push(kind(line.as_str().ok_or("non-string stream line")?, selected_kind));
                lines.push(bytes(line, raw));
            }
        }
        let info = if mode == "request_envelope" {
            Some(ck_core::registry::ModelInfo::from_raw(json!({"id": model,
                "native_capabilities": {"web_search": true}}).as_object().unwrap().clone())?)
        } else { None };
        Ok(Self { model, mode, input: bytes(&f["input"], raw), original: bytes(&f["original"], raw),
            translated: bytes(&f["translated"], raw), lines, selected,
            stream: f["stream"].as_bool().unwrap_or(false), finalize: f["finalize"].as_bool().unwrap_or(false),
            count: f["count"].as_i64().unwrap_or(0), info })
    }
    fn response(&self) -> ResponseCtx<'_> { ResponseCtx { model: &self.model,
        original_request: &self.original, translated_request: &self.translated, tool_search: false } }
    fn single(&self, client: Format, upstream: Format) -> Result<Vec<u8>> {
        let ctx = RequestCtx { model: &self.model, stream: self.stream, tool_search: false };
        let result = match self.mode.as_str() {
            "request" => translate_request(client, upstream, &ctx, &self.input),
            "request_envelope" => ck_translate::translate_request_envelope(client, upstream, &ctx,
                &self.input, self.info.as_ref()),
            "request_compat" => match (client, upstream) {
                (Format::OpenAI, Format::Claude) => ck_translate::openai_to_claude_with_compat(&ctx, &self.input),
                (Format::OpenAIResponse, Format::Claude) => ck_translate::responses_to_claude_with_compat(&ctx, &self.input),
                (Format::Claude, Format::OpenAI) => ck_translate::claude_to_openai_with_compat(&ctx, &self.input),
                (Format::Claude, Format::Gemini) => ck_translate::claude_to_gemini_with_compat(&ctx, &self.input),
                (Format::Claude, Format::Codex) => ck_translate::claude_to_codex_with_compat(&ctx, &self.input),
                (Format::Claude, Format::Interactions) => ck_translate::claude_to_interactions_with_compat(&ctx, &self.input),
                _ => return Err("unsupported baseline compat pair".into()),
            },
            "non_stream" => return match (pair(client, upstream).ok_or("unregistered pair")?.non_stream)(
                &self.response(), &self.input) {
                Ok(out) => Ok(out),
                Err(error) if error.0 == ck_translate::APPLY_PATCH_UPSTREAM_ERROR => Ok(Vec::new()),
                Err(error) => Err(error.into()),
            },
            "token_count" => return Ok(translate_token_count(client, upstream, self.count, &self.input)),
            _ => return Err("unknown single-operation mode".into()),
        };
        Ok(result.unwrap_or_else(|e| format!("REFUSED:{}", e.0).into_bytes()))
    }
}
#[derive(Default)]
struct Samples {
    event_cpu: u64, completion_cpu: u64, allocations: u64, allocated: u64, events: u64,
    encoded: u64, decoded: u64, wall: u64, completions: u64, output_chunks: u64,
    first: Vec<u64>, inter: Vec<u64>, completion: Vec<u64>, consumed_bytes: u64, no_output: u64,
}
impl Samples {
    fn consume(&mut self, outputs: &[Vec<u8>], start: Instant, first: &mut Option<u64>, previous: &mut Option<u64>) {
        let elapsed = start.elapsed().as_nanos() as u64;
        for out in outputs.iter().filter(|out| !out.is_empty()) {
            if first.is_none() { *first = Some(elapsed); }
            if let Some(last) = previous.replace(elapsed) { self.inter.push(elapsed - last); }
            self.output_chunks += 1;
            self.consumed_bytes += black_box(out).len() as u64;
        }
    }
}
fn operation(prepared: &Prepared, client: Format, upstream: Format, measured: bool,
             stats: &mut Samples, selected_kind: &str) -> Result<()> {
    let mut first = None; let mut previous = None;
    let start = Instant::now(); let mut completion_cpu = 0;
    let before_live = LIVE.load(Relaxed);
    PEAK.store(before_live, Relaxed); TRACK.store(measured, Relaxed);
    if prepared.mode == "stream" {
        // Stream construction and teardown are part of CPU/completion and heap peaks.
        let construct_start = cpu();
        let mut stream = go_stream(client, upstream).ok_or("unregistered stream")?(&prepared.response());
        completion_cpu += cpu_since(construct_start);
        for (line, selected) in prepared.lines.iter().zip(&prepared.selected) {
            let before_allocs = ALLOCS.load(Relaxed); let before_bytes = BYTES.load(Relaxed);
            COUNT.store(measured && *selected, Relaxed);
            let event_start = cpu();
            let mut outputs = stream.line(line)?;
            for chunk in &mut outputs {
                if let Some(frame) = ck_translate::stream::frame(client, chunk) { *chunk = frame; }
                else { chunk.clear(); }
            }
            let event_cpu = cpu_since(event_start);
            completion_cpu += event_cpu;
            COUNT.store(false, Relaxed); TRACK.store(false, Relaxed);
            if measured {
                if *selected {
                    stats.event_cpu += event_cpu; stats.events += 1;
                    stats.allocations += ALLOCS.load(Relaxed) - before_allocs;
                    stats.allocated += BYTES.load(Relaxed) - before_bytes;
                    stats.decoded += line.len() as u64;
                    stats.encoded += outputs.iter().map(|out| out.len() as u64).sum::<u64>();
                }
                stats.consume(&outputs, start, &mut first, &mut previous);
            } else { stats.output_chunks += outputs.iter().filter(|out| !out.is_empty()).count() as u64; black_box(&outputs); }
            drop(outputs); TRACK.store(measured, Relaxed);
        }
        if prepared.finalize {
            let selected = selected_kind == "finalize";
            let before_allocs = ALLOCS.load(Relaxed); let before_bytes = BYTES.load(Relaxed);
            COUNT.store(measured && selected, Relaxed);
            let finalize_start = cpu();
            let mut outputs = stream.finalize_tool_input();
            for chunk in &mut outputs {
                if let Some(frame) = ck_translate::stream::frame(client, chunk) { *chunk = frame; }
                else { chunk.clear(); }
            }
            let elapsed_cpu = cpu_since(finalize_start);
            completion_cpu += elapsed_cpu;
            COUNT.store(false, Relaxed); TRACK.store(false, Relaxed);
            if measured {
                if selected {
                    stats.events += 1; stats.event_cpu += elapsed_cpu;
                    stats.allocations += ALLOCS.load(Relaxed) - before_allocs;
                    stats.allocated += BYTES.load(Relaxed) - before_bytes;
                    stats.encoded += outputs.iter().map(|out| out.len() as u64).sum::<u64>();
                }
                stats.consume(&outputs, start, &mut first, &mut previous);
            } else { stats.output_chunks += outputs.iter().filter(|out| !out.is_empty()).count() as u64; black_box(&outputs); }
            drop(outputs); TRACK.store(measured, Relaxed);
        }
        black_box(stream.tool_input_failed());
        let drop_start = cpu(); drop(stream); completion_cpu += cpu_since(drop_start);
        if prepared.lines.is_empty() && selected_kind == "empty-stream" && measured { stats.events += 1; }
    } else {
        let before_allocs = ALLOCS.load(Relaxed); let before_bytes = BYTES.load(Relaxed);
        COUNT.store(measured, Relaxed);
        let event_start = cpu();
        let output = prepared.single(client, upstream)?;
        let event_cpu = cpu_since(event_start);
        completion_cpu += event_cpu;
        COUNT.store(false, Relaxed); TRACK.store(false, Relaxed);
        let outputs = std::slice::from_ref(&output);
        if measured {
            stats.event_cpu += event_cpu; stats.events += 1;
            stats.allocations += ALLOCS.load(Relaxed) - before_allocs;
            stats.allocated += BYTES.load(Relaxed) - before_bytes;
            stats.decoded += prepared.input.len() as u64;
            stats.encoded += outputs.iter().map(|out| out.len() as u64).sum::<u64>();
            stats.consume(&outputs, start, &mut first, &mut previous);
        } else { stats.output_chunks += outputs.iter().filter(|out| !out.is_empty()).count() as u64; black_box(&outputs); }
        drop(output);
    }
    TRACK.store(false, Relaxed);
    let elapsed = start.elapsed().as_nanos() as u64;
    if measured {
        stats.completion_cpu += completion_cpu; stats.wall += elapsed; stats.completions += 1;
        if first.is_none() { stats.no_output += 1; }
        stats.first.push(first.unwrap_or(0)); stats.completion.push(elapsed);
    }
    Ok(())
}
fn percentile(values: &mut [u64], percent: usize) -> u64 {
    if values.is_empty() { return 0; }
    values.sort_unstable(); values[(values.len() * percent).div_ceil(100).saturating_sub(1)]
}
fn measure(row: &Value, manifest: &Value, cg: &Path) -> Result<(Value, Value)> {
    let client = Format::parse(row["client_protocol"].as_str().ok_or("missing client protocol")?).ok_or("unknown client")?;
    let upstream = Format::parse(row["upstream_protocol"].as_str().ok_or("missing upstream protocol")?).ok_or("unknown upstream")?;
    let selected = row["event_kinds"][0].as_str().ok_or("missing selected kind")?;
    let cases: Vec<Prepared> = row["cases"].as_array().ok_or("missing cases")?.iter()
        .map(|case| Prepared::new(&case["input"], selected)).collect::<Result<_>>()?;
    if cases.is_empty() { return Err("empty workload".into()); }
    let warmup = manifest["warmup_iterations"].as_u64().ok_or("missing warmup iterations")?;
    let iterations = manifest["measured_iterations"].as_u64().filter(|n| *n > 0).ok_or("invalid measured iterations")?;
    // Process baseline before any codec work: RSS/PSS exclude harness startup and inputs.
    let start_os = smaps()?;
    let mut output_capacity = 0;
    for case in &cases {
        let mut observed = 0;
        for _ in 0..warmup.max(1) {
            let mut sample = Samples::default();
            operation(case, client, upstream, false, &mut sample, selected)?;
            observed = observed.max(sample.output_chunks);
        }
        output_capacity += observed as usize * iterations as usize;
    }
    let mut stats = Samples::default();
    let capacity = cases.len() * iterations as usize;
    stats.first.reserve(capacity); stats.completion.reserve(capacity);
    stats.inter.reserve(output_capacity);
    let before_os = smaps()?; let before_faults = faults()?;
    let before_charge = cg_value(cg, "memory.current")?;
    let mut charge_peak_file = fs::OpenOptions::new().read(true).write(true).open(cg.join("memory.peak"))?;
    charge_peak_file.write_all(b"0")?;
    let before_heap = LIVE.load(Relaxed);
    let mut peak_heap = 0; let mut rss = before_os["Rss"]; let mut pss = before_os["Pss"];
    for case in &cases {
        for iteration in 0..iterations {
            operation(case, client, upstream, true, &mut stats, selected)?;
            peak_heap = peak_heap.max(PEAK.load(Relaxed).saturating_sub(before_heap));
            if iteration == 0 {
                let sample = smaps()?; rss = rss.max(sample["Rss"]); pss = pss.max(sample["Pss"]);
            }
        }
    }
    if stats.events == 0 { return Err("selected event kind executed no events".into()); }
    let after_heap = LIVE.load(Relaxed);
    let after_faults = faults()?; let after_os = smaps()?;
    let charge = cg_value(cg, "memory.current")?;
    charge_peak_file.seek(SeekFrom::Start(0))?;
    let mut peak_text = String::new(); charge_peak_file.read_to_string(&mut peak_text)?;
    let charge_peak: u64 = peak_text.trim().parse()?;
    let mut values = BTreeMap::<String, f64>::new();
    for (key, value) in [
        ("cpu_ns_per_event", stats.event_cpu as f64 / stats.events as f64),
        ("allocations_per_event", stats.allocations as f64 / stats.events as f64),
        ("allocated_bytes_per_event", stats.allocated as f64 / stats.events as f64),
        ("encoded_bytes_per_event", stats.encoded as f64 / stats.events as f64),
        ("decoded_bytes_per_event", stats.decoded as f64 / stats.events as f64),
        ("cpu_ns_per_completion", stats.completion_cpu as f64 / stats.completions as f64),
        ("throughput", stats.events as f64 * 1e9 / stats.wall.max(1) as f64),
        ("heap_peak", peak_heap as f64), ("heap_retained", after_heap.saturating_sub(before_heap) as f64),
        ("rss", rss.max(after_os["Rss"]).saturating_sub(start_os["Rss"]) as f64),
        ("pss", pss.max(after_os["Pss"]).saturating_sub(start_os["Pss"]) as f64),
        ("output_chunks", stats.output_chunks as f64), ("no_output_completions", stats.no_output as f64),
        ("rss_anon", after_os["Anonymous"] as f64), ("rss_shmem", after_os["Pss_Shmem"] as f64),
        ("mapped_resident", (after_os["Pss_File"] + after_os["Pss_Shmem"]) as f64),
        // The old pair backend has no pool, stream metadata store or tmpfs files.
        ("metadata", 0.0), ("tmpfs_charge", 0.0), ("pool_reserved", 0.0), ("pool_used", 0.0),
        ("cgroup_charge", charge as f64), ("total_charged", charge_peak as f64),
        ("minor_faults", (after_faults.0 - before_faults.0) as f64),
        ("major_faults", (after_faults.1 - before_faults.1) as f64),
    ] { values.insert(key.into(), value); }
    for (name, samples) in [("first_deliverable", &mut stats.first), ("inter_event", &mut stats.inter),
                            ("completion", &mut stats.completion)] {
        for p in [50, 95, 99] { values.insert(format!("{name}_p{p}"), percentile(samples, p) as f64); }
    }
    let mut metrics = serde_json::Map::new();
    for (name, spec) in manifest["metrics"].as_object().ok_or("missing metric definitions")? {
        let value = values.remove(name).ok_or_else(|| format!("unsupported metric {name}"))?;
        if !value.is_finite() || value < 0.0 { return Err(format!("invalid measurement {name}").into()); }
        metrics.insert(name.clone(), json!({"unit": spec["unit"], "value": value}));
    }
    let evidence = json!({"workload_id": row["id"], "events": stats.events, "completions": stats.completions,
        "output_chunks": stats.output_chunks, "consumed_bytes": stats.consumed_bytes,
        "cpu_ns": stats.event_cpu, "allocations": stats.allocations, "allocated_bytes": stats.allocated,
        "encoded_bytes": stats.encoded, "decoded_bytes": stats.decoded, "wall_ns": stats.wall,
        "heap_before": before_heap, "heap_after": after_heap, "smaps_start": start_os, "smaps_before": before_os,
        "smaps_after": after_os, "private_cgroup": cg, "charge_before": before_charge,
        "charge_after": charge, "charge_peak": charge_peak,
        "no_output_completions": stats.no_output, "cpu_clock_overhead_ns": CLOCK_OVERHEAD.load(Relaxed)});
    Ok((Value::Object(metrics), evidence))
}
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let provenance = fs::read_to_string(std::env::var("CODEC_BASELINE_PROVENANCE")?)?;
    if args.get(1).map(String::as_str) == Some("--describe") { println!("{provenance}"); return Ok(()); }
    if args.len() != 5 || args[1] != "--run" || args[3] != "--round" {
        return Err("usage: baseline-harness --describe | --run MANIFEST --round N".into());
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
    for row in manifest["workloads"].as_array().ok_or("missing workload matrix")? {
        let (metrics, mut detail) = measure(row, &manifest, &cg)?;
        detail["execution_manifest_sha256"] = execution_checksum.clone().into();
        detail["original_manifest_sha256"] = checksum.clone().into();
        fs::write(evidence.join(format!("{}.json", row["id"].as_str().ok_or("missing id")?)), serde_json::to_vec_pretty(&detail)?)?;
        println!("{}", json!({"schema_version": 1, "workload_id": row["id"], "manifest_sha256": checksum,
                            "round": round, "metrics": metrics}));
    }
    Ok(())
}
