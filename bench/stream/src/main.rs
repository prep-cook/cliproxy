mod backend;
mod heap;
mod payload;
#[cfg(test)]
mod tests;

use anyhow::{Context, Result, bail, ensure};
use backend::Backend;
use ck_stream::{FRAME_BYTES, Read, Terminal};
use payload::{Check, pattern};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

const MIB: usize = 1024 * 1024;
const WORKERS: usize = 8;

fn percentile(values: &mut [u64], p: f64) -> u64 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * p).round() as usize]
}

async fn stream(
    backend: Arc<Backend>,
    readers: usize,
    chunk: usize,
    total: usize,
    yield_bytes: usize,
) -> Result<(u64, Vec<u64>)> {
    let start = Instant::now();
    let mut writer = backend.create().context("create stream")?;
    let mut handles = Vec::new();
    let id = writer.id();
    for _ in 0..readers {
        let mut reader = backend.reader(id)?;
        handles.push(tokio::spawn(async move {
            let mut check = Check::new(id);
            let mut first = None;
            while let Some(lease) = reader.next().await? {
                first.get_or_insert(start.elapsed().as_nanos() as u64);
                lease.visit(|bytes| check.visit(bytes))?;
                lease.acknowledge();
            }
            check.finish(total)?;
            first.context("reader observed no bytes")
        }));
    }
    let mut written = 0;
    let mut next_yield = yield_bytes;
    while written < total {
        let n = chunk.min(total - written);
        writer
            .append(pattern(id, written, n), chunk == 49152)
            .with_context(|| format!("append at {written}/{total} bytes; {}", backend.counters()))?;
        written += n;
        if written >= next_yield {
            next_yield += yield_bytes;
            tokio::task::yield_now().await;
        }
    }
    writer.finish();
    drop(writer);
    let mut firsts = Vec::new();
    for handle in handles {
        firsts.push(handle.await??);
    }
    Ok((start.elapsed().as_nanos() as u64, firsts))
}

async fn throughput(backend: Arc<Backend>, readers: usize, chunk: usize, total: usize) -> Result<Value> {
    let (elapsed, _) = stream(backend, readers, chunk, total, 256 * 1024).await?;
    Ok(json!({"mb_s":total as f64 / (elapsed as f64 / 1e9) / 1e6}))
}
async fn concurrent(backend: Arc<Backend>, streams: usize) -> Result<Value> {
    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..streams {
        let backend = backend.clone();
        handles.push(tokio::spawn(async move {
            stream(backend, 1, 256, 256 * 1024, 16 * 256).await
        }));
    }
    let mut firsts = Vec::new();
    for handle in handles {
        firsts.extend(handle.await??.1);
    }
    Ok(json!({"wall_ns":start.elapsed().as_nanos() as u64,"ttfb_p99_ns":percentile(&mut firsts,0.99)}))
}
async fn small(backend: Arc<Backend>, tasks: usize, requests: usize) -> Result<Value> {
    // Batches expire outside the timed region; disconnect alone never evicts responses.
    let mut remaining = requests;
    let mut elapsed = 0u64;
    let mut latencies = Vec::with_capacity(requests);
    while remaining > 0 {
        let batch = remaining.min(2048);
        let start = Instant::now();
        let mut handles = Vec::new();
        for task in 0..tasks {
            let backend = backend.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::new();
                for _ in (task..batch).step_by(tasks) {
                    let start = Instant::now();
                    let mut writer = backend.create()?;
                    let id = writer.id();
                    let mut reader = backend.reader(id)?;
                    for record in 0..32 {
                        writer.append(pattern(id, record * 64, 64), false)?;
                    }
                    writer.finish();
                    let mut check = Check::new(id);
                    loop {
                        match reader.try_next()? {
                            Poll::Ready(Some(lease)) => {
                                lease.visit(|bytes| check.visit(bytes))?;
                                lease.acknowledge();
                            }
                            Poll::Ready(None) => break,
                            Poll::Pending => bail!("completed small stream remained pending"),
                        }
                    }
                    check.finish(2048)?;
                    drop(reader);
                    drop(writer);
                    latencies.push(start.elapsed().as_nanos() as u64);
                    if tasks > 1 {
                        tokio::task::yield_now().await;
                    }
                }
                Ok::<_, anyhow::Error>(latencies)
            }));
        }
        for handle in handles {
            latencies.extend(handle.await??);
        }
        elapsed += start.elapsed().as_nanos() as u64;
        backend.expire()?;
        remaining -= batch;
    }
    Ok(
        json!({"req_s":requests as f64 / (elapsed as f64 / 1e9),"p50_ns":percentile(&mut latencies,0.5),"p99_ns":percentile(&mut latencies,0.99),"requests":requests,"expiry_batch":2048}),
    )
}
async fn latency(backend: Arc<Backend>, readers: usize, records: usize) -> Result<Value> {
    let base = Instant::now();
    let mut writer = backend.create()?;
    let mut handles = Vec::new();
    for _ in 0..readers {
        let mut reader = backend.reader(writer.id())?;
        handles.push(tokio::spawn(async move {
            let mut delays = Vec::new();
            let mut first = None;
            while let Some(lease) = reader.next().await? {
                let observed = base.elapsed().as_nanos() as u64;
                first.get_or_insert(observed);
                let mut stamp = [0u8; 8];
                let mut copied = 0;
                let mut count = 0;
                lease.visit(|bytes| {
                    let n = (8 - copied).min(bytes.len());
                    stamp[copied..copied + n].copy_from_slice(&bytes[..n]);
                    copied += n;
                    count += bytes.len();
                })?;
                ensure!(count == 64 && copied == 8, "latency record framing");
                delays.push(observed - u64::from_le_bytes(stamp));
                lease.acknowledge();
            }
            ensure!(delays.len() == records, "missing latency observations");
            Ok::<_, anyhow::Error>((delays, first.context("no first byte")?))
        }));
    }
    for _ in 0..records {
        let mut bytes = [85; 64];
        bytes[..8].copy_from_slice(&(base.elapsed().as_nanos() as u64).to_le_bytes());
        writer.append(&bytes, false)?;
        let deadline = Instant::now() + Duration::from_micros(20);
        while Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
    }
    writer.finish();
    let mut delays = Vec::new();
    let mut firsts = Vec::new();
    for handle in handles {
        let (values, first) = handle.await??;
        delays.extend(values);
        firsts.push(first);
    }
    Ok(
        json!({"p50_ns":percentile(&mut delays,0.5),"p99_ns":percentile(&mut delays,0.99),"ttfb_p50_ns":percentile(&mut firsts,0.5),"ttfb_p99_ns":percentile(&mut firsts,0.99),"records":records,"gap_us":20}),
    )
}

fn snapshot(directory: &Path) -> Result<Value> {
    let status = fs::read_to_string("/proc/self/status")?;
    let mut result = serde_json::Map::new();
    for key in ["VmRSS", "RssAnon", "RssShmem", "RssFile"] {
        let line = status
            .lines()
            .find(|line| line.starts_with(&format!("{key}:")))
            .with_context(|| format!("missing {key}"))?;
        let kib: u64 = line.split_whitespace().nth(1).context("status value")?.parse()?;
        result.insert(key.into(), json!(kib * 1024));
    }
    let stat = Command::new("stat")
        .args(["-f", "-c", "%S %b %f"])
        .arg(directory)
        .output()?;
    ensure!(stat.status.success(), "stat filesystem failed");
    let numbers: Vec<u64> = String::from_utf8(stat.stdout)?
        .split_whitespace()
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    ensure!(numbers.len() == 3, "stat filesystem shape");
    result.insert("tmpfs_used".into(), json!(numbers[0] * (numbers[1] - numbers[2])));
    let cgroup = fs::read_to_string("/proc/self/cgroup")?;
    let relative = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("cgroup v2 membership unavailable")?;
    let root = PathBuf::from("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
    let current: u64 = fs::read_to_string(root.join("memory.current"))?.trim().parse()?;
    result.insert("cgroup_current".into(), json!(current));
    let stat = fs::read_to_string(root.join("memory.stat"))?;
    for key in ["anon", "shmem"] {
        let value: u64 = stat
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key} ")))
            .context("cgroup memory.stat field")?
            .parse()?;
        result.insert(format!("cgroup_{key}"), json!(value));
    }
    Ok(Value::Object(result))
}
async fn memory(backend: Arc<Backend>, directory: &Path, baseline: Value, count: usize, size: usize) -> Result<Value> {
    let mut handles = Vec::new();
    for worker in 0..WORKERS {
        let backend = backend.clone();
        handles.push(tokio::spawn(async move {
            let mut held = Vec::new();
            for _ in (worker..count).step_by(WORKERS) {
                let mut writer = backend.create()?;
                let mut reader = backend.reader(writer.id())?;
                let payload = [85; 49152];
                let mut written = 0;
                while written < size {
                    let n = payload.len().min(size - written);
                    writer.append(&payload[..n], true)?;
                    written += n;
                }
                writer.finish();
                let mut leases = Vec::new();
                while let Some(lease) = reader.next().await? {
                    leases.push(lease);
                }
                held.push((writer, leases));
            }
            Ok::<_, anyhow::Error>(held)
        }));
    }
    let mut held = Vec::new();
    for handle in handles {
        held.extend(handle.await??);
    }
    let peak = snapshot(directory)?;
    backend.expire()?;
    let pinned = backend.counters();
    let release = Instant::now();
    drop(held);
    backend.tick()?;
    tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(1))).await?;
    backend.tick()?;
    let after = snapshot(directory)?;
    let elapsed = release.elapsed().as_millis();
    ensure!(
        elapsed <= 2000,
        "memory sample exceeded two decay intervals: {elapsed} ms"
    );
    Ok(
        json!({"baseline":baseline,"held":peak,"after":after,"pinned_after_expiry":pinned,"after_counters":backend.counters(),"release_sample_ms":elapsed,"workers":WORKERS,"count":count,"size":size,"decay_intervals":1}),
    )
}

fn terminal_error(reader: &mut ck_stream::Reader) -> Result<()> {
    loop {
        match reader.try_next() {
            Poll::Ready(Some(Read::Record(_))) => {}
            Poll::Ready(Some(Read::Terminal(Terminal::StorageExhausted))) => return Ok(()),
            _ => bail!("missing visible storage-exhausted terminal"),
        }
    }
}
fn totalcap(backend: &Backend) -> Result<Value> {
    let Backend::Pool(store, _) = backend else {
        bail!("totalcap requires pool");
    };
    ensure!(
        store.limits().pool_frames == 85899 && store.limits().max_frames == 5368,
        "host startup cap lowered pool: {}",
        backend.counters()
    );
    let mut held = Vec::new();
    let mut counts = Vec::new();
    let mut errors = Vec::new();
    for stream in 0..17 {
        let mut writer = store.create()?;
        let mut reader = store.reader(writer.id(), 0)?;
        let payload = vec![stream as u8; FRAME_BYTES];
        let mut leases = Vec::new();
        let mut count = 0;
        let error = loop {
            match writer.append(&payload) {
                Ok(_) => {
                    count += 1;
                    match reader.try_next() {
                        Poll::Ready(Some(Read::Record(lease))) => {
                            lease.visit(|bytes| assert!(bytes.iter().all(|&x| x == stream as u8)))?;
                            leases.push(lease);
                        }
                        _ => bail!("missing pinned frame"),
                    }
                }
                Err(error) => break error,
            }
        };
        ensure!(
            matches!(error, ck_stream::Error::StorageExhausted),
            "unexpected cap error {error}"
        );
        terminal_error(&mut reader)?;
        // 85,899 − 16 × 5,368 = 11: stream 17 takes exactly the remaining frames.
        ensure!(
            count == if stream < 16 { 5368 } else { 11 },
            "stream {} stopped at {count} frames",
            stream + 1
        );
        counts.push(count);
        errors.push(error.to_string());
        held.push((writer, reader, leases));
    }
    ensure!(store.used_frames() >= 85888, "pinned frames unexpectedly released");
    Ok(json!({"stream_frames":counts,"errors":errors,"survived":true,"counters":backend.counters()}))
}
fn enospc(backend: &Backend, directory: &Path) -> Result<Value> {
    let Backend::Pool(store, _) = backend else {
        bail!("enospc requires pool");
    };
    ensure!(
        store.limits().pool_frames <= (32 * MIB / FRAME_BYTES) as u32,
        "private tmpfs half-size cap not applied"
    );
    let mut writer = store.create()?;
    let mut reader = store.reader(writer.id(), 0)?;
    let filler_path = directory.join("enospc-filler");
    let mut filler = fs::OpenOptions::new().write(true).create_new(true).open(&filler_path)?;
    let bytes = [0u8; MIB];
    let mut filler_bytes = 0;
    loop {
        match filler.write(&bytes) {
            Ok(0) => bail!("filler short write"),
            Ok(n) => filler_bytes += n,
            Err(error) if error.raw_os_error() == Some(28) => break,
            Err(error) => return Err(error.into()),
        }
    }
    let mut frames = 0;
    let failure = loop {
        match writer.append(&[85; FRAME_BYTES]) {
            Ok(_) => frames += 1,
            Err(error) => break error,
        }
    };
    ensure!(
        matches!(&failure,ck_stream::Error::Io(error) if error.raw_os_error()==Some(28)),
        "expected actual growth ENOSPC, got {failure}"
    );
    terminal_error(&mut reader)?;
    let result = json!({"error":failure.to_string(),"errno":28,"frames":frames,"filler_bytes":filler_bytes,"survived":true,"survivor_pid":std::process::id(),"counters":backend.counters()});
    drop(filler);
    fs::remove_file(filler_path)?;
    Ok(result)
}

async fn run(
    kind: &str,
    row: &str,
    directory: &Path,
    reserve_bytes: Option<usize>,
    idle_seconds: u64,
) -> Result<Value> {
    let baseline = if row.starts_with("memory-") {
        Some(snapshot(directory)?)
    } else {
        None
    };
    let before_open = reserve_bytes.map(|_| snapshot(directory)).transpose()?;
    let open_start = Instant::now();
    let backend = Arc::new(Backend::open(kind, directory, reserve_bytes).context("open backend")?);
    let open_ns = open_start.elapsed().as_nanos() as u64;
    let idle = reserve_bytes.map(|_| snapshot(directory)).transpose()?;
    // Comparison rows run one identical untimed pass, expire it and let the
    // pool decay once, so both backends start the measured pass with warm code
    // and idle storage: freed heap memory follows the allocator's trimming, and
    // freed pool frames are punched except each free list's warm frames.
    // Startup diagnostics stay cold.
    let warmup = reserve_bytes.is_none() && !matches!(row.split('-').next(), Some("memory" | "totalcap" | "enospc"));
    if warmup {
        workload(backend.clone(), row, directory, None)
            .await
            .context("untimed warmup")?;
        backend.expire()?;
        backend.tick()?;
    }
    let mut value = workload(backend.clone(), row, directory, baseline.clone()).await?;
    let startup = if let Some(before_open) = before_open {
        let first_burst = value.clone();
        backend.expire()?;
        backend.tick()?;
        for _ in 0..idle_seconds {
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(1))).await?;
            backend.tick()?;
        }
        let after_decay = snapshot(directory)?;
        if idle_seconds > 0 {
            value = workload(backend.clone(), row, directory, baseline).await?;
        }
        Some(
            json!({"open_ns":open_ns,"before_open":before_open,"idle":idle,"after_decay":after_decay,"idle_seconds":idle_seconds,"first_burst":first_burst}),
        )
    } else {
        None
    };
    Ok(
        json!({"status":"ok","backend":kind,"row":row,"warmup":warmup,"metrics":value,"counters":backend.counters(),"startup":startup}),
    )
}

/// Process-wide minor page faults (all threads), from /proc/self/stat field 10.
fn minor_faults() -> Result<u64> {
    let stat = fs::read_to_string("/proc/self/stat")?;
    let fields = stat.rsplit_once(')').context("stat shape")?.1;
    Ok(fields.split_whitespace().nth(7).context("minflt")?.parse()?)
}

async fn workload(backend: Arc<Backend>, row: &str, directory: &Path, baseline: Option<Value>) -> Result<Value> {
    let fields: Vec<&str> = row.split('-').collect();
    let faults = minor_faults()?;
    let mut value = match fields.as_slice() {
        ["small", tasks] => small(backend.clone(), tasks.parse()?, 20000).await?,
        ["smoke"] => small(backend.clone(), 1, 8).await?,
        ["concurrent", streams] => concurrent(backend.clone(), streams.parse()?).await?,
        ["throughput", readers, chunk, mib] => {
            throughput(
                backend.clone(),
                readers.parse()?,
                chunk.parse()?,
                mib.parse::<usize>()? * MIB,
            )
            .await?
        }
        ["latency", readers] => latency(backend.clone(), readers.parse()?, 20000).await?,
        ["memory", count, mib] => {
            memory(
                backend.clone(),
                directory,
                baseline.unwrap(),
                count.parse()?,
                mib.parse::<usize>()? * MIB,
            )
            .await?
        }
        ["totalcap"] => totalcap(&backend)?,
        ["enospc"] => enospc(&backend, directory)?,
        _ => bail!("unknown workload {row}"),
    };
    if let Value::Object(metrics) = &mut value {
        metrics.insert("minor_faults".into(), json!(minor_faults()? - faults));
    }
    Ok(value)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if !(4..=6).contains(&args.len()) {
        eprintln!("usage: ck-stream-bench pool|heap ROW DIRECTORY [RESERVE_BYTES [IDLE_SECONDS]]");
        std::process::exit(2);
    }
    let reserve_bytes = args.get(4).map(|value| value.parse::<usize>()).transpose();
    let idle_seconds = args.get(5).map(|value| value.parse::<u64>()).transpose();
    let (Ok(reserve_bytes), Ok(idle_seconds)) = (reserve_bytes, idle_seconds) else {
        eprintln!("reserve bytes and idle seconds must be unsigned integers");
        std::process::exit(2);
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .build()
        .expect("eight-worker runtime");
    match runtime.block_on(run(
        &args[1],
        &args[2],
        Path::new(&args[3]),
        reserve_bytes,
        idle_seconds.unwrap_or(0),
    )) {
        Ok(value) => println!("{value}"),
        Err(error) => {
            println!(
                "{}",
                json!({"status":"error","backend":args[1],"row":args[2],"error":format!("{error:#}")})
            );
            std::process::exit(1);
        }
    }
}
