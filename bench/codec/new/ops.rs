//! One boundary operation per fixture case, timed over the same regions as `baseline.rs`.
use crate::codec::{BodyMode, Operation};
use crate::harness::{ALLOCS, BYTES, COUNT, LIVE, PEAK, Result, TRACK, cpu, cpu_since};
use crate::protocols::{Protocol, RequestIn, ResponseIn, ResponseOut};
use ck_stream::{Read, Reader, ScratchLease, Store, Terminal, Writer};
use serde_json::Value;
use std::{hint::black_box, sync::atomic::Ordering::Relaxed, task::Poll, time::Instant};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Request,
    Stream,
    Buffered,
    Count,
}

pub struct Prepared {
    mode: Mode,
    input: Vec<u8>,
    lines: Vec<Vec<u8>>,
    selected: Vec<bool>,
    stream: bool,
    include_usage: bool,
    upstream: BodyMode,
}

/// Same selection as `baseline.rs` and `gate.py`: a line's external wire kind.
pub fn kind(line: &str, selected: &str) -> bool {
    if let Some(name) = line.strip_prefix("event:") {
        return name.trim() == selected || (name.trim().is_empty() && selected == "unnamed");
    }
    let payload = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
    if payload == "[DONE]" {
        return selected == "[DONE]";
    }
    match serde_json::from_str::<Value>(payload) {
        Ok(value) => value.get("type").or_else(|| value.get("object")).and_then(Value::as_str).unwrap_or("json") == selected,
        Err(_) => selected == "framing-or-fragment",
    }
}

fn bytes(v: &Value, raw: bool) -> Vec<u8> {
    let s = v.as_str().unwrap_or_default();
    if raw { s.chars().map(|c| c as u32 as u8).collect() } else { s.as_bytes().to_vec() }
}

/// The recorded pair corpus stores SSE as lines without terminators, which the old pair
/// translators consumed one at a time. The new ingress parses SSE framing, so each line gets
/// its terminator, and every line except an `event:`, `id:` or `retry:` field also ends its
/// event with a blank line. Each framed line is still fed as one separate read.
fn frame(mut line: Vec<u8>) -> Vec<u8> {
    let field = [&b"event:"[..], b"id:", b"retry:"].iter().any(|p| line.starts_with(p));
    line.extend_from_slice(if field { b"\n" } else { b"\n\n" });
    line
}

/// A buffered reply recorded as Go-scanner SSE lines is what an upstream sends when the
/// gateway streamed the upstream request; ingress then reads it as SSE, framed as above.
fn upstream_body(input: Vec<u8>) -> (BodyMode, Vec<u8>) {
    let lines = || input.split(|b| *b == b'\n').filter(|line| !line.trim_ascii().is_empty());
    let first = lines().next().unwrap_or_default();
    if !(first.starts_with(b"data:") || first.starts_with(b"event:")) {
        return (BodyMode::Buffered, input);
    }
    let body = lines().flat_map(|line| frame(line.to_vec())).collect();
    (BodyMode::Streaming, body)
}

/// The corpus keeps the routed model beside the body, as the old `RequestCtx` received it.
/// Chat and Anthropic clients send it in the body, which the new ingress validates, so a
/// JSON object without `model` gets the fixture's model as its first member.
fn with_model(input: Vec<u8>, model: &str) -> Vec<u8> {
    let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&input) else { return input };
    let Some(open) = input.iter().position(|b| *b == b'{').filter(|_| !map.contains_key("model")) else {
        return input;
    };
    let mut body = format!("{{\"model\":{}", Value::from(model)).into_bytes();
    if !map.is_empty() { body.push(b','); }
    body.extend_from_slice(&input[open + 1..]);
    body
}

impl Prepared {
    pub fn new(f: &Value, selected_kind: &str) -> Result<Self> {
        let raw = f["bytes"].as_bool().unwrap_or(false);
        let mode = match f["path"].as_str().ok_or("missing mode")? {
            "request" | "request_compat" | "request_envelope" => Mode::Request,
            "stream" => Mode::Stream,
            "non_stream" => Mode::Buffered,
            "token_count" => Mode::Count,
            _ => return Err("unknown fixture mode".into()),
        };
        let (mut lines, mut selected) = (Vec::new(), Vec::new());
        if mode == Mode::Stream {
            for line in f["lines"].as_array().ok_or("missing stream lines")? {
                selected.push(kind(line.as_str().ok_or("non-string stream line")?, selected_kind));
                lines.push(frame(bytes(line, raw)));
            }
        }
        // The client's original request decides Chat usage chunks, as it does in production.
        let original: Value = serde_json::from_slice(&bytes(&f["original"], raw)).unwrap_or(Value::Null);
        let include_usage = original["stream_options"]["include_usage"].as_bool().unwrap_or(false);
        let mut input = bytes(&f["input"], raw);
        let mut upstream = BodyMode::Buffered;
        match mode {
            Mode::Request => input = with_model(input, f["model"].as_str().ok_or("missing model")?),
            Mode::Buffered => (upstream, input) = upstream_body(input),
            Mode::Stream | Mode::Count => {}
        }
        Ok(Self { mode, input, lines, selected, stream: f["stream"].as_bool().unwrap_or(false),
            include_usage, upstream })
    }
}

/// Deliverable output: an encoded scratch lease, or the harness's refusal marker when the
/// boundary refused before any codec could encode a reply (as `baseline.rs` does for requests).
pub enum Chunk {
    Lease(ScratchLease),
    Refused(Vec<u8>),
}
impl Chunk {
    fn len(&self) -> usize {
        match self { Self::Lease(lease) => lease.len(), Self::Refused(bytes) => bytes.len() }
    }
    fn touch(&self) -> u64 {
        match self {
            Self::Refused(bytes) => black_box(bytes).len() as u64,
            Self::Lease(lease) => {
                let mut offset = 0;
                while offset < lease.len() {
                    offset += black_box(lease.chunk(offset).expect("live scratch lease")).len();
                }
                offset as u64
            }
        }
    }
}

#[derive(Default)]
pub struct Samples {
    pub event_cpu: u64, pub completion_cpu: u64, pub allocations: u64, pub allocated: u64, pub events: u64,
    pub encoded: u64, pub decoded: u64, pub wall: u64, pub completions: u64, pub output_chunks: u64,
    pub first: Vec<u64>, pub inter: Vec<u64>, pub completion: Vec<u64>, pub consumed_bytes: u64, pub no_output: u64,
    pub pool_used: u32, pub metadata: usize, pub refused: u64, pub ingress_errors: u64, pub egress_errors: u64,
}

struct Timing { start: Instant, first: Option<u64>, previous: Option<u64>, completion_cpu: u64 }

impl Samples {
    fn consume(&mut self, outputs: &[Chunk], t: &mut Timing) {
        let elapsed = t.start.elapsed().as_nanos() as u64;
        for out in outputs.iter().filter(|out| out.len() > 0) {
            if t.first.is_none() { t.first = Some(elapsed); }
            if let Some(last) = t.previous.replace(elapsed) { self.inter.push(elapsed - last); }
            self.output_chunks += 1;
            self.consumed_bytes += out.touch();
        }
    }
    /// Record one event region's outputs; the cost is attributed only to the selected kind.
    fn event(&mut self, measured: bool, selected: bool, cost: Cost, decoded: usize, outputs: &[Chunk], t: &mut Timing) {
        if !measured {
            self.output_chunks += outputs.iter().filter(|out| out.len() > 0).count() as u64;
            return;
        }
        if selected {
            self.account(cost, decoded, outputs.iter().map(|out| out.len() as u64).sum());
        }
        self.consume(outputs, t);
    }
    fn account(&mut self, cost: Cost, decoded: usize, encoded: u64) {
        self.event_cpu += cost.cpu; self.events += 1;
        self.allocations += cost.allocations; self.allocated += cost.bytes;
        self.decoded += decoded as u64; self.encoded += encoded;
    }
    fn storage(&mut self, store: &Store, writer: &Writer) {
        self.pool_used = self.pool_used.max(store.used_frames());
        self.metadata = self.metadata.max(writer.metadata_bytes());
    }
}

#[derive(Clone, Copy, Default)]
struct Cost { cpu: u64, allocations: u64, bytes: u64 }
impl std::ops::Add for Cost {
    type Output = Self;
    fn add(self, o: Self) -> Self {
        Cost { cpu: self.cpu + o.cpu, allocations: self.allocations + o.allocations, bytes: self.bytes + o.bytes }
    }
}

/// CPU and allocations of one region, with counting enabled only when the event is selected.
fn region<T>(count: bool, work: impl FnOnce() -> T) -> (T, Cost) {
    let (allocations, bytes) = (ALLOCS.load(Relaxed), BYTES.load(Relaxed));
    COUNT.store(count, Relaxed);
    let start = cpu();
    let value = work();
    let cpu = cpu_since(start);
    COUNT.store(false, Relaxed);
    (value, Cost { cpu, allocations: ALLOCS.load(Relaxed) - allocations, bytes: BYTES.load(Relaxed) - bytes })
}

pub struct Run<'a> {
    pub store: &'a Store,
    pub client: Protocol,
    pub upstream: Protocol,
    pub selected: &'a str,
    pub limit: u64,
}

/// Upstream response ingress feeding client response egress through the writer's canonical log.
struct Pipe {
    ingress: Option<ResponseIn>,
    egress: Option<ResponseOut>,
    reader: Reader,
    ingress_failed: bool,
    egress_failed: bool,
}

impl Pipe {
    fn new(run: &Run, owner: &mut Writer, operation: Operation, modes: (BodyMode, BodyMode), usage: bool)
           -> Result<Self> {
        let reader = run.store.reader(owner.id(), 0)?;
        let ingress = ResponseIn::new(run.upstream, owner, run.store, operation, modes.0, run.limit).ok();
        let egress = ResponseOut::new(run.client, owner, operation, modes.1, usage).ok();
        let (ingress_failed, egress_failed) = (ingress.is_none(), egress.is_none());
        Ok(Self { ingress, egress, reader, ingress_failed, egress_failed })
    }
    /// A failed codec stops consuming input, as the boundary terminates that stream.
    fn feed(&mut self, owner: &mut Writer, bytes: &[u8], outputs: &mut Vec<Chunk>) {
        if let Some(ingress) = self.ingress.as_mut().filter(|_| !self.ingress_failed) {
            self.ingress_failed = ingress.advance(owner, bytes).is_err();
        }
        self.drain(owner, outputs);
    }
    fn eof(&mut self, owner: &mut Writer, outputs: &mut Vec<Chunk>) {
        if let Some(ingress) = self.ingress.as_mut().filter(|_| !self.ingress_failed) {
            self.ingress_failed = ingress.finish(owner).is_err();
        }
        self.drain(owner, outputs);
        if let Some(egress) = self.egress.as_mut().filter(|_| !self.egress_failed) {
            self.egress_failed = egress.finish().is_err();
        }
    }
    fn drain(&mut self, owner: &mut Writer, outputs: &mut Vec<Chunk>) {
        while let Poll::Ready(Some(read)) = self.reader.try_next() {
            let (Read::Record(lease), Some(egress)) = (read, self.egress.as_mut()) else { continue };
            if !self.egress_failed {
                let pushed = egress.advance(owner, lease, |part| { outputs.push(Chunk::Lease(part)); Ok(()) });
                self.egress_failed = pushed.is_err();
            }
        }
    }
    fn errors(&self, stats: &mut Samples) {
        stats.ingress_errors += self.ingress_failed as u64;
        stats.egress_errors += self.egress_failed as u64;
    }
}

/// Release-at-completion: `Gone` invalidates the stream and frees its frames and header slot.
fn release(mut writer: Writer) {
    writer.finish(Terminal::Gone);
    drop(writer);
}

fn stream(case: &Prepared, run: &Run, measured: bool, stats: &mut Samples, outputs: &mut Vec<Chunk>,
          t: &mut Timing) -> Result<()> {
    // Stream construction, EOF validation and teardown are completion cost, as in baseline.rs.
    let (built, cost) = region(false, || -> Result<_> {
        let mut writer = run.store.create()?;
        let pipe = Pipe::new(run, &mut writer, Operation::Generate, (BodyMode::Streaming, BodyMode::Streaming),
                             case.include_usage)?;
        Ok((writer, pipe))
    });
    t.completion_cpu += cost.cpu;
    let (mut writer, mut pipe) = built?;
    for (line, selected) in case.lines.iter().zip(&case.selected) {
        let ((), cost) = region(measured && *selected, || pipe.feed(&mut writer, line, outputs));
        t.completion_cpu += cost.cpu;
        TRACK.store(false, Relaxed);
        stats.event(measured, *selected, cost, line.len(), outputs, t);
        outputs.clear();
        TRACK.store(measured, Relaxed);
    }
    let ((), cost) = region(false, || pipe.eof(&mut writer, outputs));
    t.completion_cpu += cost.cpu;
    TRACK.store(false, Relaxed);
    if measured {
        stats.consume(outputs, t);
        stats.storage(run.store, &writer);
        pipe.errors(stats);
    }
    outputs.clear();
    TRACK.store(measured, Relaxed);
    let ((), cost) = region(false, || { drop(pipe); release(writer) });
    t.completion_cpu += cost.cpu;
    if case.lines.is_empty() && run.selected == "empty-stream" && measured { stats.events += 1; }
    Ok(())
}

fn request(case: &Prepared, run: &Run, owner: &mut Writer, outputs: &mut Vec<Chunk>) -> bool {
    let result = RequestIn::new(run.client, owner, run.store, run.limit).and_then(|mut ingress| {
        ingress.advance(owner, &case.input)?;
        ingress.encode(owner, run.upstream, case.stream)
    });
    match result {
        Ok(lease) => { outputs.push(Chunk::Lease(lease)); false }
        Err(error) => { outputs.push(Chunk::Refused(format!("REFUSED:{error}").into_bytes())); true }
    }
}

fn response(case: &Prepared, run: &Run, owner: &mut Writer, outputs: &mut Vec<Chunk>,
            stats: &mut Samples, measured: bool) -> Result<bool> {
    let operation = if case.mode == Mode::Count { Operation::CountTokens } else { Operation::Generate };
    let mut pipe = Pipe::new(run, owner, operation, (case.upstream, BodyMode::Buffered), case.include_usage)?;
    pipe.feed(owner, &case.input, outputs);
    pipe.eof(owner, outputs);
    if measured { pipe.errors(stats); }
    let refused = outputs.is_empty() && (pipe.ingress_failed || pipe.egress_failed);
    if refused { outputs.push(Chunk::Refused(b"REFUSED:boundary".to_vec())); }
    Ok(refused)
}

/// Request, buffered response and count: one complete boundary operation is the event.
fn single(case: &Prepared, run: &Run, measured: bool, stats: &mut Samples, outputs: &mut Vec<Chunk>,
          t: &mut Timing) -> Result<()> {
    let (done, work) = region(measured, || -> Result<_> {
        let mut writer = run.store.create()?;
        let refused = match case.mode {
            Mode::Request => request(case, run, &mut writer, outputs),
            _ => response(case, run, &mut writer, outputs, stats, measured)?,
        };
        Ok((writer, refused))
    });
    let (writer, refused) = done?;
    TRACK.store(false, Relaxed);
    let encoded = outputs.iter().map(|out| out.len() as u64).sum();
    if measured {
        stats.storage(run.store, &writer);
        stats.refused += refused as u64;
        stats.consume(outputs, t);
    } else {
        stats.output_chunks += outputs.iter().filter(|out| out.len() > 0).count() as u64;
    }
    outputs.clear();
    TRACK.store(measured, Relaxed);
    // Writer release is internal teardown, timed like the old translator's own drops.
    let ((), teardown) = region(measured, || release(writer));
    let cost = work + teardown;
    t.completion_cpu += cost.cpu;
    TRACK.store(false, Relaxed);
    if measured { stats.account(cost, case.input.len(), encoded); }
    Ok(())
}

pub fn operation(case: &Prepared, run: &Run, measured: bool, stats: &mut Samples,
                 outputs: &mut Vec<Chunk>) -> Result<()> {
    let mut t = Timing { start: Instant::now(), first: None, previous: None, completion_cpu: 0 };
    PEAK.store(LIVE.load(Relaxed), Relaxed);
    TRACK.store(measured, Relaxed);
    if case.mode == Mode::Stream {
        stream(case, run, measured, stats, outputs, &mut t)?;
    } else {
        single(case, run, measured, stats, outputs, &mut t)?;
    }
    TRACK.store(false, Relaxed);
    let elapsed = t.start.elapsed().as_nanos() as u64;
    if measured {
        stats.completion_cpu += t.completion_cpu; stats.wall += elapsed; stats.completions += 1;
        if t.first.is_none() { stats.no_output += 1; }
        stats.first.push(t.first.unwrap_or(0)); stats.completion.push(elapsed);
    }
    Ok(())
}
