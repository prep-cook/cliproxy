use super::{Mapped, Operation, codec, identity};
use codec::{
    BodyMode, Error, ResponseEgress, ResponseIngress,
    chat::egress::Diagnostic,
    document::{Document, Value},
    event::{Direction, Emitter, Kind, Scope},
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};

/// Republishes each ingress record `transform` returns a count for, that many times, then
/// delivers the trusted stream to Chat egress.
fn transformed(
    wire: &str,
    mode: BodyMode,
    diagnostic: fn(Diagnostic),
    transform: impl Fn(Kind, &mut Document) -> usize,
) -> (Vec<u8>, codec::Result<()>) {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1_000_000,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, wire.as_bytes()).unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut target = mapped.store.create().unwrap();
    let mut seq = 0;
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        let record = lease.record();
        let (kind, mut doc) = codec::internal::decode(&mut mapped.writer, lease).unwrap();
        for _ in 0..transform(kind, &mut doc) {
            let x = doc.required(doc.root(), "x_ck").unwrap();
            doc.set(x, "seq", Value::Unsigned(seq)).unwrap();
            codec::encode::publish(&mut target, record.kind, 0, |e| {
                if let Some(name) = kind.name() {
                    e.bytes(b"event: ")?;
                    e.bytes(name.as_bytes())?;
                    e.bytes(b"\n")?;
                }
                e.bytes(b"id: ")?;
                e.unsigned(seq)?;
                e.bytes(b"\ndata: ")?;
                doc.write(doc.root(), e)?;
                e.bytes(b"\n\n")
            })
            .unwrap();
            seq += 1;
        }
    }
    deliver(&mapped, &mut target, mode, diagnostic)
}

fn deliver(
    mapped: &Mapped,
    target: &mut ck_stream::Writer,
    mode: BodyMode,
    diagnostic: fn(Diagnostic),
) -> (Vec<u8>, codec::Result<()>) {
    let mut reader = mapped.store.reader(target.id(), 0).unwrap();
    let mut egress = codec::chat::egress::Egress::new(
        target,
        identity(Direction::Response),
        Operation::Generate,
        mode,
        false,
        diagnostic,
    )
    .unwrap();
    let mut output = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        if let Err(error) = egress.advance(target, lease, |lease| {
            output.extend(super::bytes(&lease));
            Ok(())
        }) {
            assert!(egress.finish().is_err());
            return (output, Err(error));
        }
    }
    let result = egress.finish();
    (output, result)
}

const VENDOR: &str = concat!(
    "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"a\",\"vendor\":1}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

const AUDIO: &str = concat!(
    "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"audio\":{\"id\":\"au\",\"format\":\"wav\",\"data\":\"AAEC\"}}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
    "data: [DONE]\n\n",
);

static LOST: AtomicUsize = AtomicUsize::new(0);

fn count_lost(diagnostic: Diagnostic) {
    if diagnostic.reason == "source field has no surviving native parent" {
        LOST.fetch_add(1, Ordering::SeqCst);
    }
}

/// Adds a provenance entry for `extra` under `pointer` to the record that carried `vendor`;
/// decoding already restored that record's own entries.
fn orphan(doc: &mut Document, pointer: &'static str) {
    let x = doc.required(doc.root(), "x_ck").unwrap();
    if doc.member(x, "original").unwrap().is_none() {
        return;
    }
    let original = doc.set(x, "original", Value::Array).unwrap();
    let entry = doc.add(None, Value::Object).unwrap();
    doc.append(original, entry).unwrap();
    doc.set(entry, "protocol", Value::Text("chat")).unwrap();
    doc.set(entry, "pointer", Value::Text(pointer)).unwrap();
    doc.set(entry, "value", Value::Text("extra")).unwrap();
}

#[test]
fn provenance_without_a_surviving_parent_reports_projection_loss() {
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        for pointer in [
            None,
            Some("/choices/0/missing/extra"),
            Some("/choices/0/delta/vendor/extra"),
        ] {
            let before = LOST.load(Ordering::SeqCst);
            let (output, result) = transformed(VENDOR, mode, count_lost, |kind, doc| {
                if kind == Kind::Chunk
                    && let Some(pointer) = pointer
                {
                    orphan(doc, pointer);
                }
                1
            });
            result.unwrap();
            let lost = LOST.load(Ordering::SeqCst) - before;
            assert_eq!(lost, usize::from(pointer.is_some()), "{mode:?} {pointer:?}");
            let text = String::from_utf8(output).unwrap();
            assert!(text.contains("\"vendor\":1") && !text.contains("extra"), "{text}");
        }
    }
}

#[test]
fn duplicated_media_start_is_rejected() {
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, result) = transformed(AUDIO, mode, |_| {}, |kind, _| 1 + usize::from(kind == Kind::MediaStart));
        assert!(matches!(result, Err(Error::Schema)), "{mode:?}: {result:?}");
        assert!(!String::from_utf8(output).unwrap().contains("[DONE]"));
    }
}

#[test]
fn done_requires_every_choice_finished_in_both_modes() {
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        let (output, result) = transformed(
            VENDOR,
            mode,
            |_| {},
            |kind, doc| {
                if kind == Kind::Chunk
                    && let Some(choices) = doc.member(doc.root(), "choices").unwrap()
                    && let Some(choice) = doc.node(choices).unwrap().first
                    && let Some(finish) = doc.member(choice, "finish_reason").unwrap()
                    && !matches!(doc.node(finish).unwrap().value, Value::Null)
                {
                    doc.set(choice, "finish_reason", Value::Null).unwrap();
                }
                1
            },
        );
        assert!(matches!(result, Err(Error::Truncated)), "{mode:?}: {result:?}");
        let text = String::from_utf8(output).unwrap();
        if mode == BodyMode::Buffered {
            assert!(text.is_empty(), "{text}");
        } else {
            // The streamed prefix may remain; no successful end follows it.
            assert!(text.contains("\"content\":\"a\"") && !text.contains("[DONE]"), "{text}");
        }
    }
}

#[test]
fn cancellation_publishes_the_cancelled_error_code() {
    for (mode, expected) in [
        (
            BodyMode::Streaming,
            "data: {\"error\":{\"code\":\"stream.cancelled\",\"message\":\"Request cancelled\"}}\n\n",
        ),
        (
            BodyMode::Buffered,
            "{\"error\":{\"code\":\"stream.cancelled\",\"message\":\"Request cancelled\"}}",
        ),
    ] {
        let mapped = Mapped::new();
        let mut target = mapped.store.create().unwrap();
        Emitter::new(identity(Direction::Response))
            .emit(&mut target, Kind::Cancelled, Scope::default(), |_| Ok(()), |_| Ok(()))
            .unwrap();
        let (output, result) = deliver(&mapped, &mut target, mode, |_| {});
        result.unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), expected);
    }
}

const TOOL: &str = concat!(
    "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n",
    "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
    "data: [DONE]\n\n",
);

#[test]
fn done_requires_every_started_tool_ended_in_both_modes() {
    for mode in [BodyMode::Streaming, BodyMode::Buffered] {
        transformed(TOOL, mode, |_| {}, |_, _| 1).1.unwrap();
        for (case, tool_start, tool_end) in [("missing end", 1, 0), ("repeated start", 2, 1), ("repeated end", 1, 2)] {
            let (output, result) = transformed(
                TOOL,
                mode,
                |_| {},
                |kind, _| match kind {
                    Kind::ToolStart => tool_start,
                    Kind::ToolEnd => tool_end,
                    _ => 1,
                },
            );
            assert!(result.is_err(), "{mode:?} {case}");
            assert!(
                !String::from_utf8(output).unwrap().contains("[DONE]"),
                "{mode:?} {case}"
            );
        }
    }
}

static TERMINAL_LOST: AtomicUsize = AtomicUsize::new(0);

fn count_terminal_loss(diagnostic: Diagnostic) {
    if diagnostic.kind == Kind::Done {
        TERMINAL_LOST.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn done_record_sse_fields_are_restored_or_reported() {
    let wire = VENDOR.replace(
        "data: [DONE]\n\n",
        "id: terminal-id\nretry: 123\nvendor: keep\n: note\ndata: [DONE]\n\n",
    );
    let (output, result) = transformed(&wire, BodyMode::Streaming, count_terminal_loss, |_, _| 1);
    result.unwrap();
    let text = String::from_utf8(output).unwrap();
    assert!(
        text.ends_with("id: terminal-id\nretry: 123\nvendor: keep\n: note\ndata: [DONE]\n\n"),
        "{text}"
    );
    let before = TERMINAL_LOST.load(Ordering::SeqCst);
    transformed(&wire, BodyMode::Buffered, count_terminal_loss, |_, _| 1)
        .1
        .unwrap();
    assert_eq!(TERMINAL_LOST.load(Ordering::SeqCst) - before, 1);
    let before = TERMINAL_LOST.load(Ordering::SeqCst);
    transformed(VENDOR, BodyMode::Buffered, count_terminal_loss, |_, _| 1)
        .1
        .unwrap();
    assert_eq!(TERMINAL_LOST.load(Ordering::SeqCst) - before, 0);
}
