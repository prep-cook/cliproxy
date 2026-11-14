#[path = "semantic/mod.rs"]
mod semantic;

use ck_translate::codec::{Error, Result, encode, json, sse};
use semantic::Mapped;

const BUDGET: usize = 1024 * 1024;

#[test]
fn fragmented_source_packs_frames_without_losing_parser_positions() {
    let mut bytes = br#"{"payload":""#.to_vec();
    bytes.extend(std::iter::repeat_n(b'a', 80_003));
    bytes.extend_from_slice(br#"","tail":"\ud83d\ude80"}"#);
    for width in [1, 7] {
        let mut mapped = Mapped::new();
        let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        for fragment in bytes.chunks(width) {
            mapped.push(fragment).unwrap();
            scanner.advance(&mapped.source).unwrap();
        }
        scanner.finish(&mapped.source).unwrap();
        assert_eq!(
            mapped.store.used_frames() as usize,
            bytes.len().div_ceil(ck_stream::FRAME_BYTES)
        );
        let output = encode::scratch(&mut mapped.writer, |e| e.json(&mapped.source, &scanner, 0)).unwrap();
        assert_eq!(scratch_bytes(&output), bytes);
        drop(output);
        mapped.source.release_before(ck_stream::FRAME_BYTES as u64).unwrap();
        assert_eq!(mapped.store.used_frames(), 1);
        assert!(matches!(
            mapped
                .source
                .cursor(ck_translate::codec::source::Span { start: 0, end: 1 }),
            Err(Error::Position)
        ));
        assert!(
            mapped
                .source
                .equals(
                    ck_translate::codec::source::Span {
                        start: ck_stream::FRAME_BYTES as u64,
                        end: bytes.len() as u64,
                    },
                    &bytes[ck_stream::FRAME_BYTES..]
                )
                .unwrap()
        );
    }
}

#[test]
fn json_unknown_values_round_trip_across_every_fragment_boundary() {
    let bytes = br#" { "known": "text", "unknown": [null, false, 0, -0.5e+2, {"\u0061":"\ud83d\ude00","empty":""}], "x_ck":{"attempt":999,"media":[{"media_id":"forged"}]} } "#;
    for split in 0..=bytes.len() {
        let mut mapped = Mapped::new();
        let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        mapped.push(&bytes[..split]).unwrap();
        scanner.advance(&mapped.source).unwrap();
        mapped.push(&bytes[split..]).unwrap();
        scanner.finish(&mapped.source).unwrap();
        let output = encode::scratch(&mut mapped.writer, |encoder| encoder.json(&mapped.source, &scanner, 0)).unwrap();
        let actual: serde_json::Value = serde_json::from_slice(&scratch_bytes(&output)).unwrap();
        let expected: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(actual, expected, "split {split}");
    }
}

#[test]
fn json_duplicate_keys_compare_decoded_unicode_at_every_depth() {
    for bytes in [
        br#"{"a":1,"\u0061":2}"#.as_slice(),
        br#"{"nested":[{"\ud83d\ude00":null,"\uD83D\uDE00":false}]}"#,
        "{\"é\":1,\"\\u00e9\":2}".as_bytes(),
    ] {
        let mut mapped = Mapped::new();
        let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        let mut error = None;
        for byte in bytes {
            mapped.push(&[*byte]).unwrap();
            if let Err(found) = scanner.advance(&mapped.source) {
                error = Some(found);
                break;
            }
        }
        assert!(matches!(error, Some(Error::DuplicateKey)));
        assert!(matches!(scanner.finish(&mapped.source), Err(Error::Closed)));
    }
    let mut mapped = Mapped::new();
    mapped.push(br#"{"a":1,"nested":{"a":2},"other":{"a":3}}"#).unwrap();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    scanner.finish(&mapped.source).unwrap();
    let nested = scanner.member(&mapped.source, 0, "nested").unwrap().unwrap();
    let value = scanner.member(&mapped.source, nested, "a").unwrap().unwrap();
    assert!(mapped.source.equals(scanner.nodes()[value].span, b"2").unwrap());
}

#[test]
fn json_rejects_malformed_utf8_syntax_and_truncated_values() {
    let cases: &[&[u8]] = &[
        b"",
        b"{",
        b"[1,]",
        b"{\"a\":1,}",
        b"{\"a\" 1}",
        b"[true false]",
        b"01",
        b"-",
        b"1.",
        b"1e+",
        b"NaN",
        b"Infinity",
        b"true false",
        b"\"unterminated",
        b"\"\\uD800\"",
        b"\"\\uDC00\"",
        b"\"\\x41\"",
        b"\"raw\nline\"",
        b"\"\xc0\xaf\"",
        b"\"\xed\xa0\x80\"",
        b"\"\xf4\x90\x80\x80\"",
        b"\"\xe2\x82",
    ];
    for bytes in cases {
        for split in 0..=bytes.len() {
            let mut mapped = Mapped::new();
            let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
            mapped.push(&bytes[..split]).unwrap();
            let result = scanner.advance(&mapped.source).and_then(|()| {
                mapped.push(&bytes[split..])?;
                scanner.finish(&mapped.source)
            });
            assert!(result.is_err(), "accepted {bytes:?} at split {split}");
        }
    }
}

#[test]
fn json_cross_frame_string_and_source_positions_remain_exact() {
    let mut bytes = b"{\"payload\":\"".to_vec();
    bytes.extend(std::iter::repeat_n(b'a', ck_stream::FRAME_BYTES));
    bytes.extend_from_slice("🚀\",\"unknown\":{\"null\":null}}".as_bytes());
    let mut mapped = Mapped::new();
    mapped.push(&bytes).unwrap();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    scanner.finish(&mapped.source).unwrap();
    let payload = scanner.member(&mapped.source, 0, "payload").unwrap().unwrap();
    let span = scanner.nodes()[payload].span;
    let mut chunks = 0;
    mapped
        .source
        .visit(span, |_| {
            chunks += 1;
            Ok(())
        })
        .unwrap();
    assert!(chunks > 1);
    let output = encode::scratch(&mut mapped.writer, |encoder| encoder.json(&mapped.source, &scanner, 0)).unwrap();
    assert_eq!(scratch_bytes(&output), bytes);
}

#[test]
fn sse_multiline_data_and_unknown_provenance_survive_all_delimiters() {
    for newline in ["\n", "\r\n", "\r"] {
        let text = [
            ": comment",
            "id: upstream-7",
            "retry: 123",
            "vendor: keep",
            "event: surprise",
            "data: {\"text\":\"你好\",",
            "data: \"unknown\":[null,0]}",
            "",
            "",
        ]
        .join(newline);
        for split in 0..=text.len() {
            let mut mapped = Mapped::new();
            let mut scanner = sse::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
            let mut found = Vec::new();
            mapped.push(&text.as_bytes()[..split]).unwrap();
            scanner
                .advance(&mapped.source, |event| {
                    found.push(event.try_clone(&mut mapped.writer)?);
                    Ok(())
                })
                .unwrap();
            mapped.push(&text.as_bytes()[split..]).unwrap();
            scanner
                .finish(&mapped.source, |event| {
                    found.push(event.try_clone(&mut mapped.writer)?);
                    Ok(())
                })
                .unwrap();
            assert_eq!(found.len(), 1, "{newline:?}, split {split}");
            let event = &found[0];
            assert!(
                mapped
                    .source
                    .equals(event.name(&mapped.source).unwrap().unwrap(), b"surprise")
                    .unwrap()
            );
            let mut data = Vec::new();
            event
                .write_data(&mapped.source, |bytes| {
                    data.extend_from_slice(bytes);
                    Ok(())
                })
                .unwrap();
            assert_eq!(data, "{\"text\":\"你好\",\n\"unknown\":[null,0]}".as_bytes());
            assert_eq!(event.data_len(&mapped.source).unwrap(), data.len());
            assert!(event.fields()[0].comment);
            assert!(mapped.source.equals(event.fields()[3].name, b"vendor").unwrap());
            assert!(mapped.source.equals(event.fields()[3].value, b"keep").unwrap());
        }
    }
}

#[test]
fn sse_empty_data_and_metadata_records_are_distinct() {
    let mut mapped = Mapped::new();
    mapped.push(b": keepalive\n\ndata:\n\nevent: future\n\n\n").unwrap();
    let mut scanner = sse::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    let mut events = Vec::new();
    scanner
        .finish(&mapped.source, |event| {
            events.push(event.try_clone(&mut mapped.writer)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(events.len(), 4);
    assert!(!events[0].has_data(&mapped.source).unwrap());
    assert!(events[1].has_data(&mapped.source).unwrap());
    assert_eq!(events[1].data_len(&mapped.source).unwrap(), 0);
    assert!(events[2].name(&mapped.source).unwrap().is_some());
    assert!(events[3].fields().is_empty());
}

#[test]
fn sse_rejects_invalid_utf8_and_does_not_flush_truncated_records() {
    for bytes in [
        b"data: {}".as_slice(),
        b"data: {}\n",
        b"data: [DONE]\r",
        b": unfinished",
        b"data: \xff\n\n",
        b"data: \xf0\x90\x80",
    ] {
        let mut mapped = Mapped::new();
        let mut scanner = sse::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        let mut emitted = 0;
        let mut result: Result<()> = Ok(());
        for byte in bytes {
            mapped.push(&[*byte]).unwrap();
            result = scanner.advance(&mapped.source, |_| {
                emitted += 1;
                Ok(())
            });
            if result.is_err() {
                break;
            }
        }
        if result.is_ok() {
            result = scanner.finish(&mapped.source, |_| {
                emitted += 1;
                Ok(())
            });
        }
        assert!(result.is_err(), "accepted {bytes:?}");
        assert_eq!(emitted, 0);
    }
}

#[test]
fn limits_fail_before_publishing_an_encoded_record() {
    let mut mapped = Mapped::new();
    mapped.push(br#"{"a":1,"b":2}"#).unwrap();
    let mut scanner = json::Scanner::new(&mut mapped.writer, 4).unwrap();
    assert!(matches!(scanner.advance(&mapped.source), Err(Error::EventTooLarge)));
    let before = mapped.writer.committed();
    assert!(encode::scratch(&mut mapped.writer, |encoder| encoder.json(&mapped.source, &scanner, 0)).is_err());
    assert_eq!(mapped.writer.committed(), before);
}

struct TextDestination;

impl ck_translate::codec::Capability for TextDestination {
    fn destination(&self) -> &'static str {
        "text-destination"
    }

    fn validate(&self, request: ck_translate::codec::RequestView<'_>) -> Result<()> {
        if request.operation != ck_translate::codec::Operation::Generate {
            return Err(Error::Unsupported);
        }
        let doc = request.document;
        if let Some(modalities) = doc.member(doc.root(), "modalities")? {
            for index in doc.children(modalities)? {
                if !doc.string_is(index, "text")? {
                    return Err(Error::Unsupported);
                }
            }
        }
        Ok(())
    }
}

#[test]
fn request_end_rejects_late_semantics_before_authorizing_egress() {
    use ck_translate::codec::{Operation, RequestIngress, chat::request::Ingress, event::Direction};
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        BUDGET as u64,
    )
    .unwrap();
    ingress
        .advance(&mut mapped.writer, br#"{"model":"m","messages":[],"modalities":["#)
        .unwrap();
    let prefix = mapped.output(&mapped.writer);
    let prefix = std::str::from_utf8(&prefix).unwrap();
    assert!(prefix.contains("event: x_ck.request.field\n"));
    assert!(!prefix.contains("event: x_ck.request.end\n"));
    ingress.advance(&mut mapped.writer, br#""audio"]}"#).unwrap();
    assert!(matches!(
        ingress.finish(&mut mapped.writer, &TextDestination),
        Err(Error::Unsupported)
    ));
    let bytes = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(!text.contains("event: x_ck.request.end\n"));
    assert!(!text.contains("event: x_ck.done\n"));
    assert!(text.contains("event: x_ck.error\n"));
}

fn document(mapped: &mut Mapped, bytes: &[u8]) -> ck_translate::codec::document::Document {
    let mut source = ck_translate::codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, bytes).unwrap();
    ck_translate::codec::document::Document::parse(&mut mapped.writer, source, BUDGET as u64).unwrap()
}

#[test]
fn validated_request_binds_destination_operation_and_source() {
    use ck_translate::codec::{Operation, RequestFence, RequestView};
    let mut mapped = Mapped::new();
    let doc = document(
        &mut mapped,
        br#"{"model":"m","messages":[],"unknown":{"vendor":true},"modalities":["text"]}"#,
    );
    let media = ck_translate::codec::media::Registry::new(&mut mapped.writer).unwrap();
    let request = RequestView {
        document: &doc,
        media: &media,
        operation: Operation::Generate,
    };
    let validated = RequestFence::default().end(request, &TextDestination).unwrap();
    assert_eq!(
        validated.for_destination("text-destination").unwrap().operation,
        Operation::Generate
    );
    assert!(matches!(
        validated.for_destination("another-destination"),
        Err(Error::Destination)
    ));
    for operation in [Operation::CountTokens, Operation::Compact] {
        assert!(matches!(
            RequestFence::default().end(RequestView { operation, ..request }, &TextDestination),
            Err(Error::Unsupported)
        ));
    }
    let mut foreign = Mapped::new();
    assert!(matches!(doc.check_owner(&foreign.writer), Err(Error::Position)));
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    foreign.push(b"{}").unwrap();
    assert!(matches!(scanner.finish(&foreign.source), Err(Error::Position)));
}

#[test]
fn poisoned_or_rebound_scanners_cannot_resume() {
    let mut original = Mapped::new();
    original.push(b"{\"a\":").unwrap();
    let mut other = Mapped::new();
    other.push(b"{\"a\":1}").unwrap();
    let mut scanner = json::Scanner::new(&mut original.writer, BUDGET as u64).unwrap();
    scanner.advance(&original.source).unwrap();
    assert!(matches!(scanner.finish(&other.source), Err(Error::Position)));
    assert!(matches!(scanner.finish(&original.source), Err(Error::Closed)));

    let mut scanner = sse::Scanner::new(&mut original.writer, BUDGET as u64).unwrap();
    scanner.advance(&original.source, |_| Ok(())).unwrap();
    assert!(matches!(
        scanner.finish(&other.source, |_| Ok(())),
        Err(Error::Position)
    ));
    assert!(matches!(
        scanner.finish(&original.source, |_| Ok(())),
        Err(Error::Closed)
    ));
}

#[test]
fn expired_sources_cannot_encode_even_empty_containers() {
    for bytes in [b"{}".as_slice(), b"[]"] {
        let clock = std::sync::Arc::new(ck_stream::ManualClock::default());
        let mut mapped = Mapped::with_clock(clock.clone());
        mapped.push(bytes).unwrap();
        let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        scanner.finish(&mapped.source).unwrap();
        clock.advance(std::time::Duration::from_secs(3601));
        mapped.store.tick().unwrap();
        let output = &mut mapped.writer;
        let before = output.committed();
        assert!(matches!(
            encode::publish(output, 0, 0, |encoder| encoder.json(&mapped.source, &scanner, 0)),
            Err(Error::Storage(ck_stream::Error::Gone))
        ));
        assert_eq!(output.committed(), before);
    }
}

#[test]
fn sse_accepts_fragmented_initial_bom_without_changing_data() {
    let bytes = b"\xef\xbb\xbfdata: value\r\n\r\n";
    for split in 0..=bytes.len() {
        let mut mapped = Mapped::new();
        let mut scanner = sse::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
        let mut events = Vec::new();
        mapped.push(&bytes[..split]).unwrap();
        scanner
            .advance(&mapped.source, |event| {
                events.push(event.try_clone(&mut mapped.writer)?);
                Ok(())
            })
            .unwrap();
        mapped.push(&bytes[split..]).unwrap();
        scanner
            .finish(&mapped.source, |event| {
                events.push(event.try_clone(&mut mapped.writer)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(events.len(), 1);
        let mut data = Vec::new();
        events[0]
            .write_data(&mapped.source, |bytes| {
                data.extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(data, b"value");
        let foreign = Mapped::new();
        assert!(matches!(
            events[0].write_data(&foreign.source, |_| Ok(())),
            Err(Error::Position)
        ));
    }
}

fn identity(direction: ck_translate::codec::event::Direction) -> ck_translate::codec::event::Identity {
    use ck_translate::codec::event::{Identity, LocalId};
    Identity {
        request_id: LocalId::new("request-1").unwrap(),
        stream_id: LocalId::new("stream-1").unwrap(),
        attempt: 3,
        direction,
    }
}

#[test]
fn canonical_events_bind_sequence_and_keep_wire_control_fields_opaque() {
    use ck_translate::codec::event::{Direction, Emitter, Kind, Scope};
    let mut mapped = Mapped::new();
    mapped
        .push(br#"{"x_ck":{"v":99,"attempt":999,"media":[{"media_id":"spoof"}]},"unknown":[null,0]}"#)
        .unwrap();
    let mut json = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    json.finish(&mapped.source).unwrap();
    let output = &mut mapped.writer;
    let mut emitter = Emitter::new(identity(Direction::Response));
    emitter
        .emit(
            output,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(br#", "id":"native","model":"m","created":0"#),
            |_| Ok(()),
        )
        .unwrap();
    emitter
        .emit(
            output,
            Kind::Opaque,
            Scope {
                choice_index: Some(0),
                ..Scope::default()
            },
            |e| {
                e.bytes(b",\"name\":\"vendor\",\"format\":\"json\",\"value\":")?;
                e.json(&mapped.source, &json, 0)
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"chat\",\"event\":\"vendor\"}"),
        )
        .unwrap();
    emitter
        .emit(output, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))
        .unwrap();
    let before = output.committed();
    assert!(matches!(
        emitter.emit(output, Kind::Heartbeat, Scope::default(), |_| Ok(()), |_| Ok(())),
        Err(Error::Closed)
    ));
    assert_eq!(output.committed(), before);
    let bytes = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&bytes).unwrap();
    let records: Vec<_> = text.trim_end().split("\n\n").collect();
    assert_eq!(records.len(), 3);
    for (sequence, record) in records.iter().enumerate() {
        let id = record.lines().find_map(|line| line.strip_prefix("id: ")).unwrap();
        let value: serde_json::Value =
            serde_json::from_str(record.lines().find_map(|line| line.strip_prefix("data: ")).unwrap()).unwrap();
        assert_eq!(id.parse::<usize>().unwrap(), sequence);
        assert_eq!(value["x_ck"]["seq"], sequence);
        assert_eq!(value["x_ck"]["attempt"], 3);
        assert_eq!(value["x_ck"]["v"], 1);
        assert_eq!(value["x_ck"]["request_id"], "request-1");
        if sequence == 1 {
            assert_eq!(value["value"]["x_ck"]["attempt"], 999);
            assert_eq!(value["value"]["unknown"], serde_json::json!([null, 0]));
            assert_eq!(value["x_ck"]["choice_index"], 0);
        }
    }
}

#[test]
fn canonical_request_end_requires_its_own_validated_document() {
    use ck_translate::codec::{
        Operation, RequestFence, RequestView,
        event::{Direction, Emitter, Kind, Scope},
    };
    let mut mapped = Mapped::new();
    let doc = document(&mut mapped, b"{\"modalities\":[\"text\"]}");
    let media = ck_translate::codec::media::Registry::new(&mut mapped.writer).unwrap();
    let view = RequestView {
        document: &doc,
        media: &media,
        operation: Operation::Generate,
    };
    let output = &mut mapped.writer;
    let mut emitter = Emitter::new(identity(Direction::Request));
    emitter.request_start(output, view).unwrap();
    assert!(matches!(
        emitter.emit(output, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(())),
        Err(Error::Schema)
    ));
    assert!(matches!(
        emitter.emit(output, Kind::RequestEnd, Scope::default(), |_| Ok(()), |_| Ok(())),
        Err(Error::Schema)
    ));
    let mut foreign = Mapped::new();
    let foreign_doc = document(&mut foreign, b"{}");
    let foreign_media = ck_translate::codec::media::Registry::new(&mut foreign.writer).unwrap();
    let foreign_ready = RequestFence::default()
        .end(
            RequestView {
                document: &foreign_doc,
                media: &foreign_media,
                operation: Operation::Generate,
            },
            &TextDestination,
        )
        .unwrap();
    assert!(matches!(
        emitter.request_end(output, &foreign_ready),
        Err(Error::Position)
    ));
    let ready = RequestFence::default().end(view, &TextDestination).unwrap();
    emitter.request_end(output, &ready).unwrap();
    emitter
        .emit(output, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))
        .unwrap();
    let bytes = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert_eq!(text.matches("event: x_ck.request.end\n").count(), 1);
    assert!(text.ends_with("\n\n"));
}

#[test]
fn canonical_media_data_fits_one_frame_and_failed_output_is_not_published() {
    use ck_translate::codec::event::{Direction, Emitter, Kind, LocalId, Scope};
    let mut mapped = Mapped::new();
    let output = &mut mapped.writer;
    let mut emitter = Emitter::new(identity(Direction::Response));
    emitter
        .emit(
            output,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(b",\"id\":\"native\",\"model\":\"m\",\"created\":0"),
            |_| Ok(()),
        )
        .unwrap();
    let scope = Scope {
        media_id: Some(LocalId::new("media-1").unwrap()),
        ..Scope::default()
    };
    emitter
        .emit(
            output,
            Kind::MediaStart,
            scope,
            |e| e.bytes(b",\"kind\":\"audio\",\"mime\":\"audio/pcm\",\"encoding\":\"base64\""),
            |_| Ok(()),
        )
        .unwrap();
    let before = output.committed();
    let too_large = "A".repeat(ck_stream::FRAME_BYTES);
    assert!(matches!(
        emitter.emit(
            output,
            Kind::Data,
            scope,
            |e| {
                e.bytes(b",\"index\":0,\"offset\":0,\"bytes\":")?;
                e.string(&too_large)
            },
            |_| Ok(())
        ),
        Err(Error::EventTooLarge)
    ));
    assert_eq!(output.committed(), before);
    let data = "A".repeat(49_152);
    let record = emitter
        .emit(
            output,
            Kind::Data,
            scope,
            |e| {
                e.bytes(b",\"index\":0,\"offset\":0,\"bytes\":")?;
                e.string(&data)
            },
            |_| Ok(()),
        )
        .unwrap();
    assert_eq!(record.start.offset, 0);
    assert!(record.len() <= ck_stream::FRAME_BYTES as u64);
    let bytes = mapped.output(&mapped.writer);
    let text = std::str::from_utf8(&bytes).unwrap();
    let value: serde_json::Value = serde_json::from_str(
        text.lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .next_back()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["x_ck"]["seq"], 2);
    use base64::Engine;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(value["bytes"].as_str().unwrap())
            .unwrap(),
        vec![0; 36_864]
    );
}

fn scratch_bytes(lease: &ck_stream::ScratchLease) -> Vec<u8> {
    let mut result = Vec::new();
    while result.len() < lease.len() {
        result.extend_from_slice(lease.chunk(result.len()).unwrap());
    }
    result
}

#[test]
fn scratch_input_and_external_output_do_not_publish_or_release_canonical_bytes() {
    use ck_translate::codec::event::{Direction, Emitter, Kind, Scope};
    let mut mapped = Mapped::new();
    mapped.push(b"{\"text\":\"kept\"}").unwrap();
    assert!(mapped.output(&mapped.writer).is_empty());
    let mut emitter = Emitter::new(identity(Direction::Response));
    emitter
        .emit(
            &mut mapped.writer,
            Kind::ResponseStart,
            Scope::default(),
            |e| e.bytes(b",\"id\":\"r\",\"model\":\"m\",\"created\":0"),
            |_| Ok(()),
        )
        .unwrap();
    let before = mapped.output(&mapped.writer);
    let external = encode::scratch(&mut mapped.writer, |e| e.bytes(b"external")).unwrap();
    mapped.source.release_before(mapped.source.len()).unwrap();
    assert_eq!(mapped.output(&mapped.writer), before);
    assert_eq!(scratch_bytes(&external), b"external");
}

#[test]
fn empty_sources_and_scanners_reject_foreign_streams() {
    let mut own = Mapped::new();
    let mut foreign = Mapped::new();
    assert!(matches!(
        own.source.push_bytes(&mut foreign.writer, b""),
        Err(Error::Position)
    ));
    let mut scanner = json::Scanner::new(&mut own.writer, BUDGET as u64).unwrap();
    foreign.push(b"{}").unwrap();
    assert!(matches!(scanner.finish(&foreign.source), Err(Error::Position)));
}

#[test]
fn scanner_metadata_competes_with_other_permits_and_releases_on_drop() {
    let mut mapped = Mapped::new();
    mapped.push(b"{\"a\":1,\"b\":2}").unwrap();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    scanner.finish(&mapped.source).unwrap();
    drop(scanner);
    let permit = mapped.writer.reserve_metadata(4 * 1024 * 1024 - 4096).unwrap();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    // Hundreds of tiny members exhaust shared bookkeeping before payload storage.
    let input = format!("[{}]", std::iter::repeat_n("0", 500).collect::<Vec<_>>().join(","));
    let mut source = ck_translate::codec::source::Source::new(&mut mapped.writer).unwrap();
    source.push_bytes(&mut mapped.writer, input.as_bytes()).unwrap();
    assert!(matches!(
        scanner.finish(&source),
        Err(Error::Storage(ck_stream::Error::StorageExhausted))
    ));
    drop(permit);
}

#[test]
fn incomplete_scanner_exposes_only_validated_subtrees_of_its_live_prefix() {
    let mut mapped = Mapped::new();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    mapped.push(br#"{"first":{"text":"ready"},"later":["#).unwrap();
    scanner.advance(&mapped.source).unwrap();
    assert!(scanner.completed_node(&mapped.source, 0).unwrap().is_none());
    let root = scanner.pending_node(&mapped.source, 0).unwrap();
    let first = root.first.unwrap();
    let value = scanner.completed_node(&mapped.source, first).unwrap().unwrap();
    assert!(mapped.source.equals(value.span, br#"{"text":"ready"}"#).unwrap());
    assert!(
        scanner
            .completed_node(&mapped.source, value.next.unwrap())
            .unwrap()
            .is_none()
    );
    let mut other = codec_source(&mut mapped.writer, br#"{"first":{"text":"ready"},"later":["#);
    assert!(matches!(scanner.completed_node(&other, first), Err(Error::Position)));
    other.push_bytes(&mut mapped.writer, b"]}").unwrap();
    mapped.push(b"]}").unwrap();
    assert!(matches!(
        scanner.completed_node(&mapped.source, first),
        Err(Error::Position)
    ));
    scanner.finish(&mapped.source).unwrap();
    assert!(scanner.completed_node(&mapped.source, 0).unwrap().is_some());
}

fn codec_source(owner: &mut ck_stream::Writer, bytes: &[u8]) -> ck_translate::codec::source::Source {
    let mut source = ck_translate::codec::source::Source::new(owner).unwrap();
    source.push_bytes(owner, bytes).unwrap();
    source
}

#[test]
fn poisoned_scanner_cannot_publish_previously_completed_subtrees() {
    let mut mapped = Mapped::new();
    let mut scanner = json::Scanner::new(&mut mapped.writer, BUDGET as u64).unwrap();
    mapped.push(br#"{"first":"ready","#).unwrap();
    scanner.advance(&mapped.source).unwrap();
    let first = scanner.pending_node(&mapped.source, 0).unwrap().first.unwrap();
    mapped.push(br#""first":false}"#).unwrap();
    assert!(matches!(scanner.advance(&mapped.source), Err(Error::DuplicateKey)));
    assert!(matches!(
        scanner.completed_node(&mapped.source, first),
        Err(Error::Closed)
    ));
}

#[test]
fn trusted_decoder_rejects_truncated_media_records_before_projection() {
    use ck_translate::codec::{event::Kind, internal};
    use std::task::Poll;
    let valid = b"event: x_ck.data\nid: 0\ndata: {\"x_ck\":{\"v\":1,\"seq\":0,\"media_id\":\"m0\"},\"index\":0,\"offset\":0,\"bytes\":\"AQID\"}\n\n";
    for removed in [1, 2, 3, 8] {
        let mut mapped = Mapped::new();
        encode::publish(&mut mapped.writer, Kind::Data as u32, 0, |e| {
            e.bytes(&valid[..valid.len() - removed])
        })
        .unwrap();
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() else {
            panic!("missing trusted record")
        };
        assert!(matches!(
            internal::decode(&mut mapped.writer, lease),
            Err(Error::Truncated | Error::Schema)
        ));
    }
}
