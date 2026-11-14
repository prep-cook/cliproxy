use super::{codec, response};
use serde_json::{Value, json};

fn stream(updates: &[Value]) -> Vec<u8> {
    let mut bytes = b"data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n".to_vec();
    for usage in updates {
        bytes.extend_from_slice(b"data: ");
        bytes.extend(serde_json::to_vec(&json!({"choices":[],"usage":usage})).unwrap());
        bytes.extend_from_slice(b"\n\n");
    }
    bytes.extend_from_slice(b"data: [DONE]\n\n");
    bytes
}

#[test]
fn usage_details_corrections_null_and_zero_replace_without_summing() {
    let updates = [
        json!({"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":2,"audio_tokens":1,"vendor":{"a":0}},"completion_tokens_details":{"reasoning_tokens":3,"audio_tokens":1,"accepted_prediction_tokens":5,"rejected_prediction_tokens":2,"text_tokens":2,"image_tokens":1}}),
        json!({"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":null,"vendor":{"b":false}},"completion_tokens_details":{"accepted_prediction_tokens":null,"rejected_prediction_tokens":0}}),
        json!({"prompt_tokens":null}),
        json!({"prompt_tokens":0,"completion_tokens":0}),
        json!({"total_tokens":null}),
    ];
    for mode in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let (output, events) = response(&stream(&updates), codec::BodyMode::Streaming, mode);
        let usage: Vec<_> = events.iter().filter_map(|e| e.get("usage")).collect();
        assert_eq!(usage.len(), 6);
        assert_eq!(usage[0]["total_tokens"], 14);
        assert_eq!(usage[0]["details"]["accepted_prediction_tokens"], 5);
        assert_eq!(usage[0]["details"]["output_text_tokens"], 2);
        assert_eq!(usage[1]["total_tokens"], 17);
        assert!(usage[1].get("cache_read_tokens").is_none());
        assert!(usage[1]["details"].get("accepted_prediction_tokens").is_none());
        assert_eq!(usage[1]["details"]["rejected_prediction_tokens"], 0);
        assert_eq!(
            usage[1]["native"]["value"]["prompt_tokens_details"]["vendor"],
            json!({"a":0,"b":false})
        );
        assert!(usage[2].get("input_tokens").is_none());
        assert!(usage[2].get("total_tokens").is_none());
        assert_eq!(usage[3]["total_tokens"], 0);
        assert!(usage[4].get("total_tokens").is_none());
        assert_eq!(usage[5]["final"], true);
        assert_eq!(usage[5]["native"]["value"]["total_tokens"], Value::Null);
        if mode == codec::BodyMode::Buffered {
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(output["usage"]["total_tokens"], Value::Null);
        } else {
            let text = std::str::from_utf8(&output).unwrap();
            assert_eq!(text.lines().filter(|line| line.contains("\"usage\":")).count(), 1);
        }
    }
}

#[test]
fn whole_usage_null_clears_counts_and_detail_null_clears_subsets() {
    for updates in [
        vec![json!({"prompt_tokens":1,"completion_tokens":2}), Value::Null],
        vec![
            json!({"prompt_tokens_details":{"audio_tokens":1},"completion_tokens_details":{"reasoning_tokens":2,"accepted_prediction_tokens":3}}),
            json!({"prompt_tokens_details":null,"completion_tokens_details":null}),
        ],
    ] {
        let (output, events) = response(&stream(&updates), codec::BodyMode::Streaming, codec::BodyMode::Buffered);
        let final_usage = events.iter().filter_map(|e| e.get("usage")).next_back().unwrap();
        for field in [
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "reasoning_tokens",
            "input_audio_tokens",
            "details",
        ] {
            assert!(final_usage.get(field).is_none(), "stale {field}");
        }
        if updates[1].is_null() {
            let output: Value = serde_json::from_slice(&output).unwrap();
            assert_eq!(output["usage"], Value::Null);
        }
    }
}

#[test]
fn choice_and_stream_usage_finalize_separately_in_all_response_modes() {
    let buffered = br#"{"id":"r","model":"m","created":1,"choices":[{"index":2,"message":{"content":"two"},"usage":{"prompt_tokens":3,"completion_tokens":1},"finish_reason":"stop"},{"index":0,"message":{"content":"zero"},"usage":{"prompt_tokens":4,"completion_tokens":2},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}"#;
    let streaming = b"data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":2,\"delta\":{\"content\":\"two\"},\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1},\"finish_reason\":\"stop\"},{\"index\":0,\"delta\":{\"content\":\"zero\"},\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n";
    for (input, source) in [
        (buffered.as_slice(), codec::BodyMode::Buffered),
        (streaming.as_slice(), codec::BodyMode::Streaming),
    ] {
        for destination in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
            let (output, events) = response(input, source, destination);
            let finals: Vec<_> = events.iter().filter(|e| e["usage"]["final"] == true).collect();
            assert_eq!(finals.len(), 3);
            assert_eq!(finals[0]["x_ck"]["choice_index"], 2);
            assert_eq!(finals[0]["usage"]["total_tokens"], 4);
            assert_eq!(finals[1]["x_ck"]["choice_index"], 0);
            assert_eq!(finals[1]["usage"]["total_tokens"], 6);
            assert!(finals[2]["x_ck"].get("choice_index").is_none());
            assert_eq!(finals[2]["usage"]["total_tokens"], 10);
            if destination == codec::BodyMode::Buffered {
                let output: Value = serde_json::from_slice(&output).unwrap();
                assert_eq!(output["choices"][0]["usage"]["total_tokens"], 4);
                assert_eq!(output["choices"][1]["usage"]["total_tokens"], 6);
                assert_eq!(output["usage"]["total_tokens"], 10);
            }
        }
    }
}

#[test]
fn failed_usage_update_finalizes_prior_snapshot_without_wrapping_or_partial_mutation() {
    use codec::ResponseIngress;
    let mut mapped = super::Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        super::identity(super::Direction::Response),
        codec::Operation::Generate,
        codec::BodyMode::Streaming,
        4096,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n",
        )
        .unwrap();
    assert!(ingress.advance(&mut mapped.writer, b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":18446744073709551615,\"completion_tokens\":1}}\n\n").is_err());
    let events = super::canonical_values(&mapped);
    let usage = events.iter().filter_map(|e| e.get("usage")).next_back().unwrap();
    assert_eq!(usage["input_tokens"], 2);
    assert_eq!(usage["output_tokens"], 3);
    assert_eq!(usage["total_tokens"], 5);
    assert_eq!(usage["final"], true);
    let output = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(output.contains("event: x_ck.error\n"));
    assert!(!output.contains("event: x_ck.done\n"));
}

#[test]
fn replaying_canonical_response_does_not_publish_an_accounting_observation() {
    use codec::{ResponseEgress, ResponseIngress};
    use std::task::Poll;
    let mut mapped = super::Mapped::new();
    let id = super::identity(super::Direction::Response);
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        id,
        codec::Operation::Generate,
        codec::BodyMode::Streaming,
        4096,
    )
    .unwrap();
    ingress
        .advance(
            &mut mapped.writer,
            &stream(&[json!({"prompt_tokens":2,"completion_tokens":3})]),
        )
        .unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let before = mapped.output(&mapped.writer);
    let mut outputs = Vec::new();
    for _ in 0..2 {
        let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
        let mut egress = codec::chat::egress::Egress::new(
            &mut mapped.writer,
            id,
            codec::Operation::Generate,
            codec::BodyMode::Buffered,
            true,
            |_| panic!("unexpected loss"),
        )
        .unwrap();
        let mut output = Vec::new();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut mapped.writer, lease, |lease| {
                    output.extend(super::bytes(&lease));
                    Ok(())
                })
                .unwrap();
        }
        egress.finish().unwrap();
        outputs.push(output);
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(mapped.output(&mapped.writer), before);
    let output: Value = serde_json::from_slice(&outputs[0]).unwrap();
    assert_eq!(output["usage"]["total_tokens"], 5);
}
