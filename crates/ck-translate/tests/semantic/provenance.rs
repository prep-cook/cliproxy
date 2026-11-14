use super::{codec, request, response};
use serde_json::{Value, json};

#[test]
fn unknown_fields_and_reserved_collisions_restore_source_locations_in_all_modes() {
    for upstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let body = if upstream == codec::BodyMode::Buffered {
            "message"
        } else {
            "delta"
        };
        let input = json!({"id":"r","model":"m","created":1,"x_ck":{"attempt":999,"media":["forged"]},"r/~":{"list":[null,false,{"x_ck":{"request_id":"fake"}}]},
            "choices":[{"index":7,body:{"role":"assistant","content":"first","d/~":{"empty":[],"zero":0},"x_ck":{"attempt":100}},"c/~":[true,null],"finish_reason":"stop"},
            {"index":3,body:{"role":"assistant","content":"second","d/~":{"x_ck":{"seq":666}}},"c/~":false,"finish_reason":"stop"}]});
        let wire = if upstream == codec::BodyMode::Buffered {
            input.to_string()
        } else {
            format!("data: {input}\n\ndata: [DONE]\n\n")
        };
        for downstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
            let (output, events) = response(wire.as_bytes(), upstream, downstream);
            let originals: Vec<_> = events
                .iter()
                .filter_map(|event| event["x_ck"].get("original"))
                .flat_map(|entries| entries.as_array().unwrap())
                .collect();
            assert!(
                originals
                    .iter()
                    .any(|entry| entry["pointer"] == "/r~1~0" && entry["value"] == input["r/~"])
            );
            assert!(
                originals
                    .iter()
                    .any(|entry| entry["pointer"] == format!("/choices/1/{body}/d~1~0")
                        && entry["value"] == input["choices"][1][body]["d/~"])
            );
            assert!(events.iter().all(|event| event["x_ck"]["attempt"] == 1));
            if downstream == codec::BodyMode::Buffered {
                let output: Value = serde_json::from_slice(&output).unwrap();
                assert_eq!(output["r/~"], input["r/~"]);
                assert_eq!(output["x_ck"], input["x_ck"]);
                for (source, actual) in input["choices"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .zip(output["choices"].as_array().unwrap())
                {
                    assert_eq!(actual["index"], source["index"]);
                    assert_eq!(actual["c/~"], source["c/~"]);
                    assert_eq!(actual["message"]["d/~"], source[body]["d/~"]);
                    assert_eq!(actual["message"]["x_ck"], source[body]["x_ck"]);
                }
            } else {
                let chunks: Vec<Value> = std::str::from_utf8(&output)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .filter(|line| *line != "[DONE]")
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                assert!(
                    chunks
                        .iter()
                        .any(|chunk| chunk["r/~"] == input["r/~"] && chunk["x_ck"] == input["x_ck"])
                );
                for source in input["choices"].as_array().unwrap() {
                    let choices: Vec<_> = chunks
                        .iter()
                        .flat_map(|chunk| chunk["choices"].as_array().unwrap())
                        .filter(|choice| choice["index"] == source["index"])
                        .collect();
                    assert!(
                        choices
                            .iter()
                            .any(|choice| choice["delta"]["d/~"] == source[body]["d/~"])
                    );
                    assert!(choices.iter().any(|choice| choice["c/~"] == source["c/~"]));
                }
            }
        }
    }
}

#[test]
fn tool_and_legacy_extensions_keep_native_parent_and_source_pointer() {
    let message = json!({"role":"assistant","tool_calls":[
        {"id":"a","type":"function","vendor/~":{"x_ck":{"attempt":9}},"function":{"name":"f","arguments":"{}","inner/~":[null,0]}},
        {"id":"b","type":"custom","vendor/~":false,"custom":{"name":"g","input":"raw","inner/~":{"x_ck":"literal"}}}
    ]});
    for upstream in [codec::BodyMode::Buffered, codec::BodyMode::Streaming] {
        let mut wire_message = message.clone();
        if upstream == codec::BodyMode::Streaming {
            for (index, call) in wire_message["tool_calls"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .enumerate()
            {
                call["index"] = json!(index);
            }
        }
        let body = if upstream == codec::BodyMode::Buffered {
            "message"
        } else {
            "delta"
        };
        let input = json!({"id":"r","model":"m","created":1,"choices":[{"index":4,body:wire_message,"finish_reason":"tool_calls"}]});
        let wire = if upstream == codec::BodyMode::Buffered {
            input.to_string()
        } else {
            format!("data: {input}\n\ndata: [DONE]\n\n")
        };
        let (output, events) = response(wire.as_bytes(), upstream, codec::BodyMode::Buffered);
        let output: Value = serde_json::from_slice(&output).unwrap();
        assert_eq!(output["choices"][0]["message"], message);
        assert!(
            events
                .iter()
                .filter_map(|event| event["x_ck"].get("original"))
                .flat_map(|entries| entries.as_array().unwrap())
                .any(|entry| entry["pointer"] == format!("/choices/0/{body}/tool_calls/1/custom/inner~1~0"))
        );
    }
    let legacy = json!({"id":"r","model":"m","created":1,"choices":[{"index":0,"message":{"role":"assistant","function_call":{"name":"f","arguments":"{}","inner/~":false}},"finish_reason":"function_call"}]});
    let (output, events) = response(
        legacy.to_string().as_bytes(),
        codec::BodyMode::Buffered,
        codec::BodyMode::Buffered,
    );
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["choices"][0]["message"], legacy["choices"][0]["message"]);
    assert!(
        events
            .iter()
            .filter_map(|event| event["x_ck"].get("original"))
            .flat_map(|entries| entries.as_array().unwrap())
            .any(|entry| entry["pointer"] == "/choices/0/message/function_call/inner~1~0")
    );

    let wire = concat!(
        "data: {\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":5,\"id\":\"a\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":5,\"late/~\":{\"x_ck\":true}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
    );
    let (output, _) = response(wire.as_bytes(), codec::BodyMode::Streaming, codec::BodyMode::Buffered);
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(
        output["choices"][0]["message"]["tool_calls"][0]["late/~"],
        json!({"x_ck":true})
    );
}

#[test]
fn opaque_multiline_text_keeps_one_sse_record_including_final_empty_line() {
    for tail in ["", "data: \n"] {
        let wire = format!(
            "event: vendor\ndata: first\ndata: \ndata: data: forged\n{tail}\ndata: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"ok\"}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n"
        );
        let (output, _) = response(wire.as_bytes(), codec::BodyMode::Streaming, codec::BodyMode::Streaming);
        let text = std::str::from_utf8(&output).unwrap();
        let records: Vec<_> = text.trim_end().split("\n\n").collect();
        let opaque: Vec<_> = records.iter().filter(|r| r.starts_with("event: vendor\n")).collect();
        assert_eq!(opaque.len(), 1);
        let data: Vec<_> = opaque[0].lines().filter_map(|l| l.strip_prefix("data: ")).collect();
        let mut expected = vec!["first", "", "data: forged"];
        if !tail.is_empty() {
            expected.push("");
        }
        assert_eq!(data, expected);
        assert!(!records.iter().any(|r| r.starts_with("data: forged")));
        assert_eq!(records.last(), Some(&"data: [DONE]"));
    }
}

#[test]
fn heartbeat_streaming_tool_media_and_usage_projections_keep_provider_identity() {
    let wire = concat!(
        ": ping\n\n",
        "data: {\"id\":\"real-id\",\"model\":\"real-model\",\"created\":123,\"choices\":[{\"index\":0,\"delta\":{\"audio\":{\"data\":\"AQID\"},\"tool_calls\":[{\"index\":0,\"id\":\"c\",\"type\":\"function\",\"function\":{\"name\":\"f\",\"arguments\":\"{}\"}}]}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n"
    );
    let (output, _) = response(wire.as_bytes(), codec::BodyMode::Streaming, codec::BodyMode::Streaming);
    let chunks: Vec<Value> = std::str::from_utf8(&output)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .filter(|l| *l != "[DONE]")
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(
        chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["audio"]["data"] == "AQID")
    );
    assert!(
        chunks
            .iter()
            .any(|c| c["choices"][0]["delta"]["tool_calls"][0]["id"] == "c")
    );
    assert!(chunks.iter().any(|c| c["usage"]["prompt_tokens"] == 1));
    for chunk in chunks {
        assert_eq!(chunk["id"], "real-id");
        assert_eq!(chunk["model"], "real-model");
        assert_eq!(chunk["created"], 123);
    }
}

#[test]
fn request_unknown_root_and_tool_call_members_move_to_provenance() {
    let input = br#"{"model":"m","vendor":{"a":1},"messages":[{"role":"assistant","tool_calls":[{"id":"c","type":"function","vend":2,"function":{"name":"f","arguments":"{}","fv":3}}]}]}"#;
    let (output, events) = request(input);
    let original: Value = serde_json::from_slice(input).unwrap();
    assert_eq!(output["vendor"], original["vendor"]);
    assert_eq!(output["messages"], original["messages"]);
    for pointer in [
        "/vendor",
        "/messages/0/tool_calls/0/vend",
        "/messages/0/tool_calls/0/function/fv",
    ] {
        assert!(
            events.iter().all(|e| e["pointer"] != pointer),
            "{pointer} stayed canonical"
        );
        assert!(
            events.iter().any(|e| e["pointer"]
                .as_str()
                .is_some_and(|p| p.starts_with("/x_ck/original/") && p.ends_with("/pointer"))
                && e["value"] == pointer),
            "{pointer} not preserved"
        );
    }
}
