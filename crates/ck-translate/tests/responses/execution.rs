use super::*;

fn wire(records: &[Value]) -> String {
    records.iter().map(|record| format!("data: {record}\n\n")).collect()
}

#[test]
fn native_server_execution_preserves_results_and_distinct_tool_identity() {
    let input = json!({"id":"r","model":"m","status":"completed","output":[
        {"id":"ci","type":"code_interpreter_call","status":"completed","container_id":"container","code":"print(1)","outputs":[{"type":"logs","logs":"1"}]},
        {"id":"ws","type":"web_search_call","status":"completed","action":{"type":"search","query":"q","sources":[{"type":"url","url":"https://example.test"}]},"results":[]},
        {"id":"fs","type":"file_search_call","status":"completed","queries":["q"],"results":[{"file_id":"f","text":"hit","score":0.7}]},
        {"id":"mc","type":"mcp_call","name":"lookup","arguments":"{}","server_label":"remote","status":"failed","output":null,"error":{"message":"denied"}}
    ]});
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let (output, canonical) = response(
            &serde_json::to_vec(&input).unwrap(),
            BodyMode::Buffered,
            mode,
            Operation::Generate,
        );
        let out: Value = if mode == BodyMode::Buffered {
            serde_json::from_slice(&output).unwrap()
        } else {
            String::from_utf8(output)
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .find(|event| event["type"] == "response.completed")
                .unwrap()["response"]
                .clone()
        };
        assert_eq!(out["output"], input["output"]);
        let starts: Vec<_> = canonical.iter().filter(|event| event["kind"] == "server").collect();
        assert_eq!(starts.len(), 4);
        for start in starts {
            assert_ne!(start["x_ck"]["item_id"], start["x_ck"]["tool_call_id"]);
        }
        assert!(
            canonical
                .iter()
                .any(|event| event["phase"] == "code" && event["value"] == "print(1)")
        );
        assert!(
            canonical
                .iter()
                .any(|event| event["value"] == input["output"][2]["results"])
        );
        assert!(
            canonical
                .iter()
                .any(|event| event["is_error"] == true && event["value"] == input["output"][3]["error"])
        );
    }
}

#[test]
fn streamed_code_reconciles_done_and_terminal_snapshots_without_replay() {
    let item = json!({"id":"ci","type":"code_interpreter_call","status":"completed","container_id":"c","code":"print(1)","outputs":[{"type":"logs","logs":"1"}]});
    let records = vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"ci","type":"code_interpreter_call","status":"in_progress","container_id":"c","code":null,"outputs":null}}),
        json!({"type":"response.code_interpreter_call_code.delta","output_index":0,"item_id":"ci","delta":"print("}),
        json!({"type":"response.code_interpreter_call_code.done","output_index":0,"item_id":"ci","code":"print(1)"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item]}}),
    ];
    let (output, canonical) = response(
        wire(&records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["output"][0], item);
    let code: String = canonical
        .iter()
        .filter(|event| event["phase"] == "code")
        .filter_map(|event| event["value"].as_str())
        .collect();
    assert_eq!(code, "print(1)");
    assert_eq!(
        canonical
            .iter()
            .filter(|event| event["value"] == item["outputs"])
            .count(),
        1
    );
    let mut bad = records;
    bad.insert(
        3,
        json!({"type":"response.code_interpreter_call_code.delta","output_index":0,"item_id":"ci","delta":"late"}),
    );
    let mut mapped = Mapped::new();
    let mut ingress = responses::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        100_000,
    )
    .unwrap();
    assert!(ingress.advance(&mut mapped.writer, wire(&bad).as_bytes()).is_err());
    let out = String::from_utf8(mapped.output(&mapped.writer)).unwrap();
    assert!(out.contains("event: x_ck.error\n"));
    assert!(!out.contains("event: x_ck.done\n"));
}

#[test]
fn mcp_arguments_and_early_search_results_survive_partial_completions() {
    let result = json!([{"file_id":"f","text":"early result"}]);
    let records = vec![
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"fs","type":"file_search_call","results":result}}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"id":"fs","type":"file_search_call","status":"completed"}}),
        json!({"type":"response.output_item.added","output_index":1,"item":{"id":"mc","type":"mcp_call","name":"lookup","server_label":"s","arguments":""}}),
        json!({"type":"response.mcp_call_arguments.delta","output_index":1,"item_id":"mc","delta":"{\"q\":"}),
        json!({"type":"response.mcp_call_arguments.done","output_index":1,"item_id":"mc","arguments":"{\"q\":1}"}),
        json!({"type":"response.output_item.done","output_index":1,"item":{"id":"mc","type":"mcp_call","status":"completed","output":"found","error":null}}),
        json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[{"id":"fs","type":"file_search_call"},{"id":"mc","type":"mcp_call"}]}}),
    ];
    let (output, canonical) = response(
        wire(&records).as_bytes(),
        BodyMode::Streaming,
        BodyMode::Buffered,
        Operation::Generate,
    );
    let output: Value = serde_json::from_slice(&output).unwrap();
    assert_eq!(output["output"][0]["results"], result);
    assert_eq!(output["output"][1]["arguments"], "{\"q\":1}");
    assert_eq!(output["output"][1]["output"], "found");
    assert_eq!(output["output"][1]["error"], Value::Null);
    let result_position = canonical.iter().position(|event| event["value"] == "found").unwrap();
    let call = &canonical[result_position]["x_ck"]["tool_call_id"];
    assert!(
        canonical[result_position + 1..]
            .iter()
            .any(|event| event["x_ck"]["tool_call_id"] == *call && event["status"] == "completed")
    );
    assert!(
        !canonical
            .iter()
            .any(|event| event["value"].is_null() && event["is_error"] == true)
    );
}
