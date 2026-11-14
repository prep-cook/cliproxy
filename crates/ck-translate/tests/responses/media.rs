use super::*;

#[test]
fn request_media_uses_canonical_data_without_inline_copy() {
    use base64::Engine;
    let raw: Vec<u8> = (0..80_003).map(|i| (i % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    // The decoy has the shape of an input part outside `input` and must stay literal.
    let input = json!({"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":format!("data:image/png;base64,{encoded}"),"detail":"high","ref":"literal"}]}],
        "prompt":{"item":{"content":[{"type":"input_image","image_url":"data:image/png;base64,AAEC"}]}}});
    let (out, events) = request(&input, Operation::Generate);
    assert_eq!(out["prompt"], input["prompt"]);
    assert_eq!(
        out["input"][0]["content"][0]["image_url"],
        input["input"][0]["content"][0]["image_url"]
    );
    assert_eq!(out["input"][0]["content"][0]["ref"], "literal");
    let mut decoded = Vec::new();
    for event in &events {
        if let Some(data) = event["bytes"].as_str() {
            decoded.extend(base64::engine::general_purpose::STANDARD.decode(data).unwrap());
        }
    }
    assert_eq!(decoded, raw);
    assert!(
        events
            .iter()
            .filter(|e| e.get("pointer").is_some())
            .all(|e| e["value"] != encoded)
    );
}

#[test]
fn generated_images_use_media_data_and_preserve_partial_boundaries() {
    use base64::Engine;
    let raw: Vec<_> = (0..80_003).map(|i| (i % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let item =
        json!({"id":"img","type":"image_generation_call","status":"completed","result":encoded,"output_format":"png"});
    let body = json!({"id":"r","model":"m","created_at":1,"status":"completed","output":[item],"x_ck_opaque":[]});
    let wire=[
        json!({"type":"response.created","response":{"id":"r","model":"m","created_at":1}}),
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"img","type":"image_generation_call"}}),
        json!({"type":"response.image_generation_call.partial_image","output_index":0,"item_id":"img","partial_image_index":3,"partial_image_b64":"AQID"}),
        json!({"type":"response.output_item.done","output_index":0,"item":item}),
        json!({"type":"response.completed","response":body}),
    ].iter().map(|event|format!("data: {event}\n\n")).collect::<String>();
    for mode in [BodyMode::Buffered, BodyMode::Streaming] {
        let input = if mode == BodyMode::Buffered {
            serde_json::to_vec(&body).unwrap()
        } else {
            wire.as_bytes().to_vec()
        };
        for downstream in [BodyMode::Buffered, BodyMode::Streaming] {
            let (output, canonical) = response(&input, mode, downstream, Operation::Generate);
            let media: Vec<_> = canonical.iter().filter(|event| event["kind"] == "image").collect();
            assert_eq!(media.len(), if mode == BodyMode::Streaming { 2 } else { 1 });
            let final_id = &media.last().unwrap()["x_ck"]["media_id"];
            let reconstructed: Vec<_> = canonical
                .iter()
                .filter(|event| event["x_ck"]["media_id"] == *final_id && event["bytes"].is_string())
                .flat_map(|event| {
                    base64::engine::general_purpose::STANDARD
                        .decode(event["bytes"].as_str().unwrap())
                        .unwrap()
                })
                .collect();
            assert_eq!(reconstructed, raw);
            assert!(
                canonical
                    .iter()
                    .all(|event| event["attributes"].get("result").is_none())
            );
            let projected: Value = if downstream == BodyMode::Buffered {
                serde_json::from_slice(&output).unwrap()
            } else {
                let events: Vec<Value> = std::str::from_utf8(&output)
                    .unwrap()
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                if mode == BodyMode::Streaming {
                    let partial = events
                        .iter()
                        .find(|event| event["type"] == "response.image_generation_call.partial_image")
                        .unwrap();
                    assert_eq!(partial["partial_image_index"], 3);
                    assert_eq!(partial["partial_image_b64"], "AQID");
                }
                events.last().unwrap()["response"].clone()
            };
            assert_eq!(projected["output"][0]["result"], encoded);
            assert_eq!(projected["output"][0]["output_format"], "png");
            if mode == BodyMode::Streaming && downstream == BodyMode::Buffered {
                let carrier = projected["x_ck_opaque"].as_array().unwrap();
                let preview = carrier
                    .iter()
                    .find(|entry| entry["type"] == "response.image_generation_call.partial_image")
                    .unwrap();
                assert_eq!(preview["partial_image_index"], 3);
                assert_eq!(preview["partial_image_b64"], "AQID");
                let native = carrier
                    .iter()
                    .find(|entry| entry["name"] == "responses.fields" && entry["value"].get("x_ck_opaque").is_some())
                    .unwrap();
                assert_eq!(native["value"]["x_ck_opaque"], json!([]));
            }
        }
    }
}

#[test]
fn image_previews_require_an_open_image_item() {
    for closed_image in [false, true] {
        let item = if closed_image {
            json!({"id":"i","type":"image_generation_call","result":"AQID"})
        } else {
            json!({"id":"i","type":"message","content":[]})
        };
        let mut records = vec![json!({"type":"response.output_item.added","output_index":0,"item":item})];
        if closed_image {
            records.push(json!({"type":"response.output_item.done","output_index":0,"item":item}));
        }
        records.push(json!({"type":"response.image_generation_call.partial_image","output_index":0,"item_id":"i","partial_image_index":0,"partial_image_b64":"AQID"}));
        records.push(json!({"type":"response.completed","response":{"id":"r","status":"completed","output":[item]}}));
        let wire = records
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect::<String>();
        let mut m = Mapped::new();
        let mut ingress = responses::response::Ingress::new(
            &mut m.writer,
            identity(Direction::Response),
            Operation::Generate,
            BodyMode::Streaming,
            100_000,
        )
        .unwrap();
        let result = ingress
            .advance(&mut m.writer, wire.as_bytes())
            .and_then(|()| ingress.finish(&mut m.writer));
        assert!(result.is_err());
        let canonical = String::from_utf8(m.output(&m.writer)).unwrap();
        assert!(canonical.contains("event: x_ck.error\n"));
        assert!(!canonical.contains("event: x_ck.done\n"));
    }
}
