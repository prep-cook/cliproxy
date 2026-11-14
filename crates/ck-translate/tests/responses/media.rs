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
            let step = if mode == BodyMode::Buffered { 8192 } else { 13 };
            let (output, canonical) = response_in(&input, mode, downstream, Operation::Generate, step);
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

#[test]
fn buffered_image_results_publish_chunks_before_the_body_ends() {
    use crate::audio::{buffered_egress, buffered_ingress, data_bytes};
    use base64::Engine;
    // 73,731 bytes: two full 36,864-byte chunks and a short one.
    let raw: Vec<u8> = (0..73_731).map(|i| (i % 251) as u8).collect();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
    let message = json!({"id":"m1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"hi","annotations":[]}]});
    let head = format!(
        r#"{{"id":"r","model":"m","created_at":1,"status":"completed","output":[{message},{{"id":"img","type":"image_generation_call","status":"completed","result":""#
    );
    let tail = br#"","output_format":"png"}]}"#;
    let mut m = Mapped::new();
    let mut ingress = buffered_ingress(&mut m, head.as_bytes());
    for chunk in encoded.as_bytes().chunks(8192) {
        ingress.advance(&mut m.writer, chunk).unwrap();
    }
    let early = events(&m);
    assert!(data_bytes(&early) > 0, "no image data before the body ends: {early:?}");
    assert!(!early.iter().any(|e| e["choices"][0]["finish_reason"].is_string()));
    ingress.advance(&mut m.writer, tail).unwrap();
    ingress.finish(&mut m.writer).unwrap();
    let canonical = events(&m);
    assert_eq!(canonical.iter().filter(|e| e["kind"] == "image").count(), 1);
    // The earlier item closes before the image opens, so the stream keeps output order.
    let message_end = canonical
        .iter()
        .position(|e| e["attributes"]["id"] == "m1" && e["status"].is_string());
    let image_start = canonical.iter().position(|e| e["kind"] == "image");
    assert!(message_end.unwrap() < image_start.unwrap());
    assert_eq!(data_bytes(&canonical), raw.len() as u64);
    let decoded: Vec<u8> = canonical
        .iter()
        .filter_map(|e| e["bytes"].as_str())
        .flat_map(|b| base64::engine::general_purpose::STANDARD.decode(b).unwrap())
        .collect();
    assert_eq!(decoded, raw);
    let ends: Vec<_> = canonical
        .iter()
        .filter(|e| e["status"] == "completed" && e["bytes"].is_u64())
        .collect();
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0]["bytes"], raw.len());
    let out = buffered_egress(&mut m);
    let image =
        json!({"id":"img","type":"image_generation_call","status":"completed","result":encoded,"output_format":"png"});
    assert_eq!(out["output"], json!([message, image]));
    // Each image opens its own item in output order; a repeated `result` is refused.
    let two = br#"{"id":"r","status":"completed","output":[{"type":"image_generation_call","result":"AQID"},{"type":"image_generation_call","result":"BAUG"}]}"#;
    let twice = br#"{"id":"r","status":"completed","output":[{"type":"image_generation_call","result":"AQID","result":"AQID"}]}"#;
    // A `result` outside an image item, or outside root `output`, stays an ordinary field.
    let decoys = br#"{"id":"r","status":"completed","output":[{"type":"message","role":"assistant","content":[],"result":"AQID"}],"extra":[{"type":"image_generation_call","result":"AQID"}]}"#;
    for (body, images) in [(&two[..], Some(2)), (&twice[..], None), (&decoys[..], Some(0))] {
        let mut m = Mapped::new();
        let mut ingress = buffered_ingress(&mut m, b"");
        // Short advances leave each `result` open at an advance boundary.
        let result = body
            .chunks(5)
            .try_for_each(|chunk| ingress.advance(&mut m.writer, chunk))
            .and_then(|()| ingress.finish(&mut m.writer));
        let canonical = events(&m);
        let count = canonical.iter().filter(|e| e["kind"] == "image").count();
        assert_eq!(result.ok().map(|()| count), images);
    }
    // A result that ends inside a base64 quartet is refused when its string closes.
    let mut m = Mapped::new();
    let mut ingress = buffered_ingress(&mut m, head.as_bytes());
    ingress.advance(&mut m.writer, b"AAECAA").unwrap();
    let result = ingress
        .advance(&mut m.writer, tail)
        .and_then(|()| ingress.finish(&mut m.writer));
    assert!(result.is_err());
}
