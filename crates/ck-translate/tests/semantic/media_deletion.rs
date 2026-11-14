//! Trusted deletions of response media records, leaves and metadata, for SSE and buffered
//! upstream input in both client delivery modes.
use super::codec;
use super::media_lifetime::{output_messages, project_filtered, project_input};
use codec::{BodyMode, event::Kind};
use serde_json::{Value, json};

/// The same single source record as SSE and as a buffered body.
fn inputs(event: &Value) -> [(BodyMode, String); 2] {
    let body = event.to_string().replace("\"delta\"", "\"message\"");
    [
        (BodyMode::Streaming, format!("data: {event}\n\ndata: [DONE]\n\n")),
        (BodyMode::Buffered, body),
    ]
}

#[test]
fn media_leaf_parent_reference_and_metadata_deletions_do_not_revive_siblings() {
    let image = |tag| json!({"type":"image_url","image_url":{"url":"data:image/png;base64,AQID","detail":"high","vendor":tag},"extra":[null,false,2]});
    let event =
        json!({"choices":[{"index":7,"delta":{"content":[image("first"),image("second")]},"finish_reason":"stop"}]});
    for (upstream, wire) in inputs(&event) {
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            for deletion in ["value", "image_url", "url", "detail", "mime", "attributes"] {
                let (output, result) = project_input(&wire, upstream, mode, |kind, doc| {
                    if kind != Kind::MediaStart {
                        return;
                    }
                    let root = doc.root();
                    let attrs = doc.required(root, "attributes").unwrap();
                    let value = doc.required(attrs, "value").unwrap();
                    let image = doc.required(value, "image_url").unwrap();
                    if !doc.string_is(doc.required(image, "vendor").unwrap(), "first").unwrap() {
                        return;
                    }
                    let parent = match deletion {
                        "attributes" | "mime" => root,
                        "value" => attrs,
                        "image_url" => value,
                        _ => image,
                    };
                    doc.remove(parent, deletion).unwrap();
                });
                result.unwrap();
                let parts: Vec<_> = output_messages(&output, mode)
                    .iter()
                    .filter_map(|message| message["content"].as_array())
                    .flatten()
                    .cloned()
                    .collect();
                let case = format!("{upstream:?}/{mode:?}/{deletion}");
                if matches!(deletion, "detail" | "mime") {
                    let mut first = image("first");
                    if deletion == "detail" {
                        first["image_url"].as_object_mut().unwrap().remove("detail");
                    } else {
                        first["image_url"]["url"] = json!("data:;base64,AQID");
                    }
                    assert_eq!(parts, vec![first, image("second")], "{case}");
                } else {
                    assert_eq!(parts, vec![image("second")], "{case}");
                }
            }
        }
    }
}

#[test]
fn transcript_and_entire_media_record_deletion_leave_no_stale_native_output() {
    let sse = concat!(
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"id\":\"deleted\",\"data\":\"AQID\",\"transcript\":\"deleted\"}}},{\"index\":3,\"delta\":{\"audio\":{\"id\":\"sibling\",\"data\":\"BAUG\",\"transcript\":\"kept\"}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{\"audio\":{\"id\":\"deleted\",\"format\":\"wav\",\"expires_at\":99}}}]}\n\n",
        "data: {\"choices\":[{\"index\":7,\"delta\":{},\"finish_reason\":\"stop\"},{\"index\":3,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
    );
    let buffered = json!({"choices":[
        {"index":7,"message":{"audio":{"id":"deleted","data":"AQID","transcript":"deleted","format":"wav","expires_at":99}},"finish_reason":"stop"},
        {"index":3,"message":{"audio":{"id":"sibling","data":"BAUG","transcript":"kept"}},"finish_reason":"stop"}
    ]})
    .to_string();
    for (upstream, wire) in [(BodyMode::Streaming, sse.to_owned()), (BodyMode::Buffered, buffered)] {
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            for removal in ["transcript", "lifetime", "reference", "parent"] {
                let (output, result) = project_filtered(&wire, upstream, mode, |kind, doc| {
                    let x = doc.required(doc.root(), "x_ck").unwrap();
                    let selected = doc
                        .member(x, "choice_index")
                        .unwrap()
                        .is_some_and(|n| doc.unsigned(n).unwrap() == 7);
                    if selected && kind == Kind::MediaStart {
                        if removal == "parent" {
                            doc.remove(doc.root(), "attributes").unwrap();
                        }
                        if removal == "reference" {
                            let attrs = doc.required(doc.root(), "attributes").unwrap();
                            let value = doc.required(attrs, "value").unwrap();
                            doc.remove(value, "data").unwrap();
                        }
                    }
                    !(selected
                        && (kind == Kind::MediaTranscript
                            || (removal == "lifetime" && doc.member(x, "media_id").unwrap().is_some())))
                });
                result.unwrap();
                assert_transcript_deletion(&output, mode, removal);
            }
        }
    }
}

fn assert_transcript_deletion(output: &[u8], mode: BodyMode, removal: &str) {
    let messages = output_messages(output, mode);
    let audio: Vec<_> = messages.iter().filter_map(|m| m.get("audio")).collect();
    let transcript: String = audio.iter().filter_map(|a| a["transcript"].as_str()).collect();
    assert_eq!(transcript, "kept", "{mode:?}/{removal}");
    assert!(audio.iter().any(|a| a["data"] == "BAUG"));
    let text = String::from_utf8_lossy(output);
    if removal != "transcript" {
        assert!(
            !text.contains("deleted") && !text.contains("AQID"),
            "{mode:?}/{removal}: {text}"
        );
    } else {
        assert!(audio.iter().any(|a| a["id"] == "deleted"));
        assert!(audio.iter().any(|a| a["data"] == "AQID"));
    }
}

#[test]
fn file_and_audio_metadata_leaf_deletions_do_not_restore_native_values() {
    let event = json!({"choices":[{"index":0,"delta":{
        "content":[{"type":"file","file":{"filename":"deleted.pdf","file_data":"data:application/pdf;base64,AQID","vendor":[null,false]}}],
        "audio":{"data":"BAUG","id":"deleted-id","format":"wav","expires_at":99,"timing":{"start":0,"unit":"milliseconds"},"vendor":false}
    },"finish_reason":"stop"}]});
    for (upstream, wire) in inputs(&event) {
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            let (output, result) = project_input(&wire, upstream, mode, |kind, doc| {
                if kind != Kind::MediaStart {
                    return;
                }
                let attrs = doc.required(doc.root(), "attributes").unwrap();
                let value = doc.required(attrs, "value").unwrap();
                if let Some(file) = doc.member(value, "file").unwrap() {
                    doc.remove(file, "filename").unwrap();
                }
                for key in ["id", "format", "expires_at", "timing"] {
                    doc.remove(value, key).unwrap();
                }
            });
            result.unwrap();
            let messages = output_messages(&output, mode);
            let parts: Vec<_> = messages
                .iter()
                .filter_map(|m| m["content"].as_array())
                .flatten()
                .collect();
            assert_eq!(
                parts,
                vec![
                    &json!({"type":"file","file":{"file_data":"data:application/pdf;base64,AQID","vendor":[null,false]}})
                ],
                "{upstream:?}/{mode:?}"
            );
            let audio: Vec<_> = messages.iter().filter_map(|m| m.get("audio")).collect();
            assert!(audio.iter().any(|a| a["data"] == "BAUG"));
            assert!(audio.iter().any(|a| a["vendor"] == false));
            for value in audio {
                for key in ["id", "format", "expires_at", "timing"] {
                    assert!(value.get(key).is_none(), "{upstream:?}/{mode:?}/{key}");
                }
            }
            assert!(!String::from_utf8_lossy(&output).contains("deleted"));
        }
    }
}
