//! Trusted edits that replace a response media part's embedded data with a URI or file source,
//! with or without changing its image or document kind.
use super::codec;
use super::media_lifetime::{output_messages, project_input};
use codec::{
    BodyMode,
    document::{Document, Value},
    event::Kind,
};
use serde_json::{Value as Json, json};

fn image(tag: &str, data: &str) -> Json {
    json!({"type":"image_url","image_url":{"url":format!("data:image/png;base64,{data}"),"detail":"high","vendor":tag},"extra":[null,false,2]})
}

fn file(tag: &str, data: &str) -> Json {
    json!({"type":"file","file":{"file_data":format!("data:application/pdf;base64,{data}"),"filename":"a.pdf","vendor":tag}})
}

/// Projects `parts` from SSE and buffered upstream input to both client modes, applying `edit`
/// to the media start whose native carrier is tagged `first`, and returns each case's parts.
fn project(parts: Json, edit: impl Fn(&mut Document, usize, usize)) -> Vec<(String, Vec<Json>)> {
    let event = json!({"choices":[{"index":0,"delta":{"content":parts},"finish_reason":"stop"}]});
    let body = event.to_string().replace("\"delta\"", "\"message\"");
    let mut cases = Vec::new();
    for (upstream, wire) in [
        (BodyMode::Streaming, format!("data: {event}\n\ndata: [DONE]\n\n")),
        (BodyMode::Buffered, body),
    ] {
        for mode in [BodyMode::Streaming, BodyMode::Buffered] {
            let (output, result) = project_input(&wire, upstream, mode, |kind, doc| {
                if kind != Kind::MediaStart {
                    return;
                }
                let attrs = doc.required(doc.root(), "attributes").unwrap();
                let value = doc.required(attrs, "value").unwrap();
                let carrier = ["image_url", "file"]
                    .into_iter()
                    .find_map(|key| doc.member(value, key).unwrap())
                    .unwrap();
                if doc
                    .string_is(doc.required(carrier, "vendor").unwrap(), "first")
                    .unwrap()
                {
                    edit(doc, value, carrier);
                }
            });
            let case = format!("{upstream:?}/{mode:?}");
            result.unwrap_or_else(|error| panic!("{case}: {error:?}"));
            let parts = output_messages(&output, mode)
                .iter()
                .filter_map(|message| message["content"].as_array())
                .flatten()
                .cloned()
                .collect();
            cases.push((case, parts));
        }
    }
    cases
}

#[test]
fn image_data_replaced_by_a_uri_keeps_the_part_and_drops_the_bytes() {
    let parts = json!([image("first", "AQID"), image("second", "BAUG")]);
    for (case, parts) in project(parts, |doc, _, carrier| {
        doc.set(carrier, "url", Value::Text("https://example.invalid/new"))
            .unwrap();
    }) {
        let mut first = image("first", "");
        first["image_url"]["url"] = json!("https://example.invalid/new");
        assert_eq!(parts, [first, image("second", "BAUG")], "{case}");
    }
}

#[test]
fn image_data_replaced_by_a_file_id_becomes_a_file_part() {
    let parts = json!([image("first", "AQID"), image("second", "BAUG")]);
    for (case, parts) in project(parts, |doc, value, _| {
        doc.set(doc.root(), "kind", Value::Text("document")).unwrap();
        doc.remove(value, "image_url").unwrap();
        doc.set(value, "type", Value::Text("file")).unwrap();
        let file = doc.set(value, "file", Value::Object).unwrap();
        doc.set(file, "file_id", Value::Text("file-new")).unwrap();
        doc.set(file, "vendor", Value::Text("first")).unwrap();
    }) {
        let first = json!({"type":"file","file":{"file_id":"file-new","vendor":"first"},"extra":[null,false,2]});
        assert_eq!(parts, [first, image("second", "BAUG")], "{case}");
    }
}

#[test]
fn file_data_replaced_by_a_file_id_or_an_image_uri_keeps_its_sibling() {
    let parts = json!([file("first", "AQID"), file("second", "BAUG")]);
    for (case, parts) in project(parts.clone(), |doc, _, carrier| {
        doc.remove(carrier, "file_data").unwrap();
        doc.set(carrier, "file_id", Value::Text("file-new")).unwrap();
    }) {
        let first = json!({"type":"file","file":{"file_id":"file-new","filename":"a.pdf","vendor":"first"}});
        assert_eq!(parts, [first, file("second", "BAUG")], "{case}");
    }
    for (case, parts) in project(parts, |doc, value, _| {
        doc.set(doc.root(), "kind", Value::Text("image")).unwrap();
        doc.remove(value, "file").unwrap();
        doc.set(value, "type", Value::Text("image_url")).unwrap();
        let image = doc.set(value, "image_url", Value::Object).unwrap();
        doc.set(image, "url", Value::Text("https://example.invalid/new"))
            .unwrap();
    }) {
        let first = json!({"type":"image_url","image_url":{"url":"https://example.invalid/new"}});
        assert_eq!(parts, [first, file("second", "BAUG")], "{case}");
    }
}
