//! Usage snapshots: inclusive output, modality breakdowns and native nulls.
use super::*;
use gemini::response::Framing;

fn usage(rows: &[Value]) -> (Value, Value) {
    let (out, canonical) = response(&sse(rows), Framing::Sse, Framing::Json, Operation::Generate);
    let usage = canonical.iter().rev().find_map(|v| v.get("usage")).unwrap().clone();
    (usage, serde_json::from_slice(&out).unwrap())
}

#[test]
fn output_without_thoughts_is_the_candidates_count() {
    let metadata = json!({"promptTokenCount":3,"candidatesTokenCount":4,"totalTokenCount":7});
    let (usage, out) = usage(&[
        json!({"candidates":[{"content":{"parts":[{"text":"a"}]},"finishReason":"STOP"}],"usageMetadata":metadata}),
    ]);
    assert_eq!(usage["output_tokens"], 4);
    assert!(usage.get("derived").is_none());
    assert_eq!(out["usageMetadata"], metadata);
}

#[test]
fn modality_details_map_to_canonical_counts_and_stay_native() {
    let metadata = json!({
        "promptTokenCount":23,"candidatesTokenCount":4,"totalTokenCount":27,
        "promptTokensDetails":[{"modality":"TEXT","tokenCount":5},{"modality":"AUDIO","tokenCount":7},{"modality":"IMAGE","tokenCount":2},{"modality":"VIDEO","tokenCount":9}],
        "candidatesTokensDetails":[{"modality":"AUDIO","tokenCount":3},{"modality":"TEXT","tokenCount":1}],
        "cacheTokensDetails":[{"modality":"TEXT","tokenCount":4}]
    });
    let (usage, out) = usage(&[
        json!({"candidates":[{"content":{"parts":[{"text":"a"}]},"finishReason":"STOP"}],"usageMetadata":metadata}),
    ]);
    assert_eq!(usage["input_audio_tokens"], 7);
    assert_eq!(usage["output_audio_tokens"], 3);
    assert_eq!(
        usage["details"],
        json!({"input_text_tokens":5,"input_image_tokens":2,"output_text_tokens":1})
    );
    assert_eq!(usage["native"]["value"], metadata);
    assert_eq!(out["usageMetadata"], metadata);
}

/// A later partial snapshot replaces the counts it supplies; its null clears a count without
/// turning it into zero.
#[test]
fn partial_usage_correction_preserves_native_null_without_zero() {
    let rows = [
        json!({"candidates":[{"content":{"parts":[{"text":"a"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2,"thoughtsTokenCount":3,"totalTokenCount":15}}),
        json!({"usageMetadata":{"promptTokenCount":null,"candidatesTokenCount":0}}),
        json!({"candidates":[{"finishReason":"STOP"}]}),
    ];
    let (usage, out) = usage(&rows);
    assert!(out["usageMetadata"]["promptTokenCount"].is_null());
    assert_eq!(out["usageMetadata"]["candidatesTokenCount"], 0);
    assert!(usage.get("input_tokens").is_none());
    assert_eq!(usage["output_tokens"], 3);
    assert_eq!(usage["total_tokens"], 15);
}

#[test]
fn null_thoughts_leave_the_output_unknown() {
    let (usage, _) = usage(&[
        json!({"candidates":[{"content":{"parts":[{"text":"a"}]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":4,"thoughtsTokenCount":null}}),
    ]);
    assert!(usage.get("output_tokens").is_none());
}
