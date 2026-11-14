//! Streamed egress under the default stream limits.
use super::*;
use gemini::response::Framing;

/// Every streamed part leaves the egress once written: a response of many parts stays within
/// the default metadata quota rather than growing with the stream.
#[test]
fn long_streams_keep_egress_metadata_bounded() {
    let directory = std::env::temp_dir().join(format!("ck-gemini-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let mut ingress = gemini::response::Ingress::with_framing(
        &mut writer,
        identity(Direction::Response),
        Operation::Generate,
        Framing::Sse,
        1 << 30,
    )
    .unwrap();
    let mut egress =
        gemini::egress::Egress::with_framing(&mut writer, Operation::Generate, Framing::Sse, |_| {}).unwrap();
    let rows = (0..3000)
        .map(|i| json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":format!("t{i} ")}]}}]}))
        .chain([json!({"candidates":[{"index":0,"finishReason":"STOP"}]})]);
    let mut output = Vec::new();
    for row in rows {
        ingress
            .advance(&mut writer, format!("data: {row}\n\n").as_bytes())
            .unwrap();
        while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
            egress
                .advance(&mut writer, lease, |part| {
                    output.extend_from_slice(&bytes(&part));
                    Ok(())
                })
                .unwrap();
        }
    }
    ingress.finish(&mut writer).unwrap();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress
            .advance(&mut writer, lease, |part| {
                output.extend_from_slice(&bytes(&part));
                Ok(())
            })
            .unwrap();
    }
    egress.finish().unwrap();
    let text: String = events(&output)
        .iter()
        .flat_map(|v| v["candidates"].as_array().cloned().unwrap_or_default())
        .flat_map(|c| c["content"]["parts"].as_array().cloned().unwrap_or_default())
        .filter_map(|p| p["text"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(text, (0..3000).map(|i| format!("t{i} ")).collect::<String>());
    drop((ingress, egress, reader, writer));
    store.shutdown().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
}
