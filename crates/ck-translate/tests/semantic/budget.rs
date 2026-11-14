use super::{Operation, codec, identity};
use codec::{BodyMode, ResponseIngress, event::Direction};

/// Runs 17 choices through a delta, a final-snapshot suffix and a finish under a metadata
/// budget. Returns the failing step and the canonical records published before it.
fn snapshot_under_budget(budget: usize) -> (Option<(usize, codec::Error)>, usize) {
    let directory = std::env::temp_dir().join(format!("ck-budget-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(64 * ck_stream::FRAME_BYTES);
    config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
    config.max_metadata_bytes = budget;
    config.header_slots = 8;
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let line = |body: &str| {
        let choices: Vec<_> = (0..17).map(|i| format!(r#"{{"index":{i},{body}}}"#)).collect();
        format!(
            "data: {{\"id\":\"r\",\"model\":\"m\",\"created\":1,\"choices\":[{}]}}\n\n",
            choices.join(",")
        )
    };
    let chunks = [
        line(r#""delta":{"role":"assistant","content":"a"},"finish_reason":null"#),
        line(r#""message":{"role":"assistant","content":"ab"},"finish_reason":null"#),
        line(r#""delta":{},"finish_reason":"stop""#) + "data: [DONE]\n\n",
    ];
    let mut ingress = codec::chat::response::Ingress::new(
        &mut writer,
        &store,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1_000_000,
    )
    .unwrap();
    let failure = chunks
        .iter()
        .enumerate()
        .find_map(|(step, chunk)| ingress.advance(&mut writer, chunk.as_bytes()).err().map(|e| (step, e)));
    drop(ingress);
    let mut reader = store.reader(writer.id(), 0).unwrap();
    let mut records = 0;
    while let std::task::Poll::Ready(Some(ck_stream::Read::Record(_))) = reader.try_next() {
        records += 1;
    }
    drop(reader);
    drop(writer);
    store.shutdown().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    (failure, records)
}

/// The duplicate-index table of a snapshot keeps its metadata quota until the suffix edits
/// are applied, so the snapshot step needs the table and the edits' scratch together. Since
/// multi-frame lease frame lists count against the metadata cap, that step is the peak of the
/// whole stream: 1 byte more than the step needs lets every later step through. Dropping the
/// table before the edits lowers the step's need by 256 bytes, and the smaller budget then
/// completes the stream. The exact budgets move with any metadata accounting change and must
/// then be re-measured against the previous behaviour.
#[test]
fn snapshot_index_table_quota_lasts_until_suffix_edits_apply() {
    let outcome = |budget| match snapshot_under_budget(budget) {
        (Some((step, codec::Error::Storage(ck_stream::Error::StorageExhausted))), records) => (Some(step), records),
        (Some((step, error)), _) => panic!("step {step} failed with {error:?}"),
        (None, records) => (None, records),
    };
    assert_eq!(outcome(354_795), (Some(1), 71));
    assert_eq!(outcome(354_796), (None, 141));
}
