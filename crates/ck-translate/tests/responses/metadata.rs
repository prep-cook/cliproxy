use super::*;

/// A request carrying `rounds` tool rounds, each a reasoning item, a function call and its
/// output: the shape whose metadata decides how long an agent conversation can grow.
fn history(rounds: usize) -> Vec<u8> {
    let mut input = vec![json!({"type":"message","role":"user","content":"q"})];
    for i in 0..rounds {
        input.push(json!({"type":"reasoning","id":format!("rs{i}"),"summary":[],"encrypted_content":"e"}));
        input.push(
            json!({"type":"function_call","id":format!("fc{i}"),"call_id":format!("c{i}"),"name":"f","arguments":"{}"}),
        );
        input.push(json!({"type":"function_call_output","call_id":format!("c{i}"),"output":"r"}));
    }
    serde_json::to_vec(&json!({"model":"m","input":input})).unwrap()
}

/// Whether the request with `rounds` tool rounds passes ingress and request egress under a
/// metadata budget of `budget` bytes.
pub(super) fn fits(rounds: usize, budget: usize) -> bool {
    let body = history(rounds);
    let directory = std::env::temp_dir().join(format!("ck-metadata-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.pool_bytes = Some(64 * ck_stream::FRAME_BYTES);
    config.max_metadata_bytes = budget;
    config.header_slots = 8;
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let mut ingress = responses::request::Ingress::new(
        &mut writer,
        &store,
        identity(Direction::Request),
        Operation::Generate,
        1 << 20,
    )
    .unwrap();
    let passed = ingress.advance(&mut writer, &body).is_ok()
        && ingress
            .finish(&mut writer, &responses::request::ResponsesCapability)
            .and_then(|ready| {
                let egress = responses::request::Egress {
                    destination: "responses",
                    upstream_stream: false,
                };
                egress.encode(&ready, &mut writer)
            })
            .is_ok();
    drop(ingress);
    drop(writer);
    store.shutdown().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    passed
}

/// The least metadata budget a 30-round tool history passes with. Each tool round costs
/// metadata for its document nodes and records, so this budget sets how many rounds fit under
/// the default quota. The ingress releases the wire scanner and node map before
/// normalization, nameless objects and arrays keep no record lease, and the pointers of
/// original fields share one imported source. The exact budget moves with any metadata
/// accounting change and must then be re-measured against the previous behaviour.
#[test]
fn tool_history_metadata_need() {
    const NEED: usize = 742_788;
    assert!(fits(30, NEED), "30 rounds no longer fit in {NEED} bytes");
    assert!(!fits(30, NEED - 1), "30 rounds now fit in less than {NEED} bytes");
}
