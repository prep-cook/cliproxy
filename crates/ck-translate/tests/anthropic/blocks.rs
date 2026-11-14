//! Block lookups by identity: indexed lookups keep the rules the retained-block scans enforced.
use crate::{
    BodyMode, Direction, Mapped, Operation, ResponseEgress, ResponseIngress, Value, anthropic, codec, events, identity,
    json, sse,
};
use std::task::Poll;

fn text(index: u64) -> [Value; 3] {
    [
        json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":"a"}}),
        json!({"type":"content_block_stop","index":index}),
    ]
}
/// Whether ingress rejects `rows` as they arrive, not only once the stream ends unfinished.
fn rejected_on_arrival(rows: &[Value]) -> bool {
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, &sse(rows)).is_err()
}

#[test]
fn response_blocks_need_a_unique_open_index() {
    let head = json!({"type":"message_start","message":{"id":"r","model":"m"}});
    let [start, delta, stop] = text(0);
    for rows in [
        [&head, &start, &stop, &start],
        [&head, &start, &stop, &delta],
        [&head, &start, &stop, &stop],
    ] {
        let rows: Vec<Value> = rows.into_iter().cloned().collect();
        assert!(rejected_on_arrival(&rows), "rows={rows:?}");
    }
}

/// Canonical records of a text block and a tool block, as Anthropic ingress publishes them:
/// record kind, event name and data.
fn canonical() -> Vec<(u32, Option<String>, Value)> {
    let mut rows = vec![json!({"type":"message_start","message":{"id":"r","model":"m"}})];
    rows.extend(text(0));
    rows.extend([
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t","name":"f","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ]);
    let mut mapped = Mapped::new();
    let mut ingress = anthropic::response::Ingress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        1024 * 1024,
    )
    .unwrap();
    ingress.advance(&mut mapped.writer, &sse(&rows)).unwrap();
    ingress.finish(&mut mapped.writer).unwrap();
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut records = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        let mut raw = Vec::new();
        lease.visit(|chunk| raw.extend_from_slice(chunk)).unwrap();
        let raw = String::from_utf8(raw).unwrap();
        let name = raw.lines().find_map(|l| l.strip_prefix("event: ")).map(str::to_owned);
        let data = events(raw.as_bytes()).pop().unwrap();
        records.push((lease.record().kind, name, data));
    }
    records
}

/// Runs `records` through streaming Anthropic egress: its native events, and the record it
/// rejected (`records.len()` when only the finish failed), if any.
fn egress(records: &[(u32, Option<String>, Value)]) -> (Vec<Value>, Option<usize>) {
    let mut mapped = Mapped::new();
    for (seq, (kind, name, data)) in records.iter().enumerate() {
        let mut data = data.clone();
        data["x_ck"]["seq"] = json!(seq);
        codec::encode::publish(&mut mapped.writer, *kind, 0, |e| {
            if let Some(name) = name {
                e.bytes(format!("event: {name}\n").as_bytes())?;
            }
            e.bytes(format!("id: {seq}\ndata: {data}\n\n").as_bytes())
        })
        .unwrap();
    }
    let mut reader = mapped.store.reader(mapped.writer.id(), 0).unwrap();
    let mut egress = anthropic::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        BodyMode::Streaming,
        |_| {},
    )
    .unwrap();
    let mut output = Vec::new();
    let mut seq = 0;
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        let result = egress.advance(&mut mapped.writer, lease, |part| {
            output.extend(crate::bytes(&part));
            Ok(())
        });
        if result.is_err() {
            return (events(&output), Some(seq));
        }
        seq += 1;
    }
    let rejected = egress.finish().is_err().then_some(seq);
    (events(&output), rejected)
}

fn position(records: &[(u32, Option<String>, Value)], name: &str, block: u64) -> usize {
    records
        .iter()
        .position(|(_, n, data)| n.as_deref() == Some(name) && data["x_ck"]["block_index"] == block)
        .unwrap()
}

#[test]
fn egress_blocks_need_a_unique_open_block() {
    let records = canonical();
    assert_eq!(egress(&records).1, None);
    let start = position(&records, "x_ck.block.start", 0);
    let end = position(&records, "x_ck.block.end", 0);
    let text = (start..end).find(|&i| records[i].1.is_none()).unwrap();
    let tool_end = position(&records, "x_ck.block.end", 1);
    let arguments = (end..tool_end)
        .find(|&i| records[i].1.is_none() && records[i].2["choices"][0]["delta"]["tool_calls"].is_array())
        .unwrap();
    for (copied, after) in [(start, start), (text, end), (arguments, tool_end)] {
        let mut edited = records.clone();
        edited.insert(after + 1, records[copied].clone());
        assert_eq!(egress(&edited).1, Some(after + 1), "copied={:?}", records[copied]);
    }
    // A tool end without a block index finds its block by tool index, within its item only.
    let tool = (end..tool_end)
        .find(|&i| records[i].1.as_deref() == Some("x_ck.tool.end"))
        .unwrap();
    let mut edited = records.clone();
    edited[tool].2["x_ck"].as_object_mut().unwrap().remove("block_index");
    assert_eq!(egress(&edited).1, None);
    edited[tool].2["x_ck"]["item_id"] = json!("other");
    assert_eq!(egress(&edited).1, Some(tool));
    // A text delta without a block index after the explicit block closed starts a new block.
    let mut edited = records.clone();
    let mut delta = records[text].clone();
    delta.2["x_ck"].as_object_mut().unwrap().remove("block_index");
    edited.insert(end + 1, delta);
    let (native, _) = egress(&edited);
    let starts = native.iter().filter(|e| e["type"] == "content_block_start").count();
    assert_eq!(starts, 3, "native={native:?}");
}
