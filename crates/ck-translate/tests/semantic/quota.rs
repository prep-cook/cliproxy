//! Ordinary requests fit the default metadata quota through ingress and the trusted reader.
use super::{ChatCapability, Ingress, Operation, RequestIngress, codec, identity};
use codec::event::Direction;

/// Runs ingress in `chunk`-byte pieces, finishes, and rebuilds the request from its records
/// under the default stream limits.
fn fits(input: &str, chunk: usize) -> Result<(), String> {
    let directory = std::env::temp_dir().join(format!("ck-quota-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let result = (|| {
        let mut ingress = Ingress::new(
            &mut writer,
            &store,
            identity(Direction::Request),
            Operation::Generate,
            1 << 30,
        )
        .map_err(|e| format!("new {e:?}"))?;
        for piece in input.as_bytes().chunks(chunk) {
            ingress
                .advance(&mut writer, piece)
                .map_err(|e| format!("advance {e:?}"))?;
        }
        ingress
            .finish(&mut writer, &ChatCapability)
            .map_err(|e| format!("finish {e:?}"))?;
        drop(ingress);
        let mut reader = store.reader(writer.id(), 0).map_err(|e| format!("reader {e:?}"))?;
        codec::chat::request::read_request(&mut writer, &mut reader).map_err(|e| format!("read_request {e:?}"))?;
        Ok(())
    })();
    drop(writer);
    store.shutdown().unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    result
}

fn messages(n: usize, message: impl Fn(usize) -> String) -> String {
    let list: Vec<_> = (0..n).map(message).collect();
    format!(r#"{{"model":"m","messages":[{}]}}"#, list.join(","))
}

/// 2 KB strings stream as text media; 16-byte network pieces must not cost a record's
/// metadata each.
#[test]
fn fragmented_long_messages_fit_the_default_quota() {
    let content = "a".repeat(2048);
    let input = messages(8, |_| format!(r#"{{"role":"user","content":"{content}"}}"#));
    fits(&input, 16).unwrap();
}

#[test]
fn plain_messages_fit_the_default_quota() {
    let input = messages(128, |_| r#"{"role":"user","content":"hi"}"#.into());
    fits(&input, 1 << 20).unwrap();
}

#[test]
fn metadata_keys_fit_the_default_quota() {
    let keys: Vec<_> = (0..512).map(|i| format!(r#""k{i}":"v""#)).collect();
    let input = format!(
        r#"{{"model":"m","metadata":{{{}}},"messages":[{{"role":"user","content":"hi"}}]}}"#,
        keys.join(",")
    );
    fits(&input, 1 << 20).unwrap();
}

/// Call and result rounds: an assistant tool call and its tool result per round.
#[test]
fn tool_rounds_fit_the_default_quota() {
    let input = messages(32, |i| {
        format!(
            r#"{{"role":"assistant","content":null,"tool_calls":[{{"id":"call_{i}","type":"function","function":{{"name":"f","arguments":"{{}}"}}}}]}},{{"role":"tool","tool_call_id":"call_{i}","content":"ok"}}"#
        )
    });
    fits(&input, 1 << 20).unwrap();
}

/// Each message carries an unknown member, which the request preserves as its own field.
#[test]
fn messages_with_unknown_members_fit_the_default_quota() {
    let input = messages(150, |i| format!(r#"{{"role":"user","content":"hi","x_vendor":{i}}}"#));
    fits(&input, 1 << 20).unwrap();
}
