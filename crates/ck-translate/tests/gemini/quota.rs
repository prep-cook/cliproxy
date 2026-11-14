//! Ordinary Gemini requests fit the default metadata quota through ingress, the trusted
//! reader and Gemini egress.
use super::*;

/// Runs ingress in `chunk`-byte pieces under the default stream limits, encodes the request
/// for Gemini and rebuilds it from its records.
fn fits(input: &str, chunk: usize) -> Result<(), String> {
    let directory = std::env::temp_dir().join(format!("ck-gemini-quota-{}", uuid::Uuid::new_v4()));
    let mut config = ck_stream::Config::new(directory.clone());
    config.workers = 1;
    let mut store =
        ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
    let mut writer = store.create().unwrap();
    let result = (|| {
        let mut ingress = gemini::request::Ingress::new(
            &mut writer,
            &store,
            identity(Direction::Request),
            Operation::Generate,
            "m",
            1 << 30,
        )
        .map_err(|e| format!("new {e:?}"))?;
        for piece in input.as_bytes().chunks(chunk) {
            ingress
                .advance(&mut writer, piece)
                .map_err(|e| format!("advance {e:?}"))?;
        }
        let ready = ingress
            .finish(&mut writer, &gemini::request::GeminiCapability)
            .map_err(|e| format!("finish {e:?}"))?;
        gemini::request::Egress {
            destination: "gemini",
            upstream_stream: true,
        }
        .encode(&ready, &mut writer)
        .map_err(|e| format!("encode {e:?}"))?;
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

/// A model call and its id-less response per round, as Gemini clients send tool history.
fn tool_rounds(n: usize) -> String {
    let rounds: Vec<_> = (0..n)
        .map(|i| {
            format!(
                r#"{{"role":"model","parts":[{{"functionCall":{{"id":"call_{i}","name":"f","args":{{"n":{i}}}}}}}]}},{{"role":"user","parts":[{{"functionResponse":{{"name":"f","response":{{"ok":true}}}}}}]}}"#
            )
        })
        .collect();
    format!(r#"{{"contents":[{}]}}"#, rounds.join(","))
}

/// The Chat codec's bar for the same history (tests/semantic/quota.rs). Measured at the
/// default 8 MiB: 484 rounds fit (485 exhaust the trusted reader); Chat fits 584.
#[test]
fn tool_rounds_fit_the_default_quota() {
    fits(&tool_rounds(256), 1 << 20).unwrap();
}

#[test]
fn fragmented_long_text_fits_the_default_quota() {
    let text = "a".repeat(2048);
    let parts: Vec<_> = (0..512)
        .map(|_| format!(r#"{{"role":"user","parts":[{{"text":"{text}"}}]}}"#))
        .collect();
    fits(&format!(r#"{{"contents":[{}]}}"#, parts.join(",")), 16).unwrap();
}
