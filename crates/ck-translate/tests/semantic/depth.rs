use super::{Direction, Mapped, Operation, bytes, codec, identity};
use codec::{BodyMode, RequestEgress, RequestIngress, ResponseEgress, ResponseIngress};
use std::task::Poll;

const STACK: usize = 1 << 20;

fn nest(depth: usize) -> String {
    format!("{}0{}", "[".repeat(depth), "]".repeat(depth))
}

fn request(body: &str) -> codec::Result<Vec<u8>> {
    use codec::chat::request::{ChatCapability, Egress, Ingress};
    let mut mapped = Mapped::new();
    let mut ingress = Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Request),
        Operation::Generate,
        1024 * 1024,
    )?;
    for chunk in body.as_bytes().chunks(19) {
        ingress.advance(&mut mapped.writer, chunk)?;
    }
    let ready = ingress.finish(&mut mapped.writer, &ChatCapability)?;
    let output = Egress {
        destination: "chat",
        upstream_stream: false,
    }
    .encode(&ready, &mut mapped.writer)?;
    Ok(bytes(&output))
}

fn response(wire: &str, upstream: BodyMode, downstream: BodyMode) -> codec::Result<Vec<u8>> {
    let mut mapped = Mapped::new();
    let mut ingress = codec::chat::response::Ingress::new(
        &mut mapped.writer,
        &mapped.store,
        identity(Direction::Response),
        Operation::Generate,
        upstream,
        1024 * 1024,
    )?;
    let mut reader = mapped.store.reader(mapped.writer.id(), 0)?;
    let mut egress = codec::chat::egress::Egress::new(
        &mut mapped.writer,
        identity(Direction::Response),
        Operation::Generate,
        downstream,
        true,
        |_| panic!("unexpected projection loss"),
    )?;
    for chunk in wire.as_bytes().chunks(31) {
        ingress.advance(&mut mapped.writer, chunk)?;
    }
    ingress.finish(&mut mapped.writer)?;
    let mut output = Vec::new();
    while let Poll::Ready(Some(ck_stream::Read::Record(lease))) = reader.try_next() {
        egress.advance(&mut mapped.writer, lease, |lease| {
            output.extend(bytes(&lease));
            Ok(())
        })?;
    }
    egress.finish()?;
    Ok(output)
}

/// The deepest nesting a location accepts: one more level must fail with `Limit`, and the
/// accepted value must reach the client output unchanged.
fn deepest(run: impl Fn(&str) -> codec::Result<Vec<u8>>) -> usize {
    for depth in (1..=128).rev() {
        match run(&nest(depth)) {
            Err(codec::Error::Limit) => continue,
            Err(error) => panic!("depth {depth}: {error:?}"),
            Ok(output) => {
                assert!(depth < 128, "nesting is unbounded");
                let output = String::from_utf8(output).unwrap();
                assert!(output.contains(&nest(depth)), "depth {depth} lost its value");
                assert!(matches!(run(&nest(depth + 1)), Err(codec::Error::Limit)));
                return depth;
            }
        }
    }
    panic!("no accepted depth");
}

fn on_small_stack(test: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(STACK)
        .spawn(test)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn maximum_request_nesting_completes_on_a_small_stack() {
    on_small_stack(|| {
        let shapes: [fn(&str) -> String; 5] = [
            |v| format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"zz":{v}}}"#),
            |v| format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi","zz":{v}}}]}}"#),
            |v| format!(r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"x_ck":{v}}}"#),
            |v| {
                format!(
                    r#"{{"model":"m","messages":[{{"role":"user","content":"hi"}}],"tools":[{{"type":"function","function":{{"name":"f","parameters":{{"type":"object","x":{v}}}}}}}]}}"#
                )
            },
            |v| {
                format!(
                    r#"{{"model":"m","messages":[{{"role":"user","content":[{{"type":"text","text":"hi","zz":{v}}}]}}]}}"#
                )
            },
        ];
        // Inner array levels; wrappers and provenance carriers take the rest of 128.
        for (shape, expected) in shapes.into_iter().zip([123, 123, 123, 122, 122]) {
            assert_eq!(deepest(|value| request(&shape(value))), expected);
        }
    });
}

#[test]
fn maximum_response_nesting_completes_on_a_small_stack() {
    on_small_stack(|| {
        let shapes: [fn(&str) -> String; 4] = [
            |v| {
                format!(
                    r#"{{"id":"r","model":"m","created":1,"zz":{v},"choices":[{{"index":0,"delta":{{"role":"assistant","content":"a"}},"finish_reason":"stop"}}]}}"#
                )
            },
            |v| {
                format!(
                    r#"{{"id":"r","model":"m","created":1,"choices":[{{"index":0,"delta":{{"role":"assistant","content":"a","zz":{v}}},"finish_reason":"stop"}}]}}"#
                )
            },
            |v| {
                format!(
                    r#"{{"id":"r","model":"m","created":1,"x_ck":{v},"choices":[{{"index":0,"delta":{{"role":"assistant","content":"a"}},"finish_reason":"stop"}}]}}"#
                )
            },
            |v| {
                format!(
                    r#"{{"id":"r","model":"m","created":1,"choices":[{{"index":0,"zz":{v},"delta":{{"role":"assistant","content":"a"}},"finish_reason":"stop"}}]}}"#
                )
            },
        ];
        for shape in shapes {
            for (upstream, downstream) in [
                (BodyMode::Streaming, BodyMode::Streaming),
                (BodyMode::Streaming, BodyMode::Buffered),
                (BodyMode::Buffered, BodyMode::Buffered),
                (BodyMode::Buffered, BodyMode::Streaming),
            ] {
                let depth = deepest(|value| {
                    let body = shape(value);
                    let wire = if upstream == BodyMode::Buffered {
                        body.replace("\"delta\"", "\"message\"")
                    } else {
                        format!("data: {body}\n\ndata: [DONE]\n\n")
                    };
                    response(&wire, upstream, downstream)
                });
                assert_eq!(depth, 124);
            }
        }
    });
}
