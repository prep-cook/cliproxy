use crate::codec::{
    self, Capability, Error, Operation, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    event::{Emitter, Identity, Kind, Scope},
    json,
    media::{self, Registry},
};
use ck_stream::{ScratchLease, Writer};

#[path = "request_string.rs"]
mod string;

#[path = "request_index.rs"]
mod index;

#[path = "request_records.rs"]
mod records;
pub use records::read_request;

#[path = "request_write.rs"]
mod write;
use write::write_request;

#[path = "request_publish.rs"]
mod publish;
use publish::{Component, field, fields};

#[path = "request_tree.rs"]
mod tree;

#[path = "request_consume.rs"]
mod consume;

#[path = "request_messages.rs"]
mod messages;

pub struct Ingress {
    owner: u64,
    scanner: Option<json::Scanner>,
    document: Document,
    started: bool,
    last_field: Option<usize>,
    messages: Option<PendingMessages>,
    current_message: Option<PendingMessage>,
    string: Option<PendingString>,
    trees: codec::table::Table<Option<PendingTree>>,
    published_media: usize,
    /// Registry entries before this index belong to finished messages and keep their lineage.
    relocated_media: usize,
    media: Registry,
    emitter: Emitter,
    fence: RequestFence,
    operation: Operation,
    failed: bool,
    reader: ck_stream::Reader,
}

struct PendingMessages {
    wire: usize,
    node: usize,
    last: Option<usize>,
    index: u64,
}

struct PendingMessage {
    wire: usize,
    node: usize,
    last: Option<usize>,
}
struct PendingString {
    wire: usize,
    node: usize,
    media: usize,
    offset: u64,
    /// Input before this offset is named by no node other than this string's and is not needed
    /// by the scanner; keys and topology stay fixed while one string streams.
    floor: u64,
    /// When input after `offset` was first held unpublished.
    held: Option<std::time::Instant>,
    publisher: media::Publisher,
    decoder: media::InputDecoder,
}

#[derive(Clone, Copy)]
struct PendingTree {
    node: usize,
    last: Option<usize>,
    index: u64,
    done: bool,
}

impl Ingress {
    pub fn new(
        owner: &mut Writer,
        store: &ck_stream::Store,
        identity: Identity,
        operation: Operation,
        max_bytes: u64,
    ) -> Result<Self> {
        if operation != Operation::Generate {
            return Err(Error::Unsupported);
        }
        let mut document = Document::streaming(owner)?;
        initialize(&mut document)?;
        Ok(Self {
            owner: owner.id(),
            scanner: Some(json::Scanner::new(owner, max_bytes)?),
            document,
            started: false,
            last_field: None,
            messages: None,
            current_message: None,
            string: None,
            trees: codec::table::Table::new(owner)?,
            published_media: 0,
            relocated_media: 0,
            media: Registry::new(owner)?,
            emitter: Emitter::new(identity),
            fence: RequestFence::default(),
            operation,
            failed: false,
            reader: store.reader(owner.id(), 0)?,
        })
    }

    fn prepare(&mut self, owner: &mut Writer) -> Result<()> {
        self.scan(owner, None)?;
        self.consume(owner)?;
        super::request_controls::declarations(&mut self.document, owner)?;
        crate::codec::provenance::bindings(&mut self.document, owner)?;
        messages::validate(&self.document)?;
        super::request_schema::validate(&self.document, &self.media)?;
        let x = self.document.required(self.document.root(), "x_ck")?;
        let mut path = [None; 128];
        path[0] = Some(Component::Name(Name::Static("x_ck")));
        fields(
            &mut self.document,
            &self.media,
            x,
            (&mut path, 1),
            &mut self.emitter,
            owner,
            &mut self.reader,
        )?;
        self.scanner = None;
        self.document.retire_scratch()
    }
}

impl codec::RequestIngress for Ingress {
    fn flush_deadline(&self) -> Option<std::time::Instant> {
        let held = self.string.as_ref()?.held?;
        Some(held + super::FLUSH_DELAY)
    }

    fn advance(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.failed {
            return Err(Error::Closed);
        }
        let result = (|| {
            if bytes.is_empty() {
                return self.flush_string(owner);
            }
            for bytes in bytes.chunks(8192) {
                self.scan(owner, Some(bytes))?;
                self.consume(owner)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.failed = true;
            if !matches!(error, Error::Storage(_)) {
                super::failure(&mut self.emitter, owner, "client")?;
            }
            return Err(error);
        }
        Ok(())
    }
    fn finish<'a>(&'a mut self, owner: &mut Writer, destination: &impl Capability) -> Result<ValidatedRequest<'a>> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.failed {
            return Err(Error::Closed);
        }
        if let Err(error) = self.prepare(owner) {
            self.failed = true;
            super::failure(&mut self.emitter, owner, "client")?;
            return Err(error);
        }
        let doc = &self.document;
        let view = RequestView {
            document: doc,
            media: &self.media,
            operation: self.operation,
        };
        let ready = match self.fence.end(view, destination) {
            Ok(ready) => ready,
            Err(error) => {
                self.failed = true;
                super::failure(&mut self.emitter, owner, "client")?;
                return Err(error);
            }
        };
        self.emitter.request_end(owner, &ready)?;
        self.emitter
            .emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
        Ok(ready)
    }
}

pub struct Egress {
    pub destination: &'static str,
    pub upstream_stream: bool,
}
impl codec::RequestEgress for Egress {
    fn encode(&self, request: &ValidatedRequest<'_>, owner: &mut Writer) -> Result<ScratchLease> {
        let view = request.for_destination(self.destination)?;
        if view.operation != Operation::Generate {
            return Err(Error::Unsupported);
        }
        view.document.check_owner(owner)?;
        encode::scratch(owner, |e| write_request(view, self.upstream_stream, e))
    }
}

/// Chat's protocol capabilities; model-specific restrictions belong in a distinct policy key.
pub struct ChatCapability;
impl Capability for ChatCapability {
    fn destination(&self) -> &'static str {
        "chat"
    }
    fn validate(&self, view: RequestView<'_>) -> Result<()> {
        if view.operation != Operation::Generate {
            return Err(Error::Unsupported);
        }
        messages::validate(view.document)?;
        super::request_schema::validate(view.document, view.media)?;
        super::request_controls::destination(view.document)
    }
}

pub(super) fn require_string(doc: &Document, node: usize) -> Result<()> {
    if matches!(
        doc.node(node)?.value,
        Value::String(_) | Value::Text(_) | Value::Id(_) | Value::Utf8Media
    ) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}
fn require_array(doc: &Document, node: usize) -> Result<()> {
    if matches!(doc.node(node)?.value, Value::Array) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn initialize(doc: &mut Document) -> Result<()> {
    let root = doc.root();
    let x = doc.set(root, "x_ck", Value::Object)?;
    doc.set(x, "v", Value::Unsigned(1))?;
    doc.set(x, "original_protocol", Value::Text("chat"))?;
    doc.set(x, "operation", Value::Text("generate"))?;
    doc.set(x, "original", Value::Array)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{
        RequestEgress, RequestIngress,
        event::{Direction, LocalId},
        source::Span,
    };

    #[test]
    fn unfinished_request_retires_raw_positions_before_completion() {
        let directory = std::env::temp_dir().join(format!("ck-request-retirement-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(48 * ck_stream::FRAME_BYTES);
        config.max_event_bytes = 16 * ck_stream::FRAME_BYTES;
        config.max_metadata_bytes = 4 * 1024 * 1024;
        config.header_slots = 8;
        config.workers = 1;
        let mut store =
            ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
        let mut owner = store.create().unwrap();
        let identity = Identity {
            request_id: LocalId::new("request").unwrap(),
            stream_id: LocalId::new("stream").unwrap(),
            attempt: 1,
            direction: Direction::Request,
        };
        let mut ingress = Ingress::new(&mut owner, &store, identity, Operation::Generate, 100_000).unwrap();
        let prefix = format!(
            r#"{{"model":"m","messages":[{{"role":"user","content":"{}"#,
            "a".repeat(8192)
        );
        ingress.advance(&mut owner, prefix.as_bytes()).unwrap();
        // Held string input is published, and its positions retired, at a flush.
        ingress.flush(&mut owner).unwrap();
        assert!(matches!(
            ingress.document.input().equals(Span { start: 2048, end: 2049 }, b"a"),
            Err(Error::Position)
        ));
        assert!(u64::from(store.used_frames()) <= owner.committed().frame + 2);
        for tail in [b"\\uD83D".as_slice(), b"\\uDE80", b"\xc3", b"\xa9", br#""}]}"#] {
            ingress.advance(&mut owner, tail).unwrap();
        }
        let ready = ingress.finish(&mut owner, &ChatCapability).unwrap();
        let output = Egress {
            destination: "chat",
            upstream_stream: false,
        }
        .encode(&ready, &mut owner)
        .unwrap();
        let mut bytes = Vec::new();
        while bytes.len() < output.len() {
            bytes.extend_from_slice(output.chunk(bytes.len()).unwrap());
        }
        let output: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(output["messages"][0]["content"], format!("{}🚀é", "a".repeat(8192)));
        drop(ingress);
        store.shutdown().unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    /// A string that fills data records retires its published input without a flush.
    #[test]
    fn filled_string_records_retire_their_input() {
        let directory = std::env::temp_dir().join(format!("ck-request-filled-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(48 * ck_stream::FRAME_BYTES);
        config.workers = 1;
        let mut store =
            ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
        let mut owner = store.create().unwrap();
        let identity = Identity {
            request_id: LocalId::new("request").unwrap(),
            stream_id: LocalId::new("stream").unwrap(),
            attempt: 1,
            direction: Direction::Request,
        };
        let mut ingress = Ingress::new(&mut owner, &store, identity, Operation::Generate, 1_000_000).unwrap();
        let prefix = format!(
            r#"{{"model":"m","messages":[{{"role":"user","content":"{}"#,
            "a".repeat(70_000)
        );
        for piece in prefix.as_bytes().chunks(1000) {
            ingress.advance(&mut owner, piece).unwrap();
        }
        assert!(matches!(
            ingress.document.input().equals(Span { start: 2048, end: 2049 }, b"a"),
            Err(Error::Position)
        ));
        drop(ingress);
        store.shutdown().unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    /// Published fields' records share one source rather than adding a source each.
    #[test]
    fn published_fields_share_one_records_source() {
        let directory = std::env::temp_dir().join(format!("ck-request-records-{}", uuid::Uuid::new_v4()));
        let mut config = ck_stream::Config::new(directory.clone());
        config.pool_bytes = Some(48 * ck_stream::FRAME_BYTES);
        config.workers = 1;
        let mut store =
            ck_stream::Store::with_clock(config, std::sync::Arc::new(ck_stream::ManualClock::default())).unwrap();
        let mut owner = store.create().unwrap();
        let identity = Identity {
            request_id: LocalId::new("request").unwrap(),
            stream_id: LocalId::new("stream").unwrap(),
            attempt: 1,
            direction: Direction::Request,
        };
        let mut ingress = Ingress::new(&mut owner, &store, identity, Operation::Generate, 100_000).unwrap();
        let messages = vec![r#"{"role":"user","content":"hi"}"#; 32].join(",");
        let input = format!(r#"{{"model":"m","messages":[{messages}]}}"#);
        ingress.advance(&mut owner, input.as_bytes()).unwrap();
        ingress.finish(&mut owner, &ChatCapability).unwrap();
        assert!(
            ingress.document.source_count() <= 2,
            "{}",
            ingress.document.source_count()
        );
        drop(ingress);
        store.shutdown().unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}
