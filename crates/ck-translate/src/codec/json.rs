mod decode;

pub use decode::{Decoded, string_is};
pub(crate) use decode::{StringDecoder, Utf8};

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hasher},
};

use super::{
    Error, Result,
    source::{Source, Span},
};
use decode::string_eq;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Object,
    Array,
    String,
    Number,
    Bool,
    Null,
}

/// A JSON value index contains coordinates and topology, never decoded payloads.
#[derive(Clone, Copy, Debug)]
pub struct Node {
    pub kind: Kind,
    pub span: Span,
    /// End of the last complete decoded scalar, excluding the closing quote.
    pub validated_end: u64,
    pub key: Option<Span>,
    pub first: Option<usize>,
    pub next: Option<usize>,
    pub parent: Option<usize>,
}

#[derive(Clone, Copy)]
enum Expect {
    ObjectFirst,
    Key,
    Colon,
    Value,
    ArrayFirst,
    ArrayValue,
    Comma,
}
struct Frame {
    node: usize,
    expect: Expect,
    key: Option<Span>,
    last: Option<usize>,
}
struct Key {
    span: Span,
    parent: usize,
    retained: Option<super::document::Name>,
    previous: Option<usize>,
}
#[derive(Clone, Copy)]
enum Mode {
    Idle,
    String { start: u64, key: bool, node: Option<usize> },
    Atom { start: u64 },
}

/// Incremental strict JSON scanner. `advance` reads only newly appended input.
///
/// Duplicate keys compare decoded strings, including escape-equivalent spellings.
/// Metadata and nesting are bounded; strings/numbers/unknown values remain spans.
/// Any error poisons the scanner. `finish` is mandatory at the body boundary.
pub struct Scanner {
    nodes: Vec<Node>,
    frames: Vec<Frame>,
    keys: Vec<Key>,
    key_heads: HashMap<(usize, u64), usize>,
    hash: std::collections::hash_map::RandomState,
    mode: Mode,
    offset: u64,
    source_id: Option<u64>,
    max_bytes: u64,
    metadata: ck_stream::MetadataPermit,
    failed: bool,
    finished: bool,
    utf8: Utf8,
    string: StringDecoder,
}

impl Scanner {
    pub fn new(owner: &mut ck_stream::Writer, max_bytes: u64) -> Result<Self> {
        Ok(Self {
            nodes: Vec::new(),
            frames: Vec::new(),
            keys: Vec::new(),
            key_heads: HashMap::new(),
            hash: Default::default(),
            mode: Mode::Idle,
            offset: 0,
            source_id: None,
            max_bytes,
            metadata: owner.reserve_metadata(0)?,
            failed: false,
            finished: false,
            utf8: Utf8::default(),
            string: StringDecoder::default(),
        })
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Inspect scanned topology without treating an open container as a value.
    /// The source must be the exact live prefix most recently scanned.
    pub fn pending_node(&self, source: &Source, index: usize) -> Result<Node> {
        self.validate_prefix(source)?;
        self.nodes.get(index).copied().ok_or(Error::Position)
    }

    /// A completed subtree is readable before the enclosing document ends.
    pub fn completed_node(&self, source: &Source, index: usize) -> Result<Option<Node>> {
        let node = self.pending_node(source, index)?;
        Ok((node.span.end != 0).then_some(node))
    }

    fn validate_prefix(&self, source: &Source) -> Result<()> {
        source.check_live()?;
        if self.failed {
            return Err(Error::Closed);
        }
        if source.stream_id() != self.metadata.stream_id()
            || self.source_id != Some(source.id())
            || self.offset != source.len()
        {
            return Err(Error::Position);
        }
        Ok(())
    }

    /// Reject pairing validated locations with another or subsequently extended source.
    pub fn validate_source(&self, source: &Source) -> Result<()> {
        self.validate_prefix(source)?;
        if !self.finished {
            return Err(Error::Truncated);
        }
        Ok(())
    }

    pub fn advance(&mut self, source: &Source) -> Result<()> {
        if self.failed || self.finished {
            return Err(Error::Closed);
        }
        let result = self.scan(source, None);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    pub(crate) fn advance_document(&mut self, doc: &super::document::Document) -> Result<()> {
        if self.failed || self.finished {
            return Err(Error::Closed);
        }
        let result = self.scan(doc.input(), Some(doc));
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    pub(crate) fn bind_key(&mut self, span: Span, name: super::document::Name) {
        if let Ok(index) = self.keys.binary_search_by_key(&span.start, |key| key.span.start) {
            self.keys[index].retained = Some(name);
        }
    }

    pub(crate) fn key_is(&self, doc: &super::document::Document, span: Span, text: &str) -> Result<bool> {
        if let Ok(index) = self.keys.binary_search_by_key(&span.start, |key| key.span.start)
            && let Some(name) = self.keys[index].retained
        {
            return doc.name_is(name, text);
        }
        string_is(doc.input(), span, text)
    }

    pub(crate) fn needs_key(&self, span: Span) -> bool {
        self.keys
            .binary_search_by_key(&span.start, |key| key.span.start)
            .ok()
            .is_some_and(|index| {
                let key = &self.keys[index];
                key.retained.is_none() && self.frames.iter().any(|frame| frame.node == key.parent)
            })
    }

    pub(crate) fn raw_floor(&self) -> u64 {
        let mut floor = match self.mode {
            Mode::String { start, key: true, .. } | Mode::Atom { start } => start,
            _ => self.offset,
        };
        for key in &self.keys {
            if key.retained.is_none() && self.frames.iter().any(|frame| frame.node == key.parent) {
                floor = floor.min(key.span.start);
            }
        }
        floor
    }

    fn scan(&mut self, source: &Source, document: Option<&super::document::Document>) -> Result<()> {
        if source.stream_id() != self.metadata.stream_id() || self.source_id.is_some_and(|id| id != source.id()) {
            return Err(Error::Position);
        }
        self.source_id = Some(source.id());
        if source.len() < self.offset {
            return Err(Error::Position);
        }
        if source.len() > self.max_bytes {
            return Err(Error::EventTooLarge);
        }
        let mut cursor = source.cursor(Span {
            start: self.offset,
            end: source.len(),
        })?;
        while let Some(byte) = cursor.next_byte()? {
            let at = self.offset;
            self.utf8.push(byte)?;
            self.offset += 1;
            self.byte(source, at, byte, document)?;
        }
        Ok(())
    }

    fn byte(&mut self, source: &Source, at: u64, byte: u8, document: Option<&super::document::Document>) -> Result<()> {
        match self.mode {
            Mode::String { start, key, node } => {
                if byte == b'"' && self.string.complete() {
                    let span = Span { start, end: at + 1 };
                    self.mode = Mode::Idle;
                    if key {
                        self.key(source, span, document)?;
                    } else {
                        self.nodes[node.ok_or(Error::Json)?].span = span;
                    }
                } else if self.string.push(byte)?.is_some()
                    && let Some(node) = node
                {
                    self.nodes[node].validated_end = at + 1;
                }
                return Ok(());
            }
            Mode::Atom { start } => {
                if !matches!(byte, b' ' | b'\t' | b'\r' | b'\n' | b',' | b']' | b'}') {
                    return Ok(());
                }
                self.atom(source, Span { start, end: at })?;
                self.mode = Mode::Idle;
            }
            Mode::Idle => {}
        }
        if matches!(byte, b' ' | b'\t' | b'\r' | b'\n') {
            return Ok(());
        }
        let expect = self.frames.last().map(|frame| frame.expect);
        match byte {
            b'}' | b']' => {
                let frame = self.frames.last().ok_or(Error::Json)?;
                let kind = self.nodes[frame.node].kind;
                let valid = match kind {
                    Kind::Object => byte == b'}' && matches!(frame.expect, Expect::ObjectFirst | Expect::Comma),
                    Kind::Array => byte == b']' && matches!(frame.expect, Expect::ArrayFirst | Expect::Comma),
                    _ => false,
                };
                if !valid {
                    return Err(Error::Json);
                }
                self.nodes[frame.node].span.end = at + 1;
                self.frames.pop();
            }
            b',' => {
                let frame = self.frames.last_mut().ok_or(Error::Json)?;
                if !matches!(frame.expect, Expect::Comma) {
                    return Err(Error::Json);
                }
                frame.expect = if self.nodes[frame.node].kind == Kind::Object {
                    Expect::Key
                } else {
                    Expect::ArrayValue
                };
            }
            b':' => {
                let frame = self.frames.last_mut().ok_or(Error::Json)?;
                if !matches!(frame.expect, Expect::Colon) {
                    return Err(Error::Json);
                }
                frame.expect = Expect::Value;
            }
            b'"' if matches!(expect, Some(Expect::ObjectFirst | Expect::Key)) => {
                self.string = StringDecoder::default();
                self.mode = Mode::String {
                    start: at,
                    key: true,
                    node: None,
                };
            }
            _ => self.start_value(at, byte, expect)?,
        }
        Ok(())
    }

    /// Open the value that `byte` begins at `at`: a container, a string or an atom.
    fn start_value(&mut self, at: u64, byte: u8, expect: Option<Expect>) -> Result<()> {
        if !matches!(
            expect,
            None | Some(Expect::Value | Expect::ArrayFirst | Expect::ArrayValue)
        ) || (expect.is_none() && !self.nodes.is_empty())
        {
            return Err(Error::Json);
        }
        match byte {
            b'{' | b'[' => {
                if self.frames.len() >= 128 {
                    return Err(Error::Limit);
                }
                let kind = if byte == b'{' { Kind::Object } else { Kind::Array };
                let node = self.value(kind, Span { start: at, end: 0 })?;
                self.reserve(0, 1, 0)?;
                self.frames.push(Frame {
                    node,
                    expect: if byte == b'{' {
                        Expect::ObjectFirst
                    } else {
                        Expect::ArrayFirst
                    },
                    key: None,
                    last: None,
                });
            }
            b'"' => {
                let node = self.value(Kind::String, Span { start: at, end: 0 })?;
                self.nodes[node].validated_end = at + 1;
                self.string = StringDecoder::default();
                self.mode = Mode::String {
                    start: at,
                    key: false,
                    node: Some(node),
                };
            }
            b'-' | b'0'..=b'9' | b't' | b'f' | b'n' => self.mode = Mode::Atom { start: at },
            _ => return Err(Error::Json),
        }
        Ok(())
    }

    fn reserve(&mut self, nodes: usize, frames: usize, keys: usize) -> Result<()> {
        let n = capacity(self.nodes.len() + nodes, self.nodes.capacity());
        let f = capacity(self.frames.len() + frames, self.frames.capacity());
        let k = capacity(self.keys.len() + keys, self.keys.capacity());
        // Hash-table capacity includes control bytes and spare buckets; reserve a
        // conservative four buckets per key before asking the allocator to grow.
        let bytes = n
            .checked_mul(std::mem::size_of::<Node>())
            .and_then(|v| v.checked_add(f * std::mem::size_of::<Frame>()))
            .and_then(|v| v.checked_add(k * (std::mem::size_of::<Key>() + 4 * 32)))
            .ok_or(Error::Limit)?;
        self.metadata.resize(bytes)?;
        if n > self.nodes.capacity() {
            self.nodes
                .try_reserve_exact(n - self.nodes.len())
                .map_err(|_| Error::Limit)?;
        }
        if f > self.frames.capacity() {
            self.frames
                .try_reserve_exact(f - self.frames.len())
                .map_err(|_| Error::Limit)?;
        }
        if k > self.keys.capacity() {
            self.keys
                .try_reserve_exact(k - self.keys.len())
                .map_err(|_| Error::Limit)?;
            self.key_heads
                .try_reserve(k - self.key_heads.len())
                .map_err(|_| Error::Limit)?;
        }
        Ok(())
    }

    fn key(&mut self, source: &Source, span: Span, document: Option<&super::document::Document>) -> Result<()> {
        let parent = self.frames.last().ok_or(Error::Json)?.node;
        let mut hash = self.hash.build_hasher();
        let mut decoded = Decoded::new(source, span)?;
        while let Some(scalar) = decoded.next_scalar()? {
            hash.write_u32(scalar as u32);
        }
        let key = (parent, hash.finish());
        let previous = self.key_heads.get(&key).copied();
        let mut cursor = previous;
        while let Some(index) = cursor {
            let equal = if let Some(name) = self.keys[index].retained {
                let doc = document.ok_or(Error::Position)?;
                let mut left = Decoded::new(source, span)?;
                let mut right = doc.name_chars(name)?;
                loop {
                    let c = left.next_scalar()?;
                    if c != right.next_scalar()? {
                        break false;
                    }
                    if c.is_none() {
                        break true;
                    }
                }
            } else {
                string_eq(source, span, self.keys[index].span)?
            };
            if equal {
                return Err(Error::DuplicateKey);
            }
            cursor = self.keys[index].previous;
        }
        self.reserve(0, 0, 1)?;
        self.key_heads.insert(key, self.keys.len());
        self.keys.push(Key {
            span,
            parent,
            retained: None,
            previous,
        });
        let frame = self.frames.last_mut().unwrap();
        frame.key = Some(span);
        frame.expect = Expect::Colon;
        Ok(())
    }

    fn value(&mut self, kind: Kind, span: Span) -> Result<usize> {
        self.reserve(1, 0, 0)?;
        let index = self.nodes.len();
        let key = if let Some(frame) = self.frames.last_mut() {
            if let Some(last) = frame.last {
                self.nodes[last].next = Some(index);
            } else {
                self.nodes[frame.node].first = Some(index);
            }
            frame.last = Some(index);
            frame.expect = Expect::Comma;
            frame.key.take()
        } else {
            None
        };
        self.nodes.push(Node {
            kind,
            span,
            validated_end: span.end,
            key,
            first: None,
            next: None,
            parent: self.frames.last().map(|frame| frame.node),
        });
        Ok(index)
    }

    fn atom(&mut self, source: &Source, span: Span) -> Result<()> {
        let kind = if source.equals(span, b"true")? || source.equals(span, b"false")? {
            Kind::Bool
        } else if source.equals(span, b"null")? {
            Kind::Null
        } else {
            number(source, span)?;
            Kind::Number
        };
        self.value(kind, span)?;
        Ok(())
    }

    pub fn finish(&mut self, source: &Source) -> Result<()> {
        self.advance(source)?;
        let result = (|| {
            if let Mode::Atom { start } = self.mode {
                self.atom(
                    source,
                    Span {
                        start,
                        end: self.offset,
                    },
                )?;
                self.mode = Mode::Idle;
            }
            self.utf8.finish()?;
            if !matches!(self.mode, Mode::Idle) || !self.frames.is_empty() || self.nodes.is_empty() {
                return Err(Error::Truncated);
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        } else {
            self.finished = true;
        }
        result
    }

    pub fn member(&self, source: &Source, object: usize, name: &str) -> Result<Option<usize>> {
        self.validate_source(source)?;
        if self.nodes.get(object).is_none_or(|node| node.kind != Kind::Object) {
            return Err(Error::Json);
        }
        let mut child = self.nodes[object].first;
        while let Some(index) = child {
            if string_is(source, self.nodes[index].key.ok_or(Error::Json)?, name)? {
                return Ok(Some(index));
            }
            child = self.nodes[index].next;
        }
        Ok(None)
    }
}

fn capacity(needed: usize, current: usize) -> usize {
    if needed > current {
        needed.max(8).next_power_of_two()
    } else {
        current
    }
}

fn number(source: &Source, span: Span) -> Result<()> {
    let mut cursor = source.cursor(span)?;
    let mut byte = cursor.next_byte()?;
    if byte == Some(b'-') {
        byte = cursor.next_byte()?;
    }
    match byte {
        Some(b'0') => byte = cursor.next_byte()?,
        Some(b'1'..=b'9') => {
            while matches!(byte, Some(b'0'..=b'9')) {
                byte = cursor.next_byte()?;
            }
        }
        _ => return Err(Error::Json),
    }
    if byte == Some(b'.') {
        byte = cursor.next_byte()?;
        if !matches!(byte, Some(b'0'..=b'9')) {
            return Err(Error::Json);
        }
        while matches!(byte, Some(b'0'..=b'9')) {
            byte = cursor.next_byte()?;
        }
    }
    if matches!(byte, Some(b'e' | b'E')) {
        byte = cursor.next_byte()?;
        if matches!(byte, Some(b'+' | b'-')) {
            byte = cursor.next_byte()?;
        }
        if !matches!(byte, Some(b'0'..=b'9')) {
            return Err(Error::Json);
        }
        while matches!(byte, Some(b'0'..=b'9')) {
            byte = cursor.next_byte()?;
        }
    }
    if byte.is_some() {
        return Err(Error::Json);
    }
    Ok(())
}
