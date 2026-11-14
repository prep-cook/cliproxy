//! Helpers shared by the native protocol boundary codecs (Anthropic, Responses, Gemini).
//!
//! Test-only inclusion, like the protocol modules, until the framework owner registers them.

use crate::codec::{
    Capability, Error, RequestFence, RequestView, Result, ValidatedRequest,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Kind, Scope},
    source::Source,
};
use ck_stream::{MetadataPermit, Writer};

/// Parses encoder output back into a document, so normalization can work on mapped JSON.
pub(crate) fn mapped(owner: &mut Writer, write: impl Fn(&mut Encoder<'_, '_>) -> Result<()>) -> Result<Document> {
    let lease = encode::scratch(owner, write)?;
    let mut source = Source::new(owner)?;
    source.push_scratch(lease)?;
    let len = source.len();
    Document::parse(owner, source, len)
}

/// Emits the sanitized terminal error for invalid client (400) or upstream (502) input, with
/// the code, status and message the Chat codec uses.
pub(crate) fn failure(emitter: &mut Emitter, owner: &mut Writer, client: bool) -> Result<()> {
    emitter.emit(
        owner,
        Kind::Error,
        Scope::default(),
        |e| {
            e.bytes(b",\"code\":\"stream.invalid_input\",\"status\":")?;
            e.unsigned(if client { 400 } else { 502 })?;
            e.bytes(b",\"message\":\"Invalid boundary input\",\"scope\":")?;
            e.string(if client { "client" } else { "upstream" })
        },
        |_| Ok(()),
    )?;
    Ok(())
}

/// Passes `result` through, emitting [`failure`] first unless the error is a storage failure,
/// which the stream reports itself.
pub(crate) fn reported<T>(result: Result<T>, emitter: &mut Emitter, owner: &mut Writer, client: bool) -> Result<T> {
    if let Err(error) = &result
        && !matches!(error, Error::Storage(_))
    {
        failure(emitter, owner, client)?;
    }
    result
}

/// Rejects a call from a writer other than the codec's owner, or after the codec closed.
pub(crate) fn enter(expected: u64, owner: &Writer, closed: bool) -> Result<()> {
    if owner.id() != expected {
        return Err(Error::Position);
    }
    if closed {
        return Err(Error::Closed);
    }
    Ok(())
}

/// Passes the request fence, then publishes `request.end` and `done`; a fence rejection
/// emits the client error instead.
pub(crate) fn end_request<'a>(
    fence: &mut RequestFence,
    view: RequestView<'a>,
    destination: &impl Capability,
    emitter: &mut Emitter,
    owner: &mut Writer,
) -> Result<ValidatedRequest<'a>> {
    let ready = match fence.end(view, destination) {
        Ok(ready) => ready,
        Err(error) => {
            failure(emitter, owner, true)?;
            return Err(error);
        }
    };
    emitter.request_end(owner, &ready)?;
    emitter.emit(owner, Kind::Done, Scope::default(), |_| Ok(()), |_| Ok(()))?;
    Ok(ready)
}

/// Requires a string-like node.
pub(crate) fn string(doc: &Document, node: usize) -> Result<()> {
    if matches!(
        doc.node(node)?.value,
        Value::String(_) | Value::Text(_) | Value::Id(_) | Value::Fragments | Value::Utf8Media
    ) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

/// Whether member `key` of `parent` is the string `value`.
pub(crate) fn is(doc: &Document, parent: usize, key: &str, value: &str) -> Result<bool> {
    doc.member(parent, key)?
        .map_or(Ok(false), |node| doc.string_is(node, value))
}

/// Writes `,"key":value` when `parent` has member `key`.
pub(crate) fn member(doc: &Document, parent: usize, key: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = doc.member(parent, key)? {
        e.bytes(b",")?;
        e.string(key)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}

/// Writes object `parent` without the members named in `skip`.
pub(crate) fn object(doc: &Document, parent: usize, skip: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for child in doc.children(parent)? {
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        if named(doc, name, skip)? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        doc.write(child, e)?;
    }
    e.bytes(b"}")
}

/// Writes `,"original":[...]` provenance entries for every member of each `(parent, pointer
/// prefix, known names)` group that is not known; writes nothing when all members are known.
pub(crate) fn original(
    doc: &Document,
    protocol: &str,
    groups: &[(usize, &str, &[&str])],
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let mut first = true;
    for &(parent, prefix, known) in groups {
        for field in doc.children(parent)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if named(doc, name, known)? {
                continue;
            }
            e.bytes(if first { b",\"original\":[" } else { b"," })?;
            first = false;
            e.bytes(b"{\"protocol\":")?;
            e.string(protocol)?;
            e.bytes(b",\"pointer\":\"")?;
            e.escaped(prefix)?;
            e.bytes(b"/")?;
            let mut chars = doc.name_chars(name)?;
            while let Some(c) = chars.next_scalar()? {
                match c {
                    '~' => e.bytes(b"~0")?,
                    '/' => e.bytes(b"~1")?,
                    c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
                }
            }
            e.bytes(b"\",\"value\":")?;
            doc.write(field, e)?;
            e.bytes(b"}")?;
        }
    }
    if !first {
        e.bytes(b"]")?;
    }
    Ok(())
}

fn named(doc: &Document, name: crate::codec::document::Name, keys: &[&str]) -> Result<bool> {
    keys.iter()
        .try_fold(false, |found, key| Ok(found || doc.name_is(name, key)?))
}

/// Capacity accounting for position/scalar tables; payload bytes never belong here.
// ponytail: copy of the private `codec::table::Table`; replace with a re-export once `codec`
// exposes that module to sibling codecs.
pub(crate) struct Table<T> {
    values: Vec<T>,
    permit: MetadataPermit,
}
impl<T> Table<T> {
    pub(crate) fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            permit: owner.reserve_metadata(0)?,
        })
    }
    pub(crate) fn push(&mut self, value: T) -> Result<usize> {
        self.permit.check_live()?;
        if self.values.len() == self.values.capacity() {
            let capacity = self.values.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
            self.permit
                .resize(capacity.checked_mul(std::mem::size_of::<T>()).ok_or(Error::Limit)?)?;
            self.values
                .try_reserve_exact(capacity - self.values.len())
                .map_err(|_| Error::Limit)?;
            self.permit.resize(self.values.capacity() * std::mem::size_of::<T>())?;
        }
        let index = self.values.len();
        self.values.push(value);
        Ok(index)
    }
}
impl<T> std::ops::Deref for Table<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}
impl<T> std::ops::DerefMut for Table<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}
