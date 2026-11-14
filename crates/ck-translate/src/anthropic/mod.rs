//! Anthropic Messages boundary codecs over the shared canonical stream.

mod content;
pub mod egress;
pub mod request;
mod request_encode;
mod request_schema;
pub mod response;
mod usage;

use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Emitter, Kind, Scope},
    source::Source,
};
use ck_stream::Writer;

fn mapped(owner: &mut Writer, write: impl Fn(&mut Encoder<'_, '_>) -> Result<()>) -> Result<Document> {
    let lease = encode::scratch(owner, write)?;
    let mut source = Source::new(owner)?;
    source.push_scratch(lease)?;
    let len = source.len();
    Document::parse(owner, source, len)
}

fn failure(emitter: &mut Emitter, owner: &mut Writer, client: bool) -> Result<()> {
    emitter.emit(owner, Kind::Error, Scope::default(), |e| {
        e.bytes(if client {
            b",\"code\":\"invalid_request\",\"status\":400,\"message\":\"Invalid request\",\"scope\":\"client\""
        } else {
            b",\"code\":\"invalid_response\",\"status\":502,\"message\":\"Invalid upstream response\",\"scope\":\"upstream\""
        })
    }, |_| Ok(()))?;
    Ok(())
}

fn string(doc: &Document, node: usize) -> Result<()> {
    if matches!(
        doc.node(node)?.value,
        Value::String(_) | Value::Text(_) | Value::Id(_) | Value::Fragments | Value::Utf8Media
    ) {
        Ok(())
    } else {
        Err(Error::Schema)
    }
}

fn is(doc: &Document, parent: usize, key: &str, value: &str) -> Result<bool> {
    doc.member(parent, key)?
        .map_or(Ok(false), |node| doc.string_is(node, value))
}

fn member(doc: &Document, parent: usize, key: &str, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(node) = doc.member(parent, key)? {
        e.bytes(b",")?;
        e.string(key)?;
        e.bytes(b":")?;
        doc.write(node, e)?;
    }
    Ok(())
}

fn object(doc: &Document, parent: usize, skip: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for child in doc.children(parent)? {
        let name = doc.node(child)?.name.ok_or(Error::Schema)?;
        if skip
            .iter()
            .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
        {
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

fn unknown(doc: &Document, parent: usize, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    unknown_at(doc, &[(parent, "", known)], e)
}

fn unknown_at(doc: &Document, groups: &[(usize, &str, &[&str])], e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for &(parent, prefix, known) in groups {
        for field in doc.children(parent)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            if known
                .iter()
                .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?
            {
                continue;
            }
            e.bytes(if first { b",\"original\":[" } else { b"," })?;
            first = false;
            e.bytes(b"{\"protocol\":\"anthropic\",\"pointer\":\"")?;
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

struct Table<T> {
    rows: Vec<T>,
    permit: ck_stream::MetadataPermit,
}
impl<T> Table<T> {
    fn new(owner: &mut Writer) -> Result<Self> {
        Ok(Self {
            rows: Vec::new(),
            permit: owner.reserve_metadata(0)?,
        })
    }
    fn push(&mut self, value: T) -> Result<()> {
        if self.rows.len() == self.rows.capacity() {
            let capacity = self.rows.capacity().max(4).checked_mul(2).ok_or(Error::Limit)?;
            self.permit
                .resize(capacity.checked_mul(std::mem::size_of::<T>()).ok_or(Error::Limit)?)?;
            self.rows
                .try_reserve_exact(capacity - self.rows.len())
                .map_err(|_| Error::Limit)?;
            self.permit.resize(self.rows.capacity() * std::mem::size_of::<T>())?;
        }
        self.rows.push(value);
        Ok(())
    }
}

fn server_result(doc: &Document, node: usize) -> Result<bool> {
    [
        "web_search_tool_result",
        "web_fetch_tool_result",
        "code_execution_tool_result",
        "bash_code_execution_tool_result",
        "text_editor_code_execution_tool_result",
        "tool_search_tool_result",
        "mcp_tool_result",
    ]
    .iter()
    .try_fold(false, |found, kind| Ok(found || is(doc, node, "type", kind)?))
}

fn equal_text(left: &Document, a: usize, right: &Document, b: usize) -> Result<bool> {
    let mut a = left.chars(a)?;
    let mut b = right.chars(b)?;
    loop {
        let value = a.next_scalar()?;
        if value != b.next_scalar()? {
            return Ok(false);
        }
        if value.is_none() {
            return Ok(true);
        }
    }
}
