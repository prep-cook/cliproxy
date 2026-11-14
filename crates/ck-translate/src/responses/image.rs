use crate::codec::{
    Error, Result,
    document::{Document, Value},
    event::{Emitter, LocalId, Scope},
    media,
};
use ck_stream::Writer;
use sha2::{Digest, Sha256};

pub(super) fn descriptor(doc: &Document, data: usize, id: LocalId) -> Result<media::Media> {
    let Value::String(text) = doc.node(data)?.value else {
        return Err(Error::Schema);
    };
    Ok(media::Media {
        node: data,
        id,
        data: Some(text),
        encoding: media::Encoding::Base64,
        kind: "image",
        mime: None,
    })
}
pub(super) fn digest(doc: &Document, data: usize) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    media::bytes(doc, descriptor(doc, data, LocalId::new("hash")?)?, |bytes| {
        hash.update(bytes);
        Ok(())
    })?;
    Ok(hash.finalize().into())
}
pub(super) fn publish(
    owner: &mut Writer,
    emitter: &mut Emitter,
    doc: &Document,
    data: usize,
    mut scope: Scope,
    id: LocalId,
    partial: Option<u64>,
) -> Result<[u8; 32]> {
    scope.media_id = Some(id);
    let mut publisher = start(owner, emitter, scope, partial)?;
    let mut hash = Sha256::new();
    let mut chunk = [0; 36_864];
    let (mut used, mut index, mut offset) = (0, 0, 0);
    let mut ceiling = emitter.data_capacity(scope, index, offset)?;
    media::bytes(doc, descriptor(doc, data, id)?, |bytes| {
        hash.update(bytes);
        for byte in bytes {
            chunk[used] = *byte;
            used += 1;
            if used == ceiling {
                publisher.append(&chunk[..used], emitter, owner)?;
                index += 1;
                offset += used as u64;
                used = 0;
                ceiling = emitter.data_capacity(scope, index, offset)?;
            }
        }
        Ok(())
    })?;
    if used != 0 {
        publisher.append(&chunk[..used], emitter, owner)?;
    }
    publisher.end(emitter, owner)?;
    Ok(hash.finalize().into())
}
/// Opens the media lifetime of a generated image or preview; `scope` names its media id.
pub(super) fn start(
    owner: &mut Writer,
    emitter: &mut Emitter,
    scope: Scope,
    partial: Option<u64>,
) -> Result<media::Publisher> {
    media::Publisher::start(emitter, owner, scope, |e| {
        e.bytes(b",\"kind\":\"image\",\"encoding\":\"base64\",\"attributes\":{\"protocol\":\"responses\",\"field\":\"result\"")?;
        if let Some(index) = partial {
            e.bytes(b",\"partial_image_index\":")?;
            e.unsigned(index)?;
        }
        e.bytes(b"}")
    })
}

pub(super) struct Projection {
    pub id: usize,
    pub data: usize,
    pub item: usize,
    pub output_index: u64,
    pub partial: Option<u64>,
    next: u64,
    bytes: u64,
    pub closed: bool,
}
impl Projection {
    pub fn start(doc: &mut Document, event: usize, item: usize, output_index: u64) -> Result<Self> {
        if !super::is(doc, event, "kind", "image")? {
            return Err(Error::Unsupported);
        }
        let attributes = doc.required(event, "attributes")?;
        if !super::is(doc, attributes, "protocol", "responses")? || !super::is(doc, attributes, "field", "result")? {
            return Err(Error::Unsupported);
        }
        let partial = doc
            .member(attributes, "partial_image_index")?
            .map(|n| doc.unsigned(n))
            .transpose()?;
        let x = doc.required(event, "x_ck")?;
        let id = doc.required(x, "media_id")?;
        let data = doc.add(None, Value::Base64Fragments)?;
        if partial.is_none() {
            if doc.member(item, "result")?.is_some() {
                return Err(Error::Schema);
            }
            doc.rename(data, "result")?;
            doc.append(item, data)?;
        }
        Ok(Self {
            id,
            data,
            item,
            output_index,
            partial,
            next: 0,
            bytes: 0,
            closed: false,
        })
    }
    pub fn append(&mut self, doc: &mut Document, event: usize) -> Result<()> {
        if self.closed
            || doc.unsigned(doc.required(event, "index")?)? != self.next
            || doc.unsigned(doc.required(event, "offset")?)? != self.bytes
        {
            return Err(Error::Schema);
        }
        let data = doc.required(event, "bytes")?;
        let Value::String(text) = doc.node(data)?.value else {
            return Err(Error::Schema);
        };
        let mut count = 0;
        media::decode_base64(doc, text, |bytes| {
            count += bytes.len() as u64;
            Ok(())
        })?;
        doc.detach(event, data)?;
        doc.append(self.data, data)?;
        self.next = self.next.checked_add(1).ok_or(Error::Limit)?;
        self.bytes = self.bytes.checked_add(count).ok_or(Error::Limit)?;
        Ok(())
    }
    pub fn end(&mut self, doc: &Document, event: usize) -> Result<()> {
        if self.closed
            || doc.unsigned(doc.required(event, "bytes")?)? != self.bytes
            || !super::is(doc, event, "status", "completed")?
        {
            return Err(Error::Schema);
        }
        self.closed = true;
        Ok(())
    }
}
