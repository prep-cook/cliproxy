//! Response identity at egress. Ingress fills a required canonical field it could not read from
//! the source with a generated value and names it in `x_ck.synthetic` (docs/INTERNAL-STREAM.md,
//! §3.2). An egress never shows such a value to its client as if the source had sent it.

use super::{Error, Result, document::Document, event::LocalId, media};

/// Whether the canonical record `node` names `field` as generated rather than supplied.
pub fn synthetic(doc: &Document, node: usize, field: &str) -> Result<bool> {
    let Some(x) = doc.member(node, "x_ck")? else {
        return Ok(false);
    };
    let Some(names) = doc.member(x, "synthetic")? else {
        return Ok(false);
    };
    for name in doc.children(names)? {
        if doc.string_is(name, field)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The response id an egress shows its client, under one rule for every egress:
///
/// - A source id, when the source supplied one, is the response id. A generated canonical id, or
///   an absent one, never reaches the client; the egress issues its own native id instead.
/// - Streaming: the first id a client sees never changes, so a source id arriving after the
///   issued id was shown is dropped.
/// - Buffered: nothing is sent before the body, so the body carries the source id if one arrived
///   at any point, because a client needs the real upstream id (for example as
///   `previous_response_id` to continue). The issued id remains only if none arrived.
pub struct ResponseId {
    /// This egress's own id while no source id has replaced it.
    issued: Option<LocalId>,
    /// The generated canonical id of the start, which ingress repeats on later records.
    generated: Option<LocalId>,
}
impl ResponseId {
    /// Issues `prefix` and a local id derived from the stream's writer, so every record of the
    /// response can repeat it.
    pub fn new(prefix: &str, owner: u64) -> Result<Self> {
        let local = media::local_id(b's', owner)?;
        let mut bytes = [0u8; 64];
        let id = bytes
            .get_mut(..prefix.len() + local.as_str().len())
            .ok_or(Error::Limit)?;
        let (head, tail) = id.split_at_mut(prefix.len());
        head.copy_from_slice(prefix.as_bytes());
        tail.copy_from_slice(local.as_str().as_bytes());
        Ok(Self {
            issued: Some(LocalId::new(std::str::from_utf8(id).map_err(|_| Error::Schema)?)?),
            generated: None,
        })
    }

    /// The issued id, until a source id replaces it.
    pub fn issued(&self) -> Option<LocalId> {
        self.issued
    }

    /// Reads the canonical start `node` and returns its source id, which replaces the issued one.
    pub fn start(&mut self, doc: &Document, node: usize) -> Result<Option<usize>> {
        let Some(id) = doc.member(node, "id")? else {
            return Ok(None);
        };
        if synthetic(doc, node, "id")? {
            self.generated = Some(local(doc, id)?);
            return Ok(None);
        }
        self.issued = None;
        Ok(Some(id))
    }

    /// Whether `id`, a later id in `doc`, becomes the response id; `shown` says whether a client
    /// has seen the current one. Ingress repeats a generated start id on later records without
    /// marking each, so that value never counts as a source id. An adopted id drops the issued one.
    pub fn adopts(&mut self, doc: &Document, id: usize, shown: bool) -> Result<bool> {
        if shown {
            return Ok(false);
        }
        if self.issued.is_none() {
            return Ok(true);
        }
        if let Some(generated) = self.generated
            && doc.string_is(id, generated.as_str())?
        {
            return Ok(false);
        }
        self.issued = None;
        Ok(true)
    }
}

/// Reads a generated canonical id, which is always a local id.
fn local(doc: &Document, node: usize) -> Result<LocalId> {
    let mut bytes = [0u8; 64];
    let mut len = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        let slot = bytes.get_mut(len).filter(|_| c.is_ascii()).ok_or(Error::Schema)?;
        *slot = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&bytes[..len]).map_err(|_| Error::Schema)?)
}
