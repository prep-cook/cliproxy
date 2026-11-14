//! Media payload decoding: base64, data URI and UTF-8 payloads read from JSON strings,
//! and re-encoding of decoded chunks as one contiguous base64 string.

use super::{Encoding, Media};
use crate::codec::{
    Error, Result,
    document::{Document, Text, Value},
    encode::Encoder,
    json::Decoded,
};

/// Decodes at most one scalar/quartet at a time. Only the emitted chunk is staged on the stack.
pub fn bytes(doc: &Document, media: Media, mut emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    let text = media.data.ok_or(Error::Position)?;
    let mut decoded = Decoded::new(doc.source(text)?, text.span)?;
    if let Encoding::DataUri { skip } = media.encoding {
        for _ in 0..skip {
            decoded.next_scalar()?.ok_or(Error::Schema)?;
        }
    }
    let mut quartet = [0u8; 4];
    let mut count = 0;
    let mut padded = false;
    while let Some(c) = decoded.next_scalar()? {
        match media.encoding {
            Encoding::Utf8 => emit(c.encode_utf8(&mut [0; 4]).as_bytes())?,
            Encoding::Base64 | Encoding::DataUri { .. } => {
                if padded || !c.is_ascii() {
                    return Err(Error::Schema);
                }
                quartet[count] = c as u8;
                count += 1;
                if count == 4 {
                    use base64::Engine;
                    let mut out = [0; 3];
                    let len = base64::engine::general_purpose::STANDARD
                        .decode_slice(quartet, &mut out)
                        .map_err(|_| Error::Schema)?;
                    padded = quartet.contains(&b'=');
                    emit(&out[..len])?;
                    count = 0;
                }
            }
        }
    }
    if count != 0 {
        return Err(Error::Schema);
    }
    Ok(())
}

pub(crate) fn decode_base64(doc: &Document, text: Text, mut emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    use base64::Engine;
    let mut decoded = Decoded::new(doc.source(text)?, text.span)?;
    let mut quartet = [0; 4];
    let mut count = 0;
    let mut padded = false;
    while let Some(c) = decoded.next_scalar()? {
        if padded || !c.is_ascii() {
            return Err(Error::Schema);
        }
        quartet[count] = c as u8;
        count += 1;
        if count == 4 {
            let mut bytes = [0; 3];
            let len = base64::engine::general_purpose::STANDARD
                .decode_slice(quartet, &mut bytes)
                .map_err(|_| Error::Schema)?;
            padded = quartet.contains(&b'=');
            emit(&bytes[..len])?;
            count = 0;
        }
    }
    if count == 0 { Ok(()) } else { Err(Error::Schema) }
}

pub(crate) fn write_base64_fragments(doc: &Document, node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let texts = doc.children(node)?.map(|child| match doc.node(child)?.value {
        Value::String(text) => Ok(text),
        _ => Err(Error::Schema),
    });
    write_encoded_chunks_checked(doc, texts, e)
}

pub(super) fn write_encoded_chunks(
    doc: &Document,
    texts: impl Iterator<Item = Text>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    write_encoded_chunks_checked(doc, texts.map(Ok), e)
}

fn write_encoded_chunks_checked(
    doc: &Document,
    texts: impl Iterator<Item = Result<Text>>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let mut encoder = Reencoder::default();
    for text in texts {
        decode_base64(doc, text?, |bytes| encoder.push(bytes, e))?;
    }
    encoder.finish(e)
}

/// Re-encodes a registered string payload as plain base64 from its mapped span, dropping any
/// data URI prefix so the caller can write the current media type.
pub(super) fn write_payload(doc: &Document, media: Media, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut encoder = Reencoder::default();
    bytes(doc, media, |bytes| encoder.push(bytes, e))?;
    encoder.finish(e)
}

/// Writes decoded bytes as one contiguous base64 string, three bytes at a time.
#[derive(Default)]
struct Reencoder {
    carry: [u8; 3],
    used: usize,
}

impl Reencoder {
    fn push(&mut self, bytes: &[u8], e: &mut Encoder<'_, '_>) -> Result<()> {
        use base64::Engine;
        for &byte in bytes {
            self.carry[self.used] = byte;
            self.used += 1;
            if self.used == 3 {
                let mut encoded = [0; 4];
                base64::engine::general_purpose::STANDARD
                    .encode_slice(self.carry, &mut encoded)
                    .map_err(|_| Error::Schema)?;
                e.bytes(&encoded)?;
                self.used = 0;
            }
        }
        Ok(())
    }

    fn finish(self, e: &mut Encoder<'_, '_>) -> Result<()> {
        use base64::Engine;
        if self.used != 0 {
            let mut encoded = [0; 4];
            base64::engine::general_purpose::STANDARD
                .encode_slice(&self.carry[..self.used], &mut encoded)
                .map_err(|_| Error::Schema)?;
            e.bytes(&encoded)?;
        }
        Ok(())
    }
}

pub(crate) struct InputDecoder {
    encoding: Encoding,
    skip: u64,
    quartet: [u8; 4],
    count: usize,
    padded: bool,
}
impl InputDecoder {
    pub(crate) fn new(encoding: Encoding) -> Self {
        Self {
            encoding,
            skip: match encoding {
                Encoding::DataUri { skip } => skip,
                _ => 0,
            },
            quartet: [0; 4],
            count: 0,
            padded: false,
        }
    }
    pub(crate) fn complete(&self) -> bool {
        self.skip == 0 && self.count == 0
    }
    pub(crate) fn push(&mut self, c: char, mut emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        use base64::Engine;
        if self.skip != 0 {
            self.skip -= 1;
            return Ok(());
        }
        if matches!(self.encoding, Encoding::Utf8) {
            return emit(c.encode_utf8(&mut [0; 4]).as_bytes());
        }
        if self.padded || !c.is_ascii() {
            return Err(Error::Schema);
        }
        self.quartet[self.count] = c as u8;
        self.count += 1;
        if self.count == 4 {
            let mut bytes = [0; 3];
            let len = base64::engine::general_purpose::STANDARD
                .decode_slice(self.quartet, &mut bytes)
                .map_err(|_| Error::Schema)?;
            self.padded = self.quartet.contains(&b'=');
            self.count = 0;
            emit(&bytes[..len])?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Base64Fragments {
    quartet: [u8; 4],
    count: usize,
}

impl Base64Fragments {
    pub(crate) fn complete(&self) -> bool {
        self.count == 0
    }

    pub(crate) fn validate_snapshot(&self, doc: &Document, data: usize, published: u64) -> Result<()> {
        use base64::Engine;
        if self.complete() {
            return Ok(());
        }
        let mut bytes = doc.decoded_bytes(data)?;
        for _ in 0..published {
            bytes.next_byte()?.ok_or(Error::Schema)?;
        }
        let mut block = [0; 3];
        let mut len = 0;
        for slot in &mut block {
            let Some(byte) = bytes.next_byte()? else { break };
            *slot = byte;
            len += 1;
        }
        let mut encoded = [0; 4];
        let length = base64::engine::general_purpose::STANDARD
            .encode_slice(&block[..len], &mut encoded)
            .map_err(|_| Error::Schema)?;
        if length < self.count || encoded[..self.count] != self.quartet[..self.count] {
            return Err(Error::Schema);
        }
        Ok(())
    }

    pub(crate) fn push(&mut self, doc: &Document, text: Text, mut emit: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        use base64::Engine;
        let mut decoded = Decoded::new(doc.source(text)?, text.span)?;
        // Each Chat audio delta is its own base64 string: padding may end a fragment and the
        // next fragment starts fresh, so `padded` is per fragment, unlike InputDecoder's.
        let mut padded = false;
        while let Some(c) = decoded.next_scalar()? {
            if padded || !c.is_ascii() {
                return Err(Error::Schema);
            }
            self.quartet[self.count] = c as u8;
            self.count += 1;
            if self.count == 4 {
                let mut bytes = [0; 3];
                let len = base64::engine::general_purpose::STANDARD
                    .decode_slice(self.quartet, &mut bytes)
                    .map_err(|_| Error::Schema)?;
                padded = self.quartet.contains(&b'=');
                self.count = 0;
                emit(&bytes[..len])?;
            }
        }
        Ok(())
    }
}
