use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode,
    media::{self, Encoding, Registry},
};
use ck_stream::Writer;

/// Register only fields produced by typed content normalization, never literal wire markers.
pub(super) fn register(doc: &mut Document, owner: &mut Writer, registry: &mut Registry) -> Result<()> {
    let messages = doc.required(doc.root(), "messages")?;
    let mut message = doc.node(messages)?.first;
    while let Some(m) = message {
        message = doc.node(m)?.next;
        let Some(content) = doc.member(m, "content")? else {
            continue;
        };
        if !matches!(doc.node(content)?.value, Value::Array) {
            continue;
        }
        let mut part = doc.node(content)?.first;
        while let Some(p) = part {
            part = doc.node(p)?.next;
            if !super::is(doc, p, "type", "image")? && !super::is(doc, p, "type", "document")? {
                continue;
            }
            let source = doc.required(p, "source")?;
            let data = if super::is(doc, source, "kind", "uri")? {
                doc.required(source, "uri")?
            } else if super::is(doc, source, "kind", "data")? {
                doc.required(source, "ref")?
            } else {
                continue;
            };
            if let Some(media) = registry.get(doc, data)?
                && !matches!(media.encoding, Encoding::Utf8)
            {
                doc.rename(data, "ref")?;
                doc.set(source, "kind", Value::Text("data"))?;
                if let Some(mime) = media.mime {
                    doc.rename(mime, "mime")?;
                    doc.append(p, mime)?;
                }
                continue;
            }
            let mut chars = doc.chars(data)?;
            let mut prefix = true;
            for expected in "data:".chars() {
                prefix &= chars.next_scalar()? == Some(expected);
            }
            let (encoding, mime) = if prefix {
                let uri = data;
                let mut skip = 5u64;
                let mut mime_end = 0u64;
                let mut semicolon = false;
                while let Some(c) = chars.next_scalar()? {
                    skip += 1;
                    if c == ';' {
                        semicolon = true;
                        mime_end = skip - 1;
                        break;
                    }
                    if c == ',' {
                        return Err(Error::Schema);
                    }
                }
                if !semicolon {
                    return Err(Error::Schema);
                }
                for expected in "base64,".chars() {
                    if chars.next_scalar()? != Some(expected) {
                        return Err(Error::Schema);
                    }
                    skip += 1;
                }
                let lease = encode::scratch(owner, |e| {
                    e.bytes(b"\"")?;
                    let mut chars = doc.chars(uri)?;
                    for _ in 0..5 {
                        chars.next_scalar()?;
                    }
                    for _ in 5..mime_end {
                        let c = chars.next_scalar()?.ok_or(Error::Schema)?;
                        e.escaped(c.encode_utf8(&mut [0; 4]))?;
                    }
                    e.bytes(b"\"")
                })?;
                let mime = doc.import_json(owner, lease)?;
                doc.rename(mime, "mime")?;
                doc.append(p, mime)?;
                doc.rename(uri, "ref")?;
                doc.set(source, "kind", Value::Text("data"))?;
                (Encoding::DataUri { skip }, Some(mime))
            } else if super::is(doc, source, "kind", "data")? {
                (Encoding::Base64, doc.member(p, "mime")?)
            } else {
                continue;
            };
            let Value::String(text) = doc.node(data)?.value else {
                return Err(Error::Schema);
            };
            let kind = if super::is(doc, p, "type", "image")? {
                "image"
            } else {
                "document"
            };
            let media = registry.register(doc, data, text, encoding, kind, mime)?;
            media::bytes(doc, media, |_| Ok(()))?;
        }
    }
    Ok(())
}
