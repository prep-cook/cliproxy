use crate::codec::{
    Result,
    document::{Document, Value},
    encode::Encoder,
};
use ck_stream::Writer;

#[derive(Default)]
pub(super) struct Usage {
    native: Option<Document>,
}
impl Usage {
    pub fn update(&mut self, owner: &mut Writer, doc: &Document, node: usize) -> Result<()> {
        let incoming = super::mapped(owner, |e| doc.write(node, e))?;
        if let Some(current) = &mut self.native {
            let index = current.absorb(incoming)?;
            if matches!(current.node(index)?.value, Value::Object)
                && matches!(current.node(current.root())?.value, Value::Object)
            {
                super::merge(current, current.root(), index)?;
            } else {
                current.transplant(current.root(), index)?;
            }
        } else {
            self.native = Some(incoming);
        }
        Ok(())
    }
    pub fn seen(&self) -> bool {
        self.native.is_some()
    }
    pub fn write(&self, e: &mut Encoder<'_, '_>, final_snapshot: bool) -> Result<()> {
        let doc = self.native.as_ref().ok_or(crate::codec::Error::Schema)?;
        let root = doc.root();
        e.bytes(b"{\"native\":{\"protocol\":\"responses\",\"value\":")?;
        doc.write(root, e)?;
        e.bytes(b"},\"final\":")?;
        e.bytes(if final_snapshot { b"true" } else { b"false" })?;
        if matches!(doc.node(root)?.value, Value::Object) {
            for name in [
                "input_tokens",
                "output_tokens",
                "total_tokens",
                "cache_write_tokens",
                "input_uncached_tokens",
            ] {
                if let Some(n) = doc.member(root, name)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                {
                    doc.unsigned(n)?;
                    e.bytes(b",")?;
                    e.string(name)?;
                    e.bytes(b":")?;
                    doc.write(n, e)?;
                }
            }
            if doc.member(root, "total_tokens")?.is_none()
                && let (Some(a), Some(b)) = (doc.member(root, "input_tokens")?, doc.member(root, "output_tokens")?)
                && !matches!(doc.node(a)?.value, Value::Null)
                && !matches!(doc.node(b)?.value, Value::Null)
            {
                let total = doc
                    .unsigned(a)?
                    .checked_add(doc.unsigned(b)?)
                    .ok_or(crate::codec::Error::Schema)?;
                e.bytes(b",\"total_tokens\":")?;
                e.unsigned(total)?;
                e.bytes(b",\"derived\":[\"total_tokens\"]")?;
            }
            for (details, fields) in [
                (
                    "input_tokens_details",
                    &[
                        ("cached_tokens", "cache_read_tokens"),
                        ("audio_tokens", "input_audio_tokens"),
                        ("cache_write_tokens", "cache_write_tokens"),
                    ][..],
                ),
                (
                    "output_tokens_details",
                    &[
                        ("reasoning_tokens", "reasoning_tokens"),
                        ("audio_tokens", "output_audio_tokens"),
                    ][..],
                ),
            ] {
                if let Some(details) = doc.member(root, details)?
                    && matches!(doc.node(details)?.value, Value::Object)
                {
                    for (from, to) in fields {
                        if let Some(n) = doc.member(details, from)?
                            && !matches!(doc.node(n)?.value, Value::Null)
                            && (to != &"cache_write_tokens"
                                || doc
                                    .member(root, to)?
                                    .is_none_or(|n| matches!(doc.node(n).map(|n| n.value), Ok(Value::Null))))
                        {
                            doc.unsigned(n)?;
                            e.bytes(b",")?;
                            e.string(to)?;
                            e.bytes(b":")?;
                            doc.write(n, e)?;
                        }
                    }
                }
            }
            if let Some(ttl) = doc.member(root, "cache_write_by_ttl")? {
                e.bytes(b",\"cache_write_by_ttl\":")?;
                doc.write(ttl, e)?;
            }
        }
        e.bytes(b"}")
    }
}

/// Responses usage for canonical snapshot `usage`: a Responses source's native value, or the
/// mapped counts. The snapshot itself replaces the one retained in the `usage.snapshot` opaque
/// carrier, whose value slot `retained` tracks.
pub(super) fn project(doc: &mut Document, usage: usize, retained: &mut Option<usize>) -> Result<usize> {
    if let Some(native) = doc.member(usage, "native")?
        && super::is(doc, native, "protocol", "responses")?
    {
        return doc.required(native, "value");
    }
    let result = doc.add(None, Value::Object)?;
    for name in [
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "input_uncached_tokens",
        "cache_write_tokens",
        "cache_write_by_ttl",
    ] {
        super::move_field(doc, usage, name, result, name)?;
    }
    for (name, fields) in [
        (
            "input_tokens_details",
            &[
                ("cache_read_tokens", "cached_tokens"),
                ("input_audio_tokens", "audio_tokens"),
            ][..],
        ),
        (
            "output_tokens_details",
            &[
                ("reasoning_tokens", "reasoning_tokens"),
                ("output_audio_tokens", "audio_tokens"),
            ][..],
        ),
    ] {
        let mut details = None;
        for &(from, to) in fields {
            if doc.member(usage, from)?.is_some() {
                let target = match details {
                    Some(target) => target,
                    None => *details.insert(doc.set(result, name, Value::Object)?),
                };
                super::move_field(doc, usage, from, target, to)?;
            }
        }
    }
    let slot = match *retained {
        Some(slot) => slot,
        None => {
            let carrier = match doc.member(doc.root(), "x_ck_opaque")? {
                Some(carrier) => carrier,
                None => doc.set(doc.root(), "x_ck_opaque", Value::Array)?,
            };
            let entry = doc.add(None, Value::Object)?;
            doc.append(carrier, entry)?;
            doc.set(entry, "name", Value::Text("usage.snapshot"))?;
            doc.set(entry, "format", Value::Text("json"))?;
            *retained.insert(doc.set(entry, "value", Value::Null)?)
        }
    };
    doc.transplant(slot, usage)?;
    Ok(result)
}
