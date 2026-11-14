//! Usage snapshots: partial native usage merges into the retained snapshot it updates.

use super::State;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    event::{Emitter, Kind, Scope},
};
use ck_stream::Writer;

impl State {
    /// Merges a partial `usageMetadata` into the retained snapshot: supplied members replace
    /// their old values, omitted members keep them. Publishes the merged snapshot.
    pub(super) fn usage(&mut self, owner: &mut Writer, doc: &Document, usage: usize) -> Result<()> {
        super::super::object(doc, usage)?;
        let mut next = super::super::mapped(owner, |e| doc.write(usage, e))?;
        if let Some(previous) = self.usage.take() {
            let old = next.absorb(previous)?;
            let mut field = next.node(old)?.first;
            while let Some(n) = field {
                field = next.node(n)?.next;
                let name = next.node(n)?.name.ok_or(Error::Schema)?;
                let mut present = false;
                for current in next.children(next.root())? {
                    present |= next.names_equal(name, next.node(current)?.name.ok_or(Error::Schema)?)?;
                }
                if !present {
                    next.detach(old, n)?;
                    next.append(next.root(), n)?;
                }
            }
        }
        emit(&mut self.emitter, owner, &next, false)?;
        self.usage = Some(next);
        Ok(())
    }
}
pub(super) fn emit(emitter: &mut Emitter, owner: &mut Writer, doc: &Document, final_snapshot: bool) -> Result<()> {
    let root = doc.root();
    for key in [
        "promptTokenCount",
        "candidatesTokenCount",
        "thoughtsTokenCount",
        "totalTokenCount",
        "cachedContentTokenCount",
    ] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Null)
        {
            doc.unsigned(n)?;
        }
    }
    emitter.emit(
        owner,
        Kind::Usage,
        Scope::default(),
        |e| {
            e.bytes(b",\"usage\":{\"final\":")?;
            e.bytes(if final_snapshot { b"true" } else { b"false" })?;
            for (wire, canonical) in [
                ("promptTokenCount", "input_tokens"),
                ("totalTokenCount", "total_tokens"),
                ("cachedContentTokenCount", "cache_read_tokens"),
                ("thoughtsTokenCount", "reasoning_tokens"),
            ] {
                if let Some(n) = doc.member(root, wire)?
                    && !matches!(doc.node(n)?.value, Value::Null)
                {
                    e.bytes(b",")?;
                    e.string(canonical)?;
                    e.bytes(b":")?;
                    doc.write(n, e)?;
                }
            }
            if let (Some(candidates), Some(thoughts)) = (
                doc.member(root, "candidatesTokenCount")?,
                doc.member(root, "thoughtsTokenCount")?,
            ) && !matches!(doc.node(candidates)?.value, Value::Null)
                && !matches!(doc.node(thoughts)?.value, Value::Null)
            {
                e.bytes(b",\"output_tokens\":")?;
                e.unsigned(
                    doc.unsigned(candidates)?
                        .checked_add(doc.unsigned(thoughts)?)
                        .ok_or(Error::Schema)?,
                )?;
                e.bytes(b",\"derived\":[\"output_tokens\"]")?;
            }
            e.bytes(b",\"native\":{\"protocol\":\"gemini\",\"value\":")?;
            doc.write(root, e)?;
            e.bytes(b"}}")
        },
        |_| Ok(()),
    )?;
    Ok(())
}
