//! Usage snapshots: partial native usage merges into the retained snapshot it updates.

use super::State;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode::Encoder,
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
            output(doc, root, e)?;
            modalities(doc, root, e)?;
            e.bytes(b",\"native\":{\"protocol\":\"gemini\",\"value\":")?;
            doc.write(root, e)?;
            e.bytes(b"}}")
        },
        |_| Ok(()),
    )?;
    Ok(())
}

/// A count the snapshot reports: present and not null.
fn known(doc: &Document, parent: usize, key: &str) -> Result<Option<u64>> {
    match doc.member(parent, key)? {
        Some(n) if !matches!(doc.node(n)?.value, Value::Null) => Ok(Some(doc.unsigned(n)?)),
        _ => Ok(None),
    }
}

/// Inclusive output: `candidatesTokenCount` excludes thoughts, so a reported thought count is
/// added and marks the sum derived. A null thought count leaves the output unknown.
fn output(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(candidates) = known(doc, root, "candidatesTokenCount")? else {
        return Ok(());
    };
    let thoughts = doc.member(root, "thoughtsTokenCount")?;
    let total = match thoughts {
        None => candidates,
        Some(_) => match known(doc, root, "thoughtsTokenCount")? {
            Some(thoughts) => candidates.checked_add(thoughts).ok_or(Error::Schema)?,
            None => return Ok(()),
        },
    };
    e.bytes(b",\"output_tokens\":")?;
    e.unsigned(total)?;
    if thoughts.is_some() {
        e.bytes(b",\"derived\":[\"output_tokens\"]")?;
    }
    Ok(())
}

/// Audio counts from the per-modality breakdowns, and text and image counts as details. Other
/// modalities (video, document) and the cache and tool-use breakdowns stay in `native`.
fn modalities(doc: &Document, root: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    // Top-level counts come first: the details object closes the run.
    const MAP: [(&str, &str, &str, bool); 6] = [
        ("promptTokensDetails", "AUDIO", "input_audio_tokens", false),
        ("candidatesTokensDetails", "AUDIO", "output_audio_tokens", false),
        ("promptTokensDetails", "TEXT", "input_text_tokens", true),
        ("promptTokensDetails", "IMAGE", "input_image_tokens", true),
        ("candidatesTokensDetails", "TEXT", "output_text_tokens", true),
        ("candidatesTokensDetails", "IMAGE", "output_image_tokens", true),
    ];
    let mut details = false;
    for (list, modality, canonical, detail) in MAP {
        let Some(count) = modality_count(doc, root, list, modality)? else {
            continue;
        };
        e.bytes(if detail && !details { b",\"details\":{" } else { b"," })?;
        details |= detail;
        e.string(canonical)?;
        e.bytes(b":")?;
        e.unsigned(count)?;
    }
    if details {
        e.bytes(b"}")?;
    }
    Ok(())
}

/// The reported `tokenCount` of one modality in a `ModalityTokenCount` list.
fn modality_count(doc: &Document, root: usize, list: &str, modality: &str) -> Result<Option<u64>> {
    let Some(list) = doc.member(root, list)? else {
        return Ok(None);
    };
    if matches!(doc.node(list)?.value, Value::Null) {
        return Ok(None);
    }
    super::super::array(doc, list)?;
    for entry in doc.children(list)? {
        super::super::object(doc, entry)?;
        if let Some(name) = doc.member(entry, "modality")?
            && doc.string_is(name, modality)?
        {
            return known(doc, entry, "tokenCount");
        }
    }
    Ok(None)
}
