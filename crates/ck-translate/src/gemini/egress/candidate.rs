//! Candidate and response members: finish reasons, metadata, usage, counts and native extras.

use super::{Diagnostic, Egress, fields, mapped, native};
use crate::codec::{
    Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::Kind,
};
use ck_stream::{ScratchLease, Writer};

impl Egress {
    /// Applies one Chat chunk: its delta first, then its logprobs and finish, which are copied
    /// before the delta's part absorbs the record.
    pub(super) fn chunk(
        &mut self,
        owner: &mut Writer,
        event: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        let Some(choice) = event.node(event.required(root, "choices")?)?.first else {
            return Ok(());
        };
        if event.node(choice)?.next.is_some() {
            return Err(Error::Schema);
        }
        if native(&event, x)? {
            self.refresh(owner, &event, x)?;
        }
        let slot = self.open_choice(event.unsigned(event.required(choice, "index")?)?)?;
        let logprobs = match present(&event, choice, "logprobs")? {
            Some(node) => Some(mapped(owner, |e| event.write(node, e))?),
            None => None,
        };
        let finish = match present(&event, choice, "finish_reason")? {
            Some(finish) => Some(mapped(owner, |e| finish_reason(&event, x, finish, e))?),
            None => None,
        };
        let message = match native(&event, x)? {
            true => finish_message(owner, &event, x)?,
            false => None,
        };
        if let Some(delta) = event.member(choice, "delta")? {
            self.delta(owner, event, delta, output)?;
        }
        self.choices[slot].finished = finish.is_some();
        for (name, value) in [
            ("logprobsResult", logprobs),
            ("finishReason", finish),
            ("finishMessage", message),
        ] {
            if let Some(value) = value {
                self.candidate_field(owner, output, slot, name, |e| value.write(value.root(), e))?;
            }
        }
        Ok(())
    }
    fn delta(
        &mut self,
        owner: &mut Writer,
        event: Document,
        delta: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        for (key, reasoning) in [("content", false), ("reasoning_content", true)] {
            if let Some(text) = present(&event, delta, key)? {
                return self.text(owner, event, text, reasoning, output);
            }
        }
        if let Some(calls) = event.member(delta, "tool_calls")? {
            for call in event.children(calls)? {
                if let Some(function) = event.member(call, "function")?
                    && let Some(arguments) = event.member(function, "arguments")?
                {
                    return self.arguments(event, arguments);
                }
            }
        }
        Ok(())
    }
    /// Sets candidate member `name`: a streamed record of its own, or a member of the buffered
    /// candidate, replacing an earlier value.
    pub(super) fn candidate_field(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        slot: usize,
        name: &'static str,
        value: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        let index = self.choices[slot].index;
        match self.choices[slot].node {
            Some(candidate) => set_copy(owner, self.body.as_mut().ok_or(Error::Schema)?, candidate, name, value),
            None => self.record(owner, output, |first, e| {
                *first = false;
                e.bytes(b"\"candidates\":[{\"index\":")?;
                e.unsigned(index)?;
                e.bytes(b",")?;
                e.string(name)?;
                e.bytes(b":")?;
                value(e)?;
                e.bytes(b"}]")
            }),
        }
    }
    /// Sets response member `name`, as `candidate_field` does for a candidate.
    fn top_field(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        name: &'static str,
        value: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        match &mut self.body {
            Some(body) => {
                let root = body.root();
                set_copy(owner, body, root, name, value)
            }
            None => self.record(owner, output, |first, e| {
                *first = false;
                e.string(name)?;
                e.bytes(b":")?;
                value(e)
            }),
        }
    }
    /// Writes Gemini's own grounding, citation, safety and URL-context metadata; another
    /// protocol's native payload has no Gemini shape and is dropped.
    pub(super) fn annotation(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        event: &Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = event.root();
        let x = event.required(root, "x_ck")?;
        if !native(event, x)? {
            (self.diagnostic)(Diagnostic::UnsupportedEvent(kind));
            return Ok(());
        }
        let source = event.required(x, "source")?;
        let name = match kind {
            Kind::Grounding => "groundingMetadata",
            Kind::UrlContext => "urlContextMetadata",
            _ => ["promptFeedback", "safetyRatings", "citationMetadata"]
                .into_iter()
                .find(|name| super::super::is(event, source, "event", name).unwrap_or(false))
                .ok_or(Error::Schema)?,
        };
        let value = event.required(root, "value")?;
        match event.member(x, "choice_index")? {
            Some(index) => {
                let slot = self.choice(event.unsigned(index)?)?;
                self.candidate_field(owner, output, slot, name, |e| event.write(value, e))
            }
            None => self.top_field(owner, output, name, |e| event.write(value, e)),
        }
    }
    pub(super) fn usage(
        &mut self,
        owner: &mut Writer,
        event: &Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let usage = event.required(event.root(), "usage")?;
        if let Some(native) = event.member(usage, "native")?
            && super::super::is(event, native, "protocol", "gemini")?
        {
            let value = event.required(native, "value")?;
            return self.top_field(owner, output, "usageMetadata", |e| event.write(value, e));
        }
        self.top_field(owner, output, "usageMetadata", |e| project_usage(event, usage, e))
    }
    pub(super) fn count(&mut self, owner: &mut Writer, event: &Document) -> Result<()> {
        if self.operation != Operation::CountTokens {
            return Err(Error::Schema);
        }
        let root = event.root();
        let details = event.member(root, "details")?;
        let total = event
            .member(root, "total_tokens")?
            .or(event.member(root, "input_tokens")?);
        self.body = Some(match details {
            Some(details) => mapped(owner, |e| event.write(details, e))?,
            None => {
                let total = total.ok_or(Error::Schema)?;
                mapped(owner, |e| {
                    e.bytes(b"{\"totalTokens\":")?;
                    event.write(total, e)?;
                    e.bytes(b"}")
                })?
            }
        });
        Ok(())
    }
    /// Restores the response and candidate members the Gemini source had no mapping for.
    pub(super) fn opaque(
        &mut self,
        owner: &mut Writer,
        event: &Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = event.root();
        let candidate = super::super::is(event, root, "name", "gemini.candidate")?;
        if !candidate && !super::super::is(event, root, "name", "gemini.response")? {
            let sse = super::super::is(event, root, "name", "gemini.sse")?;
            (self.diagnostic)(if sse {
                Diagnostic::SseMetadata
            } else {
                Diagnostic::UnsupportedEvent(Kind::Opaque)
            });
            return Ok(());
        }
        let value = event.required(root, "value")?;
        let skip: &[&str] = if candidate { &["index"] } else { &["candidates"] };
        let target = if candidate {
            let slot = self.choice(event.unsigned(event.required(value, "index")?)?)?;
            Some(slot)
        } else {
            None
        };
        if self.body.is_none() {
            if event.children(value)?.all(|n| {
                event
                    .node(n)
                    .ok()
                    .and_then(|n| n.name)
                    .is_some_and(|name| skip.iter().any(|key| event.name_is(name, key).unwrap_or(false)))
            }) {
                return Ok(());
            }
            let index = target.map(|slot| self.choices[slot].index);
            return self.record(owner, output, |first, e| match index {
                Some(index) => {
                    *first = false;
                    e.bytes(b"\"candidates\":[{\"index\":")?;
                    e.unsigned(index)?;
                    fields(event, value, skip, &mut false, e)?;
                    e.bytes(b"}]")
                }
                None => fields(event, value, skip, first, e),
            });
        }
        let body = self.body.as_mut().ok_or(Error::Schema)?;
        let parent = match target {
            Some(slot) => self.choices[slot].node.ok_or(Error::Schema)?,
            None => body.root(),
        };
        let raw = encode::scratch(owner, |e| event.write(value, e))?;
        let copy = body.import_json(owner, raw)?;
        let mut member = body.node(copy)?.first;
        while let Some(node) = member {
            member = body.node(node)?.next;
            let name = body.node(node)?.name.ok_or(Error::Schema)?;
            if skip
                .iter()
                .try_fold(false, |found, key| Ok::<_, Error>(found || body.name_is(name, key)?))?
            {
                continue;
            }
            for old in body.children(parent)? {
                if body.names_equal(name, body.node(old)?.name.ok_or(Error::Schema)?)? {
                    body.detach(parent, old)?;
                    break;
                }
            }
            body.detach(copy, node)?;
            body.append(parent, node)?;
        }
        Ok(())
    }
}
/// Member `name` of `parent` unless it is absent or null.
fn present(doc: &Document, parent: usize, name: &str) -> Result<Option<usize>> {
    match doc.member(parent, name)? {
        Some(node) if !matches!(doc.node(node)?.value, Value::Null) => Ok(Some(node)),
        _ => Ok(None),
    }
}
/// Replaces member `name` of `parent` with a copy of the JSON `value` writes.
fn set_copy(
    owner: &mut Writer,
    doc: &mut Document,
    parent: usize,
    name: &'static str,
    value: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
) -> Result<()> {
    let raw = encode::scratch(owner, value)?;
    let copy = doc.import_json(owner, raw)?;
    doc.remove(parent, name)?;
    doc.rename(copy, name)?;
    doc.append(parent, copy)
}
/// Projects a canonical usage snapshot onto `usageMetadata`. Candidates exclude thoughts.
fn project_usage(event: &Document, usage: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for (canonical, wire) in [
        ("input_tokens", "promptTokenCount"),
        ("total_tokens", "totalTokenCount"),
        ("cache_read_tokens", "cachedContentTokenCount"),
        ("reasoning_tokens", "thoughtsTokenCount"),
    ] {
        if let Some(node) = event.member(usage, canonical)? {
            if !std::mem::replace(&mut first, false) {
                e.bytes(b",")?;
            }
            e.string(wire)?;
            e.bytes(b":")?;
            event.write(node, e)?;
        }
    }
    let count = |name| -> Result<Option<u64>> {
        match event.member(usage, name)? {
            Some(node) if !matches!(event.node(node)?.value, Value::Null) => event.unsigned(node).map(Some),
            _ => Ok(None),
        }
    };
    if let (Some(output), Some(reasoning)) = (count("output_tokens")?, count("reasoning_tokens")?)
        && let Some(candidates) = output.checked_sub(reasoning)
    {
        if !first {
            e.bytes(b",")?;
        }
        e.bytes(b"\"candidatesTokenCount\":")?;
        e.unsigned(candidates)?;
    }
    e.bytes(b"}")
}

/// A Gemini source keeps its exact native reason; any other source maps its Chat reason, so a
/// foreign native spelling such as `end_turn` never reaches a Gemini client.
fn finish_reason(event: &Document, x: usize, finish: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if native(event, x)?
        && let Some(reason) = event.member(x, "native_finish_reason")?
    {
        return event.write(reason, e);
    }
    e.string(if event.string_is(finish, "length")? {
        "MAX_TOKENS"
    } else if event.string_is(finish, "content_filter")? {
        "SAFETY"
    } else if ["stop", "tool_calls", "function_call"]
        .iter()
        .try_fold(false, |found, name| {
            Ok::<_, Error>(found || event.string_is(finish, name)?)
        })?
    {
        "STOP"
    } else {
        "OTHER"
    })
}

/// The native `finishMessage` that the ingress carried as an original record on the finish.
fn finish_message(owner: &mut Writer, event: &Document, x: usize) -> Result<Option<Document>> {
    let Some(original) = event.member(x, "original")? else {
        return Ok(None);
    };
    for entry in event.children(original)? {
        if super::super::is(event, entry, "protocol", "gemini")?
            && super::super::is(event, entry, "pointer", "/finishMessage")?
            && let Some(value) = event.member(entry, "value")?
        {
            return Ok(Some(mapped(owner, |e| event.write(value, e))?));
        }
    }
    Ok(None)
}
