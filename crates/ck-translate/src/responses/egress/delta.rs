use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Projects one Chat delta: a tool argument fragment, a text, refusal or reasoning fragment,
    /// or standalone logprobs; a chunk without one only carries the finish.
    pub(super) fn chunk(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &self.document;
        let choices = doc.required(at.node, "choices")?;
        let Some(choice) = doc.node(choices)?.first else {
            return self.finish_chunk(at.node);
        };
        let delta = doc.required(choice, "delta")?;
        if let Some(calls) = doc.member(delta, "tool_calls")? {
            for call in doc.children(calls)? {
                let function = doc.required(call, "function")?;
                if let Some(args) = doc.member(function, "arguments")? {
                    // One canonical event contains at most one tool delta.
                    return self.arguments_delta(owner, at, (function, args), output);
                }
            }
        }
        for (from, native, name) in [
            ("content", "text", "response.output_text.delta"),
            ("refusal", "refusal", "response.refusal.delta"),
            ("reasoning_content", "text", "response.reasoning_summary_text.delta"),
        ] {
            if let Some(value) = doc.member(delta, from)? {
                let reasoning = from == "reasoning_content";
                return self.text_delta(owner, at, (choice, delta, value), (native, name, reasoning), output);
            }
        }
        if let Some(logprobs) = doc.member(choice, "logprobs")?
            && let Some(tokens) = doc.member(logprobs, "content")?
        {
            return self.logprobs_delta(owner, at, tokens, output);
        }
        self.finish_chunk(at.node)
    }

    fn arguments_delta(
        &mut self,
        owner: &mut Writer,
        At { item, index, .. }: At,
        (function, args): (usize, usize),
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let custom = is(doc, item, "type", "custom_tool_call")?;
        let field = if custom { "input" } else { "arguments" };
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(b",\"delta\":")?;
            doc.write(args, e)
        })?;
        doc.detach(function, args)?;
        append_fragment(doc, item, field, args)?;
        let name = if custom {
            "response.custom_tool_call_input.delta"
        } else if is(doc, item, "type", "mcp_call")? {
            "response.mcp_call_arguments.delta"
        } else {
            "response.function_call_arguments.delta"
        };
        self.emit(owner, name, |e| write_lease(&payload, e), output)
    }

    fn text_delta(
        &mut self,
        owner: &mut Writer,
        At { x, item, index, .. }: At,
        (choice, delta, value): (usize, usize, usize),
        (native, name, reasoning): (&'static str, &'static str, bool),
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let (part, summary, block) = find_part(doc, item, x)?.ok_or(Error::Schema)?;
        let name = if reasoning && !summary {
            "response.reasoning_text.delta"
        } else {
            name
        };
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(if summary {
                b",\"summary_index\":"
            } else {
                b",\"content_index\":"
            })?;
            e.unsigned(block)?;
            e.bytes(b",\"delta\":")?;
            doc.write(value, e)?;
            super::super::member(doc, choice, "logprobs", e)
        })?;
        doc.detach(delta, value)?;
        append_fragment(doc, part, native, value)?;
        self.emit(owner, name, |e| write_lease(&payload, e), output)
    }

    fn logprobs_delta(
        &mut self,
        owner: &mut Writer,
        At { x, item, index, .. }: At,
        tokens: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        // Chat logprobs may arrive without block correlation (§3.1); they belong to the item's
        // latest content part.
        let found = match find_part(doc, item, x)? {
            Some((part, _, block)) => Some((part, block)),
            None => last_part(doc, item)?,
        };
        let Some((part, block)) = found else {
            (self.diagnostic)("logprobs without a content part", index);
            return Ok(());
        };
        let payload = encode::scratch(owner, |e| {
            item_fields(doc, item, index, e)?;
            e.bytes(b",\"content_index\":")?;
            e.unsigned(block)?;
            e.bytes(b",\"delta\":\"\",\"logprobs\":")?;
            doc.write(tokens, e)
        })?;
        let target = match doc.member(part, "logprobs")? {
            Some(n) => n,
            None => doc.set(part, "logprobs", Value::Array)?,
        };
        while let Some(token) = doc.node(tokens)?.first {
            doc.detach(tokens, token)?;
            doc.append(target, token)?;
        }
        self.emit(
            owner,
            "response.output_text.delta",
            |e| write_lease(&payload, e),
            output,
        )
    }
}

/// The last content part of `item` with its native content index.
fn last_part(doc: &Document, item: usize) -> Result<Option<(usize, u64)>> {
    let Some(parts) = doc.member(item, "content")? else {
        return Ok(None);
    };
    let Some((index, part)) = doc.children(parts)?.enumerate().last() else {
        return Ok(None);
    };
    Ok(Some((part, native_index(doc, part, "content_index", index as u64)?)))
}
