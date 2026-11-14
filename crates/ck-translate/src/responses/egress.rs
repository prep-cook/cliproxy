use super::{is, response::append_fragment};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Identity, Kind},
    internal,
};
use ck_stream::{ScratchLease, Writer};

pub struct Egress<F: FnMut(&'static str, u64)> {
    owner: u64,
    mode: BodyMode,
    document: Document,
    terminal: bool,
    sequence: u64,
    diagnostic: F,
}
impl<F: FnMut(&'static str, u64)> Egress<F> {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: F,
    ) -> Result<Self> {
        if operation == Operation::CountTokens || (operation == Operation::Compact && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        let document = super::mapped(owner, |e| {
            e.bytes(b"{\"id\":")?;
            e.string(identity.stream_id.as_str())?;
            e.bytes(b",\"object\":")?;
            e.string(if operation == Operation::Compact {
                "response.compaction"
            } else {
                "response"
            })?;
            e.bytes(b",\"created_at\":0,\"model\":\"\",\"status\":\"in_progress\",\"output\":[]}")
        })?;
        Ok(Self {
            owner: owner.id(),
            mode,
            document,
            terminal: false,
            sequence: 0,
            diagnostic,
        })
    }
    fn emit(
        &mut self,
        owner: &mut Writer,
        name: &str,
        payload: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.mode == BodyMode::Buffered {
            return Ok(());
        }
        let seq = self.sequence;
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        output(encode::scratch(owner, |e| {
            e.bytes(b"event: ")?;
            e.bytes(name.as_bytes())?;
            e.bytes(b"\ndata: {\"type\":")?;
            e.string(name)?;
            e.bytes(b",\"sequence_number\":")?;
            e.unsigned(seq)?;
            payload(e)?;
            e.bytes(b"}\n\n")
        })?)
    }
    fn process(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        incoming: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.terminal {
            return Err(Error::Closed);
        }
        let root = incoming.root();
        let x = incoming.required(root, "x_ck")?;
        if let Some(choice) = incoming.member(x, "choice_index")? {
            let index = incoming.unsigned(choice)?;
            if index != 0 {
                (self.diagnostic)("choice", index);
                return Ok(());
            }
        }
        if matches!(kind, Kind::Heartbeat) {
            return Ok(());
        }
        if matches!(kind, Kind::Error | Kind::Cancelled) {
            self.terminal = true;
            let failed = encode::scratch(owner, |e| {
                if self.mode == BodyMode::Streaming {
                    e.bytes(b"event: response.failed\ndata: ")?;
                }
                e.bytes(b"{\"type\":\"response.failed\",\"error\":{\"code\":\"upstream_error\",\"message\":\"Response failed\"}}")?;
                if self.mode == BodyMode::Streaming {
                    e.bytes(b"\n\n")?;
                }
                Ok(())
            })?;
            return output(failed);
        }
        if kind == Kind::ResponseStart {
            let doc = &mut self.document;
            let target = doc.root();
            let node = doc.absorb(incoming)?;
            for (from, to) in [("id", "id"), ("model", "model"), ("created", "created_at")] {
                if let Some(value) = doc.remove(node, from)? {
                    let target = doc.set(target, to, Value::Null)?;
                    doc.transplant(target, value)?;
                }
            }
            let response = encode::scratch(owner, |e| doc.write(target, e))?;
            return self.emit(
                owner,
                "response.created",
                |e| {
                    e.bytes(b",\"response\":")?;
                    write_lease(&response, e)
                },
                output,
            );
        }
        if kind == Kind::Done {
            self.terminal = true;
            let doc = &mut self.document;
            let root = doc.root();
            if is(doc, root, "status", "in_progress")? {
                doc.set(root, "status", Value::Text("completed"))?;
            }
            let response = encode::scratch(owner, |e| write_response(doc, e))?;
            if self.mode == BodyMode::Buffered {
                return output(response);
            }
            let name = if is(doc, root, "status", "incomplete")? {
                "response.incomplete"
            } else {
                "response.completed"
            };
            return self.emit(
                owner,
                name,
                |e| {
                    e.bytes(b",\"response\":")?;
                    write_lease(&response, e)
                },
                output,
            );
        }
        if kind == Kind::Opaque {
            if is(&incoming, root, "name", "responses.response")? {
                let doc = &mut self.document;
                let node = doc.absorb(incoming)?;
                let value = doc.required(node, "value")?;
                super::merge(doc, doc.root(), value)?;
                return Ok(());
            }
            if self.mode == BodyMode::Streaming {
                return output(encode::scratch(owner, |e| super::opaque::write(&incoming, e))?);
            }
            return super::opaque::retain(&mut self.document, incoming);
        }
        if matches!(
            kind,
            Kind::Grounding | Kind::CodeExecution | Kind::UrlContext | Kind::Operation
        ) {
            (self.diagnostic)("unsupported event", 0);
            return Ok(());
        }
        let node = self.document.absorb(incoming)?;
        let doc = &mut self.document;
        let x = doc.required(node, "x_ck")?;
        let output_node = doc.required(doc.root(), "output")?;
        if kind == Kind::Usage {
            let usage = doc.required(node, "usage")?;
            let value = if let Some(native) = doc.member(usage, "native")? {
                if is(doc, native, "protocol", "responses")? {
                    doc.required(native, "value")?
                } else {
                    usage
                }
            } else {
                usage
            };
            let target = doc.set(doc.root(), "usage", Value::Null)?;
            doc.transplant(target, value)?;
            return Ok(());
        }
        if kind == Kind::Compact {
            let item = doc.required(node, "item")?;
            doc.detach(node, item)?;
            doc.append(output_node, item)?;
            return Ok(());
        }
        if kind == Kind::ItemStart {
            let item = doc.add(None, Value::Object)?;
            doc.append(output_node, item)?;
            if let Some(attributes) = doc.member(node, "attributes")? {
                super::merge(doc, item, attributes)?;
            }
            if doc.member(item, "type")?.is_none() {
                doc.set(item, "type", Value::Text("message"))?;
            }
            if doc.member(item, "id")?.is_none() {
                let id = doc.node(doc.required(x, "item_id")?)?.value;
                doc.set(item, "id", id)?;
            }
            doc.set(item, "status", Value::Text("in_progress"))?;
            if is(doc, item, "type", "message")? {
                if doc.member(item, "role")?.is_none() {
                    doc.set(item, "role", Value::Text("assistant"))?;
                }
                doc.set(item, "content", Value::Array)?;
            } else if is(doc, item, "type", "reasoning")? {
                doc.set(item, "summary", Value::Array)?;
            }
            doc.bind_origin(item, x, None)?;
            let index = doc.children(output_node)?.count() as u64 - 1;
            let payload = encode::scratch(owner, |e| {
                e.bytes(b",\"output_index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"item\":")?;
                write_item(doc, item, e)
            })?;
            return self.emit(
                owner,
                "response.output_item.added",
                |e| write_lease(&payload, e),
                output,
            );
        }
        let Some((item, index)) = find_item(doc, output_node, x)? else {
            if kind == Kind::Chunk {
                return self.finish_chunk(node);
            }
            return Err(Error::Schema);
        };
        if kind == Kind::ToolStart {
            if is(doc, node, "kind", "custom")? {
                doc.set(item, "type", Value::Text("custom_tool_call"))?;
            } else {
                doc.set(item, "type", Value::Text("function_call"))?;
            }
            for name in ["name", "namespace"] {
                if let Some(value) = doc.member(node, name)? {
                    let target = doc.set(item, name, Value::Null)?;
                    doc.transplant(target, value)?;
                }
            }
            if let Some(source) = doc.member(x, "source")?
                && let Some(ids) = doc.member(source, "ids")?
            {
                for name in ["id", "call_id"] {
                    if let Some(value) = doc.member(ids, name)? {
                        let target = doc.set(item, name, Value::Null)?;
                        doc.transplant(target, value)?;
                    }
                }
            }
            if doc.member(item, "call_id")?.is_none() {
                let value = doc.node(doc.required(x, "tool_call_id")?)?.value;
                doc.set(item, "call_id", value)?;
            }
            return Ok(());
        }
        if kind == Kind::BlockStart {
            let summary = is(doc, node, "kind", "reasoning_summary")?;
            let thinking = is(doc, node, "kind", "thinking")? || is(doc, node, "kind", "reasoning")?;
            let parts = match doc.member(item, if summary { "summary" } else { "content" })? {
                Some(p) => p,
                None => doc.set(item, if summary { "summary" } else { "content" }, Value::Array)?,
            };
            let part = doc.add(None, Value::Object)?;
            doc.append(parts, part)?;
            if let Some(attrs) = doc.member(node, "attributes")? {
                super::merge(doc, part, attrs)?;
            }
            let refusal = is(doc, node, "kind", "refusal")?;
            doc.set(
                part,
                "type",
                Value::Text(if refusal {
                    "refusal"
                } else if summary {
                    "summary_text"
                } else if thinking {
                    "reasoning_text"
                } else {
                    "output_text"
                }),
            )?;
            doc.bind_origin(part, x, None)?;
            let payload = encode::scratch(owner, |e| {
                item_fields(doc, item, index, e)?;
                e.bytes(if summary {
                    b",\"summary_index\":"
                } else {
                    b",\"content_index\":"
                })?;
                doc.write(doc.required(x, "block_index")?, e)?;
                e.bytes(b",\"part\":")?;
                write_part(doc, part, e)
            })?;
            return self.emit(
                owner,
                if summary {
                    "response.reasoning_summary_part.added"
                } else {
                    "response.content_part.added"
                },
                |e| write_lease(&payload, e),
                output,
            );
        }
        if kind == Kind::Chunk {
            let choices = doc.required(node, "choices")?;
            if let Some(choice) = doc.node(choices)?.first {
                let delta = doc.required(choice, "delta")?;
                if let Some(calls) = doc.member(delta, "tool_calls")? {
                    for call in doc.children(calls)? {
                        let function = doc.required(call, "function")?;
                        if let Some(args) = doc.member(function, "arguments")? {
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
                            } else {
                                "response.function_call_arguments.delta"
                            };
                            // One canonical event contains at most one tool delta.
                            return self.emit(owner, name, |e| write_lease(&payload, e), output);
                        }
                    }
                }
                for (from, native, delta_name) in [
                    ("content", "text", "response.output_text.delta"),
                    ("refusal", "refusal", "response.refusal.delta"),
                    ("reasoning_content", "text", "response.reasoning_summary_text.delta"),
                ] {
                    if let Some(value) = doc.member(delta, from)? {
                        let (part, summary, block) = find_part(doc, item, x)?.ok_or(Error::Schema)?;
                        let delta_name = if from == "reasoning_content" && !summary {
                            "response.reasoning_text.delta"
                        } else {
                            delta_name
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
                            super::member(doc, choice, "logprobs", e)
                        })?;
                        doc.detach(delta, value)?;
                        append_fragment(doc, part, native, value)?;
                        return self.emit(owner, delta_name, |e| write_lease(&payload, e), output);
                    }
                }
                if let Some(logprobs) = doc.member(choice, "logprobs")?
                    && let Some(tokens) = doc.member(logprobs, "content")?
                {
                    let (part, _, block) = find_part(doc, item, x)?.ok_or(Error::Schema)?;
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
                    return self.emit(
                        owner,
                        "response.output_text.delta",
                        |e| write_lease(&payload, e),
                        output,
                    );
                }
            }
            return self.finish_chunk(node);
        }
        if kind == Kind::ReasoningSignature {
            let fragment = doc.required(node, "fragment")?;
            doc.detach(node, fragment)?;
            append_fragment(doc, item, "encrypted_content", fragment)?;
            return Ok(());
        }
        if kind == Kind::ReasoningRedacted {
            let data = doc.required(node, "data")?;
            let lease = encode::scratch(owner, |e| {
                e.bytes(b"\"claude-redacted-thinking:")?;
                doc.write_contents(data, e)?;
                e.bytes(b"\"")
            })?;
            let carrier = doc.import_json(owner, lease)?;
            let target = doc.set(item, "encrypted_content", Value::Null)?;
            doc.transplant(target, carrier)?;
            return Ok(());
        }
        if kind == Kind::Annotation {
            let (part, _, block) = find_part(doc, item, x)?.ok_or(Error::Schema)?;
            let annotations = match doc.member(part, "annotations")? {
                Some(a) => a,
                None => doc.set(part, "annotations", Value::Array)?,
            };
            let value = doc.required(node, "value")?;
            doc.detach(node, value)?;
            doc.append(annotations, value)?;
            let payload = encode::scratch(owner, |e| {
                item_fields(doc, item, index, e)?;
                e.bytes(b",\"content_index\":")?;
                e.unsigned(block)?;
                e.bytes(b",\"annotation\":")?;
                doc.write(value, e)
            })?;
            return self.emit(
                owner,
                "response.output_text.annotation.added",
                |e| write_lease(&payload, e),
                output,
            );
        }
        if kind == Kind::BlockEnd {
            let Some((part, summary, block)) = find_part(doc, item, x)? else {
                return Ok(());
            };
            let payload = encode::scratch(owner, |e| {
                item_fields(doc, item, index, e)?;
                e.bytes(if summary {
                    b",\"summary_index\":"
                } else {
                    b",\"content_index\":"
                })?;
                e.unsigned(block)?;
                e.bytes(b",\"part\":")?;
                write_part(doc, part, e)
            })?;
            return self.emit(
                owner,
                if summary {
                    "response.reasoning_summary_part.done"
                } else {
                    "response.content_part.done"
                },
                |e| write_lease(&payload, e),
                output,
            );
        }
        if kind == Kind::ToolEnd {
            let custom = is(doc, item, "type", "custom_tool_call")?;
            let field = if custom { "input" } else { "arguments" };
            let payload = encode::scratch(owner, |e| {
                item_fields(doc, item, index, e)?;
                super::member(doc, item, field, e)
            })?;
            return self.emit(
                owner,
                if custom {
                    "response.custom_tool_call_input.done"
                } else {
                    "response.function_call_arguments.done"
                },
                |e| write_lease(&payload, e),
                output,
            );
        }
        if kind == Kind::ItemEnd {
            if let Some(attrs) = doc.member(node, "attributes")? {
                super::merge(doc, item, attrs)?;
            }
            doc.set(
                item,
                "status",
                Value::Text(if is(doc, node, "status", "incomplete")? {
                    "incomplete"
                } else {
                    "completed"
                }),
            )?;
            let payload = encode::scratch(owner, |e| {
                e.bytes(b",\"output_index\":")?;
                e.unsigned(index)?;
                e.bytes(b",\"item\":")?;
                write_item(doc, item, e)
            })?;
            return self.emit(owner, "response.output_item.done", |e| write_lease(&payload, e), output);
        }
        (self.diagnostic)("unsupported event", 0);
        Ok(())
    }
    fn finish_chunk(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        let choices = doc.required(node, "choices")?;
        for choice in doc.children(choices)? {
            if let Some(reason) = doc.member(choice, "finish_reason")?
                && !matches!(doc.node(reason)?.value, Value::Null)
            {
                let status = if doc.string_is(reason, "length")? || doc.string_is(reason, "content_filter")? {
                    "incomplete"
                } else {
                    "completed"
                };
                doc.set(doc.root(), "status", Value::Text(status))?;
                break;
            }
        }
        Ok(())
    }
}
impl<F: FnMut(&'static str, u64)> codec::ResponseEgress for Egress<F> {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: ck_stream::Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        let (kind, doc) = internal::decode(owner, input)?;
        self.process(owner, kind, doc, &mut output)
    }
    fn finish(&mut self) -> Result<()> {
        if self.terminal { Ok(()) } else { Err(Error::Truncated) }
    }
}
fn write_lease(lease: &ScratchLease, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut offset = 0;
    while offset < lease.len() {
        let chunk = lease.chunk(offset)?;
        e.bytes(chunk)?;
        offset += chunk.len();
    }
    Ok(())
}
fn find_item(doc: &Document, output: usize, x: usize) -> Result<Option<(usize, u64)>> {
    let Some(local) = doc.member(x, "item_id")? else {
        return Ok(None);
    };
    for (index, item) in doc.children(output)?.enumerate() {
        if let Some((origin, _)) = doc.origin(item)?
            && doc.equal_strings(doc.required(origin, "item_id")?, local)?
        {
            return Ok(Some((item, index as u64)));
        }
    }
    Ok(None)
}
fn find_part(doc: &Document, item: usize, x: usize) -> Result<Option<(usize, bool, u64)>> {
    let Some(block) = doc.member(x, "block_index")? else {
        return Ok(None);
    };
    let block = doc.unsigned(block)?;
    for (name, summary) in [("content", false), ("summary", true)] {
        if let Some(parts) = doc.member(item, name)? {
            for part in doc.children(parts)? {
                if let Some((origin, _)) = doc.origin(part)?
                    && doc.unsigned(doc.required(origin, "block_index")?)? == block
                {
                    return Ok(Some((part, summary, block)));
                }
            }
        }
    }
    Ok(None)
}
fn item_fields(doc: &Document, item: usize, index: u64, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"output_index\":")?;
    e.unsigned(index)?;
    e.bytes(b",\"item_id\":")?;
    doc.write(doc.required(item, "id")?, e)
}
fn write_part(doc: &Document, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    doc.write(part, e)
}
fn write_item(doc: &Document, item: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    let mut first = true;
    for field in doc.children(item)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if doc.name_is(name, "content")? || doc.name_is(name, "summary")? {
            e.bytes(b"[")?;
            for (i, part) in doc.children(field)?.enumerate() {
                if i > 0 {
                    e.bytes(b",")?;
                }
                write_part(doc, part, e)?;
            }
            e.bytes(b"]")?;
        } else {
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}
fn write_response(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{")?;
    for (i, field) in doc.children(doc.root())?.enumerate() {
        if i > 0 {
            e.bytes(b",")?;
        }
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        doc.write_name(name, e)?;
        e.bytes(b":")?;
        if doc.name_is(name, "output")? {
            e.bytes(b"[")?;
            for (j, item) in doc.children(field)?.enumerate() {
                if j > 0 {
                    e.bytes(b",")?;
                }
                write_item(doc, item, e)?;
            }
            e.bytes(b"]")?;
        } else {
            doc.write(field, e)?;
        }
    }
    e.bytes(b"}")
}
