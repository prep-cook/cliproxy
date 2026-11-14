use super::{Table, response::Framing};
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Identity, Kind, LocalId},
    internal,
};
use ck_stream::{Lease, ScratchLease, Writer};

#[derive(Clone, Copy, Debug)]
pub enum Diagnostic {
    UnsupportedEvent(Kind),
    SseMetadata,
}
pub struct Egress {
    owner: u64,
    operation: Operation,
    framing: Framing,
    document: Document,
    choices: Table<Choice>,
    blocks: Table<Block>,
    terminal: bool,
    started: bool,
    array_started: bool,
    diagnostic: fn(Diagnostic),
}
struct Choice {
    index: u64,
    node: usize,
    parts: usize,
    finished: bool,
}
struct Block {
    item: LocalId,
    index: u64,
    choice: u64,
    node: usize,
    arguments: Option<usize>,
    media: Option<usize>,
    ended: bool,
}
impl Egress {
    pub fn new(
        owner: &mut Writer,
        _identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        Self::with_framing(
            owner,
            operation,
            if mode == BodyMode::Buffered {
                Framing::Json
            } else {
                Framing::Sse
            },
            diagnostic,
        )
    }
    pub fn with_framing(
        owner: &mut Writer,
        operation: Operation,
        framing: Framing,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && framing != Framing::Json) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            operation,
            framing,
            document: super::mapped(owner, |e| e.bytes(b"{\"candidates\":[]}"))?,
            choices: Table::new(owner)?,
            blocks: Table::new(owner)?,
            terminal: false,
            started: false,
            array_started: false,
            diagnostic,
        })
    }
    fn choice(&mut self, index: u64) -> Result<usize> {
        if let Some(slot) = self.choices.iter().position(|c| c.index == index) {
            return Ok(slot);
        }
        let doc = &mut self.document;
        let node = doc.add(None, Value::Object)?;
        let candidates = doc.required(doc.root(), "candidates")?;
        doc.append(candidates, node)?;
        doc.set(node, "index", Value::Unsigned(index))?;
        let content = doc.set(node, "content", Value::Object)?;
        doc.set(content, "role", Value::Text("model"))?;
        let parts = doc.set(content, "parts", Value::Array)?;
        self.choices.push(Choice {
            index,
            node,
            parts,
            finished: false,
        })
    }
    fn block(&self, x: usize) -> Result<usize> {
        let doc = &self.document;
        let item = local(doc, doc.required(x, "item_id")?)?;
        let index = doc.unsigned(doc.required(x, "block_index")?)?;
        self.blocks
            .iter()
            .position(|b| b.item == item && b.index == index && !b.ended)
            .ok_or(Error::Schema)
    }
    fn emit(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        write: impl Fn(&Document, &mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        let prefix = self.framing == Framing::JsonArray && !self.array_started;
        let separator = self.framing == Framing::JsonArray && self.array_started;
        output(encode::scratch(owner, |e| {
            if self.framing == Framing::Sse {
                e.bytes(b"data: ")?;
            } else if prefix {
                e.bytes(b"[")?;
            } else if separator {
                e.bytes(b",")?;
            }
            write(&self.document, e)?;
            if self.framing == Framing::Sse {
                e.bytes(b"\n\n")?;
            }
            Ok(())
        })?)?;
        self.array_started = true;
        Ok(())
    }
    fn candidate_output(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        choice: u64,
        part: Option<usize>,
        metadata: Option<usize>,
    ) -> Result<()> {
        if self.framing == Framing::Json {
            return Ok(());
        }
        self.emit(owner, output, |doc, e| {
            e.bytes(b"{\"candidates\":[{\"index\":")?;
            e.unsigned(choice)?;
            if let Some(part) = part {
                e.bytes(b",\"content\":{\"role\":\"model\",\"parts\":[")?;
                doc.write(part, e)?;
                e.bytes(b"]}")?;
            }
            if let Some(metadata) = metadata {
                super::members(doc, metadata, &["index", "content"], e)?;
            }
            e.bytes(b"}]}")
        })
    }
    fn consume(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        event: Document,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let root = self.document.absorb(event)?;
        let x = self.document.required(root, "x_ck")?;
        if !self.started && !matches!(kind, Kind::ResponseStart | Kind::Error | Kind::Cancelled) {
            return Err(Error::Schema);
        }
        match kind {
            Kind::ResponseStart => {
                if self.started {
                    return Err(Error::Schema);
                }
                self.started = true;
                for (canonical, native) in [("id", "responseId"), ("model", "modelVersion")] {
                    if let Some(n) = self.document.member(root, canonical)? {
                        move_to_root(&mut self.document, root, n, native)?;
                    }
                }
                if let Some(source) = self.document.member(x, "source")?
                    && let Some(time) = self.document.member(source, "timestamp")?
                {
                    move_to_root(&mut self.document, source, time, "createTime")?;
                }
            }
            Kind::ItemStart | Kind::ItemEnd | Kind::Heartbeat => (),
            Kind::BlockStart => {
                let choice = self.document.unsigned(self.document.required(x, "choice_index")?)?;
                let slot = self.choice(choice)?;
                if self.choices[slot].finished {
                    return Err(Error::Schema);
                }
                let node = self.document.add(None, Value::Object)?;
                self.document.append(self.choices[slot].parts, node)?;
                let item = local(&self.document, self.document.required(x, "item_id")?)?;
                let index = self.document.unsigned(self.document.required(x, "block_index")?)?;
                if self.blocks.iter().any(|b| b.item == item && b.index == index) {
                    return Err(Error::Schema);
                }
                if super::is(&self.document, root, "kind", "thinking")?
                    || super::is(&self.document, root, "kind", "reasoning")?
                {
                    self.document.set(node, "thought", Value::Bool(true))?;
                }
                restore_originals(&mut self.document, x, node)?;
                self.blocks.push(Block {
                    item,
                    index,
                    choice,
                    node,
                    arguments: None,
                    media: None,
                    ended: false,
                })?;
            }
            Kind::Chunk => {
                let choices = self.document.required(root, "choices")?;
                let mut next = self.document.node(choices)?.first;
                while let Some(c) = next {
                    next = self.document.node(c)?.next;
                    let index = self.document.unsigned(self.document.required(c, "index")?)?;
                    let choice = self.choice(index)?;
                    if self.choices[choice].finished {
                        return Err(Error::Schema);
                    }
                    if let Some(delta) = self.document.member(c, "delta")? {
                        for key in ["content", "reasoning_content"] {
                            if let Some(text) = self.document.member(delta, key)? {
                                let block = self.block(x)?;
                                let part = self.blocks[block].node;
                                append_fragment(&mut self.document, part, "text", delta, text, false)?;
                                if key == "reasoning_content" {
                                    self.document.set(part, "thought", Value::Bool(true))?;
                                }
                            }
                        }
                        if let Some(calls) = self.document.member(delta, "tool_calls")? {
                            let mut call = self.document.node(calls)?.first;
                            while let Some(n) = call {
                                call = self.document.node(n)?.next;
                                if let Some(function) = self.document.member(n, "function")?
                                    && let Some(args) = self.document.member(function, "arguments")?
                                {
                                    let block = self.block(x)?;
                                    let node = self.blocks[block].arguments.ok_or(Error::Schema)?;
                                    self.document.detach(function, args)?;
                                    self.document.append(node, args)?;
                                }
                            }
                        }
                    }
                    if let Some(logprobs) = self.document.member(c, "logprobs")? {
                        let target = self.choices[choice].node;
                        replace_from(&mut self.document, c, logprobs, target, "logprobsResult")?;
                        self.candidate_output(owner, output, index, None, Some(target))?;
                    }
                    if let Some(finish) = self.document.member(c, "finish_reason")?
                        && !matches!(self.document.node(finish)?.value, Value::Null)
                    {
                        let node = self.choices[choice].node;
                        if let Some(native) = self.document.member(x, "native_finish_reason")? {
                            replace_from(&mut self.document, x, native, node, "finishReason")?;
                        } else {
                            let reason = if self.document.string_is(finish, "length")? {
                                "MAX_TOKENS"
                            } else if self.document.string_is(finish, "content_filter")? {
                                "SAFETY"
                            } else {
                                "STOP"
                            };
                            self.document.set(node, "finishReason", Value::Text(reason))?;
                        }
                        self.choices[choice].finished = true;
                        self.candidate_output(owner, output, index, None, Some(node))?;
                    }
                }
            }
            Kind::ReasoningSignature => {
                if !super::is(&self.document, root, "kind", "gemini")? {
                    self.opaque(owner, output, kind, root)?;
                } else {
                    let block = self.block(x)?;
                    let node = self.blocks[block].node;
                    let fragment = self.document.required(root, "fragment")?;
                    append_fragment(&mut self.document, node, "thoughtSignature", root, fragment, false)?;
                }
            }
            Kind::ToolStart => {
                let block = self.block(x)?;
                let part = self.blocks[block].node;
                let call = self.document.set(part, "functionCall", Value::Object)?;
                let name = self.document.required(root, "name")?;
                replace_from(&mut self.document, root, name, call, "name")?;
                if let Some(source) = self.document.member(x, "source")?
                    && let Some(ids) = self.document.member(source, "ids")?
                    && let Some(id) = self.document.member(ids, "id")?
                {
                    replace_from(&mut self.document, ids, id, call, "id")?;
                }
                let arguments = self.document.add(None, Value::Fragments)?;
                self.blocks[block].arguments = Some(arguments);
            }
            Kind::ToolEnd => {
                let block = self.block(x)?;
                let arguments = self.blocks[block].arguments.ok_or(Error::Schema)?;
                if self.document.node(arguments)?.first.is_some() {
                    let raw = encode::scratch(owner, |e| {
                        let mut chars = self.document.chars(arguments)?;
                        while let Some(c) = chars.next_scalar()? {
                            e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                        }
                        Ok(())
                    })?;
                    let value = self.document.import_json(owner, raw)?;
                    super::object(&self.document, value)?;
                    let call = self.document.required(self.blocks[block].node, "functionCall")?;
                    self.document.rename(value, "args")?;
                    self.document.append(call, value)?;
                }
            }
            Kind::ToolResult => {
                let block = self.block(x)?;
                let value = self.document.required(root, "value")?;
                replace_from(
                    &mut self.document,
                    root,
                    value,
                    self.blocks[block].node,
                    "functionResponse",
                )?;
            }
            Kind::MediaStart => {
                let block = self.block(x)?;
                let inline = self
                    .document
                    .set(self.blocks[block].node, "inlineData", Value::Object)?;
                if let Some(mime) = self.document.member(root, "mime")? {
                    replace_from(&mut self.document, root, mime, inline, "mimeType")?;
                }
                self.blocks[block].media = Some(self.document.set(inline, "data", Value::Base64Fragments)?);
            }
            Kind::Data => {
                let block = self.block(x)?;
                let target = self.blocks[block].media.ok_or(Error::Schema)?;
                let bytes = self.document.required(root, "bytes")?;
                self.document.detach(root, bytes)?;
                self.document.append(target, bytes)?;
            }
            Kind::MediaEnd => (),
            Kind::MediaTranscript => {
                let block = self.block(x)?;
                let text = self.document.required(root, "text")?;
                replace_from(
                    &mut self.document,
                    root,
                    text,
                    self.blocks[block].node,
                    "audioTranscription",
                )?;
            }
            Kind::CodeExecution => {
                let block = self.block(x)?;
                let name = if super::is(&self.document, root, "phase", "code")? {
                    "executableCode"
                } else {
                    "codeExecutionResult"
                };
                let value = self.document.required(root, "value")?;
                replace_from(&mut self.document, root, value, self.blocks[block].node, name)?;
            }
            Kind::BlockEnd => {
                let block = self.block(x)?;
                self.blocks[block].ended = true;
                self.candidate_output(
                    owner,
                    output,
                    self.blocks[block].choice,
                    Some(self.blocks[block].node),
                    None,
                )?;
            }
            Kind::Grounding | Kind::UrlContext | Kind::Annotation => {
                let source = self.document.member(x, "source")?;
                let name = if kind == Kind::Grounding {
                    "groundingMetadata"
                } else if kind == Kind::UrlContext {
                    "urlContextMetadata"
                } else if source
                    .map(|n| super::is(&self.document, n, "event", "promptFeedback"))
                    .transpose()?
                    .unwrap_or(false)
                {
                    "promptFeedback"
                } else if source
                    .map(|n| super::is(&self.document, n, "event", "safetyRatings"))
                    .transpose()?
                    .unwrap_or(false)
                {
                    "safetyRatings"
                } else {
                    "citationMetadata"
                };
                let value = self.document.required(root, "value")?;
                if let Some(index) = self.document.member(x, "choice_index")? {
                    let index = self.document.unsigned(index)?;
                    let choice = self.choice(index)?;
                    let node = self.choices[choice].node;
                    replace_from(&mut self.document, root, value, node, name)?;
                    self.candidate_output(owner, output, index, None, Some(node))?;
                } else {
                    move_to_root(&mut self.document, root, value, name)?;
                    self.top_output(owner, output, name)?;
                }
            }
            Kind::Usage => {
                let usage = self.document.required(root, "usage")?;
                if let Some(native) = self.document.member(usage, "native")?
                    && super::is(&self.document, native, "protocol", "gemini")?
                {
                    let value = self.document.required(native, "value")?;
                    move_to_root(&mut self.document, native, value, "usageMetadata")?;
                } else {
                    let native = self
                        .document
                        .set(self.document.root(), "usageMetadata", Value::Object)?;
                    for (canonical, wire) in [
                        ("input_tokens", "promptTokenCount"),
                        ("total_tokens", "totalTokenCount"),
                        ("cache_read_tokens", "cachedContentTokenCount"),
                        ("reasoning_tokens", "thoughtsTokenCount"),
                    ] {
                        if let Some(n) = self.document.member(usage, canonical)? {
                            let value = self.document.node(n)?.value;
                            self.document.set(native, wire, value)?;
                        }
                    }
                    if let Some(output) = self.document.member(usage, "output_tokens")? {
                        let count = self.document.unsigned(output)?;
                        let reasoning = self
                            .document
                            .member(usage, "reasoning_tokens")?
                            .map(|n| self.document.unsigned(n))
                            .transpose()?;
                        if let Some(reasoning) = reasoning {
                            self.document.set(
                                native,
                                "candidatesTokenCount",
                                Value::Unsigned(count.checked_sub(reasoning).ok_or(Error::Schema)?),
                            )?;
                        }
                    }
                    let extra = self.document.clone_tree(usage)?;
                    self.document.rename(extra, "x_ck_usage")?;
                    self.document.remove(self.document.root(), "x_ck_usage")?;
                    self.document.append(self.document.root(), extra)?;
                }
                self.top_output(owner, output, "usageMetadata")?;
            }
            Kind::CountTokens => {
                if self.operation != Operation::CountTokens {
                    return Err(Error::Schema);
                }
                if let Some(details) = self.document.member(root, "details")? {
                    let value = self.document.clone_tree(details)?;
                    self.document.transplant(self.document.root(), value)?;
                } else {
                    let count = self
                        .document
                        .member(root, "total_tokens")?
                        .or(self.document.member(root, "input_tokens")?)
                        .ok_or(Error::Schema)?;
                    let value = self.document.node(count)?.value;
                    self.document.replace(self.document.root(), Value::Object)?;
                    self.document.set(self.document.root(), "totalTokens", value)?;
                }
            }
            Kind::Opaque => {
                if super::is(&self.document, root, "name", "gemini.response")? {
                    let value = self.document.required(root, "value")?;
                    let mut field = self.document.node(value)?.first;
                    while let Some(n) = field {
                        field = self.document.node(n)?.next;
                        let name = self.document.node(n)?.name.ok_or(Error::Schema)?;
                        if self.document.name_is(name, "candidates")? {
                            continue;
                        }
                        replace_named(&mut self.document, value, n, 0)?;
                    }
                } else if super::is(&self.document, root, "name", "gemini.candidate")? {
                    let value = self.document.required(root, "value")?;
                    let index = self.document.unsigned(self.document.required(value, "index")?)?;
                    let choice = self.choice(index)?;
                    let target = self.choices[choice].node;
                    let mut field = self.document.node(value)?.first;
                    while let Some(n) = field {
                        field = self.document.node(n)?.next;
                        if self
                            .document
                            .name_is(self.document.node(n)?.name.ok_or(Error::Schema)?, "index")?
                        {
                            continue;
                        }
                        replace_named(&mut self.document, value, n, target)?;
                    }
                } else {
                    self.opaque(owner, output, kind, root)?;
                }
            }
            Kind::Done => {
                if self.blocks.iter().any(|b| !b.ended)
                    || (self.operation == Operation::Generate && self.choices.iter().any(|c| !c.finished))
                {
                    return Err(Error::Truncated);
                }
                if self.framing == Framing::Json {
                    self.emit(owner, output, |doc, e| doc.write(doc.root(), e))?;
                } else if self.framing == Framing::JsonArray {
                    output(encode::scratch(owner, |e| {
                        e.bytes(if self.array_started { b"]" } else { b"[]" })
                    })?)?;
                }
                self.terminal = true;
            }
            Kind::Error | Kind::Cancelled => {
                self.emit(owner, output, |_, e| e.bytes(b"{\"error\":{\"code\":502,\"message\":\"Incomplete upstream response\",\"status\":\"INTERNAL\"}}"))?;
                if self.framing == Framing::JsonArray {
                    output(encode::scratch(owner, |e| e.bytes(b"]"))?)?;
                }
                self.terminal = true;
            }
            _ => self.opaque(owner, output, kind, root)?,
        }
        Ok(())
    }
    fn top_output(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        name: &'static str,
    ) -> Result<()> {
        if self.framing == Framing::Json {
            return Ok(());
        }
        self.emit(owner, output, |doc, e| {
            e.bytes(b"{")?;
            e.string(name)?;
            e.bytes(b":")?;
            doc.write(doc.required(doc.root(), name)?, e)?;
            e.bytes(b"}")
        })
    }
    fn opaque(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
        kind: Kind,
        event: usize,
    ) -> Result<()> {
        let root = self.document.root();
        let carrier = match self.document.member(root, "x_ck_opaque")? {
            Some(n) => n,
            None => self.document.set(root, "x_ck_opaque", Value::Array)?,
        };
        let copy = self.document.clone_tree(event)?;
        self.document.append(carrier, copy)?;
        if kind == Kind::Opaque && super::is(&self.document, event, "name", "gemini.sse")? {
            (self.diagnostic)(Diagnostic::SseMetadata);
        }
        self.top_output(owner, output, "x_ck_opaque")
    }
}
impl codec::ResponseEgress for Egress {
    fn advance(
        &mut self,
        owner: &mut Writer,
        input: Lease,
        mut output: impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if owner.id() != self.owner {
            return Err(Error::Position);
        }
        if self.terminal {
            return Err(Error::Closed);
        }
        let (kind, event) = internal::decode(owner, input)?;
        self.consume(owner, kind, event, &mut output)
    }
    fn finish(&mut self) -> Result<()> {
        if self.terminal { Ok(()) } else { Err(Error::Truncated) }
    }
}
fn local(doc: &Document, node: usize) -> Result<LocalId> {
    let mut buffer = [0; 64];
    let mut len = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == buffer.len() {
            return Err(Error::Schema);
        }
        buffer[len] = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&buffer[..len]).map_err(|_| Error::Schema)?)
}
fn move_to_root(doc: &mut Document, parent: usize, field: usize, name: &'static str) -> Result<()> {
    replace_from(doc, parent, field, doc.root(), name)
}
fn replace_from(doc: &mut Document, parent: usize, field: usize, target: usize, name: &'static str) -> Result<()> {
    doc.remove(target, name)?;
    doc.detach(parent, field)?;
    doc.rename(field, name)?;
    doc.append(target, field)
}
fn replace_named(doc: &mut Document, parent: usize, field: usize, target: usize) -> Result<()> {
    let name = doc.node(field)?.name.ok_or(Error::Schema)?;
    let mut previous = doc.node(target)?.first;
    while let Some(n) = previous {
        previous = doc.node(n)?.next;
        if doc.names_equal(name, doc.node(n)?.name.ok_or(Error::Schema)?)? {
            doc.detach(target, n)?;
            break;
        }
    }
    doc.detach(parent, field)?;
    doc.append(target, field)
}
fn append_fragment(
    doc: &mut Document,
    parent: usize,
    key: &'static str,
    from: usize,
    value: usize,
    base64: bool,
) -> Result<()> {
    let target = match doc.member(parent, key)? {
        Some(n) => n,
        None => doc.set(
            parent,
            key,
            if base64 {
                Value::Base64Fragments
            } else {
                Value::Fragments
            },
        )?,
    };
    doc.detach(from, value)?;
    doc.append(target, value)
}
fn restore_originals(doc: &mut Document, x: usize, target: usize) -> Result<()> {
    if let Some(original) = doc.member(x, "original")? {
        let mut next = doc.node(original)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            if !super::is(doc, entry, "protocol", "gemini")? {
                continue;
            }
            let pointer = doc.required(entry, "pointer")?;
            let Value::String(text) = doc.node(pointer)?.value else {
                return Err(Error::Schema);
            };
            let value = doc.required(entry, "value")?;
            let node = doc.add(Some(crate::codec::document::Name::Pointer(text)), Value::Null)?;
            doc.transplant(node, value)?;
            doc.append(target, node)?;
        }
    }
    Ok(())
}
