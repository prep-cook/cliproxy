use super::Table;
use crate::codec::{
    self, BodyMode, Error, Operation, Result,
    document::{Document, Value},
    encode::{self, Encoder},
    event::{Identity, Kind, LocalId},
    internal,
};
use ck_stream::{Lease, ScratchLease, Writer};

pub struct Diagnostic {
    pub identity: Identity,
    pub kind: Kind,
    pub choice_index: Option<u64>,
    pub reason: &'static str,
    pub destination: &'static str,
}
struct Block {
    item: LocalId,
    canonical: u64,
    index: u64,
    document: Document,
    open: bool,
    started: bool,
    tool: Option<u64>,
}
pub struct Egress {
    owner: u64,
    identity: Identity,
    operation: Operation,
    mode: BodyMode,
    sequence: u64,
    base: Option<Document>,
    blocks: Table<Block>,
    usage: Option<Document>,
    finish: Option<Document>,
    count: Option<Document>,
    started: bool,
    terminal: bool,
    failed: bool,
    diagnostic: fn(Diagnostic),
}
impl Egress {
    pub fn new(
        owner: &mut Writer,
        identity: Identity,
        operation: Operation,
        mode: BodyMode,
        diagnostic: fn(Diagnostic),
    ) -> Result<Self> {
        if operation == Operation::Compact || (operation == Operation::CountTokens && mode == BodyMode::Streaming) {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            owner: owner.id(),
            identity,
            operation,
            mode,
            sequence: 0,
            base: None,
            blocks: Table::new(owner)?,
            usage: None,
            finish: None,
            count: None,
            started: false,
            terminal: false,
            failed: false,
            diagnostic,
        })
    }
    fn drop_event(&self, kind: Kind, choice: Option<u64>, reason: &'static str) {
        (self.diagnostic)(Diagnostic {
            identity: self.identity,
            kind,
            choice_index: choice,
            reason,
            destination: "anthropic",
        });
    }
    fn begin(&mut self, owner: &mut Writer, output: &mut impl FnMut(ScratchLease) -> Result<()>) -> Result<()> {
        if self.started || self.mode == BodyMode::Buffered {
            return Ok(());
        }
        let base = self.base.as_ref().ok_or(Error::Schema)?;
        output(event(owner, "message_start", |e| {
            e.bytes(b",\"message\":{")?;
            write_base(base, e)?;
            e.bytes(b",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null")?;
            if let Some(usage) = &self.usage {
                e.bytes(b",\"usage\":")?;
                write_usage(usage, e)?;
            }
            e.bytes(b"}")
        })?)?;
        self.started = true;
        Ok(())
    }
    fn accept(
        &mut self,
        owner: &mut Writer,
        lease: Lease,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.terminal || self.failed {
            return Err(Error::Closed);
        }
        let (kind, doc) = internal::decode(owner, lease)?;
        let root = doc.root();
        let x = doc.required(root, "x_ck")?;
        if doc.unsigned(doc.required(x, "seq")?)? != self.sequence
            || doc.unsigned(doc.required(x, "attempt")?)? != self.identity.attempt
            || !doc.string_is(doc.required(x, "request_id")?, self.identity.request_id.as_str())?
            || !doc.string_is(doc.required(x, "stream_id")?, self.identity.stream_id.as_str())?
            || !doc.string_is(doc.required(x, "direction")?, "response")?
        {
            return Err(Error::Schema);
        }
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Limit)?;
        let choice = doc.member(x, "choice_index")?.map(|n| doc.unsigned(n)).transpose()?;
        if choice.is_some_and(|index| index != 0) {
            self.drop_event(kind, choice, "single-choice destination selects index zero");
            return Ok(());
        }
        match kind {
            Kind::ResponseStart => {
                if self.base.is_some() {
                    return Err(Error::Schema);
                }
                self.base = Some(doc);
            }
            Kind::ItemStart | Kind::ItemEnd | Kind::Heartbeat => {}
            Kind::BlockStart => {
                if self.finish.is_some() {
                    return Err(Error::Schema);
                }
                let item = local(&doc, doc.required(x, "item_id")?)?;
                let canonical = doc.unsigned(doc.required(x, "block_index")?)?;
                if self
                    .blocks
                    .rows
                    .iter()
                    .any(|b| b.item == item && b.canonical == canonical)
                {
                    return Err(Error::Schema);
                }
                let native = if super::is(&doc, root, "kind", "text")? {
                    "text"
                } else if super::is(&doc, root, "kind", "thinking")? || super::is(&doc, root, "kind", "reasoning")? {
                    "thinking"
                } else if super::is(&doc, root, "kind", "redacted")? {
                    "redacted_thinking"
                } else if super::is(&doc, root, "kind", "function")? {
                    "tool_use"
                } else if super::is(&doc, root, "kind", "server")? {
                    "server_tool_use"
                } else if super::is(&doc, root, "kind", "tool_result")? {
                    "tool_result"
                } else {
                    "opaque"
                };
                let mut body = super::mapped(owner, |e| {
                    if let Some(attrs) = doc.member(root, "attributes")? {
                        super::object(&doc, attrs, &["type", "citations"], e)
                    } else {
                        e.bytes(b"{}")
                    }
                })?;
                let body_root = body.root();
                if matches!(native, "tool_result" | "server_tool_use")
                    && let Some(attrs) = doc.member(root, "attributes")?
                    && let Some(kind) = doc.member(attrs, "type")?
                {
                    let from = body.absorb(super::mapped(owner, |e| doc.write(kind, e))?)?;
                    copy_value(&mut body, body_root, "type", from)?;
                } else {
                    body.set(body_root, "type", Value::Text(native))?;
                }
                for (k, field) in [("text", "text"), ("thinking", "thinking")] {
                    if native == k {
                        body.set(body_root, field, Value::Fragments)?;
                    }
                }
                let index = self.blocks.rows.len() as u64;
                self.blocks.push(Block {
                    item,
                    canonical,
                    index,
                    document: body,
                    open: true,
                    started: false,
                    tool: None,
                })?;
                if matches!(native, "text" | "thinking") {
                    self.start_block(owner, index as usize, output)?;
                }
            }
            Kind::ToolStart => {
                let index = self.find_or_tool(owner, &doc, x)?;
                let block = &mut self.blocks.rows[index];
                if block.tool.is_some() {
                    return Err(Error::Schema);
                }
                block.tool = Some(doc.unsigned(doc.required(x, "tool_index")?)?);
                let body = &mut block.document;
                let from = body.absorb(doc)?;
                let x = body.required(from, "x_ck")?;
                let id = if let Some(source) = body.member(x, "source")? {
                    if let Some(ids) = body.member(source, "ids")? {
                        body.member(ids, "id")?.or(body.member(ids, "call_id")?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                if let Some(id) = id {
                    copy_value(body, body.root(), "id", id)?;
                } else {
                    let id = local(body, body.required(x, "tool_call_id")?)?;
                    body.set(body.root(), "id", Value::Id(id))?;
                }
                let name = body.required(from, "name")?;
                copy_value(body, body.root(), "name", name)?;
                if super::is(body, from, "kind", "server")? {
                    body.set(body.root(), "type", Value::Text("server_tool_use"))?;
                }
                if !super::is(body, from, "arguments_format", "json")? {
                    return Err(Error::Unsupported);
                }
                self.start_block(owner, index, output)?;
            }
            Kind::Chunk => {
                // Source metadata can replace a synthetic response.start after a heartbeat.
                if !self.started && metadata_changed(self.base.as_ref().ok_or(Error::Schema)?, &doc)? {
                    let base = self.base.as_mut().ok_or(Error::Schema)?;
                    let update = super::mapped(owner, |e| {
                        e.bytes(b"{")?;
                        for (i, key) in ["id", "model", "created"].iter().enumerate() {
                            if i != 0 {
                                e.bytes(b",")?;
                            }
                            e.string(key)?;
                            e.bytes(b":")?;
                            if let Some(n) = doc.member(root, key)? {
                                doc.write(n, e)?;
                            } else {
                                base.write(base.required(base.root(), key)?, e)?;
                            }
                        }
                        super::member(base, base.root(), "x_ck", e)?;
                        e.bytes(b"}")
                    })?;
                    *base = update;
                }
                let choices = doc.required(root, "choices")?;
                for choice_node in doc.children(choices)? {
                    let index = doc.unsigned(doc.required(choice_node, "index")?)?;
                    if index != 0 {
                        self.drop_event(kind, Some(index), "single-choice destination selects index zero");
                        continue;
                    }
                    let delta = doc.required(choice_node, "delta")?;
                    for (field, native, delta_kind) in [
                        ("content", "text", "text_delta"),
                        ("reasoning_content", "thinking", "thinking_delta"),
                        ("refusal", "text", "text_delta"),
                    ] {
                        if let Some(value) = doc.member(delta, field)?
                            && !matches!(doc.node(value)?.value, Value::Null)
                        {
                            let index = self.find_or_text(owner, &doc, x, native, output)?;
                            self.fragment(owner, &doc, value, index, (native, delta_kind), output)?;
                        }
                    }
                    if let Some(calls) = doc.member(delta, "tool_calls")? {
                        for call in doc.children(calls)? {
                            let tool = doc.unsigned(doc.required(call, "index")?)?;
                            let index = self
                                .blocks
                                .rows
                                .iter()
                                .position(|b| b.tool == Some(tool) && b.open)
                                .ok_or(Error::Schema)?;
                            let function = doc.required(call, "function")?;
                            if let Some(args) = doc.member(function, "arguments")? {
                                self.fragment(owner, &doc, args, index, ("partial_json", "input_json_delta"), output)?;
                            }
                        }
                    }
                    if let Some(reason) = doc.member(choice_node, "finish_reason")?
                        && !matches!(doc.node(reason)?.value, Value::Null)
                    {
                        if self.finish.is_some() || self.blocks.rows.iter().any(|b| b.open) {
                            return Err(Error::Schema);
                        }
                        self.finish = Some(super::mapped(owner, |e| {
                            e.bytes(b"{\"stop_reason\":")?;
                            let native = doc.member(x, "native_finish_reason")?;
                            let anthropic = match doc.member(x, "source")? {
                                Some(s) => super::is(&doc, s, "protocol", "anthropic")?,
                                None => false,
                            };
                            if anthropic && let Some(native) = native {
                                doc.write(native, e)?;
                            } else {
                                e.string(if doc.string_is(reason, "length")? {
                                    "max_tokens"
                                } else if doc.string_is(reason, "tool_calls")? {
                                    "tool_use"
                                } else if doc.string_is(reason, "content_filter")? {
                                    "refusal"
                                } else {
                                    "end_turn"
                                })?;
                            }
                            original_members(&doc, x, e)?;
                            e.bytes(b"}")
                        })?);
                        self.begin(owner, output)?;
                    }
                    if doc.member(choice_node, "logprobs")?.is_some() {
                        self.drop_event(kind, Some(0), "Anthropic has no logprobs carrier");
                    }
                }
            }
            Kind::ReasoningSignature => {
                if !super::is(&doc, root, "kind", "anthropic")? {
                    self.drop_event(
                        kind,
                        choice,
                        "foreign encrypted or signed reasoning has no Anthropic carrier",
                    );
                    return Ok(());
                }
                let index = self.find(&doc, x)?;
                self.fragment(
                    owner,
                    &doc,
                    doc.required(root, "fragment")?,
                    index,
                    ("signature", "signature_delta"),
                    output,
                )?;
            }
            Kind::ToolResult => {
                let index = self.find(&doc, x)?;
                let body = &mut self.blocks.rows[index].document;
                let from = body.absorb(doc)?;
                copy_value(body, body.root(), "content", body.required(from, "value")?)?;
                if let Some(error) = body.member(from, "is_error")? {
                    copy_value(body, body.root(), "is_error", error)?;
                }
                let x = body.required(from, "x_ck")?;
                let source = body.required(x, "source")?;
                let ids = body.required(source, "ids")?;
                copy_value(body, body.root(), "tool_use_id", body.required(ids, "tool_use_id")?)?;
                self.start_block(owner, index, output)?;
            }
            Kind::ReasoningRedacted => {
                let index = self.find(&doc, x)?;
                let body = &mut self.blocks.rows[index].document;
                let from = body.absorb(doc)?;
                copy_value(body, body.root(), "data", body.required(from, "data")?)?;
                self.start_block(owner, index, output)?;
            }
            Kind::Annotation => {
                let index = self.find(&doc, x)?;
                let value = doc.required(root, "value")?;
                let many = super::is(&doc, root, "kind", "citations")?;
                if !many && !super::is(&doc, root, "kind", "citation")? {
                    self.drop_event(kind, choice, "annotation has no native citation carrier");
                    return Ok(());
                }
                if many {
                    if self.mode == BodyMode::Buffered {
                        let body = &mut self.blocks.rows[index].document;
                        if body.member(body.root(), "citations")?.is_none() {
                            body.set(body.root(), "citations", Value::Array)?;
                        }
                    }
                    for citation in doc.children(value)? {
                        self.citation(owner, &doc, citation, index, output)?;
                    }
                } else {
                    self.citation(owner, &doc, value, index, output)?;
                }
            }
            Kind::ToolEnd => {
                let index = self.find(&doc, x)?;
                let body = &mut self.blocks.rows[index].document;
                if let Some(fragments) = body.remove(body.root(), "partial_json")? {
                    let raw = encode::scratch(owner, |e| {
                        for fragment in body.children(fragments)? {
                            let mut chars = body.chars(fragment)?;
                            while let Some(c) = chars.next_scalar()? {
                                e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
                            }
                        }
                        Ok(())
                    })?;
                    let input = body.import_json(owner, raw)?;
                    body.rename(input, "input")?;
                    body.append(body.root(), input)?;
                }
            }
            Kind::BlockEnd => {
                let index = self.find(&doc, x)?;
                self.start_block(owner, index, output)?;
                let block = &mut self.blocks.rows[index];
                if !block.open {
                    return Err(Error::Schema);
                }
                block.open = false;
                if self.mode == BodyMode::Streaming {
                    output(event(owner, "content_block_stop", |e| {
                        e.bytes(b",\"index\":")?;
                        e.unsigned(block.index)
                    })?)?;
                }
            }
            Kind::Usage => {
                let usage = doc.required(root, "usage")?;
                self.usage = Some(super::mapped(owner, |e| doc.write(usage, e))?);
            }
            Kind::CountTokens => {
                if self.operation != Operation::CountTokens || self.count.is_some() {
                    return Err(Error::Schema);
                }
                self.count = Some(doc);
            }
            Kind::Opaque => {
                let source = doc.member(x, "source")?;
                let native = source
                    .map(|s| super::is(&doc, s, "protocol", "anthropic"))
                    .transpose()?
                    .unwrap_or(false);
                if native && source.is_some_and(|s| super::is(&doc, s, "event", "content_block").unwrap_or(false)) {
                    let index = self.find(&doc, x)?;
                    self.blocks.rows[index].document =
                        super::mapped(owner, |e| doc.write(doc.required(root, "value")?, e))?;
                    self.start_block(owner, index, output)?;
                } else if native && self.mode == BodyMode::Streaming {
                    output(encode::scratch(owner, |e| write_opaque(&doc, x, e))?)?;
                } else {
                    self.drop_event(kind, choice, "opaque event has no native buffered carrier");
                }
            }
            Kind::Done => {
                if self.operation == Operation::CountTokens {
                    let count = self.count.as_ref().ok_or(Error::Schema)?;
                    output(encode::scratch(owner, |e| {
                        e.bytes(b"{\"input_tokens\":")?;
                        count.write(count.required(count.root(), "input_tokens")?, e)?;
                        original_members(count, count.required(count.root(), "x_ck")?, e)?;
                        e.bytes(b"}")
                    })?)?;
                } else {
                    if self.finish.is_none() || self.blocks.rows.iter().any(|b| b.open) {
                        return Err(Error::Truncated);
                    }
                    self.begin(owner, output)?;
                    if self.mode == BodyMode::Streaming {
                        let finish = self.finish.as_ref().ok_or(Error::Schema)?;
                        output(event(owner, "message_delta", |e| {
                            e.bytes(b",\"delta\":")?;
                            finish.write(finish.root(), e)?;
                            if let Some(usage) = &self.usage {
                                e.bytes(b",\"usage\":")?;
                                write_usage(usage, e)?;
                            }
                            Ok(())
                        })?)?;
                        output(event(owner, "message_stop", |_| Ok(()))?)?;
                    } else {
                        output(encode::scratch(owner, |e| self.buffered(e))?)?;
                    }
                }
                self.terminal = true;
            }
            Kind::Error | Kind::Cancelled => {
                if self.mode == BodyMode::Streaming {
                    output(event(owner, "error", |e| {
                        e.bytes(b",\"error\":{\"type\":\"api_error\",\"message\":\"Response failed\"}")
                    })?)?;
                } else {
                    output(encode::scratch(owner, |e| {
                        e.bytes(
                            b"{\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Response failed\"}}",
                        )
                    })?)?;
                }
                self.terminal = true;
            }
            _ => self.drop_event(kind, choice, "event has no Anthropic response carrier"),
        }
        Ok(())
    }
    fn find(&self, doc: &Document, x: usize) -> Result<usize> {
        let item = local(doc, doc.required(x, "item_id")?)?;
        if let Some(index) = doc.member(x, "block_index")? {
            let index = doc.unsigned(index)?;
            self.blocks
                .rows
                .iter()
                .position(|b| b.item == item && b.canonical == index && b.open)
                .ok_or(Error::Schema)
        } else if let Some(tool) = doc.member(x, "tool_index")? {
            let tool = doc.unsigned(tool)?;
            self.blocks
                .rows
                .iter()
                .position(|b| b.item == item && b.tool == Some(tool) && b.open)
                .ok_or(Error::Schema)
        } else {
            Err(Error::Schema)
        }
    }
    fn find_or_tool(&mut self, owner: &mut Writer, doc: &Document, x: usize) -> Result<usize> {
        if doc.member(x, "block_index")?.is_some() {
            return self.find(doc, x);
        }
        let index = self.blocks.rows.len();
        self.blocks.push(Block {
            item: local(doc, doc.required(x, "item_id")?)?,
            canonical: u64::MAX - index as u64,
            index: index as u64,
            document: super::mapped(owner, |e| e.bytes(b"{\"type\":\"tool_use\"}"))?,
            open: true,
            started: false,
            tool: None,
        })?;
        Ok(index)
    }
    fn find_or_text(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        x: usize,
        native: &'static str,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<usize> {
        if doc.member(x, "block_index")?.is_some() {
            return self.find(doc, x);
        }
        let item = local(doc, doc.required(x, "item_id")?)?;
        if let Some(index) = self.blocks.rows.iter().position(|b| {
            b.item == item && b.open && super::is(&b.document, b.document.root(), "type", native).unwrap_or(false)
        }) {
            return Ok(index);
        }
        let index = self.blocks.rows.len();
        let mut body = super::mapped(owner, |e| e.bytes(b"{}"))?;
        body.set(body.root(), "type", Value::Text(native))?;
        body.set(body.root(), native, Value::Fragments)?;
        self.blocks.push(Block {
            item,
            canonical: u64::MAX - index as u64,
            index: index as u64,
            document: body,
            open: true,
            started: false,
            tool: None,
        })?;
        self.start_block(owner, index, output)?;
        Ok(index)
    }
    fn start_block(
        &mut self,
        owner: &mut Writer,
        index: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        self.begin(owner, output)?;
        let block = &mut self.blocks.rows[index];
        if block.started {
            return Ok(());
        }
        if self.mode == BodyMode::Streaming {
            output(event(owner, "content_block_start", |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"content_block\":")?;
                super::object(&block.document, block.document.root(), &["partial_json"], e)
            })?)?;
        }
        block.started = true;
        Ok(())
    }
    fn fragment(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        value: usize,
        index: usize,
        (field, delta): (&'static str, &'static str),
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        super::string(doc, value)?;
        self.start_block(owner, index, output)?;
        let block = &mut self.blocks.rows[index];
        if self.mode == BodyMode::Streaming {
            output(event(owner, "content_block_delta", |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"delta\":{\"type\":")?;
                e.string(delta)?;
                e.bytes(b",")?;
                e.string(field)?;
                e.bytes(b":")?;
                doc.write(value, e)?;
                e.bytes(b"}")
            })?)?;
        }
        // Tool JSON still needs validation at its end; other streamed payloads need no second state copy.
        if self.mode == BodyMode::Buffered || field == "partial_json" {
            let incoming = super::mapped(owner, |e| doc.write(value, e))?;
            let fragment = block.document.absorb(incoming)?;
            let root = block.document.root();
            let fragments = match block.document.member(root, field)? {
                Some(n) => n,
                None => block.document.set(root, field, Value::Fragments)?,
            };
            block.document.append(fragments, fragment)?;
        }
        Ok(())
    }
    fn citation(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        value: usize,
        index: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        if self.mode == BodyMode::Streaming {
            let block = &self.blocks.rows[index];
            output(event(owner, "content_block_delta", |e| {
                e.bytes(b",\"index\":")?;
                e.unsigned(block.index)?;
                e.bytes(b",\"delta\":{\"type\":\"citations_delta\",\"citation\":")?;
                doc.write(value, e)?;
                e.bytes(b"}")
            })?)?;
        } else {
            let incoming = super::mapped(owner, |e| doc.write(value, e))?;
            let body = &mut self.blocks.rows[index].document;
            let value = body.absorb(incoming)?;
            let array = match body.member(body.root(), "citations")? {
                Some(n) => n,
                None => body.set(body.root(), "citations", Value::Array)?,
            };
            body.append(array, value)?;
        }
        Ok(())
    }
    fn buffered(&self, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b"{")?;
        write_base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
        e.bytes(b",\"content\":[")?;
        for (index, block) in self.blocks.rows.iter().enumerate() {
            if index != 0 {
                e.bytes(b",")?;
            }
            super::object(&block.document, block.document.root(), &["partial_json"], e)?;
        }
        e.bytes(b"]")?;
        let finish = self.finish.as_ref().ok_or(Error::Schema)?;
        for field in finish.children(finish.root())? {
            e.bytes(b",")?;
            finish.write_name(finish.node(field)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            finish.write(field, e)?;
        }
        if let Some(usage) = &self.usage {
            e.bytes(b",\"usage\":")?;
            write_usage(usage, e)?;
        }
        e.bytes(b"}")
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
        let result = self.accept(owner, input, &mut output);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn finish(&mut self) -> Result<()> {
        if self.terminal && !self.failed {
            Ok(())
        } else {
            Err(Error::Truncated)
        }
    }
}
fn local(doc: &Document, node: usize) -> Result<LocalId> {
    let mut bytes = [0u8; 64];
    let mut len = 0;
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if !c.is_ascii() || len == bytes.len() {
            return Err(Error::Schema);
        }
        bytes[len] = c as u8;
        len += 1;
    }
    LocalId::new(std::str::from_utf8(&bytes[..len]).map_err(|_| Error::Schema)?)
}
fn copy_value(doc: &mut Document, parent: usize, key: &'static str, from: usize) -> Result<()> {
    let target = doc.set(parent, key, Value::Null)?;
    doc.transplant(target, from)
}
fn event(
    owner: &mut Writer,
    name: &'static str,
    fields: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
) -> Result<ScratchLease> {
    encode::scratch(owner, |e| {
        e.bytes(b"event: ")?;
        e.bytes(name.as_bytes())?;
        e.bytes(b"\ndata: {\"type\":")?;
        e.string(name)?;
        fields(e)?;
        e.bytes(b"}\n\n")
    })
}
fn write_base(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"type\":\"message\",\"role\":\"assistant\"")?;
    for key in ["id", "model"] {
        super::member(doc, doc.root(), key, e)?;
    }
    if let Some(x) = doc.member(doc.root(), "x_ck")? {
        original_members(doc, x, e)?;
    }
    Ok(())
}
fn write_usage(doc: &Document, e: &mut Encoder<'_, '_>) -> Result<()> {
    let root = doc.root();
    if let Some(native) = doc.member(root, "native")?
        && super::is(doc, native, "protocol", "anthropic")?
    {
        return doc.write(doc.required(native, "value")?, e);
    }
    e.bytes(b"{")?;
    let mut first = true;
    for (from, to) in [
        ("input_uncached_tokens", "input_tokens"),
        ("cache_read_tokens", "cache_read_input_tokens"),
        ("cache_write_tokens", "cache_creation_input_tokens"),
        ("output_tokens", "output_tokens"),
    ] {
        if let Some(value) = doc.member(root, from)? {
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            e.string(to)?;
            e.bytes(b":")?;
            doc.write(value, e)?;
        }
    }
    e.bytes(b"}")
}
fn original_members(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    if let Some(original) = doc.member(x, "original")? {
        for entry in doc.children(original)? {
            if !super::is(doc, entry, "protocol", "anthropic")? {
                continue;
            }
            let pointer = doc.required(entry, "pointer")?;
            let mut chars = doc.chars(pointer)?;
            if chars.next_scalar()? != Some('/') {
                return Err(Error::Schema);
            }
            e.bytes(b",\"")?;
            while let Some(c) = chars.next_scalar()? {
                let c = if c == '~' {
                    match chars.next_scalar()? {
                        Some('0') => '~',
                        Some('1') => '/',
                        _ => return Err(Error::Schema),
                    }
                } else {
                    c
                };
                e.escaped(c.encode_utf8(&mut [0; 4]))?;
            }
            e.bytes(b"\":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
    }
    Ok(())
}
fn write_opaque(doc: &Document, x: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let source = doc.required(x, "source")?;
    if let Some(name) = doc.member(source, "event")? {
        e.bytes(b"event: ")?;
        raw_string(doc, name, e, false)?;
        e.bytes(b"\n")?;
    }
    if let Some(fields) = doc.member(source, "sse")? {
        for field in doc.children(fields)? {
            raw_string(doc, doc.required(field, "name")?, e, false)?;
            e.bytes(b": ")?;
            raw_string(doc, doc.required(field, "value")?, e, false)?;
            e.bytes(b"\n")?;
        }
    }
    e.bytes(b"data: ")?;
    let value = doc.required(doc.root(), "value")?;
    if super::is(doc, doc.root(), "format", "text")? {
        raw_string(doc, value, e, true)?;
    } else {
        doc.write(value, e)?;
    }
    e.bytes(b"\n\n")
}
fn raw_string(doc: &Document, node: usize, e: &mut Encoder<'_, '_>, multiline: bool) -> Result<()> {
    let mut chars = doc.chars(node)?;
    while let Some(c) = chars.next_scalar()? {
        if c == '\n' && multiline {
            e.bytes(b"\ndata: ")?;
        } else if matches!(c, '\n' | '\r') {
            return Err(Error::Schema);
        } else {
            e.bytes(c.encode_utf8(&mut [0; 4]).as_bytes())?;
        }
    }
    Ok(())
}

fn metadata_changed(base: &Document, incoming: &Document) -> Result<bool> {
    for key in ["id", "model", "created"] {
        let Some(next) = incoming.member(incoming.root(), key)? else {
            continue;
        };
        let Some(current) = base.member(base.root(), key)? else {
            return Ok(true);
        };
        if key == "created" {
            if base.unsigned(current)? != incoming.unsigned(next)? {
                return Ok(true);
            }
        } else {
            let mut a = base.chars(current)?;
            let mut b = incoming.chars(next)?;
            loop {
                let c = a.next_scalar()?;
                if c != b.next_scalar()? {
                    return Ok(true);
                }
                if c.is_none() {
                    break;
                }
            }
        }
    }
    Ok(false)
}
