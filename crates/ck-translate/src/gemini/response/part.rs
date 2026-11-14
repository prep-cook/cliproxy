//! One native part: its own canonical item and block, holding text, a call, a result, media,
//! code execution or a signature.

use super::State;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    event::{Kind, Scope},
    media::{self, Encoding, Media},
};
use ck_stream::Writer;

/// Part members with their own canonical record; the rest stay as block provenance.
const PART_KNOWN: &[&str] = &[
    "text",
    "thoughtSignature",
    "functionCall",
    "functionResponse",
    "inlineData",
    "executableCode",
    "codeExecutionResult",
];

impl State {
    pub(super) fn part(&mut self, owner: &mut Writer, doc: &Document, part: usize, slot: usize) -> Result<()> {
        super::super::object(doc, part)?;
        let choice = &mut self.choices[slot];
        let block = choice.next_part;
        choice.next_part += 1;
        let item = self.next_item;
        self.next_item += 1;
        let scope = Scope {
            choice_index: Some(choice.index),
            item_id: Some(media::local_id(b'i', item)?),
            block_index: Some(block),
            ..Scope::default()
        };
        let reasoning = match doc.member(part, "thought")? {
            Some(n) => match doc.node(n)?.value {
                Value::Bool(b) => b,
                _ => return Err(Error::Schema),
            },
            None => false,
        };
        let kind = if doc.member(part, "text")?.is_some() {
            if reasoning { "thinking" } else { "text" }
        } else if doc.member(part, "functionCall")?.is_some() {
            "function"
        } else if doc.member(part, "inlineData")?.is_some() {
            "media"
        } else {
            "opaque"
        };
        self.emitter.emit(
            owner,
            Kind::ItemStart,
            scope,
            |e| e.bytes(b",\"kind\":\"message\",\"role\":\"assistant\""),
            |_| Ok(()),
        )?;
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(kind)
            },
            |e| super::super::provenance(doc, part, PART_KNOWN, e),
        )?;
        self.payload(owner, doc, part, scope, (slot, item), reasoning)?;
        if let Some(signature) = doc.member(part, "thoughtSignature")? {
            super::super::string(doc, signature)?;
            self.emitter.emit(
                owner,
                Kind::ReasoningSignature,
                scope,
                |e| {
                    e.bytes(b",\"kind\":\"gemini\",\"final\":true,\"fragment\":")?;
                    doc.write(signature, e)
                },
                |_| Ok(()),
            )?;
        }
        self.emitter
            .emit(owner, Kind::BlockEnd, scope, |_| Ok(()), |_| Ok(()))?;
        self.emitter.emit(
            owner,
            Kind::ItemEnd,
            scope,
            |e| e.bytes(b",\"status\":\"completed\""),
            |_| Ok(()),
        )?;
        Ok(())
    }
    /// Publishes the part's text, call, result, media and code-execution payloads in order.
    /// `place` is the choice slot and the part's item number.
    fn payload(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        part: usize,
        scope: Scope,
        (slot, item): (usize, u64),
        reasoning: bool,
    ) -> Result<()> {
        if let Some(text) = doc.member(part, "text")? {
            super::super::string(doc, text)?;
            let key: &[u8] = if reasoning {
                b"{\"reasoning_content\":"
            } else {
                b"{\"content\":"
            };
            self.chunk(
                owner,
                scope,
                |e| {
                    e.bytes(key)?;
                    doc.write(text, e)?;
                    e.bytes(b"}")
                },
                None,
                None,
                |_| Ok(()),
            )?;
        }
        if let Some(call) = doc.member(part, "functionCall")? {
            self.call(owner, doc, call, scope, slot, item)?;
        }
        if let Some(result) = doc.member(part, "functionResponse")? {
            self.emitter.emit(
                owner,
                Kind::ToolResult,
                scope,
                |e| {
                    e.bytes(b",\"value\":")?;
                    doc.write(result, e)
                },
                |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
            )?;
        }
        if let Some(inline) = doc.member(part, "inlineData")? {
            self.inline(owner, doc, inline, scope, item)?;
        }
        for (field, phase) in [("executableCode", "code"), ("codeExecutionResult", "result")] {
            if let Some(value) = doc.member(part, field)? {
                self.emitter.emit(
                    owner,
                    Kind::CodeExecution,
                    scope,
                    |e| {
                        e.bytes(b",\"phase\":")?;
                        e.string(phase)?;
                        e.bytes(b",\"value\":")?;
                        doc.write(value, e)
                    },
                    |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
                )?;
            }
        }
        Ok(())
    }
    /// Inline media: its decoded bytes travel as data records under one media lifetime.
    fn inline(&mut self, owner: &mut Writer, doc: &Document, inline: usize, scope: Scope, item: u64) -> Result<()> {
        let data = doc.required(inline, "data")?;
        let Value::String(text) = doc.node(data)?.value else {
            return Err(Error::Schema);
        };
        let mime = doc.member(inline, "mimeType")?;
        let media = Media {
            node: data,
            id: media::local_id(b'm', item)?,
            data: Some(text),
            encoding: Encoding::Base64,
            kind: mime
                .map(|m| super::super::request_content::modality(doc, m))
                .transpose()?
                .unwrap_or("document"),
            mime,
        };
        media::publish(doc, media, &mut self.emitter, owner, scope)?;
        Ok(())
    }
    /// A whole function call: start, one arguments delta, end.
    fn call(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        call: usize,
        mut scope: Scope,
        slot: usize,
        item: u64,
    ) -> Result<()> {
        let name = doc.required(call, "name")?;
        super::super::string(doc, name)?;
        let choice = &mut self.choices[slot];
        let tool = choice.next_tool;
        choice.next_tool += 1;
        scope.tool_index = Some(tool);
        scope.tool_call_id = Some(media::local_id(b't', item)?);
        self.emitter.emit(
            owner,
            Kind::ToolStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":\"function\",\"arguments_format\":\"json\",\"name\":")?;
                doc.write(name, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"ids\":{")?;
                if let Some(id) = doc.member(call, "id")? {
                    e.bytes(b"\"id\":")?;
                    doc.write(id, e)?;
                }
                e.bytes(b"}}")
            },
        )?;
        if let Some(args) = doc.member(call, "args")? {
            super::super::object(doc, args)?;
            self.chunk(
                owner,
                scope,
                |e| {
                    e.bytes(b"{\"tool_calls\":[{\"index\":")?;
                    e.unsigned(tool)?;
                    e.bytes(b",\"function\":{\"arguments\":")?;
                    e.quoted_json(|e| doc.write(args, e))?;
                    e.bytes(b"}}]}")
                },
                None,
                None,
                |_| Ok(()),
            )?;
        }
        self.emitter.emit(
            owner,
            Kind::ToolEnd,
            scope,
            |e| e.bytes(b",\"status\":\"completed\",\"valid_json\":true"),
            |_| Ok(()),
        )?;
        Ok(())
    }
}
