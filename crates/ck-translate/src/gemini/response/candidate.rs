//! One native response body: identity, counts, prompt feedback, candidates and extras.

use super::{Choice, State};
use crate::codec::{
    Error, Operation, Result,
    document::Document,
    event::{Kind, Scope},
};
use ck_stream::Writer;

/// Candidate members with their own canonical record; the rest travel as `gemini.candidate`.
const CANDIDATE_KNOWN: &[&str] = &[
    "index",
    "content",
    "finishReason",
    "groundingMetadata",
    "citationMetadata",
    "safetyRatings",
    "urlContextMetadata",
    "logprobsResult",
];
/// A finished candidate's `finishMessage` travels on its finish chunk.
const FINISHED_KNOWN: &[&str] = &[
    "index",
    "content",
    "finishReason",
    "finishMessage",
    "groundingMetadata",
    "citationMetadata",
    "safetyRatings",
    "urlContextMetadata",
    "logprobsResult",
];
/// Response members with their own canonical record; the rest travel as `gemini.response`.
const RESPONSE_KNOWN: &[&str] = &[
    "candidates",
    "usageMetadata",
    "promptFeedback",
    "responseId",
    "modelVersion",
    "createTime",
];

impl State {
    pub(super) fn body(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        super::super::object(doc, root)?;
        if doc.member(root, "error")?.is_some() {
            return Err(Error::Schema);
        }
        if self.base.is_none() {
            self.start(owner, doc)?;
        }
        if self.operation == Operation::CountTokens {
            return self.count(owner, doc);
        }
        if let Some(feedback) = doc.member(root, "promptFeedback")? {
            if let Some(block) = doc.member(feedback, "blockReason")?
                && !doc.string_is(block, "BLOCK_REASON_UNSPECIFIED")?
            {
                self.blocked = true;
            }
            self.named(
                owner,
                doc,
                feedback,
                Kind::Annotation,
                Scope::default(),
                "promptFeedback",
            )?;
        }
        if let Some(candidates) = doc.member(root, "candidates")? {
            super::super::array(doc, candidates)?;
            for (ordinal, candidate) in doc.children(candidates)?.enumerate() {
                self.candidate(owner, doc, candidate, ordinal as u64)?;
            }
        }
        if let Some(usage) = doc.member(root, "usageMetadata")? {
            self.usage(owner, doc, usage)?;
        }
        self.extras(owner, doc, root, None, RESPONSE_KNOWN)
    }
    fn start(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let base = super::super::mapped(owner, |e| {
            e.bytes(b"{\"id\":")?;
            if let Some(n) = doc.member(root, "responseId")? {
                super::super::string(doc, n)?;
                doc.write(n, e)?;
            } else {
                e.string(self.identity.stream_id.as_str())?;
            }
            e.bytes(b",\"model\":")?;
            if let Some(n) = doc.member(root, "modelVersion")? {
                super::super::string(doc, n)?;
                doc.write(n, e)?;
            } else {
                e.string("")?;
            }
            e.bytes(b",\"created\":0}")
        })?;
        self.emitter.emit(
            owner,
            Kind::ResponseStart,
            Scope::default(),
            |e| super::super::members(&base, base.root(), &[], e),
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"ids\":{")?;
                if let Some(n) = doc.member(root, "responseId")? {
                    e.bytes(b"\"responseId\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"}")?;
                if let Some(n) = doc.member(root, "createTime")? {
                    e.bytes(b",\"timestamp\":")?;
                    doc.write(n, e)?;
                }
                e.bytes(b"},\"synthetic\":[\"created\"]")
            },
        )?;
        self.base = Some(base);
        Ok(())
    }
    fn count(&mut self, owner: &mut Writer, doc: &Document) -> Result<()> {
        let root = doc.root();
        let total = doc.required(root, "totalTokens")?;
        doc.unsigned(total)?;
        self.emitter.emit(
            owner,
            Kind::CountTokens,
            Scope::default(),
            |e| {
                e.bytes(b",\"total_tokens\":")?;
                doc.write(total, e)?;
                e.bytes(b",\"details\":")?;
                doc.write(root, e)
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
        )?;
        Ok(())
    }
    fn candidate(&mut self, owner: &mut Writer, doc: &Document, candidate: usize, ordinal: u64) -> Result<()> {
        let index = match doc.member(candidate, "index")? {
            Some(n) => doc.unsigned(n)?,
            None => ordinal,
        };
        let slot = match self.choices.iter().position(|c| c.index == index) {
            Some(n) => n,
            None => self.choices.push(Choice {
                index,
                finished: false,
                next_part: 0,
                next_tool: 0,
            })?,
        };
        if self.choices[slot].finished {
            return Err(Error::Schema);
        }
        if let Some(content) = doc.member(candidate, "content")? {
            if let Some(role) = doc.member(content, "role")?
                && !doc.string_is(role, "model")?
            {
                return Err(Error::Schema);
            }
            if let Some(parts) = doc.member(content, "parts")? {
                super::super::array(doc, parts)?;
                for part in doc.children(parts)? {
                    self.part(owner, doc, part, slot)?;
                }
            }
        }
        let scope = Scope {
            choice_index: Some(index),
            ..Scope::default()
        };
        for (field, kind) in [
            ("groundingMetadata", Kind::Grounding),
            ("citationMetadata", Kind::Annotation),
            ("safetyRatings", Kind::Annotation),
            ("urlContextMetadata", Kind::UrlContext),
        ] {
            if let Some(n) = doc.member(candidate, field)? {
                self.named(owner, doc, n, kind, scope, field)?;
            }
        }
        if let Some(logprobs) = doc.member(candidate, "logprobsResult")? {
            self.chunk(
                owner,
                scope,
                |e| e.bytes(b"{}"),
                None,
                Some((doc, logprobs)),
                |_| Ok(()),
            )?;
        }
        let finish = doc.member(candidate, "finishReason")?;
        let known = if finish.is_some() {
            FINISHED_KNOWN
        } else {
            CANDIDATE_KNOWN
        };
        self.extras(owner, doc, candidate, Some(index), known)?;
        if let Some(finish) = finish {
            self.finish(owner, doc, candidate, finish, slot)?;
        }
        Ok(())
    }
    /// Publishes the finish chunk: the Chat reason, the exact native reason and the native
    /// `finishMessage`, which belongs to the finish rather than to the candidate's extras.
    fn finish(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        candidate: usize,
        finish: usize,
        slot: usize,
    ) -> Result<()> {
        super::super::string(doc, finish)?;
        let reason = reason(doc, finish, self.choices[slot].next_tool != 0)?;
        let message = doc.member(candidate, "finishMessage")?;
        let scope = Scope {
            choice_index: Some(self.choices[slot].index),
            ..Scope::default()
        };
        self.chunk(
            owner,
            scope,
            |e| e.bytes(b"{}"),
            Some(reason),
            None,
            |e| {
                e.bytes(b",\"native_finish_reason\":")?;
                doc.write(finish, e)?;
                if let Some(message) = message {
                    e.bytes(b",\"original\":[{\"protocol\":\"gemini\",\"pointer\":\"/finishMessage\",\"value\":")?;
                    doc.write(message, e)?;
                    e.bytes(b"}]")?;
                }
                Ok(())
            },
        )?;
        self.choices[slot].finished = true;
        Ok(())
    }
    /// Publishes the members of a response or candidate (`choice`) that have no canonical
    /// record, when there are any, as one opaque record.
    fn extras(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        parent: usize,
        choice: Option<u64>,
        known: &[&str],
    ) -> Result<()> {
        let mut extra = false;
        for node in doc.children(parent)? {
            let name = doc.node(node)?.name.ok_or(Error::Schema)?;
            extra |= !known
                .iter()
                .try_fold(false, |found, key| Ok::<_, Error>(found || doc.name_is(name, key)?))?;
        }
        if !extra {
            return Ok(());
        }
        let scope = Scope {
            choice_index: choice,
            ..Scope::default()
        };
        self.emitter.emit(
            owner,
            Kind::Opaque,
            scope,
            |e| {
                e.bytes(b",\"format\":\"json\",\"name\":")?;
                match choice {
                    Some(index) => {
                        e.bytes(b"\"gemini.candidate\",\"value\":{\"index\":")?;
                        e.unsigned(index)?;
                    }
                    None => e.bytes(b"\"gemini.response\",\"value\":{\"candidates\":[]")?,
                }
                super::super::members(doc, parent, known, e)?;
                e.bytes(b"}")
            },
            |e| e.bytes(b",\"source\":{\"protocol\":\"gemini\"}"),
        )?;
        Ok(())
    }
    pub(super) fn named(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        node: usize,
        kind: Kind,
        scope: Scope,
        name: &'static str,
    ) -> Result<()> {
        self.emitter.emit(
            owner,
            kind,
            scope,
            |e| {
                if kind == Kind::Annotation {
                    e.bytes(b",\"kind\":")?;
                    e.string(name)?;
                }
                e.bytes(b",\"value\":")?;
                doc.write(node, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"gemini\",\"event\":")?;
                e.string(name)?;
                e.bytes(b"}")
            },
        )?;
        Ok(())
    }
}

/// The Chat finish reason for a native one. Chat has no value for the other native reasons
/// (`OTHER`, `MALFORMED_FUNCTION_CALL`, `FINISH_REASON_UNSPECIFIED`, future ones), so they
/// finish as `stop`; the exact native reason rides along as `native_finish_reason`.
fn reason(doc: &Document, finish: usize, called: bool) -> Result<&'static str> {
    const FILTERED: &[&str] = &[
        "SAFETY",
        "RECITATION",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
        "IMAGE_RECITATION",
    ];
    Ok(if doc.string_is(finish, "MAX_TOKENS")? {
        "length"
    } else if FILTERED.iter().try_fold(false, |found, name| {
        Ok::<_, Error>(found || doc.string_is(finish, name)?)
    })? {
        "content_filter"
    } else if called && doc.string_is(finish, "STOP")? {
        "tool_calls"
    } else {
        "stop"
    })
}
