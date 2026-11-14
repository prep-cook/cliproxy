use super::*;

impl State {
    /// Opens, extends or closes one content or summary part of item `at`; `snapshot` parts are
    /// full values whose already published prefix is not repeated.
    pub(super) fn block(
        &mut self,
        owner: &mut Writer,
        at: usize,
        (index, summary): (u64, bool),
        doc: &Document,
        part: usize,
        (snapshot, close): (bool, bool),
    ) -> Result<()> {
        let item = &mut self.items[at];
        let existing = item
            .blocks
            .iter()
            .position(|b| b.index == index && b.summary == summary);
        if item.closed && existing.is_none() {
            return Err(Error::Schema);
        }
        let block = match existing {
            Some(block) => block,
            None => self.open_block(owner, at, (index, summary), doc, part)?,
        };
        if !self.items[at].blocks[block].opaque {
            self.block_text(owner, at, block, doc, part, snapshot)?;
            self.block_logprobs(owner, at, block, doc, part, snapshot)?;
            self.block_annotations(owner, at, block, doc, part, snapshot)?;
        }
        let item = &mut self.items[at];
        if close && !item.blocks[block].closed {
            // Attributes the closing snapshot adds after the block started travel as originals.
            let late = late_attributes(owner, &item.doc, &item.blocks[block], doc, part)?;
            self.emitter.emit(
                owner,
                Kind::BlockEnd,
                item.scope(Some(block as u64)),
                |_| Ok(()),
                |e| crate::boundary::original(&late, "responses", &[(late.root(), "", &[])], e),
            )?;
            item.blocks[block].closed = true;
        }
        Ok(())
    }

    fn open_block(
        &mut self,
        owner: &mut Writer,
        at: usize,
        (index, summary): (u64, bool),
        doc: &Document,
        part: usize,
    ) -> Result<usize> {
        let item = &mut self.items[at];
        let typed = doc.member(part, "type")?.is_some();
        let kind = if summary {
            if !typed || is(doc, part, "type", "summary_text")? {
                "reasoning_summary"
            } else {
                "opaque"
            }
        } else if is(doc, part, "type", "reasoning_text")? {
            "thinking"
        } else if is(doc, part, "type", "refusal")? {
            "refusal"
        } else if !typed || is(doc, part, "type", "output_text")? {
            "text"
        } else {
            "opaque"
        };
        let opaque = kind == "opaque";
        // Empty `annotations` and `logprobs` stay attributes, so their presence survives; content
        // events append to them.
        let mut content = ["text", "refusal", "", ""];
        for (slot, name) in content[2..].iter_mut().zip(["annotations", "logprobs"]) {
            if let Some(array) = doc.member(part, name)?
                && doc.node(array)?.first.is_some()
            {
                *slot = name;
            }
        }
        let lease = encode::scratch(owner, |e| object(doc, part, if opaque { &[] } else { &content }, e))?;
        let value = item.doc.import_json(owner, lease)?;
        let scope = item.scope(Some(item.blocks.len() as u64));
        self.emitter.emit(
            owner,
            Kind::BlockStart,
            scope,
            |e| {
                e.bytes(b",\"kind\":")?;
                e.string(kind)?;
                e.bytes(b",\"attributes\":")?;
                item.doc.write(value, e)
            },
            |e| {
                e.bytes(b",\"source\":{\"protocol\":\"responses\"")?;
                e.bytes(if summary {
                    b",\"summary_index\":"
                } else {
                    b",\"content_index\":"
                })?;
                e.unsigned(index)?;
                e.bytes(b"}")
            },
        )?;
        item.blocks.push(Block {
            index,
            summary,
            value,
            closed: false,
            opaque,
        })
    }

    fn block_text(
        &mut self,
        owner: &mut Writer,
        at: usize,
        block: usize,
        doc: &Document,
        part: usize,
        snapshot: bool,
    ) -> Result<()> {
        let item = &mut self.items[at];
        let summary = item.blocks[block].summary;
        let value = item.blocks[block].value;
        for (native, delta) in [
            (
                "text",
                if summary || is(doc, part, "type", "reasoning_text")? {
                    "reasoning_content"
                } else {
                    "content"
                },
            ),
            ("refusal", "refusal"),
        ] {
            if let Some(incoming) = doc.member(part, native)? {
                string(doc, incoming)?;
                let old = item.doc.member(value, native)?;
                let fragment = if snapshot {
                    suffix(owner, &item.doc, old, doc, incoming)?
                } else {
                    super::super::mapped(owner, |e| doc.write(incoming, e))?
                };
                let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
                if (item.blocks[block].closed || item.closed) && !empty {
                    return Err(Error::Schema);
                }
                if !empty || old.is_none() {
                    let base = self.base.as_ref().ok_or(Error::Schema)?;
                    self.emitter.emit(
                        owner,
                        Kind::Chunk,
                        item.scope(Some(block as u64)),
                        |e| {
                            chunk_base(base, e)?;
                            e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{")?;
                            e.string(delta)?;
                            e.bytes(b":")?;
                            fragment.write(fragment.root(), e)?;
                            e.bytes(b"},\"finish_reason\":null")?;
                            e.bytes(b"}]")
                        },
                        |e| e.bytes(b",\"source\":{\"protocol\":\"responses\"}"),
                    )?;
                    let imported = item.doc.absorb(fragment)?;
                    append_fragment(&mut item.doc, value, native, imported)?;
                }
            }
        }
        Ok(())
    }

    fn block_logprobs(
        &mut self,
        owner: &mut Writer,
        at: usize,
        block: usize,
        doc: &Document,
        part: usize,
        snapshot: bool,
    ) -> Result<()> {
        let item = &mut self.items[at];
        let value = item.blocks[block].value;
        if let Some(logprobs) = doc.member(part, "logprobs")? {
            let tail = super::super::snapshot::array_tail(
                owner,
                &item.doc,
                item.doc.member(value, "logprobs")?,
                doc,
                logprobs,
                snapshot,
            )?;
            if tail.node(tail.root())?.first.is_some() {
                if item.blocks[block].closed || item.closed {
                    return Err(Error::Schema);
                }
                let base = self.base.as_ref().ok_or(Error::Schema)?;
                self.emitter.emit(
                    owner,
                    Kind::Chunk,
                    item.scope(Some(block as u64)),
                    |e| {
                        chunk_base(base, e)?;
                        e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":null")?;
                        super::super::snapshot::write_logprobs(&tail, e)?;
                        e.bytes(b"}]")
                    },
                    |_| Ok(()),
                )?;
                super::super::snapshot::append_array(&mut item.doc, value, "logprobs", tail)?;
            }
        }
        Ok(())
    }

    fn block_annotations(
        &mut self,
        owner: &mut Writer,
        at: usize,
        block: usize,
        doc: &Document,
        part: usize,
        snapshot: bool,
    ) -> Result<()> {
        let item = &mut self.items[at];
        let value = item.blocks[block].value;
        if let Some(annotations) = doc.member(part, "annotations")? {
            let tail = super::super::snapshot::array_tail(
                owner,
                &item.doc,
                item.doc.member(value, "annotations")?,
                doc,
                annotations,
                snapshot,
            )?;
            for annotation in tail.children(tail.root())? {
                if item.blocks[block].closed || item.closed {
                    return Err(Error::Schema);
                }
                self.emitter.emit(
                    owner,
                    Kind::Annotation,
                    item.scope(Some(block as u64)),
                    |e| {
                        e.bytes(b",\"kind\":\"citation\",\"value\":")?;
                        tail.write(annotation, e)
                    },
                    |_| Ok(()),
                )?;
            }
            super::super::snapshot::append_array(&mut item.doc, value, "annotations", tail)?;
        }
        Ok(())
    }

    pub(super) fn arguments(
        &mut self,
        owner: &mut Writer,
        at: usize,
        doc: &Document,
        node: usize,
        snapshot: bool,
    ) -> Result<()> {
        let item = &mut self.items[at];
        if item.tool.is_none() {
            return Err(Error::Schema);
        }
        string(doc, node)?;
        let fragment = if snapshot {
            suffix(owner, &item.doc, item.arguments, doc, node)?
        } else {
            super::super::mapped(owner, |e| doc.write(node, e))?
        };
        let empty = fragment.chars(fragment.root())?.next_scalar()?.is_none();
        if (item.tool_closed || item.arguments_done) && !empty {
            return Err(Error::Schema);
        }
        if !empty || item.arguments.is_none() {
            let base = self.base.as_ref().ok_or(Error::Schema)?;
            self.emitter.emit(
                owner,
                Kind::Chunk,
                item.scope(None),
                |e| {
                    chunk_base(base, e)?;
                    e.bytes(b",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":")?;
                    e.unsigned(item.index)?;
                    e.bytes(b",\"function\":{\"arguments\":")?;
                    fragment.write(fragment.root(), e)?;
                    e.bytes(b"}}]},\"finish_reason\":null}]")
                },
                |_| Ok(()),
            )?;
            let imported = item.doc.absorb(fragment)?;
            let root = item.doc.root();
            let field = append_fragment(&mut item.doc, root, "arguments", imported)?;
            item.arguments = Some(field);
        }
        if snapshot && !item.tool_closed {
            let valid = match validate_arguments(owner, item) {
                Ok(()) => true,
                Err(Error::Json | Error::Truncated) if item.incomplete => false,
                Err(error) => return Err(error),
            };
            item.arguments_done = true;
            if item.server.is_some() {
                return Ok(());
            }
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                item.scope(None),
                |e| {
                    e.bytes(if item.incomplete {
                        b",\"status\":\"incomplete\""
                    } else {
                        b",\"status\":\"completed\""
                    })?;
                    if item.custom()? {
                        return Ok(());
                    }
                    e.bytes(b",\"valid_json\":")?;
                    e.bytes(if valid { b"true" } else { b"false" })
                },
                |_| Ok(()),
            )?;
            item.tool_closed = true;
        }
        Ok(())
    }
}

/// The members of snapshot `part` that the published block value lacks or holds with another
/// value. A mapped block's text, refusal, annotations and logprobs travel as content instead;
/// an opaque block maps none of its members.
fn late_attributes(
    owner: &mut Writer,
    stored: &Document,
    block: &Block,
    doc: &Document,
    part: usize,
) -> Result<Document> {
    let value = block.value;
    super::super::mapped(owner, |e| {
        e.bytes(b"{")?;
        let mut first = true;
        for field in doc.children(part)? {
            let name = doc.node(field)?.name.ok_or(Error::Schema)?;
            let mut known = false;
            for content in ["text", "refusal", "annotations", "logprobs"] {
                known |= !block.opaque && doc.name_is(name, content)?;
            }
            if known {
                continue;
            }
            if let Some(started) = stored_member(stored, value, doc, name)?
                && super::super::snapshot::equal(stored, started, doc, field)?
            {
                continue;
            }
            if !first {
                e.bytes(b",")?;
            }
            first = false;
            doc.write_name(name, e)?;
            e.bytes(b":")?;
            doc.write(field, e)?;
        }
        e.bytes(b"}")
    })
}

/// The member of object `parent` of `stored` named `name` of `doc`.
fn stored_member(stored: &Document, parent: usize, doc: &Document, name: Name) -> Result<Option<usize>> {
    for child in stored.children(parent)? {
        let mut a = stored.name_chars(stored.node(child)?.name.ok_or(Error::Schema)?)?;
        let mut b = doc.name_chars(name)?;
        loop {
            let (x, y) = (a.next_scalar()?, b.next_scalar()?);
            if x != y {
                break;
            }
            if x.is_none() {
                return Ok(Some(child));
            }
        }
    }
    Ok(None)
}
