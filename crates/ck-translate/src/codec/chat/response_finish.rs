use super::*;

impl State {
    pub(super) fn finish_choice(
        &mut self,
        owner: &mut Writer,
        doc: &Document,
        finish: usize,
        position: usize,
    ) -> Result<()> {
        if !self.choices[position].audio.complete() {
            return Err(Error::Truncated);
        }
        let scope = self.scope(position);
        let incomplete = doc.string_is(finish, "length")? || doc.string_is(finish, "content_filter")?;
        // Validate every tool before publishing any successful close for this choice.
        for tool in self.choices[position].tools.iter_mut() {
            if let (Some(parser), Some(arguments)) = (&mut tool.parser, &tool.arguments)
                && let Err(error) = parser.finish(arguments)
                && !incomplete
            {
                return Err(error);
            }
        }
        if let Some(audio) = &mut self.choices[position].audio_output {
            audio.finish(&mut self.emitter, owner)?;
        }
        self.close_tools(owner, position, scope, incomplete)?;
        for (block, opened) in self.choices[position].blocks.iter_mut().enumerate() {
            if *opened {
                let mut block_scope = scope;
                block_scope.block_index = Some(block as u64);
                self.emitter
                    .emit(owner, Kind::BlockEnd, block_scope, |_| Ok(()), |_| Ok(()))?;
                *opened = false;
            }
        }
        self.emitter.emit(
            owner,
            Kind::ItemEnd,
            scope,
            |e| {
                e.bytes(if incomplete {
                    b",\"status\":\"incomplete\""
                } else {
                    b",\"status\":\"completed\""
                })
            },
            |_| Ok(()),
        )?;
        self.emitter.emit(
            owner,
            Kind::Chunk,
            scope,
            |e| {
                base(self.base.as_ref().ok_or(Error::Schema)?, e)?;
                e.bytes(b",\"choices\":[{\"index\":")?;
                e.unsigned(scope.choice_index.ok_or(Error::Schema)?)?;
                e.bytes(b",\"delta\":{},\"finish_reason\":")?;
                doc.write(finish, e)?;
                e.bytes(b"}]")
            },
            |e| {
                e.bytes(b",\"native_finish_reason\":")?;
                doc.write(finish, e)
            },
        )?;
        self.choices[position].finished = true;
        Ok(())
    }
    /// Publishes ToolEnd for every call of a finishing choice and drops its argument parsers.
    fn close_tools(&mut self, owner: &mut Writer, position: usize, scope: Scope, incomplete: bool) -> Result<()> {
        for tool in self.choices[position].tools.iter_mut() {
            let mut tool_scope = scope;
            tool_scope.tool_index = Some(tool.index);
            tool_scope.tool_call_id = Some(tool.id);
            let valid = tool.parser.as_ref().map(|parser| parser.is_finished());
            self.emitter.emit(
                owner,
                Kind::ToolEnd,
                tool_scope,
                |e| {
                    e.bytes(if incomplete {
                        b",\"status\":\"incomplete\""
                    } else {
                        b",\"status\":\"completed\""
                    })?;
                    if let Some(valid) = valid {
                        e.bytes(b",\"valid_json\":")?;
                        e.bytes(if valid { b"true" } else { b"false" })?;
                    }
                    Ok(())
                },
                |_| Ok(()),
            )?;
            tool.arguments = None;
            tool.parser = None;
        }
        Ok(())
    }
    pub(super) fn emit_usage(&mut self, owner: &mut Writer, final_snapshot: bool) -> Result<()> {
        for choice in self.choices.iter_mut() {
            let scope = Scope {
                choice_index: Some(choice.index),
                item_id: Some(choice.item),
                ..Scope::default()
            };
            choice.usage.emit(owner, &mut self.emitter, scope, final_snapshot)?;
        }
        self.usage
            .emit(owner, &mut self.emitter, Scope::default(), final_snapshot)?;
        Ok(())
    }
    /// Publishes Done; `record` is the terminal SSE record whose other fields Done keeps.
    pub(super) fn done(&mut self, owner: &mut Writer, record: Option<(&Source, &sse::Event)>) -> Result<()> {
        if self.terminal || self.choices.is_empty() || self.choices.iter().any(|choice| !choice.finished) {
            return Err(Error::Truncated);
        }
        self.emit_usage(owner, true)?;
        let record = match record {
            Some((source, event)) if sse_metadata(source, event)? => Some((source, event)),
            _ => None,
        };
        self.emitter.emit(
            owner,
            Kind::Done,
            Scope::default(),
            |_| Ok(()),
            |e| match record {
                Some((source, event)) => sse_provenance(source, event, e),
                None => Ok(()),
            },
        )?;
        self.terminal = true;
        Ok(())
    }
}
