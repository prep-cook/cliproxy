use super::*;

impl Ingress {
    pub(super) fn consume_tree(
        &mut self,
        owner: &mut Writer,
        wire_index: usize,
        parent: usize,
        path: &mut [Option<Component>; 128],
        depth: usize,
        publish: bool,
    ) -> Result<bool> {
        if depth == path.len() {
            return Err(Error::Limit);
        }
        while self.trees.len() <= wire_index {
            self.trees.push(None)?;
        }
        if self.trees[wire_index].is_some_and(|state| state.done) {
            return Ok(true);
        }
        let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
        let wire = scanner.pending_node(self.document.input(), wire_index)?;
        let mut media_carrier = false;
        if let Some(name) = self.document.node(parent)?.name {
            for key in ["input_audio", "image_url", "file"] {
                media_carrier |= self.document.name_is(name, key)?;
            }
        }
        if wire.kind == json::Kind::String
            && (self.string.as_ref().is_some_and(|s| s.wire == wire_index)
                || (wire.span.end == 0 && (wire.validated_end - wire.span.start > 1024 || media_carrier)))
        {
            let (node, done) = self.stream_string(owner, wire_index, parent, &path[..depth], publish)?;
            if let Some(node) = node {
                self.trees[wire_index] = Some(PendingTree {
                    node,
                    last: None,
                    index: 0,
                    done,
                });
            }
            return Ok(done);
        }
        if self.trees[wire_index].is_none()
            && let Some(done) = self.open_tree(owner, wire_index, wire, parent, (path, depth), publish)?
        {
            return Ok(done);
        }
        self.consume_children(owner, wire_index, wire, path, depth, publish)
    }

    /// Imports a completed scalar, returning `Some(done)`, or adds an empty container to fill.
    fn open_tree(
        &mut self,
        owner: &mut Writer,
        wire_index: usize,
        wire: json::Node,
        parent: usize,
        (path, depth): (&mut [Option<Component>; 128], usize),
        publish: bool,
    ) -> Result<Option<bool>> {
        if !matches!(wire.kind, json::Kind::Object | json::Kind::Array) {
            if wire.span.end == 0 {
                return Ok(Some(false));
            }
            let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
            let node = self.document.import_completed(scanner, wire_index)?;
            self.document.append(parent, node)?;
            if publish {
                fields(
                    &mut self.document,
                    &self.media,
                    node,
                    (path, depth),
                    &mut self.emitter,
                    owner,
                    &mut self.reader,
                )?;
            }
            self.trees[wire_index] = Some(PendingTree {
                node,
                last: None,
                index: 0,
                done: true,
            });
            return Ok(Some(true));
        }
        let name = wire
            .key
            .map(|span| Name::Mapped(codec::document::Text { source: 0, span }));
        let node = self.document.add(
            name,
            if wire.kind == json::Kind::Object {
                Value::Object
            } else {
                Value::Array
            },
        )?;
        self.document.append(parent, node)?;
        if publish {
            field(
                &mut self.document,
                &self.media,
                node,
                &path[..depth],
                &mut self.emitter,
                owner,
                &mut self.reader,
            )?;
        }
        self.trees[wire_index] = Some(PendingTree {
            node,
            last: None,
            index: 0,
            done: false,
        });
        Ok(None)
    }

    fn consume_children(
        &mut self,
        owner: &mut Writer,
        wire_index: usize,
        wire: json::Node,
        path: &mut [Option<Component>; 128],
        depth: usize,
        publish: bool,
    ) -> Result<bool> {
        loop {
            let state = self.trees[wire_index].ok_or(Error::Schema)?;
            let scanner = self.scanner.as_ref().ok_or(Error::Closed)?;
            let next = match state.last {
                Some(last) => scanner.pending_node(self.document.input(), last)?.next,
                None => wire.first,
            };
            let Some(child) = next else { break };
            let value = scanner.pending_node(self.document.input(), child)?;
            path[depth] = Some(if wire.kind == json::Kind::Array {
                Component::Index(state.index)
            } else {
                Component::Name(if let Some(state) = self.trees.get(child).and_then(|s| *s) {
                    self.document.node(state.node)?.name.ok_or(Error::Schema)?
                } else {
                    Name::Mapped(codec::document::Text {
                        source: 0,
                        span: value.key.ok_or(Error::Schema)?,
                    })
                })
            });
            if !self.consume_tree(owner, child, state.node, path, depth + 1, publish)? {
                return Ok(false);
            }
            self.trees[wire_index].as_mut().ok_or(Error::Schema)?.last = Some(child);
            self.trees[wire_index].as_mut().ok_or(Error::Schema)?.index =
                state.index.checked_add(1).ok_or(Error::Limit)?;
        }
        self.trees[wire_index].as_mut().ok_or(Error::Schema)?.done = wire.span.end != 0;
        Ok(wire.span.end != 0)
    }
}
