//! Document mutations: node edits, adopted sources and provenance bindings.

use super::{Document, Name, Node, Text, Value};
use crate::codec::{Error, Result, source::Span};

/// Where an adopted record's texts now live.
#[derive(Clone, Copy)]
pub(crate) struct Adopted {
    source: usize,
    offset: u64,
}

impl Adopted {
    /// Maps a text of the adopted record, which has a single source.
    pub(crate) fn text(self, text: Text) -> Result<Text> {
        if text.source != 0 {
            return Err(Error::Position);
        }
        Ok(Text {
            source: self.source,
            span: Span {
                start: self.offset + text.span.start,
                end: self.offset + text.span.end,
            },
        })
    }
}

impl Document {
    pub(crate) fn bind_field(&mut self, node: usize, mut event: Document) -> Result<()> {
        let pointer = event.required(event.root(), "pointer")?;
        let value = event.required(event.root(), "value")?;
        let Value::String(pointer) = event.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let mut value = event.node(value)?.value;
        let adopted = self.adopt_record(&mut event)?;
        let pointer = adopted.text(pointer)?;
        if let Value::String(text) | Value::Number(text) = &mut value {
            *text = adopted.text(*text)?;
        }
        if self.nodes[node].name.is_some() {
            self.nodes[node].name = Some(Name::Pointer(pointer));
        }
        if !matches!(value, Value::Object | Value::Array) {
            self.nodes[node].value = value;
        }
        Ok(())
    }

    pub(crate) fn retain_text(&mut self, mut event: Document, text: Text) -> Result<Text> {
        self.adopt_record(&mut event)?.text(text)
    }

    /// Moves a decoded record's lease into this document's records source. The record's nodes
    /// stay behind; [`Adopted::text`] maps their texts.
    pub(crate) fn adopt_record(&mut self, event: &mut Document) -> Result<Adopted> {
        if self.stream_id() != event.stream_id() || event.sources.len() != 1 {
            return Err(Error::Position);
        }
        let source = event.sources.pop().ok_or(Error::Position)?;
        if let Some(records) = self.records {
            let offset = self.sources[records].append(source)?;
            return Ok(Adopted {
                source: records,
                offset,
            });
        }
        self.reserve(0, 1, 0)?;
        self.records = Some(self.sources.len());
        self.sources.push(source);
        Ok(Adopted {
            source: self.sources.len() - 1,
            offset: 0,
        })
    }

    /// Copies the subtree at `node` of an adopted record under `name`, mapping its texts. A
    /// record is decoded JSON, so it holds no [`Value::Id`] whose index would need mapping.
    pub(crate) fn copy_adopted(
        &mut self,
        event: &Document,
        node: usize,
        adopted: Adopted,
        name: Option<Name>,
    ) -> Result<usize> {
        let source = event.node(node)?;
        let value = match source.value {
            Value::String(text) => Value::String(adopted.text(text)?),
            Value::Number(text) => Value::Number(adopted.text(text)?),
            value => value,
        };
        let target = self.add(name, value)?;
        let mut child = source.first;
        while let Some(index) = child {
            let node = event.node(index)?;
            child = node.next;
            let name = match node.name {
                Some(Name::Mapped(text)) => Some(Name::Mapped(adopted.text(text)?)),
                Some(Name::Pointer(text)) => Some(Name::Pointer(adopted.text(text)?)),
                name => name,
            };
            let copy = self.copy_adopted(event, index, adopted, name)?;
            self.append(target, copy)?;
        }
        Ok(target)
    }

    pub(crate) fn retire_scratch(&mut self) -> Result<()> {
        for source in &mut self.sources {
            source.retire_scratch()?;
        }
        Ok(())
    }

    /// Bind trusted provenance to a stable node, independently of its array position.
    /// JSON imports never create these associations; absorb remaps their node indexes.
    pub fn bind_origin(&mut self, entry: usize, parent: usize, carrier: Option<&'static str>) -> Result<()> {
        self.node(parent)?;
        let old = self.node(entry)?.origin;
        if old == Some((parent, carrier)) {
            return Ok(());
        }
        if let Some((old_parent, _)) = old {
            let mut at = self.nodes[old_parent].origins;
            let mut previous: Option<usize> = None;
            while let Some(index) = at {
                let next = self.nodes[index].origin_next;
                if index == entry {
                    if let Some(previous) = previous {
                        self.nodes[previous].origin_next = next;
                    } else {
                        self.nodes[old_parent].origins = next;
                    }
                    if self.nodes[old_parent].last_origin == Some(entry) {
                        self.nodes[old_parent].last_origin = previous;
                    }
                    break;
                }
                previous = Some(index);
                at = next;
            }
        }
        self.nodes[entry].origin = Some((parent, carrier));
        self.nodes[entry].origin_next = None;
        if let Some(last) = self.nodes[parent].last_origin {
            self.nodes[last].origin_next = Some(entry);
        } else {
            self.nodes[parent].origins = Some(entry);
        }
        self.nodes[parent].last_origin = Some(entry);
        Ok(())
    }

    pub(crate) fn clone_tree(&mut self, node: usize) -> Result<usize> {
        let source = self.node(node)?;
        let target = self.add(source.name, source.value)?;
        let mut child = source.first;
        while let Some(index) = child {
            child = self.node(index)?.next;
            let copy = self.clone_tree(index)?;
            self.append(target, copy)?;
        }
        Ok(target)
    }

    pub fn add(&mut self, name: Option<Name>, value: Value) -> Result<usize> {
        self.reserve(1, 0, 0)?;
        let index = self.nodes.len();
        self.nodes.push(Node {
            name,
            value,
            first: None,
            next: None,
            last: None,
            parent: None,
            revision: 0,
            origin: None,
            origin_next: None,
            origins: None,
            last_origin: None,
            published: false,
            published_through: 0,
            input_key: match name {
                Some(Name::Mapped(Text { source: 0, span })) => Some(span),
                _ => None,
            },
        });
        Ok(index)
    }
    pub fn append(&mut self, parent: usize, child: usize) -> Result<()> {
        if !matches!(
            self.node(parent)?.value,
            Value::Object | Value::Array | Value::Fragments | Value::Base64Fragments | Value::Utf8Media
        ) || self.node(child)?.next.is_some()
            || self.node(child)?.parent.is_some()
        {
            return Err(Error::Schema);
        }
        if let Some(last) = self.nodes[parent].last {
            self.nodes[last].next = Some(child);
        } else {
            self.nodes[parent].first = Some(child);
        }
        self.nodes[parent].last = Some(child);
        self.nodes[child].parent = Some(parent);
        self.nodes[child].revision = self.nodes[child].revision.checked_add(1).ok_or(Error::Limit)?;
        Ok(())
    }
    pub fn detach(&mut self, parent: usize, child: usize) -> Result<()> {
        self.check_live()?;
        let mut at = self.node(parent)?.first;
        let mut previous: Option<usize> = None;
        while let Some(i) = at {
            if i == child {
                let next = self.nodes[i].next;
                if let Some(p) = previous {
                    self.nodes[p].next = next;
                } else {
                    self.nodes[parent].first = next;
                }
                self.nodes[i].next = None;
                if self.nodes[parent].last == Some(i) {
                    self.nodes[parent].last = previous;
                }
                self.nodes[i].parent = None;
                self.nodes[i].revision = self.nodes[i].revision.checked_add(1).ok_or(Error::Limit)?;
                if self.nodes[parent].published_through as usize == i + 1 {
                    self.nodes[parent].published_through = previous.map_or(0, |p| u32::try_from(p + 1).unwrap_or(0));
                }
                return Ok(());
            }
            previous = Some(i);
            at = self.nodes[i].next;
        }
        Err(Error::Position)
    }
    pub fn remove(&mut self, parent: usize, name: &str) -> Result<Option<usize>> {
        let child = self.member(parent, name)?;
        if let Some(child) = child {
            self.detach(parent, child)?;
        }
        Ok(child)
    }
    pub fn rename(&mut self, index: usize, name: &'static str) -> Result<()> {
        self.check_live()?;
        let node = self.nodes.get_mut(index).ok_or(Error::Position)?;
        node.name = Some(Name::Static(name));
        node.revision = node.revision.checked_add(1).ok_or(Error::Limit)?;
        Ok(())
    }
    pub fn replace(&mut self, index: usize, value: Value) -> Result<()> {
        self.check_live()?;
        let n = self.nodes.get_mut(index).ok_or(Error::Position)?;
        n.value = value;
        n.first = None;
        n.last = None;
        n.revision = n.revision.checked_add(1).ok_or(Error::Limit)?;
        Ok(())
    }
    pub fn set(&mut self, parent: usize, name: &'static str, value: Value) -> Result<usize> {
        if let Some(i) = self.member(parent, name)? {
            self.replace(i, value)?;
            Ok(i)
        } else {
            let i = self.add(Some(Name::Static(name)), value)?;
            self.append(parent, i)?;
            Ok(i)
        }
    }
    pub fn transplant(&mut self, target: usize, from: usize) -> Result<()> {
        let value = self.node(from)?;
        let node = self.nodes.get_mut(target).ok_or(Error::Position)?;
        node.value = value.value;
        node.first = value.first;
        node.last = value.last;
        node.revision = node.revision.checked_add(1).ok_or(Error::Limit)?;
        let mut child = value.first;
        while let Some(i) = child {
            self.nodes[i].parent = Some(target);
            self.nodes[i].revision = self.nodes[i].revision.checked_add(1).ok_or(Error::Limit)?;
            child = self.nodes[i].next;
        }
        Ok(())
    }
}
