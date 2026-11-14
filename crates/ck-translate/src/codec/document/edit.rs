//! Document mutations: node edits, adopted sources and provenance bindings.

use super::{Document, Name, Node, Text, Value};
use crate::codec::{Error, Result};

impl Document {
    pub(crate) fn bind_field(&mut self, node: usize, mut event: Document) -> Result<()> {
        let pointer = event.required(event.root(), "pointer")?;
        let value = event.required(event.root(), "value")?;
        let Value::String(mut pointer) = event.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let mut value = event.node(value)?.value;
        let base = self.adopt_sources(&mut event)?;
        pointer.source += base;
        if let Value::String(text) | Value::Number(text) = &mut value {
            text.source += base;
        }
        if self.nodes[node].name.is_some() {
            self.nodes[node].name = Some(Name::Pointer(pointer));
        }
        if !matches!(value, Value::Object | Value::Array) {
            self.nodes[node].value = value;
        }
        Ok(())
    }

    pub(crate) fn retain_text(&mut self, mut event: Document, mut text: Text) -> Result<Text> {
        text.source += self.adopt_sources(&mut event)?;
        Ok(text)
    }

    fn adopt_sources(&mut self, event: &mut Document) -> Result<usize> {
        if self.stream_id() != event.stream_id() {
            return Err(Error::Position);
        }
        self.reserve(0, event.sources.len())?;
        let base = self.sources.len();
        self.sources.append(&mut event.sources);
        Ok(base)
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
        self.reserve(1, 0)?;
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
