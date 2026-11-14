mod edit;
mod iter;
#[path = "document_pending.rs"]
pub(crate) mod pending;
#[cfg(test)]
mod tests;

pub use iter::{Characters, Children, EncodedCharacters, FragmentCharacters};

use ck_stream::{MetadataPermit, Writer};

use super::{
    Error, Result,
    encode::Encoder,
    event::LocalId,
    json,
    source::{Source, Span},
    table::grown,
};

#[derive(Clone, Copy, Debug)]
pub struct Text {
    pub source: usize,
    pub span: Span,
}

#[derive(Clone, Copy, Debug)]
pub enum Name {
    Mapped(Text),
    Pointer(Text),
    Static(&'static str),
}

#[derive(Clone, Copy, Debug)]
pub enum Value {
    Object,
    Array,
    Fragments,
    Base64Fragments,
    Utf8Media,
    String(Text),
    Number(Text),
    Bool(bool),
    Null,
    Text(&'static str),
    Unsigned(u64),
    Id(IdIndex),
}

/// A gateway id held in its document's id table, so a [`Value`] stays at the size of a
/// [`Text`] rather than of an inline [`LocalId`]. It is meaningful only in that document.
#[derive(Clone, Copy, Debug)]
pub struct IdIndex(u32);

#[derive(Clone, Copy, Debug)]
pub struct Node {
    pub name: Option<Name>,
    pub value: Value,
    pub first: Option<usize>,
    pub next: Option<usize>,
    pub parent: Option<usize>,
    pub revision: u64,
    last: Option<usize>,
    origin: Option<(usize, Option<&'static str>)>,
    origin_next: Option<usize>,
    origins: Option<usize>,
    last_origin: Option<usize>,
    published: bool,
    /// For an array: one more than the index of a published child whose preceding siblings
    /// are all published, or 0. A u32 keeps the node size unchanged; a larger index is not
    /// recorded and publication rescans from the first child.
    published_through: u32,
    input_key: Option<Span>,
}

/// Mutable normalized topology; payloads remain under mapped leases.
/// Borrowing this document for a capability token prevents subsequent mutation.
pub struct Document {
    sources: Vec<Source>,
    nodes: Vec<Node>,
    /// The ids [`Value::Id`] nodes name; charged to `metadata` with the node and source tables.
    ids: Vec<LocalId>,
    metadata: MetadataPermit,
    /// Charges transient indexes that read-only validation builds; see [`Self::with_table`].
    scratch: std::sync::Mutex<Option<MetadataPermit>>,
    /// The source that holds every adopted record's lease, so a record costs one segment.
    records: Option<usize>,
    root: usize,
}

impl Document {
    pub(crate) fn id(&self) -> u64 {
        self.sources[0].id()
    }
    pub fn parse(owner: &mut Writer, source: Source, max_bytes: u64) -> Result<Self> {
        source.check_owner(owner)?;
        let mut scanner = json::Scanner::new(owner, max_bytes)?;
        scanner.finish(&source)?;
        Self::from_scanned(owner, source, scanner)
    }

    pub fn from_scanned(owner: &mut Writer, source: Source, scanner: json::Scanner) -> Result<Self> {
        source.check_owner(owner)?;
        let mut doc = Self {
            sources: Vec::new(),
            nodes: Vec::new(),
            ids: Vec::new(),
            metadata: owner.reserve_metadata(0)?,
            scratch: std::sync::Mutex::new(Some(owner.reserve_metadata(0)?)),
            records: None,
            root: 0,
        };
        doc.import(source, &scanner)?;
        Ok(doc)
    }

    pub(crate) fn streaming(owner: &mut Writer) -> Result<Self> {
        let mut doc = Self {
            sources: Vec::new(),
            nodes: Vec::new(),
            ids: Vec::new(),
            metadata: owner.reserve_metadata(0)?,
            scratch: std::sync::Mutex::new(Some(owner.reserve_metadata(0)?)),
            records: None,
            root: 0,
        };
        doc.reserve(1, 1, 0)?;
        doc.sources.push(Source::new(owner)?);
        doc.add(None, Value::Object)?;
        Ok(doc)
    }

    /// Lends `f` an empty table charged to this document's writer quota; the charge ends when
    /// `f` returns. Tables do not nest.
    pub(crate) fn with_table<T, R>(&self, f: impl FnOnce(&mut super::table::Table<T>) -> Result<R>) -> Result<R> {
        let permit = self
            .scratch
            .lock()
            .map_err(|_| Error::Closed)?
            .take()
            .ok_or(Error::Closed)?;
        let mut table = super::table::Table::with_permit(permit);
        let result = f(&mut table);
        let permit = table.into_permit()?;
        *self.scratch.lock().map_err(|_| Error::Closed)? = Some(permit);
        result
    }

    #[cfg(test)]
    pub(crate) fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub(crate) fn input(&self) -> &Source {
        &self.sources[0]
    }

    pub(crate) fn push_input(&mut self, owner: &mut Writer, bytes: &[u8]) -> Result<()> {
        self.sources[0].push_bytes(owner, bytes)?;
        Ok(())
    }

    /// Import only a completed subtree; the input lease remains owned by this document.
    pub(crate) fn import_completed(&mut self, scanner: &json::Scanner, index: usize) -> Result<usize> {
        let node = scanner.completed_node(self.input(), index)?.ok_or(Error::Truncated)?;
        let text = Text {
            source: 0,
            span: node.span,
        };
        let value = match node.kind {
            json::Kind::Object => Value::Object,
            json::Kind::Array => Value::Array,
            json::Kind::String => Value::String(text),
            json::Kind::Number => Value::Number(text),
            json::Kind::Bool => Value::Bool(self.input().equals(node.span, b"true")?),
            json::Kind::Null => Value::Null,
        };
        let target = self.add(node.key.map(|span| Name::Mapped(Text { source: 0, span })), value)?;
        let mut child = node.first;
        while let Some(index) = child {
            child = scanner.pending_node(self.input(), index)?.next;
            let imported = self.import_completed(scanner, index)?;
            self.append(target, imported)?;
        }
        Ok(target)
    }

    /// Makes room for `nodes`, `sources` and `ids` more entries. The permit is charged for the
    /// new capacity before the tables grow, then for the capacity actually allocated.
    fn reserve(&mut self, nodes: usize, sources: usize, ids: usize) -> Result<()> {
        let n = grown(self.nodes.len(), nodes, self.nodes.capacity(), 4)?;
        let s = grown(self.sources.len(), sources, self.sources.capacity(), 4)?;
        let i = grown(self.ids.len(), ids, self.ids.capacity(), 4)?;
        self.metadata.resize(Self::table_bytes(n, s, i).ok_or(Error::Limit)?)?;
        if n > self.nodes.capacity() {
            self.nodes
                .try_reserve_exact(n - self.nodes.len())
                .map_err(|_| Error::Limit)?;
        }
        if s > self.sources.capacity() {
            self.sources
                .try_reserve_exact(s - self.sources.len())
                .map_err(|_| Error::Limit)?;
        }
        if i > self.ids.capacity() {
            self.ids
                .try_reserve_exact(i - self.ids.len())
                .map_err(|_| Error::Limit)?;
        }
        self.metadata.resize(
            Self::table_bytes(self.nodes.capacity(), self.sources.capacity(), self.ids.capacity())
                .ok_or(Error::Limit)?,
        )?;
        Ok(())
    }

    fn table_bytes(nodes: usize, sources: usize, ids: usize) -> Option<usize> {
        nodes
            .checked_mul(std::mem::size_of::<Node>())?
            .checked_add(sources.checked_mul(std::mem::size_of::<Source>())?)?
            .checked_add(ids.checked_mul(std::mem::size_of::<LocalId>())?)
    }

    /// Sets member `name` of `parent` to the gateway id `id`, kept in this document's id table.
    pub fn set_id(&mut self, parent: usize, name: &'static str, id: LocalId) -> Result<usize> {
        let index = IdIndex(u32::try_from(self.ids.len()).map_err(|_| Error::Limit)?);
        self.reserve(0, 0, 1)?;
        self.ids.push(id);
        self.set(parent, name, Value::Id(index))
    }

    pub fn local_id(&self, index: IdIndex) -> Result<&LocalId> {
        self.ids.get(index.0 as usize).ok_or(Error::Position)
    }

    fn import(&mut self, source: Source, scanner: &json::Scanner) -> Result<usize> {
        scanner.validate_source(&source)?;
        if source.stream_id() != self.stream_id() {
            return Err(Error::Position);
        }
        self.reserve(scanner.nodes().len(), 1, 0)?;
        let part = self.sources.len();
        let base = self.nodes.len();
        for n in scanner.nodes() {
            let text = Text {
                source: part,
                span: n.span,
            };
            let value = match n.kind {
                json::Kind::Object => Value::Object,
                json::Kind::Array => Value::Array,
                json::Kind::String => Value::String(text),
                json::Kind::Number => Value::Number(text),
                json::Kind::Bool => Value::Bool(source.equals(n.span, b"true")?),
                json::Kind::Null => Value::Null,
            };
            self.nodes.push(Node {
                name: n.key.map(|span| Name::Mapped(Text { source: part, span })),
                value,
                first: n.first.map(|i| i + base),
                next: n.next.map(|i| i + base),
                parent: None,
                revision: 0,
                last: None,
                origin: None,
                origin_next: None,
                origins: None,
                last_origin: None,
                published: false,
                published_through: 0,
                input_key: if part == 0 { n.key } else { None },
            });
        }
        for parent in base..self.nodes.len() {
            let mut child = self.nodes[parent].first;
            while let Some(index) = child {
                self.nodes[index].parent = Some(parent);
                self.nodes[parent].last = Some(index);
                child = self.nodes[index].next;
            }
        }
        self.sources.push(source);
        Ok(base)
    }

    pub fn import_json(&mut self, owner: &mut Writer, lease: ck_stream::ScratchLease) -> Result<usize> {
        self.check_owner(owner)?;
        let mut source = Source::new(owner)?;
        source.push_scratch(lease)?;
        let mut scanner = json::Scanner::new(owner, source.len())?;
        scanner.finish(&source)?;
        self.import(source, &scanner)
    }

    pub fn absorb(&mut self, mut other: Document) -> Result<usize> {
        self.check_live()?;
        other.check_live()?;
        if self.stream_id() != other.stream_id() {
            return Err(Error::Position);
        }
        self.reserve(other.nodes.len(), other.sources.len(), other.ids.len())?;
        let base = self.nodes.len();
        let source_base = self.sources.len();
        // Every remapped id index stays below this sum, so a u32 holds it.
        u32::try_from(self.ids.len() + other.ids.len()).map_err(|_| Error::Limit)?;
        let id_base = self.ids.len() as u32;
        for mut node in other.nodes.drain(..) {
            if let Some(Name::Mapped(text) | Name::Pointer(text)) = &mut node.name {
                text.source += source_base;
            }
            match &mut node.value {
                Value::String(text) | Value::Number(text) => text.source += source_base,
                Value::Id(IdIndex(id)) => *id += id_base,
                _ => {}
            }
            node.first = node.first.map(|i| i + base);
            node.next = node.next.map(|i| i + base);
            node.parent = node.parent.map(|i| i + base);
            node.last = node.last.map(|i| i + base);
            node.origin = node.origin.map(|(parent, carrier)| (parent + base, carrier));
            node.origin_next = node.origin_next.map(|i| i + base);
            node.origins = node.origins.map(|i| i + base);
            node.last_origin = node.last_origin.map(|i| i + base);
            node.published_through = match node.published_through {
                0 => 0,
                i => u32::try_from(i as usize + base).unwrap_or(0),
            };
            self.nodes.push(node);
        }
        self.sources.append(&mut other.sources);
        self.ids.append(&mut other.ids);
        Ok(base + other.root)
    }

    pub fn names_equal(&self, a: Name, b: Name) -> Result<bool> {
        let mut a = self.name_chars(a)?;
        let mut b = self.name_chars(b)?;
        loop {
            let c = a.next_scalar()?;
            if c != b.next_scalar()? {
                return Ok(false);
            }
            if c.is_none() {
                return Ok(true);
            }
        }
    }

    pub fn stream_id(&self) -> u64 {
        self.metadata.stream_id()
    }
    pub fn check_live(&self) -> Result<()> {
        self.metadata.check_live()?;
        Ok(())
    }
    pub fn check_owner(&self, owner: &Writer) -> Result<()> {
        self.check_live()?;
        if owner.id() != self.stream_id() {
            return Err(Error::Position);
        }
        Ok(())
    }
    pub fn root(&self) -> usize {
        self.root
    }
    pub fn node(&self, index: usize) -> Result<Node> {
        self.check_live()?;
        self.nodes.get(index).copied().ok_or(Error::Position)
    }
    pub(crate) fn published(&self, index: usize) -> Result<bool> {
        Ok(self.node(index)?.published)
    }
    pub(crate) fn mark_published(&mut self, index: usize) {
        self.nodes[index].published = true;
    }
    /// The first child of array `parent` that may be unpublished: the child after the last one
    /// recorded by [`Self::mark_published_through`] while it is still attached and published.
    pub(crate) fn unpublished_from(&self, parent: usize) -> Result<Option<usize>> {
        let node = self.node(parent)?;
        Ok(match (node.published_through as usize).checked_sub(1) {
            Some(child) if self.node(child)?.parent == Some(parent) && self.published(child)? => self.node(child)?.next,
            _ => node.first,
        })
    }
    /// Records that `child` and every sibling before it are published.
    pub(crate) fn mark_published_through(&mut self, parent: usize, child: usize) {
        self.nodes[parent].published_through = u32::try_from(child + 1).unwrap_or(0);
    }

    pub(crate) fn release_input_before(&mut self, offset: u64) -> Result<()> {
        self.sources[0].release_before(offset)
    }
    pub(crate) fn release_input_range(&mut self, span: Span) -> Result<()> {
        self.sources[0].release_range(span)
    }

    pub(crate) fn rebind_keys(&mut self, scanner: &mut json::Scanner) -> Result<()> {
        for index in 0..self.nodes.len() {
            let node = self.nodes[index];
            if let (Some(span), Some(name)) = (node.input_key, node.name) {
                if !scanner.needs_key(span) {
                    self.nodes[index].input_key = None;
                    continue;
                }
                if matches!(name, Name::Mapped(Text { source: 0, .. })) {
                    continue;
                }
                if self.names_equal(Name::Mapped(Text { source: 0, span }), name)? {
                    scanner.bind_key(span, name);
                    self.nodes[index].input_key = None;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn raw_floor(&self, index: usize, mut floor: u64) -> Result<u64> {
        let node = self.node(index)?;
        if let Some(Name::Mapped(Text { source: 0, span })) = node.name {
            floor = floor.min(span.start);
        }
        if let Value::String(Text { source: 0, span }) | Value::Number(Text { source: 0, span }) = node.value {
            floor = floor.min(span.start);
        }
        for child in self.children(index)? {
            floor = self.raw_floor(child, floor)?;
        }
        Ok(floor)
    }
    pub fn source(&self, text: Text) -> Result<&Source> {
        self.sources.get(text.source).ok_or(Error::Position)
    }
    pub fn name_is(&self, name: Name, value: &str) -> Result<bool> {
        let mut chars = self.name_chars(name)?;
        for expected in value.chars() {
            if chars.next_scalar()? != Some(expected) {
                return Ok(false);
            }
        }
        Ok(chars.next_scalar()?.is_none())
    }
    pub fn name_chars(&self, name: Name) -> Result<Characters<'_>> {
        match name {
            Name::Static(s) => Ok(Characters::Static(s.chars())),
            Name::Mapped(t) => Ok(Characters::Mapped(json::Decoded::new(self.source(t)?, t.span)?)),
            Name::Pointer(t) => {
                let mut chars = json::Decoded::new(self.source(t)?, t.span)?;
                let mut skip = 0;
                let mut length = 0;
                while let Some(c) = chars.next_scalar()? {
                    length += 1;
                    if c == '/' {
                        skip = length;
                    }
                }
                let mut chars = json::Decoded::new(self.source(t)?, t.span)?;
                for _ in 0..skip {
                    chars.next_scalar()?;
                }
                Ok(Characters::Pointer(chars))
            }
        }
    }

    pub fn member(&self, parent: usize, name: &str) -> Result<Option<usize>> {
        if !matches!(self.node(parent)?.value, Value::Object) {
            return Err(Error::Schema);
        }
        for child in self.children(parent)? {
            if self.name_is(self.node(child)?.name.ok_or(Error::Schema)?, name)? {
                return Ok(Some(child));
            }
        }
        Ok(None)
    }
    pub fn required(&self, parent: usize, name: &str) -> Result<usize> {
        self.member(parent, name)?.ok_or(Error::Schema)
    }
    pub fn string_is(&self, index: usize, value: &str) -> Result<bool> {
        let mut chars = self.chars(index)?;
        for expected in value.chars() {
            if chars.next_scalar()? != Some(expected) {
                return Ok(false);
            }
        }
        Ok(chars.next_scalar()?.is_none())
    }
    pub fn equal_strings(&self, left: usize, right: usize) -> Result<bool> {
        let mut left = self.chars(left)?;
        let mut right = self.chars(right)?;
        loop {
            let a = left.next_scalar()?;
            if a != right.next_scalar()? {
                return Ok(false);
            }
            if a.is_none() {
                return Ok(true);
            }
        }
    }
    pub fn unsigned(&self, index: usize) -> Result<u64> {
        match self.node(index)?.value {
            Value::Unsigned(n) => Ok(n),
            Value::Number(t) => {
                let mut cursor = self.source(t)?.cursor(t.span)?;
                let mut value = 0u64;
                let mut any = false;
                while let Some(b) = cursor.next_byte()? {
                    if !b.is_ascii_digit() {
                        return Err(Error::Schema);
                    }
                    value = value
                        .checked_mul(10)
                        .and_then(|v| v.checked_add(u64::from(b - b'0')))
                        .ok_or(Error::Schema)?;
                    any = true;
                }
                if any { Ok(value) } else { Err(Error::Schema) }
            }
            _ => Err(Error::Schema),
        }
    }

    pub fn origin(&self, entry: usize) -> Result<Option<(usize, Option<&'static str>)>> {
        Ok(self.node(entry)?.origin)
    }

    pub fn write_name(&self, name: Name, e: &mut Encoder<'_, '_>) -> Result<()> {
        e.bytes(b"\"")?;
        let mut chars = self.name_chars(name)?;
        while let Some(c) = chars.next_scalar()? {
            e.escaped(c.encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")
    }
    pub fn write(&self, index: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
        let n = self.node(index)?;
        match n.value {
            Value::Object | Value::Array => {
                let object = matches!(n.value, Value::Object);
                e.bytes(if object { b"{" } else { b"[" })?;
                let mut first = true;
                for child in self.children(index)? {
                    if !first {
                        e.bytes(b",")?;
                    }
                    first = false;
                    if object {
                        self.write_name(self.node(child)?.name.ok_or(Error::Schema)?, e)?;
                        e.bytes(b":")?;
                    }
                    self.write(child, e)?;
                }
                e.bytes(if object { b"}" } else { b"]" })
            }
            Value::Fragments | Value::Base64Fragments | Value::Utf8Media => {
                e.bytes(b"\"")?;
                self.write_contents(index, e)?;
                e.bytes(b"\"")
            }
            Value::String(t) | Value::Number(t) => e.span(self.source(t)?, t.span),
            Value::Text(s) => e.string(s),
            Value::Unsigned(n) => e.unsigned(n),
            Value::Id(id) => e.string(self.local_id(id)?.as_str()),
            Value::Bool(b) => e.bytes(if b { b"true" } else { b"false" }),
            Value::Null => e.bytes(b"null"),
        }
    }
    pub fn write_contents(&self, index: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
        match self.node(index)?.value {
            Value::String(t) => e.string_fragment(self.source(t)?, t.span),
            Value::Text(s) => e.escaped(s),
            Value::Utf8Media => {
                let mut chars = self.chars(index)?;
                while let Some(c) = chars.next_scalar()? {
                    e.escaped(c.encode_utf8(&mut [0; 4]))?;
                }
                Ok(())
            }
            Value::Base64Fragments => super::media::write_base64_fragments(self, index, e),
            Value::Fragments => {
                for child in self.children(index)? {
                    self.write_contents(child, e)?;
                }
                Ok(())
            }
            _ => Err(Error::Schema),
        }
    }
}
