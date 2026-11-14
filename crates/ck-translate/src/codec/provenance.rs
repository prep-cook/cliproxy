//! Protocol-neutral JSON pointers and trusted origin bindings.
//!
//! A protocol codec binds `x_ck` provenance entries to the normalized nodes they
//! describe. [`bindings`] serializes those links as `binding` pointers (plus an
//! optional native `carrier`) before canonical fields are published, and
//! [`restore_bindings`] re-binds them on a trusted reconstructed document.
//! Interpreting a provenance entry's own native `pointer` stays with the protocol
//! that wrote it.
use super::{
    Error, Result,
    document::{Characters, Document, Name, Value},
    encode::{self, Encoder},
};
use ck_stream::Writer;

/// `x_ck` lists whose entries may carry a trusted `binding`.
pub const BOUND_LISTS: [&str; 6] = [
    "original",
    "cache_control",
    "replay",
    "tools",
    "chat_legacy_calls",
    "anthropic_order",
];

/// Native carriers a binding may name. `shape` marks a native result object shape.
pub const CARRIERS: [&str; 4] = ["image_url", "input_audio", "file", "shape"];

/// Write a node's absolute JSON pointer without copying its path to the heap.
pub fn pointer(doc: &Document, mut node: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut path = [None; 128];
    let mut depth = 0;
    while let Some(parent) = doc.node(node)?.parent {
        *path.get_mut(depth).ok_or(Error::Limit)? = Some((parent, node));
        depth += 1;
        node = parent;
    }
    e.bytes(b"\"")?;
    for entry in path[..depth].iter().rev() {
        let (parent, node) = entry.ok_or(Error::Schema)?;
        e.bytes(b"/")?;
        if matches!(doc.node(parent)?.value, Value::Array) {
            // ponytail: walks the array per pointer, quadratic in elements with preserved
            // fields. The default metadata quota limits the element count; 0.14 ms is one sampled
            // shape, not a ceiling. Index positions if that quota grows (docs/DEFERRED-OPTIONS.md).
            let index = doc
                .children(parent)?
                .position(|child| child == node)
                .ok_or(Error::Schema)?;
            e.unsigned(index as u64)?;
        } else {
            let mut name = doc.name_chars(doc.node(node)?.name.ok_or(Error::Schema)?)?;
            while let Some(c) = name.next_scalar()? {
                match c {
                    '~' => e.bytes(b"~0")?,
                    '/' => e.bytes(b"~1")?,
                    c => e.escaped(c.encode_utf8(&mut [0; 4]))?,
                }
            }
        }
    }
    e.bytes(b"\"")
}

/// One escaped pointer segment, decoded lazily from document storage.
#[derive(Clone)]
pub struct Segment<'a> {
    chars: Characters<'a>,
    remaining: usize,
}
impl Segment<'_> {
    pub fn index(mut self) -> Result<usize> {
        let mut index = 0usize;
        let mut any = false;
        while let Some(c) = self.next()? {
            if !c.is_ascii_digit() {
                return Err(Error::Schema);
            }
            index = index
                .checked_mul(10)
                .and_then(|n| n.checked_add(c as usize - '0' as usize))
                .ok_or(Error::Limit)?;
            any = true;
        }
        if any { Ok(index) } else { Err(Error::Schema) }
    }
    pub(crate) fn next(&mut self) -> Result<Option<char>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        let c = self.chars.next_scalar()?.ok_or(Error::Schema)?;
        if c != '~' {
            return Ok(Some(c));
        }
        if self.remaining == 0 {
            return Err(Error::Schema);
        }
        self.remaining -= 1;
        match self.chars.next_scalar()? {
            Some('0') => Ok(Some('~')),
            Some('1') => Ok(Some('/')),
            _ => Err(Error::Schema),
        }
    }
    pub fn is(&self, expected: &str) -> Result<bool> {
        let mut chars = self.clone();
        for c in expected.chars() {
            if chars.next()? != Some(c) {
                return Ok(false);
            }
        }
        Ok(chars.next()?.is_none())
    }
    pub fn matches(&self, doc: &Document, name: Name) -> Result<bool> {
        let mut chars = self.clone();
        let mut name = doc.name_chars(name)?;
        loop {
            let c = chars.next()?;
            if c != name.next_scalar()? {
                return Ok(false);
            }
            if c.is_none() {
                return Ok(true);
            }
        }
    }
}

/// Split the next segment after a consumed `/`; the flag is true for the last one.
pub fn segment<'a>(chars: &mut Characters<'a>) -> Result<(Segment<'a>, bool)> {
    let start = chars.clone();
    let mut remaining = 0;
    let last = loop {
        match chars.next_scalar()? {
            Some('/') => break false,
            None => break true,
            Some(_) => remaining += 1,
        }
    };
    Ok((
        Segment {
            chars: start,
            remaining,
        },
        last,
    ))
}

/// The last segment of a non-empty pointer.
pub fn leaf(doc: &Document, pointer: usize) -> Result<Segment<'_>> {
    let mut chars = doc.chars(pointer)?;
    if chars.next_scalar()? != Some('/') {
        return Err(Error::Schema);
    }
    loop {
        let (name, last) = segment(&mut chars)?;
        if last {
            return Ok(name);
        }
    }
}

/// Write a segment as an unescaped JSON object key.
pub fn write_segment(mut name: Segment<'_>, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"")?;
    while let Some(c) = name.next()? {
        e.escaped(c.encode_utf8(&mut [0; 4]))?;
    }
    e.bytes(b"\"")
}

/// The node a pointer names; the empty pointer is the root.
pub fn resolve(doc: &Document, pointer: usize) -> Result<usize> {
    resolve_to(doc, pointer, false, &mut |_, _| Ok(None))
}

/// The container that holds the node a pointer names.
pub fn resolve_parent(doc: &Document, pointer: usize) -> Result<usize> {
    resolve_to(doc, pointer, true, &mut |_, _| Ok(None))
}

/// Like [`resolve_parent`]; `element(array, index)` may name an array element without a walk.
pub(crate) fn resolve_parent_indexed(
    doc: &Document,
    pointer: usize,
    element: &mut dyn FnMut(usize, usize) -> Result<Option<usize>>,
) -> Result<usize> {
    resolve_to(doc, pointer, true, element)
}

fn resolve_to(
    doc: &Document,
    pointer: usize,
    parent_only: bool,
    element: &mut dyn FnMut(usize, usize) -> Result<Option<usize>>,
) -> Result<usize> {
    let mut chars = doc.chars(pointer)?;
    match chars.next_scalar()? {
        None => return Ok(doc.root()),
        Some('/') => (),
        _ => return Err(Error::Schema),
    }
    let mut parent = doc.root();
    loop {
        let (name, last) = segment(&mut chars)?;
        if last && parent_only {
            return Ok(parent);
        }
        parent = match doc.node(parent)?.value {
            Value::Object => {
                // ponytail: scans members; `read_request` indexes them only for placement, so
                // descents stay quadratic. The metadata quota bounds the member count, not the
                // decoded name length each comparison reads; about 0.3 ms is one sampled shape,
                // not a ceiling (docs/DEFERRED-OPTIONS.md).
                let mut found = None;
                for child in doc.children(parent)? {
                    if name.matches(doc, doc.node(child)?.name.ok_or(Error::Schema)?)? {
                        found = Some(child);
                        break;
                    }
                }
                found.ok_or(Error::Schema)?
            }
            Value::Array => {
                let index = name.index()?;
                // ponytail: an element other than the indexed last one is reached by walking;
                // binding restore pays this per entry. The default metadata quota limits the
                // entries; 0.06 ms is one sampled shape, not a ceiling (docs/DEFERRED-OPTIONS.md).
                match element(parent, index)? {
                    Some(child) => child,
                    None => doc.children(parent)?.nth(index).ok_or(Error::Schema)?,
                }
            }
            _ => return Err(Error::Schema),
        };
        if last {
            return Ok(parent);
        }
    }
}

/// Serialize trusted node bindings of every [`BOUND_LISTS`] entry before the
/// canonical request fields are published.
pub fn bindings(doc: &mut Document, owner: &mut Writer) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for key in BOUND_LISTS {
        let Some(list) = doc.member(x, key)? else {
            continue;
        };
        let mut next = doc.node(list)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let Some((parent, carrier)) = doc.origin(entry)? else {
                continue;
            };
            let lease = encode::scratch(owner, |e| pointer(doc, parent, e))?;
            let binding = doc.import_json(owner, lease)?;
            doc.rename(binding, "binding")?;
            doc.append(entry, binding)?;
            if let Some(carrier) = carrier {
                doc.set(entry, "carrier", Value::Text(carrier))?;
            }
        }
    }
    Ok(())
}

/// Re-bind serialized bindings. Call only on a trusted reconstructed canonical document.
pub fn restore_bindings(doc: &mut Document) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    for key in BOUND_LISTS {
        let Some(list) = doc.member(x, key)? else {
            continue;
        };
        let mut next = doc.node(list)?.first;
        while let Some(entry) = next {
            next = doc.node(entry)?.next;
            let Some(binding) = doc.member(entry, "binding")? else {
                continue;
            };
            let parent = resolve(doc, binding)?;
            let carrier = match doc.member(entry, "carrier")? {
                Some(carrier) => Some(carrier_name(doc, carrier)?),
                None => None,
            };
            doc.bind_origin(entry, parent, carrier)?;
        }
    }
    Ok(())
}

fn carrier_name(doc: &Document, carrier: usize) -> Result<&'static str> {
    for name in CARRIERS {
        if doc.string_is(carrier, name)? {
            return Ok(name);
        }
    }
    Err(Error::Schema)
}
