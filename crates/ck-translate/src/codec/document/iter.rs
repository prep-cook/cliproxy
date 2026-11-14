//! Iterators over a document's children, provenance origins and decoded scalars.

use super::{Document, Text, Value};
use crate::codec::{Error, Result, json};

impl Document {
    pub fn children(&self, parent: usize) -> Result<Children<'_>> {
        Ok(Children {
            doc: self,
            next: self.node(parent)?.first,
        })
    }

    /// The decoded characters of a string text.
    pub(crate) fn text_chars(&self, text: Text) -> Result<Characters<'_>> {
        Ok(Characters::Mapped(json::Decoded::new(self.source(text)?, text.span)?))
    }

    pub fn chars(&self, index: usize) -> Result<Characters<'_>> {
        self.check_live()?;
        match &self.nodes.get(index).ok_or(Error::Position)?.value {
            Value::String(t) => Ok(Characters::Mapped(json::Decoded::new(self.source(*t)?, t.span)?)),
            Value::Text(s) => Ok(Characters::Static(s.chars())),
            Value::Id(id) => Ok(Characters::Static(self.local_id(*id)?.as_str().chars())),
            Value::Fragments => Ok(Characters::Fragments(FragmentCharacters {
                doc: self,
                root: index,
                next: self.nodes[index].first,
                leaf: FragmentLeaf::Empty,
            })),
            Value::Utf8Media => Ok(Characters::Encoded(EncodedCharacters {
                doc: self,
                next: self.nodes[index].first,
                decoded: None,
                bytes: [0; 3],
                used: 0,
                length: 0,
            })),
            _ => Err(Error::Schema),
        }
    }

    pub(crate) fn decoded_bytes(&self, index: usize) -> Result<EncodedCharacters<'_>> {
        self.decoded_bytes_after(index, 0)
    }

    pub(crate) fn decoded_bytes_after(&self, index: usize, skip: usize) -> Result<EncodedCharacters<'_>> {
        let node = self.node(index)?;
        let (next, decoded) = match node.value {
            Value::Base64Fragments | Value::Utf8Media if skip == 0 => (node.first, None),
            Value::String(text) => {
                let mut decoded = json::Decoded::new(self.source(text)?, text.span)?;
                for _ in 0..skip {
                    decoded.next_scalar()?.ok_or(Error::Schema)?;
                }
                (None, Some(decoded))
            }
            _ => return Err(Error::Schema),
        };
        Ok(EncodedCharacters {
            doc: self,
            next,
            decoded,
            bytes: [0; 3],
            used: 0,
            length: 0,
        })
    }

    pub(crate) fn origins(&self, parent: usize) -> Result<Origins<'_>> {
        Ok(Origins {
            doc: self,
            next: self.node(parent)?.origins,
        })
    }
}

pub struct Children<'a> {
    doc: &'a Document,
    next: Option<usize>,
}
impl Iterator for Children<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let i = self.next?;
        self.next = self.doc.nodes[i].next;
        Some(i)
    }
}

pub(crate) struct Origins<'a> {
    doc: &'a Document,
    next: Option<usize>,
}
impl Iterator for Origins<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let index = self.next?;
        self.next = self.doc.nodes[index].origin_next;
        Some(index)
    }
}

#[derive(Clone)]
pub enum Characters<'a> {
    Mapped(json::Decoded<'a>),
    Pointer(json::Decoded<'a>),
    Static(std::str::Chars<'a>),
    Encoded(EncodedCharacters<'a>),
    Fragments(FragmentCharacters<'a>),
}
impl Characters<'_> {
    pub fn next_scalar(&mut self) -> Result<Option<char>> {
        match self {
            Self::Mapped(d) => d.next_scalar(),
            Self::Static(s) => Ok(s.next()),
            Self::Encoded(s) => s.next_scalar(),
            Self::Fragments(s) => s.next_scalar(),
            Self::Pointer(d) => match d.next_scalar()? {
                Some('~') => match d.next_scalar()? {
                    Some('0') => Ok(Some('~')),
                    Some('1') => Ok(Some('/')),
                    _ => Err(Error::Schema),
                },
                value => Ok(value),
            },
        }
    }
}

#[derive(Clone)]
pub struct EncodedCharacters<'a> {
    doc: &'a Document,
    next: Option<usize>,
    decoded: Option<json::Decoded<'a>>,
    bytes: [u8; 3],
    used: usize,
    length: usize,
}

impl EncodedCharacters<'_> {
    pub(crate) fn next_byte(&mut self) -> Result<Option<u8>> {
        use base64::Engine;
        loop {
            if self.used < self.length {
                let byte = self.bytes[self.used];
                self.used += 1;
                return Ok(Some(byte));
            }
            if let Some(decoded) = &mut self.decoded {
                let mut quartet = [0; 4];
                if let Some(c) = decoded.next_scalar()? {
                    if !c.is_ascii() {
                        return Err(Error::Schema);
                    }
                    quartet[0] = c as u8;
                    for slot in &mut quartet[1..] {
                        let c = decoded.next_scalar()?.ok_or(Error::Schema)?;
                        if !c.is_ascii() {
                            return Err(Error::Schema);
                        }
                        *slot = c as u8;
                    }
                    self.length = base64::engine::general_purpose::STANDARD
                        .decode_slice(quartet, &mut self.bytes)
                        .map_err(|_| Error::Schema)?;
                    self.used = 0;
                    continue;
                }
            }
            let Some(node) = self.next else {
                return Ok(None);
            };
            let node = self.doc.node(node)?;
            self.next = node.next;
            let Value::String(text) = node.value else {
                return Err(Error::Schema);
            };
            self.decoded = Some(json::Decoded::new(self.doc.source(text)?, text.span)?);
        }
    }

    fn next_scalar(&mut self) -> Result<Option<char>> {
        let Some(byte) = self.next_byte()? else {
            return Ok(None);
        };
        let length = match byte {
            0..=0x7f => return Ok(Some(byte as char)),
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return Err(Error::Utf8),
        };
        let mut bytes = [0; 4];
        bytes[0] = byte;
        for slot in &mut bytes[1..length] {
            *slot = self.next_byte()?.ok_or(Error::Utf8)?;
        }
        Ok(Some(
            std::str::from_utf8(&bytes[..length])
                .map_err(|_| Error::Utf8)?
                .chars()
                .next()
                .ok_or(Error::Utf8)?,
        ))
    }
}

#[derive(Clone)]
pub struct FragmentCharacters<'a> {
    doc: &'a Document,
    root: usize,
    next: Option<usize>,
    leaf: FragmentLeaf<'a>,
}

#[derive(Clone)]
enum FragmentLeaf<'a> {
    Empty,
    Mapped(json::Decoded<'a>),
    Static(std::str::Chars<'a>),
    Encoded(EncodedCharacters<'a>),
}

impl FragmentCharacters<'_> {
    fn next_scalar(&mut self) -> Result<Option<char>> {
        loop {
            let value = match &mut self.leaf {
                FragmentLeaf::Empty => None,
                FragmentLeaf::Mapped(chars) => chars.next_scalar()?,
                FragmentLeaf::Static(chars) => chars.next(),
                FragmentLeaf::Encoded(chars) => chars.next_scalar()?,
            };
            if value.is_some() {
                return Ok(value);
            }
            let Some(index) = self.next else { return Ok(None) };
            let node = self.doc.node(index)?;
            if matches!(node.value, Value::Fragments) && node.first.is_some() {
                self.next = node.first;
                continue;
            }
            self.leaf = match node.value {
                Value::String(text) => FragmentLeaf::Mapped(json::Decoded::new(self.doc.source(text)?, text.span)?),
                Value::Text(text) => FragmentLeaf::Static(text.chars()),
                Value::Id(_) => return Err(Error::Schema),
                Value::Utf8Media => FragmentLeaf::Encoded(self.doc.decoded_bytes(index)?),
                Value::Fragments => FragmentLeaf::Empty,
                _ => return Err(Error::Schema),
            };
            let mut cursor = index;
            self.next = loop {
                let node = self.doc.node(cursor)?;
                if node.next.is_some() {
                    break node.next;
                }
                cursor = node.parent.ok_or(Error::Schema)?;
                if cursor == self.root {
                    break None;
                }
            };
        }
    }
}
