//! JSON string decoding: one-shot scalar decoding of a quoted span, the resumable
//! string decoder used while scanning, and the UTF-8 validator for raw input.

use crate::codec::{
    Error, Result,
    source::{Cursor, Source, Span},
};

pub fn string_is(source: &Source, span: Span, text: &str) -> Result<bool> {
    let mut decoded = Decoded::new(source, span)?;
    for scalar in text.chars() {
        if decoded.next_scalar()? != Some(scalar) {
            return Ok(false);
        }
    }
    Ok(decoded.next_scalar()?.is_none())
}

pub(super) fn string_eq(source: &Source, left: Span, right: Span) -> Result<bool> {
    let mut left = Decoded::new(source, left)?;
    let mut right = Decoded::new(source, right)?;
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

/// Decode one scalar at a time, including surrogate pairs, without a String.
#[derive(Clone)]
pub struct Decoded<'a> {
    cursor: Cursor<'a>,
}
impl<'a> Decoded<'a> {
    pub fn new(source: &'a Source, span: Span) -> Result<Self> {
        if span.len() < 2
            || !source.equals(
                Span {
                    start: span.start,
                    end: span.start + 1,
                },
                b"\"",
            )?
            || !source.equals(
                Span {
                    start: span.end - 1,
                    end: span.end,
                },
                b"\"",
            )?
        {
            return Err(Error::Json);
        }
        Ok(Self {
            cursor: source.cursor(Span {
                start: span.start + 1,
                end: span.end - 1,
            })?,
        })
    }

    pub(crate) fn fragment(source: &'a Source, span: Span) -> Result<Self> {
        Ok(Self {
            cursor: source.cursor(span)?,
        })
    }

    fn hex(&mut self) -> Result<u32> {
        let mut value = 0;
        for _ in 0..4 {
            let byte = self.cursor.next_byte()?.ok_or(Error::Json)?;
            value = value * 16
                + match byte {
                    b'0'..=b'9' => u32::from(byte - b'0'),
                    b'a'..=b'f' => u32::from(byte - b'a' + 10),
                    b'A'..=b'F' => u32::from(byte - b'A' + 10),
                    _ => return Err(Error::Json),
                };
        }
        Ok(value)
    }

    pub fn next_scalar(&mut self) -> Result<Option<char>> {
        let Some(byte) = self.cursor.next_byte()? else {
            return Ok(None);
        };
        let scalar = if byte == b'\\' {
            match self.cursor.next_byte()?.ok_or(Error::Json)? {
                b'"' => '"',
                b'\\' => '\\',
                b'/' => '/',
                b'b' => '\u{8}',
                b'f' => '\u{c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'u' => {
                    let mut code = self.hex()?;
                    if (0xd800..=0xdbff).contains(&code) {
                        if self.cursor.next_byte()? != Some(b'\\') || self.cursor.next_byte()? != Some(b'u') {
                            return Err(Error::Json);
                        }
                        let low = self.hex()?;
                        if !(0xdc00..=0xdfff).contains(&low) {
                            return Err(Error::Json);
                        }
                        code = 0x10000 + ((code - 0xd800) << 10) + low - 0xdc00;
                    }
                    char::from_u32(code).ok_or(Error::Json)?
                }
                _ => return Err(Error::Json),
            }
        } else if byte < 0x80 {
            if byte < 0x20 || byte == b'"' {
                return Err(Error::Json);
            }
            byte as char
        } else {
            let len = match byte {
                0xc2..=0xdf => 2,
                0xe0..=0xef => 3,
                0xf0..=0xf4 => 4,
                _ => return Err(Error::Utf8),
            };
            let mut bytes = [0; 4];
            bytes[0] = byte;
            for slot in &mut bytes[1..len] {
                *slot = self.cursor.next_byte()?.ok_or(Error::Utf8)?;
            }
            std::str::from_utf8(&bytes[..len])
                .map_err(|_| Error::Utf8)?
                .chars()
                .next()
                .ok_or(Error::Utf8)?
        };
        Ok(Some(scalar))
    }
}

/// Resumable JSON string decoder. Only an incomplete scalar or escape survives a call.
#[derive(Clone, Copy, Default)]
pub(crate) struct StringDecoder {
    state: u8,
    digits: u8,
    code: u32,
    high: u32,
    utf8: [u8; 4],
    used: usize,
    needed: usize,
}

impl StringDecoder {
    pub(crate) fn complete(&self) -> bool {
        self.state == 0 && self.used == 0
    }

    pub(crate) fn push(&mut self, byte: u8) -> Result<Option<char>> {
        if self.used != 0 {
            if !(0x80..=0xbf).contains(&byte) {
                return Err(Error::Utf8);
            }
            self.utf8[self.used] = byte;
            self.used += 1;
            if self.used != self.needed {
                return Ok(None);
            }
            let c = std::str::from_utf8(&self.utf8[..self.used])
                .map_err(|_| Error::Utf8)?
                .chars()
                .next()
                .ok_or(Error::Utf8)?;
            self.used = 0;
            return Ok(Some(c));
        }
        match self.state {
            0 => match byte {
                b'\\' => self.state = 1,
                0..=0x1f | b'"' => return Err(Error::Json),
                0x20..=0x7f => return Ok(Some(byte as char)),
                _ => {
                    self.needed = match byte {
                        0xc2..=0xdf => 2,
                        0xe0..=0xef => 3,
                        0xf0..=0xf4 => 4,
                        _ => return Err(Error::Utf8),
                    };
                    self.utf8[0] = byte;
                    self.used = 1;
                }
            },
            1 => {
                self.state = 0;
                return Ok(Some(match byte {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'u' => {
                        self.state = 2;
                        self.code = 0;
                        self.digits = 0;
                        return Ok(None);
                    }
                    _ => return Err(Error::Json),
                }));
            }
            2 | 5 => return self.unicode_digit(byte),
            3 => {
                if byte != b'\\' {
                    return Err(Error::Json);
                }
                self.state = 4;
            }
            4 => {
                if byte != b'u' {
                    return Err(Error::Json);
                }
                self.state = 5;
                self.code = 0;
                self.digits = 0;
            }
            _ => return Err(Error::Json),
        }
        Ok(None)
    }

    /// One hex digit of a `\u` escape; state 5 reads the low half of a surrogate pair.
    fn unicode_digit(&mut self, byte: u8) -> Result<Option<char>> {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return Err(Error::Json),
        };
        self.code = self.code * 16 + u32::from(digit);
        self.digits += 1;
        if self.digits == 4 {
            let code = self.code;
            if self.state == 5 {
                if !(0xdc00..=0xdfff).contains(&code) {
                    return Err(Error::Json);
                }
                self.state = 0;
                return Ok(Some(
                    char::from_u32(0x10000 + ((self.high - 0xd800) << 10) + code - 0xdc00).ok_or(Error::Json)?,
                ));
            }
            if (0xd800..=0xdbff).contains(&code) {
                self.high = code;
                self.state = 3;
            } else {
                self.state = 0;
                return Ok(Some(char::from_u32(code).ok_or(Error::Json)?));
            }
        }
        Ok(None)
    }
}

#[derive(Default)]
pub(crate) struct Utf8 {
    remaining: u8,
    min: u8,
    max: u8,
}
impl Utf8 {
    pub(crate) fn push(&mut self, byte: u8) -> Result<()> {
        if self.remaining != 0 {
            if byte < self.min || byte > self.max {
                return Err(Error::Utf8);
            }
            self.remaining -= 1;
            self.min = 0x80;
            self.max = 0xbf;
        } else {
            let (remaining, min, max) = match byte {
                0..=0x7f => (0, 0, 0),
                0xc2..=0xdf => (1, 0x80, 0xbf),
                0xe0 => (2, 0xa0, 0xbf),
                0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
                0xed => (2, 0x80, 0x9f),
                0xf0 => (3, 0x90, 0xbf),
                0xf1..=0xf3 => (3, 0x80, 0xbf),
                0xf4 => (3, 0x80, 0x8f),
                _ => return Err(Error::Utf8),
            };
            self.remaining = remaining;
            self.min = min;
            self.max = max;
        }
        Ok(())
    }
    pub(crate) fn finish(&self) -> Result<()> {
        if self.remaining == 0 { Ok(()) } else { Err(Error::Utf8) }
    }
}
