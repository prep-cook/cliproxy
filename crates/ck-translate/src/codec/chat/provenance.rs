use crate::codec::{
    Error, Result,
    document::{Document, Name, Value},
    encode::Encoder,
    provenance::{Segment, pointer, segment, write_segment},
};

pub(super) fn unknown(doc: &Document, object: usize, skip: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    let mut first = true;
    for field in doc.children(object)? {
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut known = false;
        for key in skip {
            known |= doc.name_is(name, key)?;
        }
        if known {
            continue;
        }
        if first {
            e.bytes(b",\"original\":[")?;
            first = false;
        } else {
            e.bytes(b",")?;
        }
        entry(doc, field, e)?;
    }
    if !first {
        e.bytes(b"]")?;
    }
    Ok(())
}

pub(super) fn one(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"original\":[")?;
    entry(doc, field, e)?;
    e.bytes(b"]")
}

fn entry(doc: &Document, field: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"{\"protocol\":\"chat\",\"pointer\":")?;
    pointer(doc, field, e)?;
    e.bytes(b",\"value\":")?;
    doc.write(field, e)?;
    e.bytes(b"}")
}

pub(super) fn restore(doc: &mut Document, x: usize) -> Result<bool> {
    let Some(original) = doc.member(x, "original")? else {
        return Ok(true);
    };
    let mut restored = true;
    let mut next = doc.node(original)?.first;
    while let Some(entry) = next {
        next = doc.node(entry)?.next;
        if !doc.string_is(doc.required(entry, "protocol")?, "chat")? {
            restored = false;
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let Value::String(text) = doc.node(pointer)?.value else {
            return Err(Error::Schema);
        };
        let mut consumed = false;
        let parent = insertion_parent(doc, x, pointer, &mut restored, &mut consumed)?;
        if let Some(parent) = parent {
            let value = doc.remove(entry, "value")?.ok_or(Error::Schema)?;
            let target = doc.add(Some(Name::Pointer(text)), Value::Null)?;
            doc.transplant(target, value)?;
            doc.append(parent, target)?;
            consumed = true;
        }
        if consumed {
            doc.detach(original, entry)?;
        }
    }
    Ok(restored)
}

/// Walks a restore pointer from the root. Returns the object that receives the value when the
/// last member is absent; returns None when the member already exists or the path is missing.
fn insertion_parent(
    doc: &Document,
    x: usize,
    pointer: usize,
    restored: &mut bool,
    consumed: &mut bool,
) -> Result<Option<usize>> {
    let mut chars = doc.chars(pointer)?;
    if chars.next_scalar()? != Some('/') {
        return Err(Error::Schema);
    }
    let mut parent = doc.root();
    Ok(loop {
        let (segment, last) = segment(&mut chars)?;
        if last {
            if !matches!(doc.node(parent)?.value, Value::Object) {
                *restored = false;
                break None;
            }
            let mut exists = false;
            for child in doc.children(parent)? {
                if let Some(name) = doc.node(child)?.name {
                    exists |= segment.matches(doc, name)?;
                }
            }
            *consumed =
                exists && !(parent == doc.root() && segment.is("x_ck")? && doc.member(parent, "x_ck")? == Some(x));
            break if exists { None } else { Some(parent) };
        }
        let child = if matches!(doc.node(parent)?.value, Value::Array) {
            array_child(doc, parent, &segment)?
        } else {
            let mut found = None;
            for child in doc.children(parent)? {
                if let Some(name) = doc.node(child)?.name
                    && segment.matches(doc, name)?
                {
                    found = Some(child);
                    break;
                }
            }
            if found.is_none() && segment.is("message")? {
                found = doc.member(parent, "delta")?;
            }
            if found.is_none() && segment.is("custom")? {
                found = doc.member(parent, "function")?;
            }
            if found.is_none()
                && segment.is("function_call")?
                && let Some(calls) = doc.member(parent, "tool_calls")?
                && let Some(call) = doc.node(calls)?.first
            {
                found = doc.member(call, "function")?;
            }
            found
        };
        let Some(child) = child else {
            *restored = false;
            break None;
        };
        parent = child;
    })
}

/// Selects the array element a pointer segment names; a one-element array always yields its element.
fn array_child(doc: &Document, parent: usize, segment: &Segment<'_>) -> Result<Option<usize>> {
    let mut digits = segment.clone();
    let mut index = 0usize;
    while let Some(c) = digits.next()? {
        let digit = c.to_digit(10).ok_or(Error::Schema)? as usize;
        index = index
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
            .ok_or(Error::Limit)?;
    }
    let first = doc.node(parent)?.first;
    if first.is_some_and(|first| doc.node(first).is_ok_and(|node| node.next.is_none())) {
        return Ok(first);
    }
    Ok(doc.children(parent)?.nth(index))
}

pub(super) fn preserve_request_members<const N: usize>(
    doc: &mut Document,
    owner: &mut ck_stream::Writer,
    parent: usize,
    target: usize,
    carrier: Option<&'static str>,
    keys: &[&str; N],
) -> Result<()> {
    let mut fields = [None; N];
    for (field, key) in fields.iter_mut().zip(keys) {
        *field = doc.member(parent, key)?;
    }
    if fields.iter().all(Option::is_none) {
        return Ok(());
    }
    let lease = crate::codec::encode::scratch(owner, |e| {
        e.bytes(b"[")?;
        for (index, field) in fields.iter().flatten().enumerate() {
            if index != 0 {
                e.bytes(b",")?;
            }
            pointer(doc, *field, e)?;
        }
        e.bytes(b"]")
    })?;
    let paths = doc.import_json(owner, lease)?;
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    for field in fields.into_iter().flatten() {
        let path = doc.node(paths)?.first.ok_or(Error::Schema)?;
        doc.detach(paths, path)?;
        doc.detach(parent, field)?;
        let entry = doc.add(None, Value::Object)?;
        doc.append(original, entry)?;
        doc.bind_origin(entry, target, carrier)?;
        doc.set(entry, "protocol", Value::Text("chat"))?;
        doc.rename(path, "pointer")?;
        doc.append(entry, path)?;
        doc.rename(field, "value")?;
        doc.append(entry, field)?;
    }
    Ok(())
}

pub(super) fn request_members(
    doc: &Document,
    parent: usize,
    carrier: Option<&str>,
    skip: &[&str],
    mapped_elsewhere: &[&str],
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    request_members_first(doc, parent, carrier, skip, mapped_elsewhere, &mut false, e)
}

pub(super) fn request_members_first(
    doc: &Document,
    parent: usize,
    carrier: Option<&str>,
    skip: &[&str],
    mapped_elsewhere: &[&str],
    first: &mut bool,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    let Some(original) = doc.member(x, "original")? else {
        return Ok(());
    };
    'entries: for entry in doc.origins(parent)? {
        if doc.node(entry)?.parent != Some(original) || doc.origin(entry)? != Some((parent, carrier)) {
            continue;
        }
        if !doc.string_is(doc.required(entry, "protocol")?, "chat")? {
            continue;
        }
        let pointer = doc.required(entry, "pointer")?;
        let mut chars = doc.chars(pointer)?;
        if chars.next_scalar()? != Some('/') {
            continue;
        }
        let name = loop {
            let (name, last) = segment(&mut chars)?;
            if last {
                break name;
            }
        };
        for key in skip {
            if name.is(key)? {
                continue 'entries;
            }
        }
        let current = match carrier {
            Some(_) => doc.required(parent, "source")?,
            None => parent,
        };
        for child in doc.children(current)? {
            let field = doc.node(child)?.name.ok_or(Error::Schema)?;
            let mut mapped = false;
            for key in mapped_elsewhere {
                mapped |= doc.name_is(field, key)?;
            }
            if !mapped && name.matches(doc, field)? {
                continue 'entries;
            }
        }
        // Only unmapped members are restored; mapped deletions must not revive source data.
        if !*first {
            e.bytes(b",")?;
        }
        *first = false;
        write_segment(name, e)?;
        e.bytes(b":")?;
        doc.write(doc.required(entry, "value")?, e)?;
    }
    Ok(())
}

pub(super) fn preserve_request_field(
    doc: &mut Document,
    owner: &mut ck_stream::Writer,
    field: usize,
    target: usize,
    carrier: Option<&'static str>,
) -> Result<usize> {
    let parent = doc.node(field)?.parent.ok_or(Error::Schema)?;
    let lease = crate::codec::encode::scratch(owner, |e| pointer(doc, field, e))?;
    let pointer = doc.import_json(owner, lease)?;
    let x = doc.required(doc.root(), "x_ck")?;
    let original = doc.required(x, "original")?;
    let entry = doc.add(None, Value::Object)?;
    doc.append(original, entry)?;
    doc.bind_origin(entry, target, carrier)?;
    doc.set(entry, "protocol", Value::Text("chat"))?;
    doc.rename(pointer, "pointer")?;
    doc.append(entry, pointer)?;
    doc.detach(parent, field)?;
    doc.rename(field, "value")?;
    doc.append(entry, field)?;
    Ok(entry)
}

pub(super) fn native_members(
    doc: &mut Document,
    owner: &mut ck_stream::Writer,
    part: usize,
    carrier: &'static str,
    known: &[&str],
) -> Result<()> {
    let native = doc.required(part, carrier)?;
    let mut next = doc.node(native)?.first;
    while let Some(field) = next {
        next = doc.node(field)?.next;
        let name = doc.node(field)?.name.ok_or(Error::Schema)?;
        let mut mapped = false;
        for key in known {
            mapped |= doc.name_is(name, key)?;
        }
        if !mapped {
            preserve_request_field(doc, owner, field, part, Some(carrier))?;
        }
    }
    Ok(())
}
