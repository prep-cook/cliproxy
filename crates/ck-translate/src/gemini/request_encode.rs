use super::request::originals;
use crate::codec::{
    Error, RequestView, Result,
    document::{Document, Value},
    encode::Encoder,
};

pub(super) fn write(view: RequestView<'_>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let root = doc.root();
    let x = doc.member(root, "x_ck")?;
    let wrapper = x
        .map(|x| doc.member(x, "gemini_count_wrapper"))
        .transpose()?
        .flatten()
        .is_some();
    if wrapper {
        e.bytes(b"{\"generateContentRequest\":")?;
    }
    e.bytes(b"{\"contents\":[")?;
    let messages = doc.required(root, "messages")?;
    let mut first = true;
    for message in doc.children(messages)? {
        if super::is(doc, message, "role", "system")? {
            continue;
        }
        if !first {
            e.bytes(b",")?;
        }
        first = false;
        write_message(view, message, false, e)?;
    }
    e.bytes(b"]")?;
    for message in doc.children(messages)? {
        if super::is(doc, message, "role", "system")? {
            e.bytes(b",\"systemInstruction\":")?;
            write_message(view, message, true, e)?;
        }
    }
    let native = super::request_schema::native(doc)?;
    let mut native_config = native.map(|n| doc.member(n, "generationConfig")).transpose()?.flatten();
    // A native null config returns as null unless mapped controls now fill the config.
    let null_config = match native_config {
        Some(n) => matches!(doc.node(n)?.value, Value::Null),
        None => false,
    };
    if null_config {
        native_config = None;
    }
    let thinking = x.map(|x| doc.member(x, "thinking")).transpose()?.flatten();
    let config_present = native_config.is_some()
        || thinking.is_some()
        || doc.member(root, "response_format")?.is_some()
        || super::request::config().iter().try_fold(false, |yes, (_, key)| {
            Ok::<_, Error>(yes || doc.member(root, key)?.is_some())
        })?;
    if config_present {
        e.bytes(b",\"generationConfig\":{")?;
        generation_config(doc, thinking, native_config, e)?;
        e.bytes(b"}")?;
    } else if null_config {
        e.bytes(b",\"generationConfig\":null")?;
    }
    tool_declarations(doc, x, e)?;
    if let Some(native) = native {
        super::members(doc, native, &["generationConfig"], e)?;
    }
    if let Some(choice) = doc.member(root, "tool_choice")? {
        tool_config(doc, choice, e)?;
    }
    if !wrapper {
        originals(doc, root, None, e)?;
    }
    for entry in doc.origins(root)? {
        if doc
            .origin(entry)?
            .is_some_and(|(_, carrier)| carrier == Some("body_model"))
        {
            e.bytes(b",\"model\":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
    }
    e.bytes(b"}")?;
    if wrapper {
        originals(doc, root, None, e)?;
        e.bytes(b"}")?;
    }
    Ok(())
}
/// Writes the members of `generationConfig`: mapped controls, response schema, thinking and the
/// native config, comma-separated.
fn generation_config(
    doc: &Document,
    thinking: Option<usize>,
    native_config: Option<usize>,
    e: &mut Encoder<'_, '_>,
) -> Result<()> {
    let root = doc.root();
    let mut first = true;
    for &(wire, canonical) in super::request::config() {
        if let Some(n) = doc.member(root, canonical)? {
            comma(&mut first, e)?;
            e.string(wire)?;
            e.bytes(b":")?;
            if canonical == "modalities" {
                modalities(doc, n, e)?;
            } else {
                doc.write(n, e)?;
            }
        }
    }
    if let Some(format) = doc.member(root, "response_format")? {
        for key in ["responseMimeType", "responseSchema", "responseJsonSchema"] {
            if let Some(n) = doc.member(format, key)? {
                comma(&mut first, e)?;
                e.string(key)?;
                e.bytes(b":")?;
                doc.write(n, e)?;
            }
        }
        if let Some(schema) = doc.member(format, "json_schema")? {
            comma(&mut first, e)?;
            e.bytes(b"\"responseMimeType\":\"application/json\",\"responseJsonSchema\":")?;
            let schema = doc.member(schema, "schema")?.unwrap_or(schema);
            doc.write(schema, e)?;
        }
    }
    if let Some(thinking) = thinking {
        comma(&mut first, e)?;
        thinking_config(doc, thinking, e)?;
    }
    if let Some(config) = native_config {
        for n in doc.children(config)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    Ok(())
}
/// Writes the modalities array in Gemini's upper-case spelling. Schema validation admits only
/// ASCII modality names.
fn modalities(doc: &Document, list: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"[")?;
    let mut first = true;
    for modality in doc.children(list)? {
        comma(&mut first, e)?;
        e.bytes(b"\"")?;
        let mut chars = doc.chars(modality)?;
        while let Some(c) = chars.next_scalar()? {
            e.escaped(c.to_ascii_uppercase().encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")?;
    }
    e.bytes(b"]")
}
/// Writes `"thinkingConfig":{...}` from the mapped controls and the native thinking config.
fn thinking_config(doc: &Document, thinking: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"thinkingConfig\":{")?;
    let mut first = true;
    for (canonical, wire) in [
        ("budget_tokens", "thinkingBudget"),
        ("include", "includeThoughts"),
        ("effort", "thinkingLevel"),
    ] {
        if let Some(n) = doc.member(thinking, canonical)? {
            comma(&mut first, e)?;
            e.string(wire)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    if let Some(native) = doc.member(thinking, "native")? {
        for n in doc.children(native)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    }
    e.bytes(b"}")
}
/// Writes `,"tools":[...]`: Chat function declarations, then native tool groups.
fn tool_declarations(doc: &Document, x: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let Some(tools) = doc.member(doc.root(), "tools")? else {
        return Ok(());
    };
    e.bytes(b",\"tools\":[")?;
    let mut first = true;
    if doc.node(tools)?.first.is_some() {
        e.bytes(b"{\"functionDeclarations\":[")?;
        let mut first_function = true;
        for tool in doc.children(tools)? {
            comma(&mut first_function, e)?;
            doc.write(doc.required(tool, "function")?, e)?;
        }
        e.bytes(b"]}")?;
        first = false;
    }
    if let Some(native) = x.map(|x| doc.member(x, "tools")).transpose()?.flatten() {
        for tool in doc.children(native)? {
            comma(&mut first, e)?;
            doc.write(tool, e)?;
        }
    }
    e.bytes(b"]")
}
/// Writes `,"toolConfig":...` for a Chat `tool_choice`.
fn tool_config(doc: &Document, choice: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b",\"toolConfig\":{\"functionCallingConfig\":{")?;
    if matches!(doc.node(choice)?.value, Value::Object) {
        let function = doc.required(choice, "function")?;
        e.bytes(b"\"mode\":\"ANY\",\"allowedFunctionNames\":[")?;
        doc.write(doc.required(function, "name")?, e)?;
        e.bytes(b"]")?;
    } else {
        e.bytes(b"\"mode\":")?;
        e.string(if doc.string_is(choice, "none")? {
            "NONE"
        } else if doc.string_is(choice, "required")? {
            "ANY"
        } else if doc.string_is(choice, "auto")? {
            "AUTO"
        } else {
            return Err(Error::Unsupported);
        })?;
    }
    e.bytes(b"}}")?;
    Ok(())
}
fn comma(first: &mut bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    if !*first {
        e.bytes(b",")?;
    }
    *first = false;
    Ok(())
}
fn write_message(view: RequestView<'_>, message: usize, system: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    e.bytes(b"{")?;
    if !system {
        e.bytes(b"\"role\":")?;
        e.string(if super::is(doc, message, "role", "assistant")? {
            "model"
        } else {
            "user"
        })?;
        e.bytes(b",")?;
    }
    e.bytes(b"\"parts\":[")?;
    let mut first = true;
    let content = doc.member(message, "content")?;
    let calls = doc.member(message, "tool_calls")?;
    let order = super::origin(doc, message, "parts")?;
    if let Some(order) = order {
        for entry in doc.children(doc.required(order, "parts")?)? {
            let (part, _) = doc.origin(entry)?.ok_or(Error::Schema)?;
            if doc.node(part)?.parent != content && doc.node(part)?.parent != calls {
                continue;
            }
            comma(&mut first, e)?;
            write_part(view, part, Some(entry), e)?;
        }
    }
    if let Some(content) = content {
        if matches!(doc.node(content)?.value, Value::Array) {
            for part in doc.children(content)? {
                if super::origin(doc, part, "part")?.is_some() {
                    continue;
                }
                comma(&mut first, e)?;
                write_part(view, part, None, e)?;
            }
        } else {
            comma(&mut first, e)?;
            e.bytes(b"{\"text\":")?;
            doc.write(content, e)?;
            e.bytes(b"}")?;
        }
    }
    if let Some(calls) = calls {
        for call in doc.children(calls)? {
            if super::origin(doc, call, "part")?.is_some() {
                continue;
            }
            comma(&mut first, e)?;
            write_part(view, call, None, e)?;
        }
    }
    e.bytes(b"]")?;
    originals(doc, message, None, e)?;
    e.bytes(b"}")
}
fn write_part(view: RequestView<'_>, part: usize, order: Option<usize>, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    let flag = |name| -> Result<bool> { Ok(order.map(|n| doc.member(n, name)).transpose()?.flatten().is_some()) };
    e.bytes(b"{")?;
    let mut first = false;
    if doc.string_is(kind, "function")? {
        function_call(doc, part, flag("synthetic_id")?, e)?;
    } else if doc.string_is(kind, "tool_result")? {
        function_response(doc, part, flag("synthetic_id")?, e)?;
    } else if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        first = flag("signature_only")?;
        if !first {
            e.bytes(b"\"text\":")?;
            doc.write(doc.required(part, "text")?, e)?;
            if doc.string_is(kind, "reasoning")? {
                e.bytes(b",\"thought\":true")?;
            }
        }
    } else if doc.string_is(kind, "opaque")? {
        first = true;
        let value = doc.required(part, "value")?;
        for n in doc.children(value)? {
            comma(&mut first, e)?;
            doc.write_name(doc.node(n)?.name.ok_or(Error::Schema)?, e)?;
            e.bytes(b":")?;
            doc.write(n, e)?;
        }
    } else {
        media(view, part, e)?;
    }
    for entry in doc.origins(part)? {
        if doc
            .origin(entry)?
            .is_some_and(|(_, carrier)| carrier == Some("signature"))
        {
            comma(&mut first, e)?;
            e.bytes(b"\"thoughtSignature\":")?;
            doc.write(doc.required(entry, "signature")?, e)?;
        }
    }
    // Original members are comma-prefixed; an empty native part needs a separate first member.
    for entry in doc.origins(part)? {
        if doc.origin(entry)?.is_some_and(|(_, carrier)| carrier.is_none())
            && super::is(doc, entry, "protocol", "gemini")?
        {
            let pointer = doc.required(entry, "pointer")?;
            let Value::String(text) = doc.node(pointer)?.value else {
                return Err(Error::Schema);
            };
            if doc.string_is(kind, "reasoning")? && doc.string_is(pointer, "/thought")? {
                continue;
            }
            comma(&mut first, e)?;
            doc.write_name(crate::codec::document::Name::Pointer(text), e)?;
            e.bytes(b":")?;
            doc.write(doc.required(entry, "value")?, e)?;
        }
    }
    e.bytes(b"}")
}
/// Writes `"functionCall":{...}`; a synthesized call id has no native field.
fn function_call(doc: &Document, part: usize, synthetic_id: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"functionCall\":{\"name\":")?;
    let function = doc.required(part, "function")?;
    doc.write(doc.required(function, "name")?, e)?;
    if !synthetic_id {
        super::member(doc, part, "id", e)?;
    }
    if let Some(args) = doc.member(function, "arguments")? {
        e.bytes(b",\"args\":")?;
        doc.write(args, e)?;
    }
    super::members(doc, function, &["name", "arguments"], e)?;
    e.bytes(b"}")
}
/// Writes `"functionResponse":{...}`; a linked id the source omitted stays omitted.
fn function_response(doc: &Document, part: usize, synthetic_id: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    e.bytes(b"\"functionResponse\":{\"name\":")?;
    let result = doc.required(part, "result")?;
    doc.write(doc.required(result, "name")?, e)?;
    if !synthetic_id {
        e.bytes(b",\"id\":")?;
        doc.write(doc.required(part, "tool_call_id")?, e)?;
    }
    super::members(doc, result, &["name"], e)?;
    e.bytes(b"}")
}
/// Writes `"inlineData":{...}` from a registered payload or `"fileData":{...}` from a URI.
fn media(view: RequestView<'_>, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let source = doc.required(part, "source")?;
    let inline = super::is(doc, source, "kind", "data")?;
    e.bytes(if inline { b"\"inlineData\":{" } else { b"\"fileData\":{" })?;
    e.bytes(if inline { b"\"data\":" } else { b"\"fileUri\":" })?;
    if inline {
        view.media.write_base64(doc, doc.required(source, "ref")?, e)?;
    } else {
        doc.write(doc.required(source, "uri")?, e)?;
    }
    if let Some(mime) = doc.member(part, "mime")? {
        e.bytes(b",\"mimeType\":")?;
        doc.write(mime, e)?;
    }
    originals(doc, source, None, e)?;
    e.bytes(b"}")
}
