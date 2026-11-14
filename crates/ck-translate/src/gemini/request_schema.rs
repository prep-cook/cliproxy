use crate::codec::{
    Error, Operation, RequestView, Result,
    document::{Document, Value},
};

pub(super) fn validate(view: RequestView<'_>) -> Result<()> {
    if view.operation == Operation::Compact {
        return Err(Error::Unsupported);
    }
    let doc = view.document;
    let root = doc.root();
    super::string(doc, doc.required(root, "model")?)?;
    let messages = doc.required(root, "messages")?;
    super::array(doc, messages)?;
    let mut system = false;
    for message in doc.children(messages)? {
        let role = doc.required(message, "role")?;
        if doc.string_is(role, "system")? {
            if system {
                return Err(Error::Unsupported);
            }
            system = true;
        } else if !doc.string_is(role, "user")? && !doc.string_is(role, "assistant")? && !doc.string_is(role, "tool")? {
            return Err(Error::Unsupported);
        }
        if let Some(content) = doc.member(message, "content")? {
            match doc.node(content)?.value {
                Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
                Value::Array => {
                    for part in doc.children(content)? {
                        validate_part(view, part)?;
                    }
                }
                _ => return Err(Error::Unsupported),
            }
        }
        if let Some(calls) = doc.member(message, "tool_calls")? {
            super::array(doc, calls)?;
            for call in doc.children(calls)? {
                if !super::is(doc, call, "type", "function")? {
                    return Err(Error::Unsupported);
                }
                if let Some(format) = doc.member(call, "arguments_format")?
                    && !doc.string_is(format, "json")?
                {
                    return Err(Error::Unsupported);
                }
                super::string(doc, doc.required(call, "id")?)?;
                let function = doc.required(call, "function")?;
                super::string(doc, doc.required(function, "name")?)?;
                if let Some(args) = doc.member(function, "arguments")? {
                    super::object(doc, args)?;
                }
            }
        }
    }
    for key in ["temperature", "top_p"] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Number(_) | Value::Unsigned(_))
        {
            return Err(Error::Schema);
        }
    }
    for key in ["max_tokens", "max_completion_tokens", "n", "seed"] {
        if let Some(n) = doc.member(root, key)? {
            doc.unsigned(n)?;
        }
    }
    if let Some(modalities) = doc.member(root, "modalities")? {
        super::array(doc, modalities)?;
        for modality in doc.children(modalities)? {
            if !["text", "image", "audio", "video", "TEXT", "IMAGE", "AUDIO", "VIDEO"]
                .iter()
                .try_fold(false, |yes, v| Ok::<_, Error>(yes || doc.string_is(modality, v)?))?
            {
                return Err(Error::Unsupported);
            }
        }
    }
    for key in [
        "parallel_tool_calls",
        "audio",
        "prediction",
        "logit_bias",
        "presence_penalty",
        "frequency_penalty",
        "store",
        "service_tier",
    ] {
        if doc.member(root, key)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    if let Some(tools) = doc.member(root, "tools")? {
        super::array(doc, tools)?;
        for tool in doc.children(tools)? {
            if !super::is(doc, tool, "type", "function")? {
                return Err(Error::Unsupported);
            }
            let function = doc.required(tool, "function")?;
            super::string(doc, doc.required(function, "name")?)?;
        }
    }
    if let Some(x) = doc.member(root, "x_ck")? {
        for key in ["session", "transport"] {
            if doc.member(x, key)?.is_some() {
                return Err(Error::Unsupported);
            }
        }
        if let Some(provider) = doc.member(x, "provider_parameters")? {
            for native in doc.children(provider)? {
                if !doc.name_is(doc.node(native)?.name.ok_or(Error::Schema)?, "gemini")?
                    && doc.node(native)?.first.is_some()
                {
                    return Err(Error::Unsupported);
                }
            }
        }
        if let Some(replay) = doc.member(x, "replay")? {
            for entry in doc.children(replay)? {
                if !super::is(doc, entry, "kind", "gemini")? {
                    return Err(Error::Unsupported);
                }
                super::string(doc, doc.required(entry, "signature")?)?;
            }
        }
        if let Some(cache) = doc.member(x, "cache_control")?
            && doc.node(cache)?.first.is_some()
        {
            return Err(Error::Unsupported);
        }
        if let Some(native) = doc.member(x, "tools")?
            && doc.node(native)?.first.is_some()
            && !super::is(doc, x, "original_protocol", "gemini")?
        {
            return Err(Error::Unsupported);
        }
    }
    view.media.validate_registrations(doc, 0)?;
    Ok(())
}
fn validate_part(view: RequestView<'_>, part: usize) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        return super::string(doc, doc.required(part, "text")?);
    }
    if doc.string_is(kind, "tool_result")? {
        super::string(doc, doc.required(part, "tool_call_id")?)?;
        return Ok(());
    }
    if doc.string_is(kind, "opaque")? {
        return if super::is(doc, part, "protocol", "gemini")? {
            Ok(())
        } else {
            Err(Error::Unsupported)
        };
    }
    for name in ["image", "audio", "video", "document"] {
        if doc.string_is(kind, name)? {
            let source = doc.required(part, "source")?;
            if super::is(doc, source, "kind", "uri")? {
                super::string(doc, doc.required(source, "uri")?)?;
            } else if super::is(doc, source, "kind", "data")? {
                view.media
                    .get(doc, doc.required(source, "ref")?)?
                    .ok_or(Error::Position)?;
            } else {
                return Err(Error::Unsupported);
            }
            return Ok(());
        }
    }
    Err(Error::Unsupported)
}

pub(super) fn native(doc: &Document) -> Result<Option<usize>> {
    let Some(x) = doc.member(doc.root(), "x_ck")? else {
        return Ok(None);
    };
    let Some(provider) = doc.member(x, "provider_parameters")? else {
        return Ok(None);
    };
    doc.member(provider, "gemini")
}
