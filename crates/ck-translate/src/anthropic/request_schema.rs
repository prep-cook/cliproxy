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
    let messages = parameters(doc, root, view.operation)?;
    let mut conversation = false;
    let mut system = false;
    // The latest earlier message with calls; results almost always answer it.
    let mut recent = None;
    for message in doc.children(messages)? {
        let role = doc.required(message, "role")?;
        if doc.string_is(role, "system")? {
            if conversation || system {
                return Err(Error::Unsupported);
            }
            system = true;
        } else if doc.string_is(role, "user")? || doc.string_is(role, "assistant")? || doc.string_is(role, "tool")? {
            conversation = true;
        } else {
            return Err(Error::Unsupported);
        }
        self::message(view, (messages, recent), message, role)?;
        if doc.member(message, "tool_calls")?.is_some() {
            recent = Some(message);
        }
    }
    let x = doc.required(root, "x_ck")?;
    extensions(doc, x)?;
    tools(doc, root, x)?;
    tool_choice(doc, root, x)
}
/// Checks the top-level generation parameters and returns the messages array.
fn parameters(doc: &Document, root: usize, operation: Operation) -> Result<usize> {
    super::string(doc, doc.required(root, "model")?)?;
    if doc.chars(doc.required(root, "model")?)?.next_scalar()?.is_none() {
        return Err(Error::Schema);
    }
    let messages = doc.required(root, "messages")?;
    if !matches!(doc.node(messages)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    if operation == Operation::Generate && doc.unsigned(doc.required(root, "max_tokens")?)? == 0 {
        return Err(Error::Schema);
    }
    for key in ["temperature", "top_p"] {
        if let Some(n) = doc.member(root, key)?
            && !matches!(doc.node(n)?.value, Value::Number(_) | Value::Unsigned(_))
        {
            return Err(Error::Schema);
        }
    }
    if let Some(stop) = doc.member(root, "stop")? {
        if matches!(doc.node(stop)?.value, Value::Array) {
            for value in doc.children(stop)? {
                super::string(doc, value)?;
            }
        } else {
            super::string(doc, stop)?;
        }
    }
    if let Some(stream) = doc.member(root, "stream")? {
        let Value::Bool(stream) = doc.node(stream)?.value else {
            return Err(Error::Schema);
        };
        if stream && operation == Operation::CountTokens {
            return Err(Error::Unsupported);
        }
    }
    for key in [
        "n",
        "seed",
        "max_completion_tokens",
        "logprobs",
        "top_logprobs",
        "modalities",
        "audio",
        "frequency_penalty",
        "presence_penalty",
        "logit_bias",
        "prediction",
        "store",
    ] {
        if doc.member(root, key)?.is_some() {
            return Err(Error::Unsupported);
        }
    }
    if let Some(format) = doc.member(root, "response_format")? {
        if !super::is(doc, format, "type", "json_schema")? {
            return Err(Error::Unsupported);
        }
        let schema = doc.required(format, "json_schema")?;
        if !matches!(doc.node(doc.required(schema, "schema")?)?.value, Value::Object) {
            return Err(Error::Schema);
        }
    }
    Ok(messages)
}
/// Checks one message's content parts, tool result link and tool calls.
fn message(view: RequestView<'_>, calls: Calls, message: usize, role: usize) -> Result<()> {
    let doc = view.document;
    if let Some(content) = doc.member(message, "content")? {
        match doc.node(content)?.value {
            Value::String(_) | Value::Text(_) | Value::Fragments | Value::Utf8Media => (),
            Value::Array => {
                for part in doc.children(content)? {
                    validate_part(view, calls, message, part)?;
                }
            }
            Value::Null if doc.member(message, "tool_calls")?.is_some() => (),
            _ => return Err(Error::Schema),
        }
    }
    if doc.string_is(role, "tool")? {
        linked(doc, calls, message, None, doc.required(message, "tool_call_id")?)?;
    }
    if let Some(calls) = doc.member(message, "tool_calls")? {
        if !doc.string_is(role, "assistant")? || !matches!(doc.node(calls)?.value, Value::Array) {
            return Err(Error::Schema);
        }
        for call in doc.children(calls)? {
            tool_call(doc, call)?;
        }
    }
    Ok(())
}
fn tool_call(doc: &Document, call: usize) -> Result<()> {
    super::string(doc, doc.required(call, "id")?)?;
    if !super::is(doc, call, "type", "function")? && !super::is(doc, call, "type", "server")? {
        return Err(Error::Unsupported);
    }
    if let Some(namespace) = doc.member(call, "namespace")? {
        super::string(doc, namespace)?;
    }
    if !super::is(doc, call, "arguments_format", "json")? {
        return Err(Error::Unsupported);
    }
    let function = doc.required(call, "function")?;
    super::string(doc, doc.required(function, "name")?)?;
    if let Some(input) = doc.member(function, "arguments")?
        && !matches!(doc.node(input)?.value, Value::Object)
    {
        return Err(Error::Schema);
    }
    Ok(())
}
/// Checks the canonical `x_ck` extensions this destination accepts.
fn extensions(doc: &Document, x: usize) -> Result<()> {
    if doc.member(x, "session")?.is_some() {
        return Err(Error::Unsupported);
    }
    if let Some(thinking) = doc.member(x, "thinking")? {
        if doc.member(thinking, "native")?.is_some() && !super::is(doc, x, "original_protocol", "anthropic")? {
            return Err(Error::Unsupported);
        }
        if let Some(enabled) = doc.member(thinking, "enabled")?
            && !matches!(doc.node(enabled)?.value, Value::Bool(_))
        {
            return Err(Error::Schema);
        }
        if let Some(budget) = doc.member(thinking, "budget_tokens")? {
            doc.unsigned(budget)?;
        }
    }
    if let Some(replay) = doc.member(x, "replay")? {
        for entry in doc.children(replay)? {
            if !super::is(doc, entry, "kind", "anthropic")? {
                return Err(Error::Unsupported);
            }
            doc.unsigned(doc.required(entry, "message_index")?)?;
            doc.unsigned(doc.required(entry, "block_index")?)?;
            if let Some(signature) = doc.member(entry, "signature")? {
                super::string(doc, signature)?;
            }
        }
    }
    if let Some(provider) = doc.member(x, "provider_parameters")? {
        for protocol in doc.children(provider)? {
            if !doc.name_is(doc.node(protocol)?.name.ok_or(Error::Schema)?, "anthropic")?
                && doc.node(protocol)?.first.is_some()
            {
                return Err(Error::Unsupported);
            }
        }
    }
    Ok(())
}
fn tools(doc: &Document, root: usize, x: usize) -> Result<()> {
    let Some(tools) = doc.member(root, "tools")? else {
        return Ok(());
    };
    if !matches!(doc.node(tools)?.value, Value::Array) {
        return Err(Error::Schema);
    }
    for tool in doc.children(tools)? {
        if super::is(doc, tool, "type", "server")? {
            let declarations = doc.required(x, "tools")?;
            if super::request_encode::declaration(doc, declarations, tool)?.is_none() {
                return Err(Error::Unsupported);
            }
        } else {
            if !super::is(doc, tool, "type", "function")? {
                return Err(Error::Unsupported);
            }
            let function = doc.required(tool, "function")?;
            super::string(doc, doc.required(function, "name")?)?;
            if !matches!(doc.node(doc.required(function, "parameters")?)?.value, Value::Object) {
                return Err(Error::Schema);
            }
        }
    }
    Ok(())
}
fn tool_choice(doc: &Document, root: usize, x: usize) -> Result<()> {
    let Some(choice) = doc.member(root, "tool_choice")? else {
        return Ok(());
    };
    if matches!(doc.node(choice)?.value, Value::Object) {
        if !super::is(doc, choice, "type", "function")? {
            return Err(Error::Unsupported);
        }
        let function = doc.required(choice, "function")?;
        let name = doc.required(function, "name")?;
        if doc.chars(name)?.next_scalar()?.is_none() {
            return Err(Error::Schema);
        }
        let tools = doc.required(root, "tools")?;
        let mut declared = false;
        for tool in doc.children(tools)? {
            if let Some(function) = doc.member(tool, "function")? {
                declared |= doc.equal_strings(name, doc.required(function, "name")?)?;
            }
        }
        // Server tools keep their native declaration, bound to a tools entry.
        if let Some(declarations) = doc.member(x, "tools")? {
            for entry in doc.children(declarations)? {
                let bound = doc
                    .origin(entry)?
                    .is_some_and(|(tool, _)| doc.node(tool).is_ok_and(|node| node.parent == Some(tools)));
                if bound
                    && let Some(declaration) = doc.member(entry, "declaration")?
                    && let Some(server) = doc.member(declaration, "name")?
                {
                    declared |= doc.equal_strings(name, server)?;
                }
            }
        }
        if !declared {
            return Err(Error::Schema);
        }
    } else if !["auto", "none", "required"]
        .iter()
        .try_fold(false, |yes, value| Ok::<_, Error>(yes || doc.string_is(choice, value)?))?
    {
        return Err(Error::Unsupported);
    }
    Ok(())
}
fn validate_part(view: RequestView<'_>, calls: Calls, message: usize, part: usize) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "text")? || doc.string_is(kind, "reasoning")? {
        super::string(doc, doc.required(part, "text")?)?;
    } else if doc.string_is(kind, "redacted")? {
        super::string(doc, doc.required(part, "data")?)?;
    } else if doc.string_is(kind, "image")? || doc.string_is(kind, "document")? {
        let source = doc.required(part, "source")?;
        if super::is(doc, source, "kind", "data")? {
            view.media
                .get(doc, doc.required(source, "ref")?)?
                .ok_or(Error::Position)?;
        } else if super::is(doc, source, "kind", "uri")? {
            super::string(doc, doc.required(source, "uri")?)?;
        } else if super::is(doc, source, "kind", "file")? {
            super::string(doc, doc.required(source, "file_id")?)?;
        } else {
            return Err(Error::Unsupported);
        }
    } else if doc.string_is(kind, "tool_result")? {
        linked(doc, calls, message, Some(part), doc.required(part, "tool_call_id")?)?;
        if let Some(content) = doc.member(part, "content")? {
            if !matches!(doc.node(content)?.value, Value::Array) {
                return Err(Error::Schema);
            }
            for child in doc.children(content)? {
                validate_part(view, calls, message, child)?;
            }
        }
        if let Some(error) = doc.member(part, "is_error")?
            && !matches!(doc.node(error)?.value, Value::Bool(_))
        {
            return Err(Error::Schema);
        }
    } else if doc.string_is(kind, "opaque")? {
        if !super::is(doc, part, "protocol", "anthropic")? {
            return Err(Error::Unsupported);
        }
    } else {
        return Err(Error::Unsupported);
    }
    Ok(())
}
/// The messages array and the latest message before the current one that has calls.
type Calls = (usize, Option<usize>);
/// Links a result to its call: first in its own message (server results only), then in
/// the latest earlier message with calls, and only then in any earlier message.
fn linked(doc: &Document, calls: Calls, message: usize, result: Option<usize>, id: usize) -> Result<()> {
    super::string(doc, id)?;
    let result_order = result
        .map(|part| super::content::history(doc, part))
        .transpose()?
        .flatten();
    let server = result_order
        .map(|entry| doc.member(entry, "server_result"))
        .transpose()?
        .flatten()
        .map(|node| doc.node(node))
        .transpose()?
        .is_some_and(|node| matches!(node.value, Value::Bool(true)));
    let (messages, recent) = calls;
    let mut found = None;
    for prior in [server.then_some(message), recent].into_iter().flatten() {
        if found.is_none() {
            found = call(doc, prior, id)?.map(|call| (prior, call));
        }
    }
    if found.is_none() {
        for prior in doc.children(messages)? {
            if prior == message {
                break;
            }
            if let Some(call) = call(doc, prior, id)? {
                found = Some((prior, call));
                break;
            }
        }
    }
    let (prior, call) = found.ok_or(Error::Schema)?;
    if server != super::is(doc, call, "type", "server")? {
        return Err(Error::Schema);
    }
    if prior == message {
        let before = super::content::history(doc, call)?.ok_or(Error::Schema)?;
        let after = result_order.ok_or(Error::Schema)?;
        if doc.unsigned(doc.required(before, "block_index")?)? >= doc.unsigned(doc.required(after, "block_index")?)? {
            return Err(Error::Schema);
        }
    }
    Ok(())
}
/// The call in `message` whose id equals `id`.
fn call(doc: &Document, message: usize, id: usize) -> Result<Option<usize>> {
    if let Some(calls) = doc.member(message, "tool_calls")? {
        for call in doc.children(calls)? {
            if doc.equal_strings(id, doc.required(call, "id")?)? {
                return Ok(Some(call));
            }
        }
    }
    Ok(None)
}
