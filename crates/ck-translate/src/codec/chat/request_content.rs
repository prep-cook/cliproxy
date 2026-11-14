use super::request::require_string;
use crate::codec::{
    Error, RequestView, Result,
    document::{Document, Name, Value},
    encode::{self, Encoder},
    media::{Encoding, Media, Registry},
};
use ck_stream::Writer;
/// Chat wire part types that ingress normalizes; any other type becomes an opaque part.
#[derive(Clone, Copy)]
enum WirePart {
    Text,
    Media(Carrier),
    Refusal,
}

/// The native member that carries a media part's payload.
#[derive(Clone, Copy)]
enum Carrier {
    Image,
    Audio,
    File,
}

impl Carrier {
    fn key(self) -> &'static str {
        match self {
            Self::Image => "image_url",
            Self::Audio => "input_audio",
            Self::File => "file",
        }
    }

    fn data_field(self) -> &'static str {
        match self {
            Self::Image => "url",
            Self::Audio => "data",
            Self::File => "file_data",
        }
    }
}

/// Compares the type in the order the accept checks run and stops at the first match.
fn classify(doc: &Document, kind: usize) -> Result<Option<WirePart>> {
    for text in ["text", "reasoning", "reasoning_summary"] {
        if doc.string_is(kind, text)? {
            return Ok(Some(WirePart::Text));
        }
    }
    for carrier in [Carrier::Image, Carrier::Audio, Carrier::File] {
        if doc.string_is(kind, carrier.key())? {
            return Ok(Some(WirePart::Media(carrier)));
        }
    }
    Ok(doc.string_is(kind, "refusal")?.then_some(WirePart::Refusal))
}

pub(super) fn normalize_part(
    doc: &mut Document,
    registry: &mut Registry,
    owner: &mut Writer,
    part: usize,
) -> Result<()> {
    let kind = doc.required(part, "type")?;
    let carrier = match classify(doc, kind)? {
        Some(WirePart::Text) => return require_string(doc, doc.required(part, "text")?),
        Some(WirePart::Media(carrier)) => carrier,
        Some(WirePart::Refusal) => {
            if let Some(value) = doc.remove(part, "refusal")? {
                doc.rename(value, "text")?;
                doc.append(part, value)?;
            }
            return Ok(());
        }
        None => return opaque(doc, part),
    };
    let mut early = None;
    if let Some(native) = doc.member(part, carrier.key())?
        && let Some(data) = doc.member(native, carrier.data_field())?
    {
        early = registry.get(doc, data)?;
    }
    preserve_native(doc, owner, part, carrier)?;
    match carrier {
        Carrier::Image => normalize_image(doc, registry, owner, part, kind, early),
        Carrier::Audio => normalize_audio(doc, registry, part, kind, early),
        Carrier::File => normalize_file(doc, registry, owner, part, kind, early),
    }
}

/// Moves native members that collide with canonical names into provenance before any rewrite.
fn preserve_native(doc: &mut Document, owner: &mut Writer, part: usize, carrier: Carrier) -> Result<()> {
    super::provenance::preserve_request_members(
        doc,
        owner,
        part,
        part,
        None,
        &["source", "mime", "detail", "filename", "transcript", "timing"],
    )?;
    let native = doc.required(part, carrier.key())?;
    if matches!(carrier, Carrier::File) {
        super::provenance::preserve_request_members(
            doc,
            owner,
            native,
            part,
            Some(carrier.key()),
            &["kind", "uri", "ref", "source"],
        )?;
    } else {
        super::provenance::preserve_request_members(
            doc,
            owner,
            native,
            part,
            Some(carrier.key()),
            &["kind", "uri", "ref", "file_id", "source"],
        )?;
    }
    super::provenance::native_members(
        doc,
        owner,
        part,
        carrier.key(),
        match carrier {
            Carrier::Image => &["url", "detail"],
            Carrier::Audio => &["data", "format"],
            Carrier::File => &["file_id", "file_data", "filename"],
        },
    )
}

fn normalize_image(
    doc: &mut Document,
    registry: &mut Registry,
    owner: &mut Writer,
    part: usize,
    kind: usize,
    early: Option<Media>,
) -> Result<()> {
    let image = doc.remove(part, "image_url")?.ok_or(Error::Schema)?;
    let url = doc.remove(image, "url")?.ok_or(Error::Schema)?;
    if early.is_none_or(|media| matches!(media.encoding, Encoding::Utf8)) {
        require_string(doc, url)?;
    }
    doc.replace(kind, Value::Text("image"))?;
    let source = doc.set(part, "source", Value::Object)?;
    if let Some(media) = early.filter(|media| !matches!(media.encoding, Encoding::Utf8)) {
        doc.set(source, "kind", Value::Text("data"))?;
        doc.rename(url, "ref")?;
        doc.append(source, url)?;
        if let Some(mime) = media.mime {
            let target = doc.set(part, "mime", Value::Null)?;
            doc.transplant(target, mime)?;
            registry.update_mime(url, Some(target));
        }
    } else if let Some((encoding, mime)) = data_uri(doc, owner, part, url)? {
        let Value::String(text) = doc.node(url)?.value else {
            return Err(Error::Schema);
        };
        doc.set(source, "kind", Value::Text("data"))?;
        doc.rename(url, "ref")?;
        doc.append(source, url)?;
        registry.register(doc, url, text, encoding, "image", Some(mime))?;
    } else {
        doc.set(source, "kind", Value::Text("uri"))?;
        doc.rename(url, "uri")?;
        doc.append(source, url)?;
    }
    if let Some(detail) = doc.remove(image, "detail")? {
        doc.append(part, detail)?;
    }
    // Noncolliding native attributes remain attached to the typed source.
    while let Some(extra) = doc.node(image)?.first {
        doc.detach(image, extra)?;
        doc.append(source, extra)?;
    }
    Ok(())
}

fn normalize_audio(
    doc: &mut Document,
    registry: &mut Registry,
    part: usize,
    kind: usize,
    early: Option<Media>,
) -> Result<()> {
    let audio = doc.remove(part, "input_audio")?.ok_or(Error::Schema)?;
    let data = doc.remove(audio, "data")?.ok_or(Error::Schema)?;
    let text = if early.is_some() {
        None
    } else {
        let Value::String(text) = doc.node(data)?.value else {
            return Err(Error::Schema);
        };
        Some(text)
    };
    let format = doc.member(audio, "format")?;
    doc.replace(kind, Value::Text("audio"))?;
    let source = doc.set(part, "source", Value::Object)?;
    doc.set(source, "kind", Value::Text("data"))?;
    doc.rename(data, "ref")?;
    doc.append(source, data)?;
    let mime = if let Some(format) = format {
        if doc.string_is(format, "wav")? {
            Some(doc.set(part, "mime", Value::Text("audio/wav"))?)
        } else if doc.string_is(format, "mp3")? {
            Some(doc.set(part, "mime", Value::Text("audio/mpeg"))?)
        } else {
            None
        }
    } else {
        None
    };
    while let Some(extra) = doc.node(audio)?.first {
        doc.detach(audio, extra)?;
        doc.append(source, extra)?;
    }
    if let Some(text) = text {
        registry.register(doc, data, text, Encoding::Base64, "audio", mime)?;
    } else {
        registry.update_mime(data, mime);
    }
    Ok(())
}

fn normalize_file(
    doc: &mut Document,
    registry: &mut Registry,
    owner: &mut Writer,
    part: usize,
    kind: usize,
    early: Option<Media>,
) -> Result<()> {
    let file = doc.remove(part, "file")?.ok_or(Error::Schema)?;
    doc.replace(kind, Value::Text("document"))?;
    let source = doc.set(part, "source", Value::Object)?;
    if doc.member(file, "file_id")?.is_some() && doc.member(file, "file_data")?.is_some() {
        return Err(Error::Schema);
    }
    if let Some(id) = doc.remove(file, "file_id")? {
        require_string(doc, id)?;
        doc.set(source, "kind", Value::Text("file"))?;
        doc.append(source, id)?;
    } else if let Some(data) = doc.remove(file, "file_data")? {
        let text = if early.is_some() {
            None
        } else {
            let Value::String(text) = doc.node(data)?.value else {
                return Err(Error::Schema);
            };
            Some(text)
        };
        doc.set(source, "kind", Value::Text("data"))?;
        doc.rename(data, "ref")?;
        doc.append(source, data)?;
        if let Some(text) = text {
            let (encoding, mime) = match data_uri(doc, owner, part, data)? {
                Some((encoding, mime)) => (encoding, Some(mime)),
                None => (Encoding::Base64, None),
            };
            registry.register(doc, data, text, encoding, "document", mime)?;
        } else if let Some(mime) = early.and_then(|media| media.mime) {
            let target = doc.set(part, "mime", Value::Null)?;
            doc.transplant(target, mime)?;
            registry.update_mime(data, Some(target));
        }
    } else {
        return Err(Error::Schema);
    }
    if let Some(filename) = doc.remove(file, "filename")? {
        doc.append(part, filename)?;
    }
    while let Some(extra) = doc.node(file)?.first {
        doc.detach(file, extra)?;
        doc.append(source, extra)?;
    }
    Ok(())
}

/// Keeps an unrecognized part verbatim under `value` as an opaque Chat part.
fn opaque(doc: &mut Document, part: usize) -> Result<()> {
    let native = doc.add(Some(Name::Static("value")), Value::Object)?;
    while let Some(child) = doc.node(part)?.first {
        doc.detach(part, child)?;
        doc.append(native, child)?;
    }
    while let Some(entry) = doc.origins(part)?.next() {
        let (_, carrier) = doc.origin(entry)?.ok_or(Error::Schema)?;
        doc.bind_origin(entry, native, carrier)?;
    }
    doc.set(part, "type", Value::Text("opaque"))?;
    doc.set(part, "protocol", Value::Text("chat"))?;
    doc.append(part, native)
}

pub(super) fn write_part(view: RequestView<'_>, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let kind = doc.required(part, "type")?;
    if doc.string_is(kind, "opaque")? {
        return super::request_controls::write(doc, doc.required(part, "value")?, e);
    }
    if doc.string_is(kind, "refusal")? {
        e.bytes(b"{\"type\":\"refusal\",\"refusal\":")?;
        doc.write(doc.required(part, "text")?, e)?;
        super::response::members(doc, part, e, &["type", "text"])?;
        super::request_controls::members(doc, part, &mut false, e)?;
        super::provenance::request_members(doc, part, None, &[], &[], e)?;
        return e.bytes(b"}");
    }
    if doc.string_is(kind, "image")? {
        write_image(view, part, e)
    } else if doc.string_is(kind, "audio")? {
        write_source_part(view, part, true, e)
    } else if doc.string_is(kind, "document")? {
        write_source_part(view, part, false, e)
    } else {
        super::request_controls::write(doc, part, e)
    }
}

fn write_image(view: RequestView<'_>, part: usize, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let source = doc.required(part, "source")?;
    e.bytes(b"{\"type\":\"image_url\",\"image_url\":{\"url\":")?;
    let source_kind = doc.required(source, "kind")?;
    if doc.string_is(source_kind, "data")? {
        view.media
            .write_value(doc, doc.required(source, "ref")?, (doc.member(part, "mime")?, true), e)?;
    } else if doc.string_is(source_kind, "uri")? {
        doc.write(doc.required(source, "uri")?, e)?;
    } else {
        return Err(Error::Unsupported);
    }
    if let Some(detail) = doc.member(part, "detail")? {
        e.bytes(b",\"detail\":")?;
        doc.write(detail, e)?;
    }
    super::response::members(doc, source, e, &["kind", "uri", "ref"])?;
    super::provenance::request_members(
        doc,
        part,
        Some("image_url"),
        &["url", "detail"],
        &["kind", "uri", "ref"],
        e,
    )?;
    e.bytes(b"}")?;
    super::request_controls::members(doc, part, &mut false, e)?;
    super::response::members(doc, part, e, &["type", "source", "detail", "mime"])?;
    super::provenance::request_members(
        doc,
        part,
        None,
        &["type", "image_url"],
        &["type", "source", "detail", "mime"],
        e,
    )?;
    e.bytes(b"}")
}

/// Writes an audio part as `input_audio`, or a document part as `file`.
fn write_source_part(view: RequestView<'_>, part: usize, audio: bool, e: &mut Encoder<'_, '_>) -> Result<()> {
    let doc = view.document;
    let source = doc.required(part, "source")?;
    e.bytes(if audio {
        b"{\"type\":\"input_audio\",\"input_audio\":{"
    } else {
        b"{\"type\":\"file\",\"file\":{"
    })?;
    let source_kind = doc.required(source, "kind")?;
    if doc.string_is(source_kind, "data")? {
        let reference = doc.required(source, "ref")?;
        e.string(if audio { "data" } else { "file_data" })?;
        e.bytes(b":")?;
        // Audio names its media type in `format`; `file_data` carries it as a data URI.
        view.media
            .write_value(doc, reference, (doc.member(part, "mime")?, !audio), e)?;
    } else if !audio && doc.string_is(source_kind, "file")? {
        e.bytes(b"\"file_id\":")?;
        doc.write(doc.required(source, "file_id")?, e)?;
    } else {
        return Err(Error::Unsupported);
    }
    if let Some(format) = doc.member(source, "format")? {
        e.bytes(b",\"format\":")?;
        doc.write(format, e)?;
    }
    if let Some(filename) = doc.member(part, "filename")? {
        e.bytes(b",\"filename\":")?;
        doc.write(filename, e)?;
    }
    super::response::members(doc, source, e, &["kind", "ref", "file_id", "format"])?;
    super::provenance::request_members(
        doc,
        part,
        Some(if audio { "input_audio" } else { "file" }),
        if audio {
            &["data", "format"]
        } else {
            &["file_data", "file_id", "filename"]
        },
        &["kind", "ref", "file_id"],
        e,
    )?;
    e.bytes(b"}")?;
    super::request_controls::members(doc, part, &mut false, e)?;
    super::response::members(doc, part, e, &["type", "source", "filename", "mime"])?;
    super::provenance::request_members(
        doc,
        part,
        None,
        &["type", "input_audio", "file"],
        &["type", "source", "filename", "mime"],
        e,
    )?;
    e.bytes(b"}")
}

pub(super) fn data_uri(
    doc: &mut Document,
    owner: &mut Writer,
    part: usize,
    data: usize,
) -> Result<Option<(Encoding, usize)>> {
    let Some((encoding, lease)) = data_uri_metadata(doc, owner, data)? else {
        return Ok(None);
    };
    let parsed = doc.import_json(owner, lease)?;
    let mime = doc.set(part, "mime", Value::Null)?;
    doc.transplant(mime, parsed)?;
    Ok(Some((encoding, mime)))
}

pub(super) fn data_uri_metadata(
    doc: &Document,
    owner: &mut Writer,
    data: usize,
) -> Result<Option<(Encoding, ck_stream::ScratchLease)>> {
    let mut chars = doc.chars(data)?;
    for expected in "data:".chars() {
        if chars.next_scalar()?.is_none_or(|c| !c.eq_ignore_ascii_case(&expected)) {
            return Ok(None);
        }
    }
    let mut count = 0u64;
    let mut suffix = ['\0'; 7];
    loop {
        let c = chars.next_scalar()?.ok_or(Error::Schema)?;
        if c == ',' {
            break;
        }
        if !c.is_ascii() || c.is_ascii_control() {
            return Err(Error::Schema);
        }
        suffix.rotate_left(1);
        suffix[6] = c;
        count = count.checked_add(1).ok_or(Error::Limit)?;
    }
    if suffix != [';', 'b', 'a', 's', 'e', '6', '4'] {
        return Err(Error::Unsupported);
    }
    let mime_chars = count.checked_sub(7).ok_or(Error::Schema)?;
    let lease = encode::scratch(owner, |e| {
        let mut chars = doc.chars(data)?;
        for _ in 0..5 {
            chars.next_scalar()?;
        }
        e.bytes(b"\"")?;
        for _ in 0..mime_chars {
            let c = chars.next_scalar()?.ok_or(Error::Schema)?;
            e.escaped(c.encode_utf8(&mut [0; 4]))?;
        }
        e.bytes(b"\"")
    })?;
    Ok(Some((Encoding::DataUri { skip: count + 6 }, lease)))
}
