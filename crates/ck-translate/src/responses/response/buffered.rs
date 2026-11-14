use super::*;
use crate::codec::{
    document::pending::Pending,
    media::{Encoding, InputDecoder, Publisher},
    source::Span,
};

/// A buffered response body parsed as it arrives, so root `audio.data` and the `result` of an
/// `image_generation_call` reach the stream in bounded chunks before the body ends (§3.4); the
/// rest of the body is validated at finish.
pub(super) struct Buffered {
    input: Pending,
    active: Option<Active>,
}

/// The open payload string: its wire node, the first payload byte and the next one to decode.
struct Active {
    wire: usize,
    start: u64,
    offset: u64,
    decoder: InputDecoder,
    /// The image media lifetime; `None` feeds the root audio in `State::audio`.
    image: Option<Publisher>,
}

impl Buffered {
    pub(super) fn new(owner: &mut Writer, limit: u64) -> Result<Self> {
        Ok(Self {
            input: Pending::new(owner, limit)?,
            active: None,
        })
    }
    pub(super) fn advance(&mut self, owner: &mut Writer, state: &mut State, bytes: &[u8]) -> Result<()> {
        for bytes in bytes.chunks(8192) {
            self.input.advance(owner, bytes)?;
            self.collect(owner, state)?;
            if self.active.is_none()
                && let Some(wire) = self.input.string
            {
                self.begin(owner, state, wire)?;
                self.collect(owner, state)?;
            }
        }
        Ok(())
    }
    pub(super) fn finish(mut self, owner: &mut Writer, state: &mut State) -> Result<()> {
        // The payload string can only close in an advance, which collects it.
        self.input.finish()?;
        let doc = &self.input.document;
        state.complete(owner, doc, doc.root())
    }

    /// Starts the media when the open string is a payload target; any other string waits for
    /// the complete body.
    fn begin(&mut self, owner: &mut Writer, state: &mut State, wire: usize) -> Result<()> {
        let data = self.input.node(wire)?;
        let doc = &self.input.document;
        let audio = audio_target(doc, data)?;
        let image = image_target(doc, data)?;
        if audio.is_none() && image.is_none() {
            return Ok(());
        }
        let scanned = self.input.scanner.pending_node(doc.input(), wire)?;
        // The payload streams from the input; the finished document keeps no second copy, so
        // the closing snapshot sees null and publishes nothing again.
        self.input.document.replace(data, Value::Null)?;
        let doc = &self.input.document;
        let image = match (audio, image) {
            (Some(audio), _) => {
                start_audio(owner, state, doc, audio)?;
                None
            }
            (None, Some((output, index))) => Some(start_image(owner, state, doc, output, index)?),
            (None, None) => return Err(Error::Schema),
        };
        self.active = Some(Active {
            wire,
            start: scanned.span.start + 1,
            offset: scanned.span.start + 1,
            decoder: InputDecoder::new(Encoding::Base64),
            image,
        });
        Ok(())
    }

    /// Publishes every validated payload byte of the open string and releases its input.
    fn collect(&mut self, owner: &mut Writer, state: &mut State) -> Result<()> {
        let Some(active) = &mut self.active else {
            return Ok(());
        };
        let input = self.input.document.input();
        let wire = self.input.scanner.pending_node(input, active.wire)?;
        let mut chars = json::Decoded::fragment(
            input,
            Span {
                start: active.offset,
                end: wire.validated_end,
            },
        )?;
        let decoder = &mut active.decoder;
        let decode = |emit: &mut dyn FnMut(&[u8]) -> Result<()>| {
            while let Some(c) = chars.next_scalar()? {
                decoder.push(c, &mut *emit)?;
            }
            Ok(())
        };
        match &mut active.image {
            None => state
                .audio
                .as_mut()
                .ok_or(Error::Schema)?
                .buffered(owner, &mut state.emitter, decode)?,
            Some(publisher) => publisher.collect(&mut state.emitter, owner, decode)?,
        }
        active.offset = wire.validated_end;
        self.input.document.release_input_range(Span {
            start: active.start,
            end: active.offset,
        })?;
        if wire.span.end != 0 {
            if !active.decoder.complete() {
                return Err(Error::Schema);
            }
            match &mut active.image {
                None => state.audio.as_mut().ok_or(Error::Schema)?.audio_done()?,
                Some(publisher) => publisher.end(&mut state.emitter, owner)?,
            }
            self.active = None;
        }
        Ok(())
    }
}

fn start_audio(owner: &mut Writer, state: &mut State, doc: &Document, audio: usize) -> Result<()> {
    if state.audio.is_some() {
        return Err(Error::Schema);
    }
    state.start(owner, Some((doc, doc.root())))?;
    let id = media::local_id(b'm', state.next_media)?;
    state.next_media = state.next_media.checked_add(1).ok_or(Error::Limit)?;
    state.audio = Some(super::super::audio::Ingress::start(
        owner,
        &mut state.emitter,
        id,
        Some((doc, audio)),
    )?);
    Ok(())
}

/// Opens output item `index` and its image media. Items open only here before finish, so the
/// open items are a prefix of `output`; the earlier ones are complete and close first, which
/// keeps the stream in output order. An item already open has a repeated `result` and is refused.
fn start_image(owner: &mut Writer, state: &mut State, doc: &Document, output: usize, index: u64) -> Result<Publisher> {
    state.start(owner, Some((doc, doc.root())))?;
    for (i, item) in doc.children(output)?.enumerate().skip(state.items.len()) {
        let at = state.add_item(owner, doc, item, i as u64)?;
        if i as u64 == index {
            let id = media::local_id(b'm', state.next_media)?;
            state.next_media = state.next_media.checked_add(1).ok_or(Error::Limit)?;
            let mut scope = state.items[at].scope(None);
            scope.media_id = Some(id);
            return super::super::image::start(owner, &mut state.emitter, scope, None);
        }
        state.snapshot_item(owner, at, doc, item, true)?;
    }
    Err(Error::Schema)
}

/// The root `audio` object whose `data` member is the string `data`.
fn audio_target(doc: &Document, data: usize) -> Result<Option<usize>> {
    let node = doc.node(data)?;
    let (Some(name), Some(audio)) = (node.name, node.parent) else {
        return Ok(None);
    };
    let parent = doc.node(audio)?;
    let Some(audio_name) = parent.name else {
        return Ok(None);
    };
    Ok((doc.name_is(name, "data")?
        && doc.name_is(audio_name, "audio")?
        && parent.parent == Some(doc.root())
        && matches!(parent.value, Value::Object))
    .then_some(audio))
}

/// The root `output` array and the index of the item whose `result` is the string `data`, when
/// that item's `type` was scanned before it as `image_generation_call`.
fn image_target(doc: &Document, data: usize) -> Result<Option<(usize, u64)>> {
    let node = doc.node(data)?;
    let (Some(name), Some(item)) = (node.name, node.parent) else {
        return Ok(None);
    };
    if !doc.name_is(name, "result")? || !is(doc, item, "type", "image_generation_call")? {
        return Ok(None);
    }
    let Some(output) = doc.node(item)?.parent else {
        return Ok(None);
    };
    let parent = doc.node(output)?;
    let Some(output_name) = parent.name else {
        return Ok(None);
    };
    if !doc.name_is(output_name, "output")?
        || parent.parent != Some(doc.root())
        || !matches!(parent.value, Value::Array)
    {
        return Ok(None);
    }
    let index = doc.children(output)?.position(|n| n == item).ok_or(Error::Schema)?;
    Ok(Some((output, index as u64)))
}
