use super::*;
use crate::codec::{
    document::pending::Pending,
    media::{Encoding, InputDecoder},
    source::Span,
};

/// A buffered response body parsed as it arrives, so root `audio.data` reaches the stream in
/// bounded chunks before the body ends (§3.4); the rest of the body is validated at finish.
pub(super) struct Buffered {
    input: Pending,
    active: Option<Active>,
}

/// The open `audio.data` string: its wire node, the first payload byte and the next one to decode.
struct Active {
    wire: usize,
    start: u64,
    offset: u64,
    decoder: InputDecoder,
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

    /// Starts the audio media when the open string is root `audio.data`; any other string waits
    /// for the complete body.
    fn begin(&mut self, owner: &mut Writer, state: &mut State, wire: usize) -> Result<()> {
        let data = self.input.node(wire)?;
        let doc = &self.input.document;
        let Some(audio) = audio_target(doc, data)? else {
            return Ok(());
        };
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
        let scanned = self.input.scanner.pending_node(doc.input(), wire)?;
        // The payload streams from the input; the finished document keeps no second copy.
        self.input.document.replace(data, Value::Null)?;
        self.active = Some(Active {
            wire,
            start: scanned.span.start + 1,
            offset: scanned.span.start + 1,
            decoder: InputDecoder::new(Encoding::Base64),
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
        let audio = state.audio.as_mut().ok_or(Error::Schema)?;
        audio.buffered(owner, &mut state.emitter, |emit| {
            while let Some(c) = chars.next_scalar()? {
                active.decoder.push(c, &mut *emit)?;
            }
            Ok(())
        })?;
        active.offset = wire.validated_end;
        self.input.document.release_input_range(Span {
            start: active.start,
            end: active.offset,
        })?;
        if wire.span.end != 0 {
            if !active.decoder.complete() {
                return Err(Error::Schema);
            }
            audio.audio_done()?;
            self.active = None;
        }
        Ok(())
    }
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
