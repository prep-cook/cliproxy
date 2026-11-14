use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Opens the audio projection for a media start.
    pub(super) fn audio_start(&mut self, node: usize) -> Result<()> {
        let doc = &mut self.document;
        self.audio.push(super::super::audio::Projection::start(doc, node)?)?;
        Ok(())
    }

    /// Index of the audio projection that owns media event `node`, if any.
    pub(super) fn audio_index(&self, node: usize) -> Result<Option<usize>> {
        let doc = &self.document;
        let x = doc.required(node, "x_ck")?;
        let Some(id) = doc.member(x, "media_id")? else {
            return Ok(None);
        };
        for (index, audio) in self.audio.iter().enumerate() {
            if super::super::snapshot::equal(doc, audio.id, doc, id)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    /// Projects audio data, transcript and end events.
    pub(super) fn audio_media(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        node: usize,
        index: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let audio = &mut self.audio[index];
        let (name, payload) = match kind {
            Kind::Data => {
                let payload = encode::scratch(owner, |e| {
                    e.bytes(b",\"delta\":")?;
                    doc.write(doc.required(node, "bytes")?, e)
                })?;
                audio.append(doc, node)?;
                ("response.audio.delta", payload)
            }
            Kind::MediaTranscript => {
                let final_value = matches!(doc.node(doc.required(node, "final")?)?.value, Value::Bool(true));
                let text = doc.required(node, "text")?;
                let nonempty = doc.chars(text)?.next_scalar()?.is_some();
                let payload = encode::scratch(owner, |e| {
                    e.bytes(b",\"delta\":")?;
                    doc.write(text, e)?;
                    super::super::member(doc, node, "timing", e)
                })?;
                audio.transcript(doc, node)?;
                if final_value {
                    if nonempty {
                        self.emit(
                            owner,
                            "response.audio.transcript.delta",
                            |e| write_lease(&payload, e),
                            output,
                        )?;
                    }
                    return self.emit(owner, "response.audio.transcript.done", |_| Ok(()), output);
                }
                ("response.audio.transcript.delta", payload)
            }
            Kind::MediaEnd => {
                audio.end(doc, node)?;
                return self.emit(owner, "response.audio.done", |_| Ok(()), output);
            }
            _ => return Err(Error::Schema),
        };
        self.emit(owner, name, |e| write_lease(&payload, e), output)
    }

    /// Retains image data, and projects a finished partial image.
    pub(super) fn image_media(
        &mut self,
        owner: &mut Writer,
        kind: Kind,
        node: usize,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &mut self.document;
        let x = doc.required(node, "x_ck")?;
        let id = doc.required(x, "media_id")?;
        let mut found = None;
        for (index, image) in self.images.iter().enumerate() {
            if super::super::snapshot::equal(doc, image.id, doc, id)? {
                found = Some(index);
                break;
            }
        }
        let image = &mut self.images[found.ok_or(Error::Schema)?];
        if kind == Kind::Data {
            return image.append(doc, node);
        }
        image.end(doc, node)?;
        if let Some(partial) = image.partial {
            if self.mode == BodyMode::Buffered {
                let array = match doc.member(doc.root(), "x_ck_opaque")? {
                    Some(n) => n,
                    None => doc.set(doc.root(), "x_ck_opaque", Value::Array)?,
                };
                let entry = doc.add(None, Value::Object)?;
                doc.append(array, entry)?;
                doc.set(
                    entry,
                    "type",
                    Value::Text("response.image_generation_call.partial_image"),
                )?;
                doc.set(entry, "output_index", Value::Unsigned(image.output_index))?;
                doc.set(entry, "partial_image_index", Value::Unsigned(partial))?;
                doc.rename(image.data, "partial_image_b64")?;
                doc.append(entry, image.data)?;
                return Ok(());
            }
            let payload = encode::scratch(owner, |e| {
                item_fields(doc, image.item, image.output_index, e)?;
                e.bytes(b",\"partial_image_index\":")?;
                e.unsigned(partial)?;
                e.bytes(b",\"partial_image_b64\":")?;
                doc.write(image.data, e)
            })?;
            return self.emit(
                owner,
                "response.image_generation_call.partial_image",
                |e| write_lease(&payload, e),
                output,
            );
        }
        Ok(())
    }

    /// Opens the image projection of an image item.
    pub(super) fn image_start(&mut self, at: At) -> Result<()> {
        let At { node, item, index, .. } = at;
        let doc = &mut self.document;
        self.images
            .push(super::super::image::Projection::start(doc, node, item, index)?)?;
        Ok(())
    }
}
