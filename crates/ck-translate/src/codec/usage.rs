use super::{
    Result,
    document::{Document, Value},
    encode::Encoder,
};

#[derive(Clone, Copy, Default)]
enum Count {
    #[default]
    Missing,
    Null,
    Known(u64),
}

/// Supplied null invalidates a count; only omission retains the previous snapshot.
#[derive(Clone, Copy, Default)]
pub struct Usage {
    counts: [Count; 15],
    seen: bool,
    total_reported: bool,
}

const FIELDS: [&str; 9] = [
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "input_audio_tokens",
    "output_audio_tokens",
    "input_uncached_tokens",
];

const DETAILS: [&str; 6] = [
    "accepted_prediction_tokens",
    "rejected_prediction_tokens",
    "input_text_tokens",
    "output_text_tokens",
    "input_image_tokens",
    "output_image_tokens",
];

impl Usage {
    pub fn seen(&self) -> bool {
        self.seen
    }
    pub fn update_chat(&mut self, doc: &Document, usage: usize) -> Result<()> {
        let mut next = *self;
        next.apply_chat(doc, usage)?;
        *self = next;
        Ok(())
    }

    fn apply_chat(&mut self, doc: &Document, usage: usize) -> Result<()> {
        if matches!(doc.node(usage)?.value, Value::Null) {
            self.seen = true;
            self.counts.fill(Count::Null);
            self.total_reported = true;
            return Ok(());
        }
        self.seen = true;
        for (slot, name) in [(0, "prompt_tokens"), (1, "completion_tokens"), (2, "total_tokens")] {
            if let Some(node) = doc.member(usage, name)? {
                self.set(doc, slot, node)?;
                if slot == 2 {
                    self.total_reported = true;
                }
            }
        }
        for (name, fields) in [
            (
                "prompt_tokens_details",
                &[
                    (3, "cached_tokens"),
                    (4, "cache_write_tokens"),
                    (6, "audio_tokens"),
                    (8, "uncached_tokens"),
                    (11, "text_tokens"),
                    (13, "image_tokens"),
                ][..],
            ),
            (
                "completion_tokens_details",
                &[
                    (5, "reasoning_tokens"),
                    (7, "audio_tokens"),
                    (9, "accepted_prediction_tokens"),
                    (10, "rejected_prediction_tokens"),
                    (12, "text_tokens"),
                    (14, "image_tokens"),
                ][..],
            ),
        ] {
            if let Some(details) = doc.member(usage, name)? {
                if matches!(doc.node(details)?.value, Value::Null) {
                    for &(slot, _) in fields {
                        self.counts[slot] = Count::Null;
                    }
                } else {
                    for &(slot, field) in fields {
                        if let Some(node) = doc.member(details, field)? {
                            self.set(doc, slot, node)?;
                        }
                    }
                }
            }
        }
        if !self.total_reported {
            self.counts[2] = match (self.counts[0], self.counts[1]) {
                (Count::Known(a), Count::Known(b)) => Count::Known(a.checked_add(b).ok_or(super::Error::Schema)?),
                _ => Count::Missing,
            };
        }
        Ok(())
    }
    fn set(&mut self, doc: &Document, slot: usize, node: usize) -> Result<()> {
        self.counts[slot] = if matches!(doc.node(node)?.value, Value::Null) {
            Count::Null
        } else {
            Count::Known(doc.unsigned(node)?)
        };
        Ok(())
    }
    pub fn write(
        &self,
        e: &mut Encoder<'_, '_>,
        final_snapshot: bool,
        native: impl Fn(&mut Encoder<'_, '_>) -> Result<()>,
    ) -> Result<()> {
        e.bytes(b"{\"native\":{\"protocol\":\"chat\",\"value\":")?;
        native(e)?;
        e.bytes(b"},\"final\":")?;
        e.bytes(if final_snapshot { b"true" } else { b"false" })?;
        for (index, count) in self.counts[..FIELDS.len()].iter().enumerate() {
            if let Count::Known(value) = count {
                e.bytes(b",")?;
                e.string(FIELDS[index])?;
                e.bytes(b":")?;
                e.unsigned(*value)?;
            }
        }
        let mut first = true;
        for (name, count) in DETAILS.iter().zip(&self.counts[FIELDS.len()..]) {
            if let Count::Known(value) = count {
                e.bytes(if first { b",\"details\":{" } else { b"," })?;
                first = false;
                e.string(name)?;
                e.bytes(b":")?;
                e.unsigned(*value)?;
            }
        }
        if !first {
            e.bytes(b"}")?;
        }
        if !self.total_reported && matches!(self.counts[2], Count::Known(_)) {
            e.bytes(b",\"derived\":[\"total_tokens\"]")?;
        }
        e.bytes(b"}")
    }
}
