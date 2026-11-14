//! Reasoning items split out of the message items of sources without native items.
use super::*;

impl<F: FnMut(&'static str, u64)> Egress<F> {
    /// Announces the pending message item when event `kind` gives it content, a content block. A
    /// call or reasoning split out of it and a dropped redacted block do not; Responses egress
    /// refuses media inside a message item.
    pub(super) fn announce_for(
        &mut self,
        owner: &mut Writer,
        (kind, node, item): (Kind, usize, usize),
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let doc = &self.document;
        let content = kind == Kind::BlockStart && !reasoning(doc, node)? && !redacted(doc, node)?;
        if self.pending == Some(item) && content {
            self.announce(owner, output)?;
        }
        Ok(())
    }

    /// Publishes `response.output_item.added` for the pending message item.
    pub(super) fn announce(
        &mut self,
        owner: &mut Writer,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<()> {
        let Some(item) = self.pending.take() else {
            return Ok(());
        };
        let doc = &self.document;
        let payload = encode::scratch(owner, |e| {
            e.bytes(b",\"output_index\":")?;
            e.unsigned(position(doc, item)?)?;
            e.bytes(b",\"item\":")?;
            write_item(doc, item, e)
        })?;
        self.emit(
            owner,
            "response.output_item.added",
            |e| write_lease(&payload, e),
            output,
        )
    }

    /// Splits reasoning block `at` out of its message item into its own reasoning item, placed
    /// before a message that has not been announced yet, and publishes its added event.
    pub(super) fn reasoning_item(
        &mut self,
        owner: &mut Writer,
        at: At,
        output: &mut impl FnMut(ScratchLease) -> Result<()>,
    ) -> Result<(usize, u64)> {
        let doc = &mut self.document;
        let output_node = doc.required(doc.root(), "output")?;
        let message = doc.required(at.item, "id")?;
        let block = doc.unsigned(doc.required(at.x, "block_index")?)?;
        let lease = encode::scratch(owner, |e| {
            e.bytes(b"\"")?;
            doc.write_contents(message, e)?;
            e.bytes(b"_r")?;
            e.unsigned(block)?;
            e.bytes(b"\"")
        })?;
        let id = doc.import_json(owner, lease)?;
        let item = doc.add(None, Value::Object)?;
        doc.append(output_node, item)?;
        doc.rename(id, "id")?;
        doc.append(item, id)?;
        doc.set(item, "type", Value::Text("reasoning"))?;
        doc.set(item, "status", Value::Text("in_progress"))?;
        doc.set(item, "summary", Value::Array)?;
        doc.bind_origin(item, at.x, Some(REASONING_ITEM))?;
        if self.pending == Some(at.item) {
            doc.detach(output_node, at.item)?;
            doc.append(output_node, at.item)?;
        }
        let index = position(doc, item)?;
        let payload = encode::scratch(owner, |e| {
            e.bytes(b",\"output_index\":")?;
            e.unsigned(index)?;
            e.bytes(b",\"item\":")?;
            write_item(doc, item, e)
        })?;
        self.emit(
            owner,
            "response.output_item.added",
            |e| write_lease(&payload, e),
            output,
        )?;
        Ok((item, index))
    }
}
