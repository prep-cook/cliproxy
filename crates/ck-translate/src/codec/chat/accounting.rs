use super::response::mapped_document;
use crate::codec::{
    Error, Result,
    document::{Document, Value},
    encode,
    event::{Emitter, Kind, Scope},
    usage::Usage,
};
use ck_stream::Writer;

#[derive(Default)]
pub(super) struct Accounting {
    counts: Usage,
    native: Option<Document>,
    finalized: bool,
}

impl Accounting {
    pub(super) fn update(&mut self, owner: &mut Writer, doc: &Document, value: usize) -> Result<()> {
        if self.finalized {
            return Err(Error::Closed);
        }
        self.counts.update_chat(doc, value)?;
        if let Some(native) = &mut self.native {
            let lease = encode::scratch(owner, |e| doc.write(value, e))?;
            let incoming = native.import_json(owner, lease)?;
            merge(native, native.root(), incoming)?;
            *native = mapped_document(owner, |e| native.write(native.root(), e))?;
        } else {
            self.native = Some(mapped_document(owner, |e| doc.write(value, e))?);
        }
        Ok(())
    }

    pub(super) fn emit(
        &mut self,
        owner: &mut Writer,
        emitter: &mut Emitter,
        scope: Scope,
        final_snapshot: bool,
    ) -> Result<()> {
        if self.finalized {
            return Ok(());
        }
        if let Some(native) = &self.native {
            emitter.emit(
                owner,
                Kind::Usage,
                scope,
                |e| {
                    e.bytes(b",\"usage\":")?;
                    self.counts.write(e, final_snapshot, |e| native.write(native.root(), e))
                },
                |_| Ok(()),
            )?;
            self.finalized = final_snapshot;
        }
        Ok(())
    }
}

fn merge(doc: &mut Document, target: usize, incoming: usize) -> Result<()> {
    if matches!(doc.node(target)?.value, Value::Object) && matches!(doc.node(incoming)?.value, Value::Object) {
        while let Some(child) = doc.node(incoming)?.first {
            doc.detach(incoming, child)?;
            let name = doc.node(child)?.name.ok_or(Error::Schema)?;
            let mut existing = None;
            for current in doc.children(target)? {
                if doc.names_equal(name, doc.node(current)?.name.ok_or(Error::Schema)?)? {
                    existing = Some(current);
                    break;
                }
            }
            if let Some(existing) = existing {
                merge(doc, existing, child)?;
            } else {
                doc.append(target, child)?;
            }
        }
    } else {
        doc.transplant(target, incoming)?;
    }
    Ok(())
}
