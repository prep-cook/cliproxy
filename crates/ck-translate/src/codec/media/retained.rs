//! Registration of media payloads rebuilt from trusted canonical records.
use super::{Encoding, Entry, Media, Registry, lineage, local_id};
use crate::codec::{
    Error, Result,
    document::{Document, Text, Value},
    event::LocalId,
    table::Table,
};

impl Registry {
    /// Registers a payload rebuilt from its records at its reference `node`. `id` must be the
    /// next registry ID, so a later registration cannot reuse it. A payload whose reference did
    /// not survive only reserves its ID.
    pub(crate) fn adopt(
        &mut self,
        doc: &mut Document,
        node: Option<usize>,
        (id, kind, encoding): (LocalId, &'static str, Encoding),
        chunks: Table<Text>,
    ) -> Result<()> {
        self.validate_document(doc)?;
        if id != local_id(b'm', self.entries.len() as u64)? {
            return Err(Error::Schema);
        }
        self.reserve()?;
        let media = Media {
            node: node.unwrap_or(usize::MAX),
            id,
            data: None,
            encoding,
            kind,
            mime: None,
        };
        let Some(node) = node else {
            self.entries.push(Entry {
                media,
                lineage: [None; 128],
                chunks: None,
                active: false,
            });
            return Ok(());
        };
        self.document = Some(doc.id());
        if matches!(encoding, Encoding::Utf8) {
            doc.replace(node, Value::Utf8Media)?;
            for text in chunks.iter() {
                let child = doc.add(None, Value::String(*text))?;
                doc.append(node, child)?;
            }
        }
        self.entries.push(Entry {
            media,
            lineage: lineage(doc, node)?,
            chunks: Some(chunks),
            active: true,
        });
        self.active.insert(node, self.entries.len() - 1);
        Ok(())
    }
}
