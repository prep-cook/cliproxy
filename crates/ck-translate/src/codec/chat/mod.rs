//! Chat Completions boundary codecs. Transport, credentials and pair dispatch stay outside.
mod accounting;
mod aggregate;
pub mod egress;
mod provenance;
mod reduction;
pub mod request;
mod request_content;
mod request_controls;
mod request_provenance;
mod request_schema;
pub mod response;

pub(crate) fn prepare_response(doc: &mut crate::codec::document::Document) -> crate::codec::Result<()> {
    let x = doc.required(doc.root(), "x_ck")?;
    provenance::restore(doc, x)?;
    Ok(())
}

use super::{
    Result,
    event::{Emitter, Kind, Scope},
};
use ck_stream::Writer;

/// Streamed string input held per data record, counting an incomplete scalar or escape. Advance
/// feeds at most 8,192 bytes between steps, and decoding never grows bytes, so held input plus
/// one step stays within a 36,864-byte decoded chunk.
const HELD_BYTES: u64 = 36_864 - 8_192;
/// Collection delay of held input (docs/INTERNAL-STREAM.md, §3.4).
const FLUSH_DELAY: std::time::Duration = std::time::Duration::from_millis(100);

fn failure(emitter: &mut Emitter, owner: &mut Writer, scope: &'static str) -> Result<()> {
    emitter.emit(
        owner,
        Kind::Error,
        Scope::default(),
        |e| {
            e.bytes(b",\"code\":\"stream.invalid_input\",\"status\":")?;
            e.unsigned(if scope == "client" { 400 } else { 502 })?;
            e.bytes(b",\"message\":\"Invalid boundary input\",\"scope\":")?;
            e.string(scope)
        },
        |_| Ok(()),
    )?;
    Ok(())
}
