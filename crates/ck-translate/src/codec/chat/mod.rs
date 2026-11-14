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
