//! Anthropic Messages boundary codecs over the shared canonical stream.

mod content;
pub mod egress;
mod index;
pub mod request;
mod request_encode;
mod request_schema;
pub mod response;
mod usage;

use crate::boundary::{failure, is, mapped, member, object, string};
use crate::codec::table::Table;
use crate::codec::{Result, document::Document, encode::Encoder};

fn unknown(doc: &Document, parent: usize, known: &[&str], e: &mut Encoder<'_, '_>) -> Result<()> {
    unknown_at(doc, &[(parent, "", known)], e)
}

fn unknown_at(doc: &Document, groups: &[(usize, &str, &[&str])], e: &mut Encoder<'_, '_>) -> Result<()> {
    crate::boundary::original(doc, "anthropic", groups, e)
}

fn server_result(doc: &Document, node: usize) -> Result<bool> {
    [
        "web_search_tool_result",
        "web_fetch_tool_result",
        "code_execution_tool_result",
        "bash_code_execution_tool_result",
        "text_editor_code_execution_tool_result",
        "tool_search_tool_result",
        "mcp_tool_result",
    ]
    .iter()
    .try_fold(false, |found, kind| Ok(found || is(doc, node, "type", kind)?))
}

fn equal_text(left: &Document, a: usize, right: &Document, b: usize) -> Result<bool> {
    let mut a = left.chars(a)?;
    let mut b = right.chars(b)?;
    loop {
        let value = a.next_scalar()?;
        if value != b.next_scalar()? {
            return Ok(false);
        }
        if value.is_none() {
            return Ok(true);
        }
    }
}
