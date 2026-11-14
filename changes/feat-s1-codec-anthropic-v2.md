Internal

- Add the Anthropic Messages boundary codec: request normalization and encoding, the response event state machine, media, server tools, MCP namespaces, cache directives, usage projection and unknown-field preservation, with errors reported as `stream.invalid_input` (400 for client input, 502 for upstream).
- Reject duplicate tool call IDs, and link tool results to their calls and response blocks to their identities through quota-charged indexes, so request and response work stays linear or near-linear in input size.
