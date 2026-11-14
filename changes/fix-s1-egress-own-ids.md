Fixed

- When an upstream Chat Completions, Responses API or Anthropic Messages reply has no response id, clients receive an id the gateway generates (`chatcmpl-…`, `msg_…` or `resp_…`) instead of the gateway's internal stream id.
- A streamed response keeps the first id a client sees. A buffered response carries the upstream's response id if it arrives at any point, for example only in the final Responses event, so clients can continue from it.
