Fixed

- When an upstream Responses API reply has no response id, clients of the Chat Completions and Anthropic Messages APIs receive an id the gateway generates (`chatcmpl-…` or `msg_…`), the same on every chunk of the response, instead of the gateway's internal stream id.
