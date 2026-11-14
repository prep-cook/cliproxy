Internal

- Complete the Chat boundary codec: request egress writes each part's current MIME, rebuilt requests reject incomplete media, tool calls must end before Done, and `[DONE]` SSE fields are kept or reported as lost.
- Buffered Chat responses publish file data while the body is still arriving; an image part with an unrelated `file` member keeps its image carrier.
- Request preparation and rebuild stay linear in input size through per-array cursors, node-indexed media, a sorted tool-call index and quota-charged object member indexes.
- Add a plain base64 writer for registered media so protocol encoders can emit raw base64 sources.
