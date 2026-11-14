Added

- Add the Gemini generateContent and streamGenerateContent boundary codec: requests and responses in JSON, SSE and JSON-array framings map to and from the internal stream, keeping part order, source role spelling, signatures, provided call IDs, native controls and unknown fields.
- Map Gemini media to mapped media records with exact decoded bytes, MIME types and video offsets as media timing, and publish unfinished request strings and media before the body ends.
- Map Gemini finish reasons and `finishMessage` in both directions, report output and per-modality usage counts, and carry a response identity that arrives after the first chunk.
- Refuse another protocol's opaque history or signatures and a `system` role inside `contents` at the Gemini request fence, and keep other protocols' unknown fields out of Gemini requests.
