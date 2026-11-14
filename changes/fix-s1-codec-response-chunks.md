Internal

- Buffered Chat responses hold media input by position and publish it at 28,672 bytes, at string end, on flush or after 100 ms, so an image read in small network pieces no longer uses a frame per read. `ResponseIngress` gains `flush_deadline` and `flush`, with the same contract as request ingress.
- Buffered Chat responses open the reasoning block before the answer, matching streamed responses, unless more than 1,024 bytes of `content` precede `reasoning_content` in the body.
- Held Chat input, request strings and buffered response media, is published before a terminal parse or truncation error instead of being dropped.
