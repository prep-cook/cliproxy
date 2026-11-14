Internal

- Cut the stream codec's per-request metadata cost: sources start with one segment, adopted record leases share one source per document, Chat request rebuild keeps only each record's pointer and value, and tables grow by a quarter instead of to the next power of two. With the default 1 MiB quota a Chat request now fits about 340 plain messages instead of 59 and 72 tool rounds instead of 11.
- Streamed Chat strings hold their input by position and publish it at 28,672 bytes, at string end, on flush or after 100 ms, so small network fragments no longer use a frame each.
