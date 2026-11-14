Internal

- The stream store reports flow counters for operators: cache bytes, cached streams, oldest read age, evictions, expiries, allocation failures, the frames charged at the last storage failure, and frames held by reader leases.
- Streams ended as gone leave the stream cache at once instead of staying until expiry.
- A reader that cannot pin a record for lack of metadata budget ends the stream with `stream.storage_exhausted` instead of failing only that read.
