Fixed

- The stream cache no longer refuses new streams once 4,096 have been created within an hour; it evicts the least recently read stream for a header slot.
- Eviction under memory pressure invalidates only streams that return space now, and invalidates nothing when the request cannot be covered, so one failed allocation no longer ends every cached stream.
- Running out of tmpfs space evicts once before the storage error is returned.
- A panic in stream maintenance is contained and counted; the next round runs.
- A stream whose producer stops without finishing ends with `stream.producer_lost` (502) instead of looking like a client cancellation.
