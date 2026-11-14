Changed

- The stream pool serves small and concurrent streams faster: small single-frame requests run at about 380k req/s instead of 244k, and 64 concurrent small streams at about 1.8M req/s instead of 651k, on the bench/stream gate.
- Stream cache eviction orders streams by last read at 1 ms resolution; streams read within the same millisecond are unordered.
- Reader leases that span several frames count toward the per-stream metadata limit.
