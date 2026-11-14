Internal

- Streamed Chat responses hold audio bytes across SSE records and publish them at 28,672 bytes, at choice finish, on flush, after 100 ms, or before the choice's next transcript or other delta member, so a stream of small audio deltas no longer uses a 50,000 B frame per delta. `ResponseIngress::flush_deadline` reports the held audio's deadline, and held audio is published once before a terminal error.
