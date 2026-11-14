Changed

- On SIGTERM or Ctrl+C the server logs `shutdown signal received: SIGTERM` (or `SIGINT`) before it shuts down, so container logs show that a stop request arrived.
