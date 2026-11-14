Added

- The OpenAI Responses protocol codec on the internal stream: request ingress with incremental publication, normalization and request fence, buffered and streaming response ingress, and buffered and streaming egress. Unknown members and events are kept as opaque carriers. Usage, identity and errors come only from recognized events, and failed responses publish only their identity and usage. Streaming output passes the official OpenAI Python SDK accumulator.
