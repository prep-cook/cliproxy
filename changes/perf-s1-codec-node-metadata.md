Internal

- Codec documents keep gateway ids in a side table charged to the stream's metadata quota, so a document node shrinks from 288 to 248 bytes, and stream frame tables and record indexes grow by a quarter instead of doubling. A Chat request now fits 73 tool rounds at the default 1 MiB metadata quota instead of 72.
