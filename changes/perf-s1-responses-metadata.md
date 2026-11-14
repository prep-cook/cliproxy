Internal

- A Responses request holds less metadata per tool round: the ingress releases the wire scanner before normalization, keeps no record lease for nameless objects and arrays, and imports all original-field pointers in one source. At the 8 MiB default quota a Responses request fits 375 tool rounds instead of 260.
