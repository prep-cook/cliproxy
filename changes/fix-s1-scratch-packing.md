Internal

- Scratch leases shorter than a frame now share frames instead of taking a whole 50,000 B frame each, so a Responses request with many tool rounds no longer exhausts the 5,368-frame stream limit at about 1,068 rounds. 10,000 leases of 40 B use 8 frames instead of 10,000.
