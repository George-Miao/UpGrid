# Manage forwarding idempotency server-side

The first Node receiving a mutation assigns an internal operation ID that is preserved through forwarding and leadership retries, allowing the leader to deduplicate repeated internal delivery without requiring client idempotency keys. A new client submission remains a new operation, so clients must not automatically retry an ambiguous non-idempotent request. This narrows the guarantee to failures UpGrid can identify while keeping idempotency concerns out of the public API.
