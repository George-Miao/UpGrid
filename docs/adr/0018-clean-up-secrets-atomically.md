# Clean up unreferenced secrets atomically

## Decision

The replicated application state derives secret references from active targets, trashed targets, and notification channels. Secret listing reports whether each secret appears in that complete reference set.

Bulk cleanup is one state-machine command. It recomputes references when the command commits, removes only secret IDs absent from that set, and returns the deterministically ordered deleted IDs. The preview is informative; it is never trusted as authorization to delete a specific secret.

## Consequences

Operators can discover and remove orphaned encrypted material without inspecting Raft storage or racing concurrent target and channel changes. Secrets retained only by recoverable targets remain protected. Reference discovery scans replicated configuration, which is acceptable for an explicit operator operation and avoids maintaining a second index that could drift.
