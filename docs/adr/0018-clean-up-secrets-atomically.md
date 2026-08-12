# Clean up unreferenced Secrets atomically

## Decision

The replicated application state derives Secret references from active Targets, trashed Targets, and Notification Channels. Secret listing reports whether each Secret appears in that complete reference set.

Bulk cleanup is one state-machine command. It recomputes references when the command commits, removes only Secret IDs absent from that set, and returns the deterministically ordered deleted IDs. The preview is informative; it is never trusted as authorization to delete a specific Secret.

## Consequences

Operators can discover and remove orphaned encrypted material without inspecting Raft storage or racing concurrent Target and Channel changes. Secrets retained only by recoverable Targets remain protected. Reference discovery scans replicated configuration, which is acceptable for an explicit operator operation and avoids maintaining a second index that could drift.
