# Replicate recoverable Target deletion

## Decision

Target deletion moves the complete `TargetState` into a replicated trash map instead of immediately destroying it. The trash entry also owns its raw and hourly history, evaluation-location count, notification-default choice, stable Target ID, and deletion timestamp. The move and assignment release happen in one state-machine command.

Restore and permanent deletion are separate replicated commands. Restore is allowed only before the replicated retention deadline and puts the retained state back under the same Target ID. Secret and Notification Channel deletion continues to reject references held by trashed Targets.

The Cluster stores one trash-retention window, defaulting to 30 days. Configuration on Node startup updates that replicated value. Expiry pruning uses timestamps carried by commands so every replica makes the same decision. Older persisted states and snapshots migrate to an empty trash with the default window.

## Consequences

Operators can recover accidental deletion without a separate database or backup workflow, and restored Targets preserve monitoring continuity. Deleted Targets stop receiving assignments immediately. Trash consumes replicated storage until expiry or permanent deletion, and referenced Secrets and Notification Channels remain protected during that period.
