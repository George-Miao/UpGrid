# Replicate recoverable target deletion

## Decision

Target deletion moves the complete `TargetState` into a replicated trash map instead of immediately destroying it. The trash entry also owns its raw and hourly history, evaluation-location count, notification-default choice, stable target ID, and deletion timestamp. The move and assignment release happen in one state-machine command.

Restore and permanent deletion are separate replicated commands. Restore is allowed only before the replicated retention deadline and puts the retained state back under the same target ID. Secret and notification channel deletion continues to reject references held by trashed targets.

The cluster stores one trash-retention window, defaulting to 30 days. Configuration on node startup updates that replicated value. Expiry pruning uses timestamps carried by commands so every replica makes the same decision. Older persisted states and snapshots migrate to an empty trash with the default window.

## Consequences

Operators can recover accidental deletion without a separate database or backup workflow, and restored targets preserve monitoring continuity. Deleted targets stop receiving assignments immediately. Trash consumes replicated storage until expiry or permanent deletion, and referenced secrets and notification channels remain protected during that period.
