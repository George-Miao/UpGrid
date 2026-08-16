# Bound evaluation history in Raft state

The replicated state machine retains target configuration, current availability state, failure streak, latest evaluation, and a bounded raw evaluation history. History retention defaults to 24 hours, is configurable deployment-wide, and is pruned deterministically so every node derives the same state. This supports useful MVP timelines without allowing snapshots and memory to grow indefinitely; long-term aggregation and external archival are deferred.
