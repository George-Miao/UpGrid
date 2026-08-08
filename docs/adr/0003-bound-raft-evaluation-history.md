# Bound evaluation history in Raft state

The replicated state machine retains Target configuration, current Availability State, failure streak, latest Evaluation, and a bounded raw Evaluation History. History retention defaults to 24 hours, is configurable deployment-wide, and is pruned deterministically so every Node derives the same state. This supports useful MVP timelines without allowing snapshots and memory to grow indefinitely; long-term aggregation and external archival are deferred.
