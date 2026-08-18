# Use a transactional Raft log and batched state checkpoints

The original persistence path serialized and fsynced the complete Raft log and application state after nearly every committed entry. Its cost grew with the 1,000-target reference workload and prevented evaluations from completing within one interval.

Store Raft log entries and safety metadata in redb transactions so appends update only the affected records. Keep the in-memory mirror for OpenRaft reads and migrate an existing `raft-log.postcard` into `raft-log.redb` once without deleting the legacy backup.

Checkpoint the application state after a bounded batch of applied entries, immediately on membership changes, and before publishing or installing a snapshot. Every Raft log append remains durable before acknowledgment; after a restart, OpenRaft replays committed entries newer than the last state-machine checkpoint. This preserves Raft safety while avoiding full-state fsyncs on the evaluation hot path.
