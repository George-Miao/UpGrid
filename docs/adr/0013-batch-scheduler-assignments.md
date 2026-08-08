# Batch scheduler assignments through Raft

Committing every Evaluation assignment as a separate Raft operation doubles consensus-write demand on the evaluation hot path. At 1,000 Targets per minute, this made the reference workload sensitive to filesystem latency even after moving the Raft log to transactional storage.

The leader plans assignments exactly as before, but submits up to 128 assignments in one replicated command. The state machine validates the complete batch before applying its assignments deterministically. Evaluation results remain separate durable operations because they complete independently and drive availability transitions and Alerts.

Bounding each batch avoids oversized RPCs and lets newly due work interleave with other Cluster mutations. This is an internal consensus optimization; the Cluster API and the one-authoritative-Evaluation contract do not change.
