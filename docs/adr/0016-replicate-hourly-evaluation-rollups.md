# Replicate hourly Evaluation rollups

Every accepted Target or Cluster Node Evaluation updates one deterministic one-hour rollup keyed by Target and scheduled-time bucket. Rollups retain sample, success, and failure counts plus latency total, minimum, and maximum. The state machine updates them only after duplicate rejection and multi-location aggregation, preserving exactly one contribution per authoritative interval result.

Raw Evaluations remain available for short-term diagnostics under `history_retention_hours`. Hourly rollups use an independent replicated `history_rollup_retention_days` window, defaulting to 365 days. This bounds Raft state while retaining enough precision for availability and latency trends without introducing an external database.

The authenticated history API exposes rollups chronologically through bounded time ranges and result pages with an exclusive cursor. External systems archive from this API rather than reading storage files, so exports retain the same linearizable-read semantics as other Cluster state.
