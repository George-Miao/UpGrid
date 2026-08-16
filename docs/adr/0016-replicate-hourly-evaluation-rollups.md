# Replicate hourly evaluation rollups

Every accepted target or cluster node evaluation updates one deterministic one-hour rollup keyed by target and scheduled-time bucket. Rollups retain sample, success, and failure counts plus latency total, minimum, and maximum. The state machine updates them only after duplicate rejection and multi-location aggregation, preserving exactly one contribution per authoritative interval result.

Raw evaluations remain available for short-term diagnostics under `history_retention_hours`. Hourly rollups use an independent replicated `history_rollup_retention_days` window, defaulting to 365 days. This bounds Raft state while retaining enough precision for availability and latency trends without introducing an external database.

The authenticated history API exposes rollups chronologically through bounded time ranges and result pages with an exclusive cursor. External systems archive from this API rather than reading storage files, so exports retain the same linearizable-read semantics as other cluster state.
