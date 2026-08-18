# Aggregate optional multi-location evaluations

Targets may request between 1 and 32 evaluation locations. The leader assigns one location per distinct eligible voting node, capped by the current number of eligible voters. A target configured for one location retains the original scheduling behavior. This supersedes the one-node-per-interval decision in ADR 0001 when a target explicitly requests more than one location.

All assignments for one target interval are committed in the same bounded Raft command. The replicated state records the expected result count before probes complete. Timed-out locations are replaced without increasing that count, and draining nodes receive no new assignments. Assignment batches never split the locations belonging to one interval.

The state machine waits for every expected location result, then commits one deterministic aggregate evaluation. The interval succeeds only when every location succeeds. Its recorded time and latency are the maxima, received bytes are summed, and a failed aggregate identifies the failed-location count plus bounded per-node diagnostics. Availability transitions, history, and alerts consume only this aggregate, preserving one authoritative outcome per interval.
