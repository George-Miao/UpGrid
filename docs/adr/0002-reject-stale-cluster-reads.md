# Reject stale cluster reads

Cluster API reads are linearizable by default: a Node establishes a Raft read barrier, waits for its local state machine to apply through that index, and only then serves the response. If freshness cannot be established, the API returns `503 Service Unavailable` instead of silently returning potentially stale Target state. This sacrifices read availability during partitions for a trustworthy cluster view; explicitly node-local diagnostics do not use this contract.
