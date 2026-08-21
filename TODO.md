# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active iteration

## Backlog

- Implement ADR 0020 to separate node identity from network reachability.
  - Replace the single `raft_url` with multiple startup-only local IP addresses, one shared Raft port, and multiple optional reachable addresses. Preserve the safe loopback default.
  - Accept configured reachable addresses from command-line arguments, environment variables, and configuration files. Let only the node that owns them replace the configured set.
  - During WebUI cluster join, show reachable-address inputs with a fixed `up://` prefix and a hostname/port placeholder. Show configured values and disable their inputs. Permit an empty set while discovery is pending.
  - Replicate cluster-owned configured addresses, discovered candidates, verified reachable addresses, provenance, and renewable discovery leases. Prefer configured addresses while retaining discovered fallbacks.
  - Discover direct address candidates through other nodes and third-party discovery services. Do not include traffic relaying.
  - Track process-local route connectivity for each source node and destination reachable address. Require at least one working route for every ordered node pair.
  - Keep a new node as a learner until one complete directed connectivity sweep passes with bounded retries. Continue checks after admission, mark repeated failures as degraded cluster status, raise an alert, and clear degradation automatically after recovery.
  - Before moving this item into an active iteration, select discovery protocols, lease duration, retry timing, local-result aggregation, degraded-status surfaces, and the WebUI progress model.

## Iteration policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
