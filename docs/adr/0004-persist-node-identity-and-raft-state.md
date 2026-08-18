# Persist node identity and Raft state

Each node owns a data directory containing its stable generated identity and durable Raft vote, log, snapshot, and state-machine data. A normal restart reuses that identity and state; loss of the data directory creates a new node that must explicitly join the cluster and must not impersonate the lost member. This replaces the prototype's restart-unsafe generated identity and in-memory stores.
