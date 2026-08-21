# Separate node identity from network reachability

UpGrid currently uses one Raft URL as both a node identity attribute, a local listener, and the address stored for other nodes to dial. This coupling does not support NAT, multiple interfaces, asymmetric routes, or address discovery. A node will therefore be identified only by its durable Node ID, while local addresses, reachable addresses, and route connectivity have separate ownership and lifecycles.

## Decision

A node owns a set of local addresses and one shared Raft port. Local addresses are startup-only configuration, accept only IP literals or wildcards, are not replicated, and default to loopback when omitted.

The cluster owns each node's set of complete `up://` reachable addresses. A node can provide multiple configured addresses, and only that node can replace its configured set. Configured addresses are trusted immediately, remain until explicit replacement or node removal, and are preferred when dialing. A node can also have discovered addresses at the same time, with provenance retained for every address.

Discovery produces reachable address candidates. A candidate becomes a reachable address after another node successfully connects to it. Discovered addresses use renewable leases and expire when they are not renewed. Third-party services can discover direct addresses but do not relay cluster traffic. A node can start joining without a configured reachable address and remain a learner while discovery and verification complete.

Route connectivity is observed locally for a directed pair of one source node and one destination reachable address; individual observations are not replicated. Different source nodes can use different reachable addresses for the same destination. Node-to-node connectivity succeeds when at least one route succeeds. Admission requires a complete directed connectivity matrix involving the learner, with bounded retries; the learner is promoted only after every ordered node pair has a working route. The checks run immediately after membership changes and continue after admission. Repeated loss of any required node-to-node connection degrades cluster status and raises an alert; recovery clears the degraded status automatically.

## Consequences

Changing network addresses does not change node identity. Deployments can use different local and reachable addresses across NAT, VPN, container, and multi-interface boundaries. The current single `raft_url` configuration and single-address Raft membership model must be replaced. Exact discovery providers, lease duration, retry timing, result aggregation, and WebUI presentation remain implementation decisions for the backlog item.