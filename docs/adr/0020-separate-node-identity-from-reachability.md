# Separate node identity from network reachability

UpGrid currently uses one reachable address as a node identity attribute, a local listener, and the address stored for other nodes to dial. This coupling does not support NAT, multiple interfaces, asymmetric routes, or address discovery. A node will therefore be identified only by its durable node ID, while local addresses, reachable addresses, and route connectivity have separate ownership and lifecycles.

## Decision

A node owns a set of local addresses and one shared Raft port. Local addresses are startup-only configuration, accept only IP literals or wildcards, are not replicated, and default to loopback when omitted.

The cluster owns each node's set of complete `up://` reachable addresses. A node can provide multiple configured addresses, and only that node can replace its configured set. Configured addresses are trusted immediately, remain until explicit replacement or node removal, and are preferred when dialing. A node can also have discovered addresses at the same time, with provenance retained for every address.

Discovery produces reachable address candidates. A candidate becomes a reachable address after another node successfully connects to it. Discovered addresses use renewable reachability leases and expire when they are not renewed. Third-party services can discover direct addresses but do not relay cluster traffic. A node can start joining without a configured reachable address and remain a learner while discovery and verification complete.

Authenticated QUIC certificates bind each connection to the caller's durable node ID. Connectivity probes return the socket address that the destination received the connection on. The leader records that address as a renewable reachability lease, so a node can change its endpoint after admission.

Route connectivity is recorded for a directed source-node and destination-node pair. Different source nodes can use different reachable addresses for the same destination. A node-to-node route succeeds when at least one address works. Admission requires a complete directed connectivity matrix involving the learner, with bounded retries. The learner is promoted only after every ordered node pair has a working route. The checks run immediately after membership changes and continue after admission. Three matching failure scans degrade cluster status and raise an alert. Three matching successful scans clear the degraded status and raise a recovery alert.

The replicated state records each availability transition independently of notification channels. For each configured channel, it also creates one durable alert.

## Consequences

Changing network addresses does not change node identity. Deployments can use different local and reachable addresses across NAT, VPN, container, and multi-interface boundaries. UpGrid uses authenticated node discovery and up to eight optional HTTP or HTTPS discovery services that return `{"addresses":["up://host:port"]}`. Discovery requests have a three-second timeout, a 64 KiB response limit, and a 32-address limit. Discovered addresses have 30-second reachability leases. Service discovery refreshes every 20 seconds, and every node refreshes the complete directed node discovery matrix every five seconds. The leader scans connectivity every two seconds and records stable results or reachability lease renewals at least every 20 seconds. The admission deadline is 20 seconds. Admission starts no new work after that deadline, and an in-flight consensus write gets one additional second to settle before cleanup starts. UpGrid keeps the node as a learner until the complete directed matrix succeeds. The WebUI shows fixed-prefix reachable-address fields, discovery service fields, configured values, and a joining state while admission is in progress.