# Authenticate node admission

Superseded by ADR 0015 for token reuse and revocation. The mutual-TLS identity and authenticated admission requirements remain in force.

The deployment key deterministically derives a pinned deployment CA; each process creates an ephemeral node key and a CA-signed certificate for its advertised hostname. All inter-node QUIC connections require certificates from that CA in both directions.

An administrator creates a single-use, expiring admission through the authenticated cluster API. The leader consumes its hash through Raft before changing membership, so reuse and leader failover cannot admit a second node. Possession of both the deployment key and an unused token is required. This replaces the prototype's verification-disabled self-signed certificates and unauthenticated join RPC. ADR 0014 replaces the operator-facing provisioning ceremony while preserving these security properties.
