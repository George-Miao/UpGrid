# Authenticate node admission

Superseded by ADR 0015 for token reuse and revocation. The mutual-TLS identity and authenticated admission requirements remain in force.

The deployment key encrypts replicated secrets. A separate user-provided QUIC certificate-authority key can define transport trust; when absent, UpGrid derives it from the deployment key for compatibility. Each process creates an ephemeral node key and a CA-signed certificate for its advertised hostname. All inter-node QUIC connections require certificates from that CA in both directions. Join links carry both long-lived keys so admitted nodes use the same trust root.

An administrator creates a single-use, expiring admission through the authenticated cluster API. The leader consumes its hash through Raft before changing membership, so reuse and leader failover cannot admit a second node. Possession of both long-lived keys and an unused token is required. This replaces the prototype's verification-disabled self-signed certificates and unauthenticated join RPC. ADR 0014 replaces the operator-facing provisioning ceremony while preserving these security properties.
