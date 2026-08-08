# Authenticate node admission

New Nodes are provisioned with the deployment key out of band. That key deterministically derives a pinned Deployment CA; each process creates an ephemeral Node key and a CA-signed certificate for its advertised hostname. All inter-Node QUIC connections require certificates from that CA in both directions.

An administrator creates a single-use, expiring Join Token through an authenticated Cluster API. The leader consumes its hash through Raft before changing membership, so reuse and leader failover cannot admit a second Node. Possession of both the deployment key and an unused Join Token is required. This replaces the prototype's verification-disabled self-signed certificates and unauthenticated join RPC.
