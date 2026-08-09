# Provision Nodes with opaque Join Links

Superseded by ADR 0015 for token reuse, revocation, and browser onboarding. The opaque `up://` provisioning format remains in force.

Joining previously required operators to copy a Raft URL, a deployment key, and a separately issued token into three command-line options. These values form one admission capability and exposing their internal split created avoidable configuration and mismatch errors.

The Cluster API now issues a versioned, opaque `up://` Join Link. Its authority identifies the reachable bootstrap Node; its URL-safe payload carries the deployment key and a random token. `--join` parses this value through one typed interface, privately persists the key, establishes the existing mutual-TLS transport, and asks the leader to atomically consume the token before adding the Node. Token consumption uses an operation ID derived from the token and joining Node identity, so a lost response can be retried by that Node without allowing another Node to reuse the link. Restarted members use their durable identity and key without another link.

A Join Link is a bearer secret. The token's expiry and single-use state limit admission, but the embedded deployment key remains sensitive after that window. Links must therefore be transferred through a trusted channel, redacted from diagnostics, and discarded after use. A future protocol may replace embedded key transport with an online enrollment exchange; that requires a separately authenticated bootstrap transport and is not layered onto the Raft RPC interface.
