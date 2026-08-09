# Reuse and revoke Join Tokens

Provisioning several Nodes previously required a separately issued single-use Join Link for each Node. This added ceremony without improving confidentiality after a link had already transported the deployment key.

A Join Token is now reusable until its replicated expiry. Authorization checks the token hash without consuming it, and every RPC attempt uses a fresh operation ID so an earlier authorization cannot bypass later revocation through request deduplication. Operators can list token IDs and expirations through the Cluster API and revoke a token explicitly; revocation removes its hash through Raft, so every leader applies the same admission decision. The bearer secret and deployment key remain write-only and are never returned by list operations.

Fresh Nodes may still pass the opaque link through `--join`. Alternatively, `--setup` starts only an authenticated onboarding WebUI, accepts a link, shuts down the setup listener, and initializes transport and Raft with the invited deployment key. Setup refuses data directories that already contain deployment or Raft state.

Reusable links increase the consequence of accidental disclosure. Keep lifetimes short, transfer links through trusted channels, and revoke each token after its provisioning batch completes.
