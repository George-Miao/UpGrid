# Reuse and revoke join tokens

Provisioning several nodes previously required a separately issued single-use join link for each node. This added ceremony without improving confidentiality after a link had already transported the deployment key.

A join token now has a replicated expiry and either a finite remaining-use count or unlimited uses. Authorization decrements finite counts and removes a token after its last use. Every RPC attempt uses a fresh operation ID so an earlier authorization cannot bypass exhaustion or later revocation through request deduplication. Operators can list token IDs, expirations, and remaining uses through the cluster API and revoke a token explicitly; revocation removes its hash through Raft, so every leader applies the same admission decision. The bearer secret and deployment key remain write-only and are never returned by list operations.

Fresh nodes may still pass the opaque link through `--join`. Alternatively, `--setup` opens the full authenticated cluster page in onboarding mode. Its **Join cluster** action accepts a link, shuts down the setup listener, and initializes transport and Raft with the invited deployment key. Setup refuses data directories that already contain deployment or Raft state.

Reusable links increase the consequence of accidental disclosure. Keep lifetimes short, transfer links through trusted channels, and revoke each token after its provisioning batch completes.
