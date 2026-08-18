# Encrypt and reference secrets

Sensitive target and notification values are stored as named, write-only secrets and referenced from configuration rather than embedded as plaintext. The cluster API returns only redacted metadata, while Raft replicates ciphertext encrypted with a deployment key available to every node. This adds key-management requirements but prevents routine API reads, snapshots, and diagnostics from exposing credentials.
