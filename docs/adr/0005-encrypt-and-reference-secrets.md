# Encrypt and reference secrets

Sensitive Target and notification values are stored as named, write-only Secrets and referenced from configuration rather than embedded as plaintext. The Cluster API returns only redacted metadata, while Raft replicates ciphertext encrypted with a deployment key available to every Node. This adds key-management requirements but prevents routine API reads, snapshots, and diagnostics from exposing credentials.
