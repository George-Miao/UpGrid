# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active iteration

No active items.

## Backlog

- Restore custom CA bundle and mutual TLS controls to the add target form.
- Support user-provided deployment keys and QUIC certificate-authority keys.
- Use dynamically linked SQLite for source builds.
  - Switch dependencies to dynamic SQLite linkage.
  - Update `Cargo.lock` for the feature change.
  - Document the system SQLite build dependency.
  - Keep Nix as an optional environment setup.
  - Compile the Rust workspace with system SQLite.
  - Confirm that the binary links system SQLite.
  - Build and inspect the single-node setup guide.

## Iteration policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
