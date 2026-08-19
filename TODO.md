# UpGrid TODO

The MVP is complete. This file records unfinished work for future agile iterations. Ordered from higher to lower priority.

## Active iteration

- Use dynamically linked SQLite for source builds.
  - Switch dependencies to dynamic SQLite linkage.
  - Update `Cargo.lock` for the feature change.
  - Document the system SQLite build dependency.
  - Keep Nix as an optional environment setup.
  - Compile the Rust workspace with system SQLite.
  - Confirm that the binary links system SQLite.
  - Build and inspect the single-node setup guide.

- Keep danger buttons red on hover.
  - Confirm the shared hover rule overrides the danger border color.
  - Add a danger-specific hover style.
  - Build and lint the frontend styles.
  - Verify the danger hover color in Chromium.

## Backlog

- No backlog items.

## Iteration policy

Before starting an item, define its acceptance criteria and move only that coherent slice into the active iteration. Remove completed items after their implementation and acceptance evidence are committed.
