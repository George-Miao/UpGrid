# Repository Guidelines

## Project Structure & Module Organization

UpGrid is a Rust 2024 binary crate with a Lit/TypeScript WebUI. `src/main.rs` wires configuration, Raft, workers, and Axum. Consensus and persistence live in `src/raft.rs`, `src/node.rs`, `src/storage.rs`, and `src/state_machine.rs`; inter-Node RPC belongs under `src/network/`. The replicated model is in `src/domain.rs`. UI source and browser tests live under `frontend/src/` and `frontend/tests/`; Vite's checked-in `frontend/dist/` output is embedded by `src/web.rs`. Design decisions live under `docs/adr/`.

## Build, Test, and Development Commands

- `nix develop` enters the pinned nightly Rust, Node 22, and pnpm development shell.
- `cargo build` compiles the debug binary and locked dependencies.
- `cargo run` starts the current binary; set `RUST_LOG=debug` to increase tracing output.
- `scripts/update-webui.sh` installs locked frontend dependencies and rebuilds `frontend/dist/`.
- `scripts/test-webui.sh` runs Playwright in Chromium against an isolated real Node.
- `cargo fmt --all -- --check` verifies formatting; run `cargo fmt --all` to apply it.
- `cargo clippy --all-targets --all-features -- -D warnings` runs lint checks.
- `cargo test --no-run` compiles all tests without starting network fixtures.

## Coding Style & Naming Conventions

Use standard `rustfmt` output (four-space indentation and trailing commas). Name Rust modules and functions with `snake_case`, types with `UpperCamelCase`, and constants with `SCREAMING_SNAKE_CASE`. TypeScript uses two spaces, strict types, `camelCase` members, and `kebab-case` custom-element names. Prefer typed API boundaries, existing `snafu` contexts, and `tracing` over ad hoc strings or printing.

## Testing Guidelines

Use `#[compio::test]` for async Rust tests and local `#[cfg(test)]` modules for units. Name tests by behavior, for example `rejects_invalid_scheme`. Browser workflows use Playwright against the real embedded app; avoid tests coupled to Lit internals. Fixed-port `master`/`worker` fixtures are manual, so never run the unfiltered Rust suite in automation. Avoid port collisions and terminate spawned tasks.

## Commit & Pull Request Guidelines

History is sparse and includes `wip` commits plus `feat: Init commit`. For new work, use concise Conventional Commit subjects such as `fix: handle expired RPC deadlines`, and squash temporary `wip` commits before review. Pull requests should explain the behavior change, list verification commands, link related issues, and call out networking, storage, or dependency-lock changes. Include logs or screenshots only when they clarify observable behavior.

## Configuration & Security

Do not commit secrets or machine-specific environment files. Keep `Cargo.lock` and `flake.lock` updated intentionally, especially because several dependencies are sourced from Git branches.
