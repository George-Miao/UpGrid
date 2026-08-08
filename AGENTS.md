# Repository Guidelines

## Project Structure & Module Organization

UpGrid is a Rust 2024 binary crate. `src/main.rs` wires configuration, the Raft Node, workers, and web server. Consensus and persistence live in `src/raft.rs`, `src/node.rs`, `src/storage.rs`, and `src/state_machine.rs`; inter-Node RPC belongs under `src/network/`. The replicated model is in `src/domain.rs`, scheduling and HTTP work are in `src/scheduler.rs` and `src/worker.rs`, and the Axum API plus embedded UI are in `src/web.rs` and `src/webui.html`. Tests are inline and in `src/test.rs`; design decisions live under `docs/adr/`.

## Build, Test, and Development Commands

- `nix develop` enters the pinned nightly Rust development shell with OpenSSL, `pkg-config`, and LLDB.
- `cargo build` compiles the debug binary and locked dependencies.
- `cargo run` starts the current binary; set `RUST_LOG=debug` to increase tracing output.
- `cargo fmt --all -- --check` verifies formatting; run `cargo fmt --all` to apply it.
- `cargo clippy --all-targets --all-features -- -D warnings` runs lint checks.
- `cargo test --no-run` compiles all tests without starting network fixtures.

## Coding Style & Naming Conventions

Use standard `rustfmt` output (four-space indentation and trailing commas). Name modules, functions, and variables with `snake_case`; types and traits with `UpperCamelCase`; constants with `SCREAMING_SNAKE_CASE`. Keep public APIs narrowly scoped and document non-obvious protocol behavior. Prefer the existing `snafu` error context and `tracing` macros over ad hoc string errors or printing.

## Testing Guidelines

Use `#[compio::test]` for asynchronous tests and place focused unit tests in a local `#[cfg(test)]` module. Test names should describe behavior, for example `rejects_invalid_scheme`. Some tests in `src/test.rs` are paired, fixed-port, or intentionally long-running (`master`/`worker`); run them deliberately in separate processes. A focused example is `cargo test network::rpc::test::test_dummy_server -- --nocapture`. Avoid port collisions and ensure spawned tasks terminate in new automated tests.

## Commit & Pull Request Guidelines

History is sparse and includes `wip` commits plus `feat: Init commit`. For new work, use concise Conventional Commit subjects such as `fix: handle expired RPC deadlines`, and squash temporary `wip` commits before review. Pull requests should explain the behavior change, list verification commands, link related issues, and call out networking, storage, or dependency-lock changes. Include logs or screenshots only when they clarify observable behavior.

## Configuration & Security

Do not commit secrets or machine-specific environment files. Keep `Cargo.lock` and `flake.lock` updated intentionally, especially because several dependencies are sourced from Git branches.
