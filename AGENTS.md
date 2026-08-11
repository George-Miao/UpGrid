# Repository Guidelines

## Project Structure & Module Organization

UpGrid is a Rust 2024 workspace with a Lit/TypeScript WebUI. Crates live under `crates/`: `upgrid` orchestrates the process, `upgrid-config` owns configuration and secrets, `upgrid-transport` owns QUIC and generic typed RPC streams, `upgrid-raft` owns consensus, persistence, and the replicated model, `upgrid-api` serves HTTP/OpenAPI, and `upgrid-notification` delivers alerts. Channel implementations belong in `crates/upgrid-notification/src/channel/`. UI source and browser tests live in `frontend/src/` and `frontend/tests/`; checked-in `frontend/dist/` assets are embedded by `upgrid-api`. Design decisions live under `docs/adr/`.

## Build, Test, and Development Commands

- `nix develop` enters the pinned nightly Rust, Node 22, and pnpm development shell.
- `cargo build --workspace` compiles all crates and locked dependencies.
- `cargo run -p upgrid` starts one Node; set `RUST_LOG=debug` for detailed tracing.
- `scripts/update-webui.sh` installs locked frontend dependencies and rebuilds `frontend/dist/`.
- `scripts/test-webui.sh` runs Playwright in Chromium against an isolated real Node.
- `cargo fmt --all -- --check` verifies formatting; run `cargo fmt --all` to apply it.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` runs lint checks.
- `cargo test --workspace --no-run` compiles tests without starting network fixtures.
- `scripts/check-rust-source-size.sh` rejects Rust files over 500 lines.

## Coding Style & Naming Conventions

Use standard `rustfmt` output (four-space indentation and trailing commas). Name Rust modules and functions with `snake_case`, types with `UpperCamelCase`, and constants with `SCREAMING_SNAKE_CASE`. Start considering a module split near 200 lines; 500 lines is a hard maximum. Compose source with ordinary `mod` declarations—never `include!`. TypeScript uses two spaces, strict types, `camelCase` members, and `kebab-case` custom-element names. Never use `String` as an error type; define typed errors with `snafu` and propagate them with the `ResultExt` and `OptionExt` context APIs. Prefer typed boundaries and `tracing`.

## Testing Guidelines

Use `#[compio::test]` for async Rust tests and local `#[cfg(test)]` modules for units. Name tests by behavior, for example `rejects_invalid_scheme`. Browser workflows use Playwright against the real embedded app; avoid tests coupled to Lit internals. Fixed-port `master`/`worker` fixtures are manual, so never run the unfiltered Rust suite in automation. Avoid port collisions and terminate spawned tasks.

## Commit & Pull Request Guidelines

History is sparse and includes `wip` commits plus `feat: Init commit`. For new work, use concise Conventional Commit subjects such as `fix: handle expired RPC deadlines`, and squash temporary `wip` commits before review. Pull requests should explain the behavior change, list verification commands, link related issues, and call out networking, storage, or dependency-lock changes. Include logs or screenshots only when they clarify observable behavior.

## Configuration & Security

Do not commit secrets or machine-specific environment files. Keep `Cargo.lock` and `flake.lock` updated intentionally, especially because several dependencies are sourced from Git branches.
