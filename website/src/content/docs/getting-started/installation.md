---
title: Install UpGrid
description: Build a release binary and prepare persistent storage.
---

UpGrid is currently distributed from source. The repository pins the required nightly Rust toolchain and native dependencies in a Nix development shell.

## Requirements

- Git
- Nix with flakes enabled
- A durable directory for each Node
- A TCP port for the API and a UDP port reachable by the other Cluster Nodes

## Build a release binary

```sh
git clone https://github.com/George-Miao/UpGrid.git
cd UpGrid
nix develop
cargo build --release
```

The binary is written to `target/release/upgrid`. Copy that binary to each host or package it for your service manager.

## Confirm the CLI

```sh
./target/release/upgrid --help
```

UpGrid reads built-in defaults, an optional TOML file, `UPGRID_` environment variables, and CLI arguments in that order. See [Configuration](/reference/configuration/) for the complete precedence and available settings.

:::caution
Every Node needs its own data directory. Never clone an existing Node's directory to provision another member.
:::

Next, [start your first Node](/getting-started/first-node/).
