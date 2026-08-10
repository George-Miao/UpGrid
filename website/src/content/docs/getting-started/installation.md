---
title: Install UpGrid
description: Build a release binary and prepare persistent storage.
---

Every commit on `main` publishes an AMD64 and ARM64 Linux image named `main-<6-character-commit>`. A Git tag publishes the image under that tag and creates a GitHub Release with Linux binaries and SHA-256 checksums. The repository also pins the required nightly Rust toolchain and native dependencies in a Nix development shell for source builds.

## Run the container image

```sh
docker run --name upgrid \
  --publish 8080:8080 \
  --publish 11451:11451/udp \
  --volume upgrid-data:/var/lib/upgrid \
  --env UPGRID_USERNAME=admin \
  --env UPGRID_PASSWORD='replace-this-password' \
  ghcr.io/george-miao/upgrid:v0.1.0
```

The image listens for HTTP on port `8080`, exposes QUIC/Raft on UDP port `11451`, and stores durable state in `/var/lib/upgrid`. Set `UPGRID_RAFT_URL` to an advertised hostname reachable by every Node before building a multi-Node Cluster.

## Requirements

- Git
- Nix with flakes enabled
- A durable directory for each Node
- A TCP port for the API and a UDP port reachable by the other Cluster Nodes

## Build a release binary from source

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
