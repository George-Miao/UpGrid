---
title: Install UpGrid
description: Install the published container or a precompiled Linux binary.
---

Docker is the preferred way to install UpGrid. Tagged releases also provide precompiled Linux binaries for hosts where a container runtime is not appropriate. Build from source only for development or an unsupported target.

## Run with Docker

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

See the [environment-variable reference](/reference/configuration/#settings) for every supported `UPGRID_` setting and its default.

## Install a precompiled binary

The [v0.1.0 GitHub Release](https://github.com/George-Miao/UpGrid/releases/tag/v0.1.0) provides dynamically linked Linux binaries for both common server architectures:

- [Linux AMD64](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/upgrid-v0.1.0-linux-amd64.tar.gz)
- [Linux ARM64](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/upgrid-v0.1.0-linux-arm64.tar.gz)
- [SHA-256 checksums](https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/SHA256SUMS)

Download the archive matching the host, then verify and extract it. This AMD64 example keeps the binary in the current directory:

```sh
curl --remote-name https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/upgrid-v0.1.0-linux-amd64.tar.gz
curl --remote-name https://github.com/George-Miao/UpGrid/releases/download/v0.1.0/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
tar --extract --gzip --file upgrid-v0.1.0-linux-amd64.tar.gz
./upgrid --help
```

Use a durable directory for each Node and ensure its API TCP port and Raft UDP port are reachable where required.

## Build from source

Source builds require:

- Git
- Nix with flakes enabled

```sh
git clone https://github.com/George-Miao/UpGrid.git
cd UpGrid
nix develop
cargo build --release
```

The binary is written to `target/release/upgrid`. UpGrid reads built-in defaults, an optional TOML file, environment variables, and CLI arguments in that order; see the full [Configuration reference](/reference/configuration/).

:::caution
Every Node needs its own data directory. Never clone an existing Node's directory to provision another member.
:::

Next, [start your first Node](/getting-started/first-node/).
