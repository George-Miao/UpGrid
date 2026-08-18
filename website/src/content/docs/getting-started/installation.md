---
title: Install UpGrid
description: Choose the published container or a precompiled Linux binary.
---

Docker is the preferred way to install UpGrid. Tagged releases also provide precompiled Linux binaries for hosts where a container runtime is not appropriate. Build from source only for development or an unsupported target. Use `latest` for the current release or `latest-unstable` for the newest image from `main`.

## Install with Docker

Pull the image before you start the setup that fits your deployment:

```sh
docker pull ghcr.io/george-miao/upgrid:latest
```

The image listens for HTTP on port `8080` and stores durable state in `/var/lib/upgrid`. It also includes the `up://` transport, but a single-node setup does not need to publish its UDP port. The multi-node setup explains when to publish it.

See the [environment-variable reference](/reference/configuration/#settings) for every supported `UPGRID_` setting and its default.

### Allow `io_uring` in Docker

UpGrid uses Compio with `io_uring` on Linux. Docker's default seccomp profile blocks the three required `io_uring` system calls, so an UpGrid container cannot start with the default profile.

The project root includes [`upgrid-seccomp.json`](https://github.com/George-Miao/UpGrid/blob/main/upgrid-seccomp.json). It is based on the [Moby v0.2.1 default profile](https://github.com/moby/profiles/blob/seccomp/v0.2.1/seccomp/default.json) and adds only `io_uring_setup`, `io_uring_enter`, and `io_uring_register`. Download it to the directory where you run Docker:

```sh
curl --fail --remote-name \
  https://raw.githubusercontent.com/George-Miao/UpGrid/main/upgrid-seccomp.json
```

With `docker run`, pass `--security-opt seccomp=./upgrid-seccomp.json` to use this policy.

::::Caution
You can use `--security-opt seccomp=unconfined` when a custom profile is not practical, but it disables seccomp syscall filtering for the container. UpGrid does not require privileged mode.
::::

## Install a precompiled binary

Pre-built Linux binaries and checksum files are available on the [GitHub releases page](https://github.com/George-Miao/UpGrid/releases). Download the archive for your host architecture and follow the instructions for that release.

Use a durable directory for each node. The setup guides list the API and cluster transport ports that each deployment needs.

## Build from source

Source builds require:

- Git
- A nightly Rust toolchain with `rustc` and Cargo
- A native C build toolchain with a C compiler, linker, and archiver, such as `build-essential` on Debian and Ubuntu or the Xcode Command Line Tools on macOS

The Rust build uses bundled SQLite and `rustls`, so it does not require system SQLite or OpenSSL development packages. To rebuild the checked-in WebUI assets, also install Node.js 22 and pnpm. These JavaScript tools are optional for a normal binary build.

Nix with flakes is a convenient, optional way to prepare this environment:

```sh
nix develop
```

Whether you use Nix or install the dependencies directly, build UpGrid with:

```sh
git clone https://github.com/George-Miao/UpGrid.git
cd UpGrid
cargo build --locked --release -p upgrid
```

The binary is written to `target/release/upgrid`. UpGrid reads built-in defaults, an optional TOML file, environment variables, and CLI arguments in that order; see the full [configuration reference](/reference/configuration/).

:::Caution
Every node needs its own data directory. Never clone an existing node's directory to provision another member.
:::

## Choose a setup

- [Set up one node](/getting-started/first-node/) when one process will run all monitoring work.
- [Set up multiple nodes](/getting-started/multi-node/) when nodes must share monitoring work and replicated state.
