---
title: Single-node setup
description: Install and start UpGrid without a public cluster transport endpoint.
---

Use this setup when one node will run all monitoring work. You do not need to publish the `up://` UDP port or add a firewall rule for cluster transport.

## 1. Choose how to run UpGrid

Each option includes the installation and start commands. Docker is the preferred option. Precompiled Linux binaries are available when a container runtime is not suitable. Build from source only for development or an unsupported target.

Use `latest` for the current container release or `latest-unstable` for the newest image from `main`.

:::note[Docker on Linux]
UpGrid containers require an `io_uring` seccomp profile. Both Docker options below use the provided profile. See [Allow io_uring in Docker](/reference/docker/#allow-io_uring-in-docker) for the reason and the less secure fallback.
:::

<details class="run-option" id="docker-compose">
<summary>Docker Compose</summary>

Download [`docker-compose.yaml`](https://upgrid.rs/docker-compose.yaml) and [`upgrid-seccomp.json`](https://upgrid.rs/upgrid-seccomp.json) to the same directory:

```sh
curl --fail --remote-name https://upgrid.rs/docker-compose.yaml
curl --fail --remote-name https://upgrid.rs/upgrid-seccomp.json
```

Start UpGrid from that directory:

```sh
docker compose pull
docker compose up -d
```

Check the process and follow its logs:

```sh
docker compose ps
docker compose logs --follow upgrid
```

</details>

<details class="run-option" id="docker-cli">
<summary>Docker CLI</summary>

Pull the published image and download the seccomp profile:

```sh
docker pull ghcr.io/george-miao/upgrid:latest
curl --fail --remote-name https://upgrid.rs/upgrid-seccomp.json
```

Start the container with the profile:

```sh
docker run --name upgrid \
  --security-opt seccomp=./upgrid-seccomp.json \
  --publish 8080:8080 \
  --volume upgrid-data:/var/lib/upgrid \
  ghcr.io/george-miao/upgrid:latest
```

</details>

<details class="run-option" id="precompiled-linux-binary">
<summary>Precompiled Linux binary</summary>

Precompiled Linux binaries and checksum files are available on the [GitHub releases page](https://github.com/George-Miao/UpGrid/releases). Linux binaries require a system SQLite 3.34.1 or newer runtime library; macOS provides SQLite, and Windows archives include the required DLL. Download the archive for your host architecture and follow the instructions for that release.

Run the extracted binary with a durable data directory:

```sh
./upgrid \
  --bind 127.0.0.1:8080 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one
```

</details>

<details class="run-option" id="build-from-source">
<summary>Build from source</summary>

Source builds require:

- Git
- A nightly Rust toolchain with `rustc` and Cargo
- A native C build toolchain with a C compiler, linker, and archiver, such as `build-essential` on Debian and Ubuntu or the Xcode Command Line Tools on macOS
- SQLite 3.34.1 or newer development files, such as `libsqlite3-dev` on Debian and Ubuntu or `sqlite` from Homebrew; for Homebrew, add `$(brew --prefix sqlite)/lib/pkgconfig` to `PKG_CONFIG_PATH`
- `pkg-config` on Unix-like systems so the build can locate SQLite

The Rust build links to the installed SQLite library and uses `rustls`, so it does not require OpenSSL development packages. To rebuild the checked-in WebUI assets, also install Node.js 22 and pnpm. These JavaScript tools are optional for a normal binary build.

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

Start the built binary with a durable data directory:

```sh
./target/release/upgrid \
  --bind 127.0.0.1:8080 \
  --data-dir /var/lib/upgrid \
  --node-name edge-one
```

</details>

See the [configuration reference](/reference/configuration/) for TOML, environment variable, and CLI settings.

:::caution
Every node needs its own data directory. Never clone an existing node's directory to provision another member.
:::

## 2. Create the cluster

### Browser setup

Open `http://<host>:8080/setup`, review the node name, and choose **Create new cluster**. Enter the first administrator username and password when prompted. This operator identity is stored in the cluster state. The setup flow can then create a notification channel and target, or you can skip both steps.

### Unattended setup

Pass `--new-cluster` to skip the browser decision:

```sh
upgrid \
  --new-cluster \
  --bind 127.0.0.1:8080 \
  --data-dir /var/lib/upgrid \
  --username admin \
  --password 'replace-this-password'
```

The equivalent environment variable is `UPGRID_NEW_CLUSTER=true`. Unattended cluster creation also requires `--username` and `--password`. There are no default credentials.

## 3. Verify the node

`/healthz` is public and suitable for a process health check:

```sh
curl --fail 'http://<host>:8080/healthz'
```

All other API and WebUI routes require an operator identity or API token. Keep plain HTTP on a trusted network or place it behind a TLS reverse proxy.

## 4. Add a target

The node is now ready for [service targets](/guides/targets/). Use the [multi-node setup](/getting-started/multi-node/) instead when you need cluster members on separate hosts.
