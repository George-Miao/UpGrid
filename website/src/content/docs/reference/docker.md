---
title: Docker
description: Run UpGrid with the required io_uring seccomp policy.
---

## Allow `io_uring` in Docker

UpGrid uses Compio with `io_uring` on Linux. Docker's default seccomp profile blocks the three required `io_uring` system calls, so an UpGrid container cannot start with the default profile.

UpGrid publishes [`upgrid-seccomp.json`](https://upgrid.rs/upgrid-seccomp.json). The profile is based on the [Moby v0.2.1 default profile](https://github.com/moby/profiles/blob/seccomp/v0.2.1/seccomp/default.json) and adds only `io_uring_setup`, `io_uring_enter`, and `io_uring_register`.

Download the profile to the directory where you run Docker:

```sh
curl --fail --remote-name https://upgrid.rs/upgrid-seccomp.json
```

Pass the profile to `docker run`:

```sh
--security-opt seccomp=./upgrid-seccomp.json
```

For Docker Compose, set the same profile in the service configuration:

```yaml
security_opt:
  - seccomp=./upgrid-seccomp.json
```

:::caution
You can use `--security-opt seccomp=unconfined` when the custom profile is not practical, but this option disables seccomp syscall filtering for the container. UpGrid does not require privileged mode.
:::
