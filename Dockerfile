# syntax=docker/dockerfile:1.7

FROM rustlang/rust:nightly-bookworm AS builder

WORKDIR /src
COPY . .

ARG TARGETARCH
RUN --mount=type=cache,id=upgrid-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=upgrid-cargo-git-${TARGETARCH},target=/usr/local/cargo/git \
    --mount=type=cache,id=upgrid-target-${TARGETARCH},target=/src/target \
    cargo build --locked --release -p upgrid && \
    strip target/release/upgrid && \
    cp target/release/upgrid /tmp/upgrid

FROM scratch AS binary

COPY --from=builder /tmp/upgrid /upgrid

FROM debian:bookworm-slim AS runtime

# Debian security updates intentionally select the current CA bundle package.
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install --yes --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system upgrid && \
    useradd --system --gid upgrid --home-dir /var/lib/upgrid upgrid && \
    install --directory --owner upgrid --group upgrid /var/lib/upgrid

COPY --from=builder /tmp/upgrid /usr/local/bin/upgrid

ENV UPGRID_BIND=0.0.0.0:8080 \
    UPGRID_DATA_DIR=/var/lib/upgrid

VOLUME ["/var/lib/upgrid"]
EXPOSE 8080/tcp 11451/udp

USER upgrid:upgrid
ENTRYPOINT ["/usr/local/bin/upgrid"]
