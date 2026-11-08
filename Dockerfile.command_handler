# syntax=docker/dockerfile:1.4
# Hardened rootless FROM scratch container build for ferrous-cqrses command handler daemon
# Adheres strictly to AD-24 (<35MB footprint, UID 10001, static musl, Day-0 defense)

FROM rust:1.99-alpine AS builder

RUN apk add --no-cache musl-dev gcc make ca-certificates

WORKDIR /usr/src/ferrous-cqrses

# Create unprivileged user / group files and empty secure tmpfs mount point for scratch runtime
RUN echo "cqrses:x:10001:10001:FerrousCQRS Daemon:/nonexistent:/sbin/nologin" > /etc/passwd.scratch && \
    echo "cqrses:x:10001:" > /etc/group.scratch && \
    mkdir -m 1777 /tmp_scratch

COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-musl --bin command_handler

FROM scratch

LABEL org.opencontainers.image.title="ferrous-cqrses-command-handler" \
      org.opencontainers.image.description="Hardened rootless FROM scratch sequential command handler daemon for FerrousCX" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="FerrousCX" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /etc/passwd.scratch /etc/passwd
COPY --from=builder /etc/group.scratch /etc/group
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /tmp_scratch /tmp
COPY --from=builder /usr/src/ferrous-cqrses/target/x86_64-unknown-linux-musl/release/command_handler /command_handler

USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/command_handler", "--healthcheck"]

ENTRYPOINT ["/command_handler"]
CMD []
