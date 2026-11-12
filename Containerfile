# ── builder ────────────────────────────────────────────────────────────────
# bookworm en las dos etapas: el binario se enlaza contra la misma glibc que corre.
FROM rust:1-bookworm AS builder
WORKDIR /app

# Cachear dependencias antes de copiar el código real
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── runtime ────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/nats-ntfy /usr/local/bin/nats-ntfy
USER 65534
CMD ["nats-ntfy"]
