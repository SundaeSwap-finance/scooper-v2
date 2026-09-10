# ── Base ──────────────────────────────────────────────────────────────────────
FROM rust:1.98-bookworm AS base
RUN cargo install cargo-chef --locked

# ── Planner ───────────────────────────────────────────────────────────────────
FROM base AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Builder ───────────────────────────────────────────────────────────────────
FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --locked --release --recipe-path recipe.json
COPY . .
RUN cargo build --locked --release

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 10001 scooper && \
    useradd -u 10001 -g scooper -m scooper

COPY --from=builder /app/target/release/scooper-v2 /usr/local/bin/scooper-v2

RUN mkdir -p /app/data && chown scooper:scooper /app/data
VOLUME /app/data

USER scooper
WORKDIR /app
# 9999 private (dashboard, metrics), 9998 public (strategy intents, /health)
EXPOSE 9999 9998

# No network config ships in the image; bind-mount one at /app/config.json.
# /app/data is the only writable path — see README.md.
ENTRYPOINT ["scooper-v2"]
CMD ["--config", "/app/config.json"]
