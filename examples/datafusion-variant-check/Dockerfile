# Builds the DataFusion + datafusion-variant query tool.
# Build context: examples/datafusion-variant-check
#
# datafusion-variant `main` needs a recent Rust toolchain (it declares 1.95+).
FROM rust:1-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends git pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian13:latest
COPY --from=builder /build/target/release/datafusion-variant-check /usr/local/bin/datafusion-variant-check
ENTRYPOINT ["/usr/local/bin/datafusion-variant-check"]
