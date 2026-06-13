FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p dfc-server

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /app/target/release/dfc-server /dfc-server
USER nonroot:nonroot
EXPOSE 8080
ENV DFC_HOST=0.0.0.0
ENV DFC_PORT=8080
ENTRYPOINT ["/dfc-server"]
