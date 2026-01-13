FROM rust:1.91.1-slim-bullseye AS builder
WORKDIR /usr/src/app
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -f target/release/deps/brunhilde*
COPY ./src ./src
RUN cargo build --release

FROM debian:bullseye-slim
WORKDIR /usr/src/app
COPY --from=builder /usr/src/app/target/release/brunhilde ./brunhilde
RUN mkdir db
CMD ["./brunhilde", "db"]
