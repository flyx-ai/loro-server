FROM scratch

COPY ./target/x86_64-unknown-linux-musl/release/loro-server /

ENTRYPOINT ["/loro-server"]
