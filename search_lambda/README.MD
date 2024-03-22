# AVRO record search AWS lambda

This is an AWS lambda that allow to search for recors in AVRO files located in S3.

It takes S3 prefix and predicate as input parameter, and return the result as an AVRO file. We assume that all the files we search among, have same schema.

The lambda can be called directly via Function URL or via API Gateway.

## Building

### Prepare environment

Make sure you have Rust installed: https://www.rust-lang.org/tools/install

Also, you need to have installed Zig (https://ziglang.org/download/) compiler and musl-tools (on deb based distributions `sudo apt install musl-tools` and `sudo apt install gcc-multilib`).

And to build Lambda project, you need to have installed `cargo-lambda`

```
cargo install cargo-lambda
```

### Deployment

To deploy to x86-based Lambda, you need first install corresponding Rust build target.
For more information go to https://rust-lang.github.io/rustup/cross-compilation.html

```shell
rustup target add x86_64-unknown-linux-musl
```

Using following commands, assembly the binary and pack it into a zip archive compatible with AWS Lambda.

```shell
cargo lambda build --bin search_lambda --release --target x86_64-unknown-linux-musl
```

### Environment variables

Lambda requires following environment variables to be set:

* S3_BUCKET - the bucket AVRO files are placed in
* S3_BASE_PATH - base path that will automatically prepended to any prefix specified in request