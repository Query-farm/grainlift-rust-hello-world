# grainlift-rust-hello-world

A complete ADBC service in about 300 lines of Rust, built with the
[Grainlift](https://github.com/Query-farm/grainlift) server library
(`grainlift-server`). Any ADBC application connects to it through the native
Grainlift driver; the service itself needs no database, SQL engine or
downstream driver.

## Quickstart

Requires Rust 1.97+ (which also builds the native Grainlift ADBC driver once).

    git clone https://github.com/Query-farm/grainlift.git ../grainlift
    (cd ../grainlift && cargo build --locked -p adbc-driver-grainlift)

Start the service:

    cargo run --release

No credentials are needed. The service is read-only, so it accepts anonymous
clients (see [Authentication](#authentication)).

### Query it from SQL

[Haybarn](https://github.com/Query-farm-haybarn/haybarn), Query.Farm's DuckDB
distribution, loads the Grainlift driver through the `adbc_scanner` extension.
In a second terminal, run [`examples/query.sql`](examples/query.sql):

    export GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib  # .so on Linux
    uvx haybarn-cli < examples/query.sql

The same script runs unchanged in the DuckDB CLI. It prints:

    ┌───────────────┐
    │    message    │
    │    varchar    │
    ├───────────────┤
    │ Hello, world! │
    └───────────────┘
    ┌─────────┬────────────┐
    │ numbers │   total    │
    │  int64  │   int128   │
    ├─────────┼────────────┤
    │  100000 │ 4999950000 │
    └─────────┴────────────┘
    ...

`adbc_scan` sends its quoted SQL to this service. The rows come back as an
ordinary relation that you can join, aggregate or export locally.

### Query it from Rust

[`examples/client.rs`](examples/client.rs) uses the standard ADBC driver
manager (`adbc_driver_manager`):

    cargo run --example client

It prints:

    message: ["Hello, world!"]
    numbers(2500): [1024, 1024, 452] rows per Arrow batch
    running_total(2500): last row number=2499, total=3123750
    Empty result: 0 rows, schema: Field { "number": nullable Int64 }

## What's in the crate

| File | Contents |
| --- | --- |
| [`src/lib.rs`](src/lib.rs) | The service: `HelloBackend` → `HelloConnection` → `HelloStatement`, plus the two result styles below |
| [`src/main.rs`](src/main.rs) | The `grainlift-rust-hello-world` command |

The service answers three queries:

| Query | Result | Demonstrates |
| --- | --- | --- |
| `SELECT 'Hello, world!' AS message` | one row | the smallest possible result |
| `SELECT * FROM numbers(n)` | 0..n-1 | a **`RecordBatchReader`** of Arrow batches |
| `SELECT * FROM running_total(n)` | 0..n-1 with a running sum | a serializable **`ResultProducer`** |

`n` ranges from 0 to 100000. Anything else is an ADBC `INVALID_ARGUMENT` error
with SQLSTATE 42000. The example matches these queries exactly rather than
pretending to parse SQL.

`HelloStatement` implements the ADBC statement lifecycle: set the SQL, then
`prepare`, `execute_schema` and `execute_result`. Preparation matters because
clients such as `adbc_scanner` prepare every query before running it. Every
operation the example does not implement (transactions, binding, metadata and
so on) returns ADBC `NOT_IMPLEMENTED` through the Grainlift traits' defaults.

### Readers vs. producers

Both styles stream lazily in batches of at most 1024 rows, and you can mix them
freely within one service.

- **Reader** (`numbers`): return `QueryResult::from_reader(reader)` (or
  implement `BackendStatement::execute`). It's the simplest option, and it can
  hold resources such as an open database cursor. The reader lives in server
  memory until the client finishes or releases the result.
- **Producer** (`running_total`): a struct whose fields are the entire
  resumable state, deriving `Serialize`, `Deserialize` and VGI-RPC's
  `StreamState`. Implement `ResultProducer::produce` and return
  `QueryResult::from_producer(schema, state)`. Over HTTP the state is
  serialized into the encrypted continuation token after each batch. The server
  keeps no iterator or replay batch between fetches, and a retried fetch
  recomputes its batch from the token. This is the same approach VGI-RPC
  streams use.

Pick a producer when the state is small and serializable, such as offsets,
keyset cursors or counters (64 KiB by default). Pick a reader when it isn't.

## Authentication

Anonymous access is opt-in in Grainlift. This example enables it because it
only serves public, read-only data: its `main` calls
`dev::run(..., RunOptions::new(...).auth(Auth::Anonymous))`. Requests without
credentials act as the shared `anonymous` principal.

- Set `GRAINLIFT_TOKEN` on both sides to connect as an authenticated principal
  instead. A client that sends a wrong token is rejected, never downgraded to
  anonymous.
- Run `cargo run --release -- --auth token` to require a token. The server
  prints a generated token when `GRAINLIFT_TOKEN` is unset.

For a service that can write data or expose private data, keep token
authentication. In your own hosting code, anonymous access is
`grainlift_server::hosting::http_authenticator(tokens, Some("anonymous"))`,
served with `grainlift_server::dev::Service::serve_http`.

## Hosting options

`cargo run --release -- --help` lists them.

- `--host http` (default): loopback HTTP for development; it drains on
  SIGTERM/Ctrl-C.
- `--host mtls`: verified TCP/mTLS; client certificates identify callers.
- `--port`: listening port (default 8080). Point the client at a different
  port with `GRAINLIFT_ENDPOINT`.

For mTLS, supply the server chain, key, client CA and the authorized client's
SPIFFE ID (its certificate URI SAN):

    cargo run --release -- --host mtls --port 8443 \
      --tls-cert server.pem --tls-key server-key.pem \
      --client-ca clients-ca.pem --client-uri spiffe://example.org/client

    export GRAINLIFT_ENDPOINT=tls+tcp://127.0.0.1:8443
    export GRAINLIFT_TLS_CA=server-ca.pem GRAINLIFT_TLS_CERT=client.pem GRAINLIFT_TLS_KEY=client-key.pem
    export GRAINLIFT_TLS_SERVER_NAME=localhost   # the DNS name in the server certificate
    cargo run --example client

These hosts are for development and bind to loopback. For production, build
your own host from `grainlift_server`'s session manager, limits and listeners;
see the Grainlift [security guide](https://github.com/Query-farm/grainlift/blob/main/docs/security.md).

## Development

    cargo fmt --check
    cargo clippy --all-targets --locked -- -D warnings
    GRAINLIFT_DRIVER=$PWD/../grainlift/target/debug/libadbc_driver_grainlift.dylib cargo test --locked

Native integration tests skip when `GRAINLIFT_DRIVER` is unset. The Haybarn
test also needs the Haybarn CLI (`uv tool install haybarn-cli`, or set
`HAYBARN` to its path); it downloads the `adbc_scanner` extension on first use.
CI builds a pinned native-driver revision and runs everything on Linux and
macOS.

The `grainlift-server` dependency is pinned to a Git revision of
[Query-farm/grainlift](https://github.com/Query-farm/grainlift); update it
together with the driver revision in CI. The synthetic benchmark worker that
used to live here is now Grainlift's own validation fixture,
[`validation/synthetic-worker`](https://github.com/Query-farm/grainlift/tree/main/validation/synthetic-worker).
