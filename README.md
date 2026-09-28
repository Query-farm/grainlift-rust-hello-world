# grainlift-rust-hello-world

A bounded synthetic Rust ADBC service for comparing Grainlift's Rust and Python
server paths. It implements the same workload as
`grainlift/validation/regression/soak/worker.py`; it does not run a SQL engine.

The application uses Grainlift's existing server library for protocol 0.4,
authentication, principal ownership, handle lifetimes, quotas, replay, errors,
and pull-based Arrow results. Clients use the ordinary native Grainlift ADBC
driver. The Grainlift dependency is pinned to a public Git revision; VGI-RPC
comes from crates.io, with resolved dependencies in `Cargo.lock`. VGI-RPC 0.27.3
wakes the native TCP/mTLS listener on socket readiness; the old 50 ms accept
sleep no longer delays new connections.

## Workload

`QUERY` returns two nullable fields: `number: int64` and `payload: binary`.
Defaults are 4,096 rows, numbered 0 through 4,095, with 64 `x` bytes per row,
in exactly eight 512-row batches. Every batch is allocated lazily on its pull.
`FAIL` returns ADBC `INVALID_DATA` with SQLSTATE `22000`. Another command
returns `INVALID_ARGUMENTS`; execution before setting a command returns
`INVALID_STATE`. Commands are exact and case-sensitive, matching the Python
synthetic worker.

The worker supports enabling autocommit. Transactions, preparation, binding,
ingestion, metadata, partitions, Substrait and downstream cancellation return
`NOT_IMPLEMENTED`. These limitations belong to this synthetic backend; they
do not describe the capabilities of the shared Grainlift server.

## Run

Requires Rust 1.97 or newer. Build and benchmark on the designated EC2 machine
for this project, not the developer laptop:

```console
cargo build --locked --release
export GRAINLIFT_HELLO_TOKEN=local-development-token-change-me
./target/release/grainlift-rust-hello-world --port 8080 --report /tmp/synthetic-report.json
```

The listener is always loopback-only. HTTP requires bearer authentication;
the optional TCP listener requires a verified and authorized client certificate.
The first stdout line is a small JSON readiness message containing the endpoint
and serving PID. It never contains the token. A stdin byte, stdin EOF, Ctrl-C,
or SIGTERM requests shutdown. Keep stdin open when supervising the process.
The shutdown report contains aggregate counters and resource counts, never
SQL, values, credentials or raw downstream errors.

Connect with `autocommit=True` and database options `grainlift.uri`,
`grainlift.target=default`, and `grainlift.auth.bearer_token`. Run `QUERY` through
the ordinary ADBC cursor.

For two-principal interoperability tests, set a different token of at least
16 bytes in `GRAINLIFT_HELLO_OTHER_TOKEN`. The primary and secondary tokens then
identify separate principals authorized for the same `default` target. The
secondary token is optional for the single-client benchmark; duplicate or short
secondary tokens cause startup to fail. Handle ownership remains enforced by
the shared Grainlift server.

The shared worker conformance fixture supplies both tokens and runs the native
ADBC client against this process. From the sibling Grainlift checkout, pass the
compiled driver and worker paths:

```console
python -m pytest validation/conformance -q \
  --native-driver /absolute/path/libadbc_driver_grainlift.so \
  --worker-command '["/absolute/path/grainlift-rust-hello-world"]'
```

To select authenticated TCP, pass `--tls-dir /private/test-certificates`.
The directory must contain `server.pem`, `server-key.pem`, and `ca.pem`.
The server verifies client certificates against that CA and permits only
`spiffe://benchmark.test/client` and `spiffe://benchmark.test/other` in trust
domain `benchmark.test`; the `denied` fixture identity is not authorized. The client
uses `grainlift.tls.ca`, `grainlift.tls.cert`, `grainlift.tls.key`, and
`grainlift.tls.server_name=localhost` instead of a bearer token. The native
URI is `tls+tcp://127.0.0.1:PORT`. The companion validation script creates
one-day test certificates; never commit these files or use them in production.
The shared conformance fixture selects this mode with `--worker-transport mtls`
and `--worker-tls-dir`. An explicit `--transport http` is also accepted;
unsupported transport names fail at argument parsing.

Dimensions are configurable using `--rows`, `--batch-rows`, and
`--payload-bytes`. Limits match the Python worker: 1–1,000,000 rows, 1–4,096
rows per batch, 0–1,024 payload bytes, and
`batch_rows * (payload_bytes + 16) <= 1 MiB`. The service admits three sessions,
32 statements and 32 results per session, a 2 MiB HTTP body, a 64 KiB command,
and a ten-second idle lifetime. Server options cannot be overridden by callers.
This is a local comparison application, not an Internet-facing deployment.
The HTTP body limit is not a TCP framing limit. TCP retains VGI 0.27.3's
existing IPC message guard (up to `u32::MAX` bytes), TLS handshake deadline,
and shared Grainlift session/result limits. Its listener does not provide
a configurable total accepted-connection ceiling. The diagnostic uses one
trusted client and bounded synthetic inputs; this does not qualify the
listener for hostile clients or public exposure.

## Matched single-client comparison

The companion Grainlift harness runs Rust, Python/Granian in-process, and
Python/Granian with an isolated backend **sequentially**, using the exact same
native driver binary and Python verification loop. Each case opens one
connection, warms ten queries, then measures 1,000 queries with expected errors
every ten queries. Schema, values, batch boundaries and cleanup are checked.
Three repetitions rotate host order. A separate one-batch comparison holds
total rows and payload constant while reducing batch RPC count.

```console
bash ../grainlift/validation/diagnostics/run_matched.sh \
  /absolute/grainlift /absolute/evidence \
  /absolute/libadbc_driver_grainlift.so \
  /absolute/grainlift-rust-hello-world
```

The primary Rust/Python comparison uses an in-process backend on both sides.
Python process isolation is measured separately. This compares complete service
implementations, not isolated language execution: Rust retains Grainlift's
session actor and Rust transport, while Python uses its SDK and Granian.
All cases use one client, so no concurrency scaling claim follows from them.

`validation/diagnostics/run_transports.sh` in Grainlift additionally compares
Rust HTTP, Python/Granian HTTP, Rust TCP/mTLS, and Python TCP/mTLS with
in-process backends and the same native driver. TCP results use a separate
stream connection per query, including a TLS handshake; only initial session
startup is excluded. See that harness's README for reproduction and transport
limitations. HTTP bearer authentication and TCP certificate authentication
remain enabled, so this compares complete paths rather than framing alone.

The [2026-09-26 EC2 results](https://github.com/Query-farm/grainlift/blob/main/validation/load-results/ec2-matched-synthetic-20260926/README.md)
average 9.39 ms/query for Rust, 17.72 ms for Python/Granian in-process, and
22.19 ms for Python/Granian with an isolated backend. Every case passed exact
result verification and resource recovery. The report includes raw evidence,
stage timings, resource measurements and limitations.

## Checks

```console
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Tests cover schema/value/batch parity, allocation limits, lazy early close,
independent cursors, structured error recovery, unsupported operations and
option rejection. The shared Grainlift conformance suite also exercises the
ordinary ADBC driver against this worker over HTTP or mTLS, including distinct
authenticated principals. This synthetic worker cannot exercise SQL write
visibility, transactions, or ingestion; those belong to the real-database
end-to-end suite. See Grainlift's recorded validation evidence for native
C-ABI integration, single-client timings, resource sampling and limitations.
