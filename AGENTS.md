# Working on this example

- This is a hello-world for people learning to build a Grainlift service in
  Rust. Keep it small and readable; it is not a conformance or benchmark
  fixture (that lives in Grainlift's `validation/synthetic-worker`).
- Keep behaviour in step with the Python reference,
  `grainlift-hello-world-python`: the same target, queries, errors, examples
  and tests.
- Reuse `grainlift-server` for protocol, authentication, handle lifecycle and
  hosting. Unsupported operations return ADBC `NOT_IMPLEMENTED` through the
  trait defaults rather than fabricating success.
- Keep the public client API ADBC and pull one bounded Arrow batch at a time.
- Never log queries, credentials, values or raw downstream errors.
- Pin `grainlift-server` to a Git revision of Query-farm/grainlift; CI builds
  the native driver from the same revision. Never commit local path
  replacements.
- Use Rust 1.97+. Run `cargo fmt --check`, `cargo clippy --all-targets --locked
  -- -D warnings` and `cargo test --locked` (with `GRAINLIFT_DRIVER` for the
  native and Haybarn tests).
- Preserve unrelated work.
