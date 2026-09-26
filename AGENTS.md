# Working on this example

- Preserve exact parity with `../grainlift/validation/regression/soak/worker.py`.
- Reuse the Grainlift server for protocol, authentication and handle lifecycle.
- Keep the public client API ADBC and pull one bounded Arrow batch at a time.
- Never log commands, credentials, values or raw downstream errors.
- Keep the example authenticated and loopback-only. Unsupported operations
  must return ADBC `NOT_IMPLEMENTED` rather than fabricate success.
- Pin public dependencies; never commit local transport path replacements.
- Use Rust 1.97+. Run formatting, workspace tests and Clippy with warnings denied.
- Run builds, tests and benchmarks on the authorized EC2 host, not locally.
- Do not overlap compilers, profilers, tests or measured benchmark cases.
- Keep benchmark artifacts in the Grainlift validation repository. Record
  workload, warmup, repetitions, batch boundaries, errors and cleanup honestly.
- Preserve unrelated work and use `apply_patch` for source edits.
