// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The `grainlift-rust-hello-world` command.

use std::process::ExitCode;

use grainlift_rust_hello_world::HelloBackend;
use grainlift_server::dev::{self, Auth, RunOptions};

/// Serve [`HelloBackend`] as the `hello` target on loopback; run with
/// `--help` for hosting options.
///
/// The service is read-only, so it accepts anonymous clients by default; pass
/// `--auth token` to require a bearer token.
fn main() -> ExitCode {
    dev::run(
        HelloBackend,
        "hello",
        RunOptions::new("Grainlift hello-world ADBC service").auth(Auth::Anonymous),
    )
}
