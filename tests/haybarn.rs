// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Run the shipped SQL example with the Haybarn CLI and its adbc_scanner
//! extension.
//!
//! Skips unless `GRAINLIFT_DRIVER` is set and the Haybarn CLI is available as
//! `HAYBARN` or `haybarn` on `PATH` (`uv tool install haybarn-cli`). Set
//! `GRAINLIFT_REQUIRE_NATIVE` to fail instead of skipping.

mod common;

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use common::Server;
use grainlift_rust_hello_world::HelloBackend;
use serde_json::{Value, json};

fn haybarn() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("HAYBARN") {
        return Some(PathBuf::from(path));
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|directory| directory.join("haybarn"))
        .find(|candidate| candidate.is_file())
}

#[test]
fn sql_example_in_haybarn() {
    let driver = require_driver!();
    let Some(haybarn) = haybarn() else {
        common::skip("the Haybarn CLI is not installed; install haybarn-cli or set HAYBARN");
        return;
    };
    // Serve anonymously, as `grainlift-rust-hello-world` does by default.
    let server = Server::start(HelloBackend, &[], Some("anonymous"));
    let script =
        include_str!("../examples/query.sql").replace("http://127.0.0.1:8080", &server.url);
    let mut child = Command::new(haybarn)
        .arg("-json")
        .env("GRAINLIFT_DRIVER", driver)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");

    // The CLI prints one JSON array per result set.
    let results = serde_json::Deserializer::from_slice(&output.stdout)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let [hello, numbers, running_total, disconnected] = results.as_slice() else {
        panic!("unexpected output: {results:?}");
    };
    assert_eq!(*hello, json!([{"message": "Hello, world!"}]));
    // HUGEINT sums are emitted as strings.
    assert_eq!(
        *numbers,
        json!([{"numbers": 100000, "total": "4999950000"}])
    );
    assert_eq!(
        *running_total,
        json!([
            {"number": 2499, "total": 3123750},
            {"number": 2498, "total": 3121251},
            {"number": 2497, "total": 3118753},
        ])
    );
    let disconnected = disconnected[0].as_object().unwrap();
    assert_eq!(disconnected.values().collect::<Vec<_>>(), [&json!(true)]);
}
