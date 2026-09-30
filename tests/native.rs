// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Real C-ABI coverage through the native Grainlift ADBC driver.
//!
//! Every test skips (passes with a note) unless `GRAINLIFT_DRIVER` names the
//! native driver library; set `GRAINLIFT_REQUIRE_NATIVE` to fail instead.

mod common;

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use adbc_core::{Connection, Statement};
use adbc_driver_manager::ManagedConnection;
use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::Schema;
use common::{Server, connect};
use grainlift_rust_hello_world::HelloBackend;
use grainlift_server::backend::{Backend, BackendConnection, BackendStatement, QueryResult};
use grainlift_server::config::TargetConfig;
use grainlift_server::hosting::{ANONYMOUS_DOMAIN, BEARER_DOMAIN};

fn token_server() -> Server {
    Server::start(
        HelloBackend,
        &[("test-token", "alice"), ("other-token", "bob")],
        None,
    )
}

/// Serve anonymous clients as `public` while still accepting one bearer token.
fn anonymous_server() -> Server {
    Server::start(HelloBackend, &[("test-token", "alice")], Some("public"))
}

fn query(connection: &mut ManagedConnection, sql: &str) -> Result<Vec<RecordBatch>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    statement
        .execute()?
        .map(|batch| batch.map_err(Error::from))
        .collect()
}

fn rows(batches: &[RecordBatch]) -> Vec<usize> {
    batches.iter().map(RecordBatch::num_rows).collect()
}

fn last_int(batches: &[RecordBatch], column: &str) -> i64 {
    let batch = batches.last().unwrap();
    let values = batch.column_by_name(column).unwrap();
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    values.value(values.len() - 1)
}

fn sqlstate(error: &Error) -> String {
    error
        .sqlstate
        .iter()
        .map(|byte| *byte as u8 as char)
        .collect()
}

#[test]
fn real_adbc_queries_schema_errors_and_cleanup() {
    let _driver = require_driver!();
    let server = token_server();
    let mut connection = connect(&server.url, Some("test-token")).unwrap();

    let hello = query(&mut connection, "SELECT 'Hello, world!' AS message").unwrap();
    let message = hello[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(message.value(0), "Hello, world!");

    let numbers = query(&mut connection, "SELECT * FROM numbers(2500)").unwrap();
    assert_eq!(rows(&numbers), [1024, 1024, 452]);
    assert_eq!(last_int(&numbers, "number"), 2499);

    // Three batches over HTTP: the producer resumes from each continuation token.
    let totals = query(&mut connection, "SELECT * FROM running_total(2500)").unwrap();
    assert_eq!(rows(&totals), [1024, 1024, 452]);
    assert_eq!(last_int(&totals, "total"), 2499 * 2500 / 2);

    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT * FROM numbers(0)").unwrap();
    let empty = statement.execute().unwrap();
    let schema = empty.schema();
    assert_eq!(
        empty.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
        0
    );
    assert_eq!(schema.field(0).name(), "number");
    assert_eq!(statement.execute_schema().unwrap(), schema.as_ref().clone());

    let error = query(&mut connection, "unsupported").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(sqlstate(&error), "42000");

    // Release a result after its first batch.
    statement
        .set_sql_query("SELECT * FROM numbers(100000)")
        .unwrap();
    let mut reader = statement.execute().unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1024);
    drop(reader);
    drop(statement);
    assert_eq!(server.open_handles().1, 0);
    drop(connection);
    assert_eq!(server.open_handles(), (0, 0));
}

#[test]
fn prepared_statements() {
    // ADBC StatementPrepare, used by DuckDB's adbc_scanner before every scan,
    // validates and then executes.
    let _driver = require_driver!();
    let server = token_server();
    let mut connection = connect(&server.url, Some("test-token")).unwrap();
    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query("SELECT * FROM running_total(3)")
        .unwrap();
    statement.prepare().unwrap();
    assert_eq!(statement.get_parameter_schema().unwrap(), Schema::empty());
    let batches = statement
        .execute()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>();
    let totals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(totals.values().to_vec(), [0, 1, 3]);
    statement.set_sql_query("DROP TABLE x").unwrap();
    let error = statement.prepare().unwrap_err();
    assert_eq!(sqlstate(&error), "42000");
}

#[test]
fn authentication_required() {
    // A wrong or missing bearer token is rejected before a session is opened.
    let _driver = require_driver!();
    let server = token_server();
    for token in [Some("wrong-token"), None] {
        assert!(connect(&server.url, token).is_err());
    }
    assert_eq!(server.open_handles(), (0, 0));
    assert!(server.principals().is_empty());
}

#[test]
fn anonymous_client_queries_without_a_token() {
    let _driver = require_driver!();
    let server = anonymous_server();
    let mut connection = connect(&server.url, None).unwrap();
    let hello = query(&mut connection, "SELECT 'Hello, world!' AS message").unwrap();
    assert_eq!(rows(&hello), [1]);
    let numbers = query(&mut connection, "SELECT * FROM numbers(2500)").unwrap();
    assert_eq!(rows(&numbers), [1024, 1024, 452]);
    // Anonymous continuation tokens resume the producer too.
    let totals = query(&mut connection, "SELECT * FROM running_total(2500)").unwrap();
    assert_eq!(last_int(&totals, "total"), 2499 * 2500 / 2);
    assert_eq!(
        server.principals(),
        [(ANONYMOUS_DOMAIN.to_string(), "public".to_string())]
    );
    drop(connection);
    assert_eq!(server.open_handles(), (0, 0));
}

#[test]
fn anonymous_endpoint_still_authenticates_tokens() {
    // Token clients keep their own principal, and a wrong token is rejected
    // rather than treated as anonymous.
    let _driver = require_driver!();
    let server = anonymous_server();
    let mut alice = connect(&server.url, Some("test-token")).unwrap();
    let mut anonymous = connect(&server.url, None).unwrap();
    assert_eq!(server.open_handles().0, 2);
    for connection in [&mut alice, &mut anonymous] {
        let numbers = query(connection, "SELECT * FROM numbers(3)").unwrap();
        let values = numbers[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values().to_vec(), [0, 1, 2]);
    }
    assert_eq!(
        server.principals(),
        [
            (BEARER_DOMAIN.to_string(), "alice".to_string()),
            (ANONYMOUS_DOMAIN.to_string(), "public".to_string()),
        ]
    );
    assert!(connect(&server.url, Some("wrong-token")).is_err());
    drop((alice, anonymous));
    assert_eq!(server.open_handles(), (0, 0));
}

#[test]
fn structured_adbc_error_reaches_the_client() {
    struct Failing;
    struct FailingConnection;
    struct FailingStatement;
    impl Backend for Failing {
        fn open(
            &self,
            _target: &TargetConfig,
            _database_options: Vec<(String, OptionValue)>,
            _connection_options: Vec<(String, OptionValue)>,
        ) -> Result<Box<dyn BackendConnection>> {
            Ok(Box::new(FailingConnection))
        }
    }
    impl BackendConnection for FailingConnection {
        fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
            Ok(Box::new(FailingStatement))
        }
    }
    impl BackendStatement for FailingStatement {
        fn set_sql_query(&mut self, _query: &str) -> Result<()> {
            Ok(())
        }
        fn execute_result(&mut self) -> Result<QueryResult> {
            let mut error = Error::with_message_and_status("Invalid data", Status::InvalidData);
            error.sqlstate = b"22000".map(|byte| byte as std::ffi::c_char);
            error.vendor_code = 42;
            error.details = Some(vec![("binary".into(), vec![0, 255])]);
            Err(error)
        }
    }

    let _driver = require_driver!();
    let server = Server::start(Failing, &[("test-token", "alice")], None);
    let mut connection = connect(&server.url, Some("test-token")).unwrap();
    let error = query(&mut connection, "SELECT 'Hello, world!' AS message").unwrap_err();
    assert_eq!(error.status, Status::InvalidData);
    assert_eq!(sqlstate(&error), "22000");
    assert!(error.message.contains("Invalid data"), "{}", error.message);
    assert_eq!(
        error.details,
        Some(vec![("binary".to_string(), vec![0, 255])])
    );
}

/// Run the example's own command and query it through the native driver,
/// with an exported token and anonymously.
#[cfg(unix)]
#[test]
fn command_line_host_through_native_adbc() {
    let _driver = require_driver!();
    for token in [Some("test-token"), None] {
        let port = {
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            reservation.local_addr().unwrap().port()
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_grainlift-rust-hello-world"));
        command
            .args(["--port", &port.to_string()])
            .env_remove("GRAINLIFT_TOKEN")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(token) = token {
            command.env("GRAINLIFT_TOKEN", token);
        }
        let mut child = command.spawn().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(child.try_wait().unwrap().is_none(), "server exited early");
            assert!(Instant::now() < deadline, "server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }
        let url = format!("http://127.0.0.1:{port}");
        let queried = std::panic::catch_unwind(|| {
            let mut connection = connect(&url, token).unwrap();
            let numbers = query(&mut connection, "SELECT * FROM numbers(2500)").unwrap();
            assert_eq!(rows(&numbers), [1024, 1024, 452]);
        });
        let terminated = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(terminated.success());
        let status = child.wait().unwrap();
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        queried.unwrap();
        assert!(status.success(), "{status}");
        assert!(stdout.contains("Anonymous access enabled"), "{stdout}");
        assert!(
            stdout.contains(&format!("Grainlift listening on http://127.0.0.1:{port}")),
            "{stdout}"
        );
    }
}
