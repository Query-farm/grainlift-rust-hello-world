// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Query the hello-world service from Rust with the ADBC driver manager.
//!
//! Set `GRAINLIFT_DRIVER` to the native driver library, start the service,
//! then run `cargo run --example client`. Optionally set `GRAINLIFT_ENDPOINT`
//! and `GRAINLIFT_TOKEN` (omit it to connect anonymously). For a
//! `tls+tcp://` endpoint also set `GRAINLIFT_TLS_CA`, `GRAINLIFT_TLS_CERT`,
//! `GRAINLIFT_TLS_KEY` and `GRAINLIFT_TLS_SERVER_NAME`.

use std::env;
use std::error::Error;
use std::process::ExitCode;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let mut connection = connect()?;

    let hello = query(&mut connection, "SELECT 'Hello, world!' AS message")?;
    let messages = hello[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("message")?;
    println!(
        "message: {:?}",
        messages.iter().flatten().collect::<Vec<_>>()
    );

    let numbers = query(&mut connection, "SELECT * FROM numbers(2500)")?;
    let sizes = numbers
        .iter()
        .map(RecordBatch::num_rows)
        .collect::<Vec<_>>();
    println!("numbers(2500): {sizes:?} rows per Arrow batch");

    let totals = query(&mut connection, "SELECT * FROM running_total(2500)")?;
    let last = totals.last().ok_or("no rows")?;
    let row = last.num_rows() - 1;
    let column = |index: usize| {
        last.column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|values| values.value(row))
    };
    println!(
        "running_total(2500): last row number={}, total={}",
        column(0).ok_or("number")?,
        column(1).ok_or("total")?
    );

    let mut statement = connection.new_statement()?;
    statement.set_sql_query("SELECT * FROM numbers(0)")?;
    let reader = statement.execute()?;
    let schema = reader.schema();
    let rows = reader
        .map(|batch| batch.map(|batch| batch.num_rows()))
        .sum::<Result<usize, _>>()?;
    println!("Empty result: {rows} rows, schema: {schema}");
    Ok(())
}

/// Connect through the native driver; `autocommit` is the ADBC default.
fn connect() -> Result<ManagedConnection, Box<dyn Error>> {
    let driver_path = env::var("GRAINLIFT_DRIVER").map_err(|_| "set GRAINLIFT_DRIVER")?;
    let endpoint = env::var("GRAINLIFT_ENDPOINT").unwrap_or("http://127.0.0.1:8080".into());
    let mut options: Vec<(OptionDatabase, OptionValue)> = vec![
        (
            OptionDatabase::Other("grainlift.target".into()),
            "hello".into(),
        ),
        (
            OptionDatabase::Other("grainlift.uri".into()),
            endpoint.as_str().into(),
        ),
    ];
    if endpoint.starts_with("tls+tcp://") {
        for (key, variable) in [
            ("grainlift.tls.ca", "GRAINLIFT_TLS_CA"),
            ("grainlift.tls.cert", "GRAINLIFT_TLS_CERT"),
            ("grainlift.tls.key", "GRAINLIFT_TLS_KEY"),
            ("grainlift.tls.server_name", "GRAINLIFT_TLS_SERVER_NAME"),
        ] {
            let value = env::var(variable).map_err(|_| format!("set {variable}"))?;
            options.push((OptionDatabase::Other(key.into()), value.into()));
        }
    } else if let Ok(token) = env::var("GRAINLIFT_TOKEN") {
        options.push((
            OptionDatabase::Other("grainlift.auth.bearer_token".into()),
            token.into(),
        ));
    }
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        driver_path,
        Some(b"AdbcDriverGrainliftInit"),
        AdbcVersion::V110,
    )?;
    Ok(driver.new_database_with_opts(options)?.new_connection()?)
}

fn query(
    connection: &mut ManagedConnection,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    Ok(statement.execute()?.collect::<Result<Vec<_>, _>>()?)
}
