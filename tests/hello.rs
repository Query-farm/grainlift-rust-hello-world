// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the hello-world backend, called in-process without a transport.

use adbc_core::error::{Result, Status};
use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::Schema;
use grainlift_rust_hello_world::{HelloConnection, RunningTotal};
use grainlift_server::backend::{BackendConnection, BackendStatement, QueryResult, ResultProducer};
use vgi_rpc::stream_codec::StreamStateCodec;

/// Create a statement holding the given query.
fn statement(sql: &str) -> Box<dyn BackendStatement> {
    let mut statement = HelloConnection.new_statement().unwrap();
    statement.set_sql_query(sql).unwrap();
    statement
}

/// Execute a query the way the service does: through a new statement.
fn execute(sql: &str) -> Result<QueryResult> {
    statement(sql).execute_result()
}

fn batches(result: QueryResult) -> Vec<RecordBatch> {
    result.into_reader().map(|batch| batch.unwrap()).collect()
}

fn int64_column(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).unwrap();
            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.null_count(), 0);
            values.values().to_vec()
        })
        .collect()
}

#[test]
fn hello_returns_one_row() {
    let batches = batches(execute("SELECT 'Hello, world!' AS message").unwrap());
    assert_eq!(batches.len(), 1);
    let messages = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(messages.iter().collect::<Vec<_>>(), [Some("Hello, world!")]);
}

#[test]
fn numbers_yields_sequential_rows_in_bounded_batches() {
    for count in [0, 1, 1023, 1024, 1025, 100_000] {
        let result = execute(&format!("SELECT * FROM numbers({count})")).unwrap();
        assert!(!result.is_producer());
        let batches = batches(result);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 1024));
        assert_eq!(
            int64_column(&batches, "number"),
            (0..count).collect::<Vec<_>>()
        );
    }
}

#[test]
fn running_total_carries_its_sum_across_batch_boundaries() {
    for count in [0, 1, 1024, 2500] {
        let result = execute(&format!("SELECT * FROM running_total({count})")).unwrap();
        assert!(result.is_producer());
        let batches = batches(result);
        assert!(batches.iter().all(|batch| batch.num_rows() <= 1024));
        assert_eq!(
            int64_column(&batches, "number"),
            (0..count).collect::<Vec<_>>()
        );
        assert_eq!(
            int64_column(&batches, "total"),
            (0..count).map(|n| n * (n + 1) / 2).collect::<Vec<_>>()
        );
    }
}

#[test]
fn running_total_state_round_trips() {
    // The producer resumes identically after serialization, as it does
    // between HTTP fetches.
    let mut producer = RunningTotal::new(3000);
    let first = producer.produce().unwrap();
    let mut resumed = RunningTotal::decode(&producer.encode().unwrap()).unwrap();
    assert!(first.is_some());
    assert_eq!(resumed, producer);
    assert_eq!(resumed.produce().unwrap(), producer.produce().unwrap());
}

#[test]
fn invalid_queries_fail_preparation_and_execution() {
    for query in [
        "SELECT * FROM numbers(100001)",
        "SELECT * FROM numbers(-1)",
        "SELECT * FROM running_total(100001)",
        "DROP TABLE x",
    ] {
        for error in [
            statement(query).prepare().unwrap_err(),
            execute(query).err().unwrap(),
        ] {
            assert_eq!(error.status, Status::InvalidArguments, "{query}");
            assert_eq!(error.sqlstate.map(|byte| byte as u8), *b"42000");
            assert!(error.message.starts_with("Supported queries:"));
        }
    }
}

#[test]
fn prepare_and_schemas_work_before_execution() {
    // DuckDB's adbc_scanner prepares and asks for schemas before executing.
    for (query, columns) in [
        ("SELECT 'Hello, world!' AS message;", vec!["message"]),
        ("select * from NUMBERS(5)", vec!["number"]),
        (
            "  SELECT * FROM running_total(5) ; ",
            vec!["number", "total"],
        ),
    ] {
        let mut prepared = statement(query);
        prepared.prepare().unwrap();
        assert_eq!(prepared.get_parameter_schema().unwrap(), Schema::empty());
        let names = |schema: &Schema| {
            schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&prepared.execute_schema().unwrap()), columns);
        let result = prepared.execute_result().unwrap();
        assert_eq!(names(&result.schema()), columns);
        assert_eq!(
            result.into_reader().schema(),
            prepared.execute_schema().unwrap().into()
        );
    }
}

#[test]
fn statement_without_query_is_invalid_state() {
    let mut statement = HelloConnection.new_statement().unwrap();
    assert_eq!(
        statement.execute_result().err().unwrap().status,
        Status::InvalidState
    );
    assert_eq!(
        statement.prepare().unwrap_err().status,
        Status::InvalidState
    );
}

#[test]
fn unsupported_operations_are_not_implemented() {
    let mut connection = HelloConnection;
    assert_eq!(
        connection.commit().unwrap_err().status,
        Status::NotImplemented
    );
    assert_eq!(
        statement("SELECT * FROM numbers(1)")
            .execute_update()
            .unwrap_err()
            .status,
        Status::NotImplemented
    );
}
