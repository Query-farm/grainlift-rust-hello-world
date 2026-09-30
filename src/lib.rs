// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! The hello-world service: three queries, no SQL engine.
//!
//! - `SELECT 'Hello, world!' AS message`: a single-row result.
//! - `SELECT * FROM numbers(n)`: rows 0..n-1 from an ordinary
//!   **[`RecordBatchReader`]**. Simple, and fine whenever the server keeps the
//!   cursor in memory; it can also hold resources such as a database cursor.
//! - `SELECT * FROM running_total(n)`: rows 0..n-1 with a running sum, from a
//!   serializable **[`ResultProducer`]**. Over HTTP the producer's fields
//!   travel in the encrypted continuation token after every batch, so the
//!   server keeps no per-result iterator, and a retried fetch recomputes its
//!   batch from the token.
//!
//! Run it with `cargo run --release`; `src/main.rs` hands [`HelloBackend`] to
//! the Grainlift development host.

use std::sync::Arc;

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::OptionValue;
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use grainlift_server::backend::{
    Backend, BackendConnection, BackendStatement, QueryResult, ResultProducer,
};
use grainlift_server::config::TargetConfig;
use serde::{Deserialize, Serialize};
use vgi_rpc::StreamState;

/// Largest `n` accepted by `numbers(n)` and `running_total(n)`.
pub const MAX_ROWS: i64 = 100_000;
/// Rows per Arrow batch.
pub const BATCH_ROWS: i64 = 1024;

const SUPPORTED: &str = "Supported queries: SELECT 'Hello, world!' AS message; \
    SELECT * FROM numbers(n) or running_total(n), where 0 <= n <= 100000";

/// Schema of `SELECT 'Hello, world!' AS message`.
pub fn hello_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "message",
        DataType::Utf8,
        true,
    )]))
}

/// Schema of `numbers(n)`.
pub fn numbers_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::Int64,
        true,
    )]))
}

/// Schema of `running_total(n)`.
pub fn running_total_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("number", DataType::Int64, true),
        Field::new("total", DataType::Int64, true),
    ]))
}

/// Read 0..count-1 lazily, in batches of at most [`BATCH_ROWS`] rows.
pub fn numbers(count: i64) -> Box<dyn RecordBatchReader + Send> {
    let schema = numbers_schema();
    let batches = (0..count).step_by(BATCH_ROWS as usize).map({
        let schema = schema.clone();
        move |start| {
            let values = Int64Array::from_iter_values(start..count.min(start + BATCH_ROWS));
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values)])
        }
    });
    Box::new(RecordBatchIterator::new(batches, schema))
}

/// Resumable state for `running_total(n)`; each field survives between batches.
///
/// Deriving `Serialize`, `Deserialize` and VGI-RPC's `StreamState` is all the
/// server needs to carry the state in continuation tokens.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, StreamState)]
pub struct RunningTotal {
    /// Number of rows to produce.
    pub count: i64,
    /// Next number to emit.
    pub position: i64,
    /// Sum of every number emitted so far.
    pub total: i64,
}

impl RunningTotal {
    /// Start a running total over 0..count-1.
    pub fn new(count: i64) -> Self {
        Self {
            count,
            position: 0,
            total: 0,
        }
    }
}

impl ResultProducer for RunningTotal {
    /// Emit the next batch and advance the state, or return `None` when done.
    fn produce(&mut self) -> Result<Option<RecordBatch>> {
        if self.position >= self.count {
            return Ok(None);
        }
        let numbers = self.position..self.count.min(self.position + BATCH_ROWS);
        let totals = numbers
            .clone()
            .map(|number| {
                self.total += number;
                self.total
            })
            .collect::<Vec<_>>();
        self.position = numbers.end;
        let batch = RecordBatch::try_new(
            running_total_schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(numbers)),
                Arc::new(Int64Array::from(totals)),
            ],
        )?;
        Ok(Some(batch))
    }
}

/// A recognized query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Query {
    /// `SELECT 'Hello, world!' AS message`
    Hello,
    /// `SELECT * FROM numbers(n)`
    Numbers(i64),
    /// `SELECT * FROM running_total(n)`
    RunningTotal(i64),
}

impl Query {
    /// Recognize one of the supported queries (case-insensitive, surrounding
    /// whitespace and one trailing semicolon allowed).
    pub fn parse(sql: &str) -> Result<Self> {
        let text = sql.trim();
        let text = text.strip_suffix(';').unwrap_or(text).trim().to_lowercase();
        if text == "select 'hello, world!' as message" {
            return Ok(Self::Hello);
        }
        let call = text.strip_prefix("select * from ").unwrap_or_default();
        let (function, argument) = call.split_once('(').unwrap_or_default();
        let digits = argument.strip_suffix(')').unwrap_or_default();
        let is_count =
            (1..=6).contains(&digits.len()) && digits.bytes().all(|byte| byte.is_ascii_digit());
        let count = is_count
            .then(|| digits.parse::<i64>().ok())
            .flatten()
            .filter(|count| *count <= MAX_ROWS);
        match (function, count) {
            ("numbers", Some(count)) => Ok(Self::Numbers(count)),
            ("running_total", Some(count)) => Ok(Self::RunningTotal(count)),
            _ => {
                let mut error = Error::with_message_and_status(SUPPORTED, Status::InvalidArguments);
                error.sqlstate = sqlstate(b"42000");
                Err(error)
            }
        }
    }

    /// The Arrow schema of this query's result.
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Hello => hello_schema(),
            Self::Numbers(_) => numbers_schema(),
            Self::RunningTotal(_) => running_total_schema(),
        }
    }

    /// Start producing this query's result.
    pub fn run(&self) -> Result<QueryResult> {
        Ok(match *self {
            Self::Hello => {
                let batch = RecordBatch::try_new(
                    hello_schema(),
                    vec![Arc::new(StringArray::from(vec!["Hello, world!"]))],
                )?;
                QueryResult::from_reader(Box::new(RecordBatchIterator::new(
                    [Ok(batch)],
                    hello_schema(),
                )))
            }
            Self::Numbers(count) => QueryResult::from_reader(numbers(count)),
            Self::RunningTotal(count) => {
                QueryResult::from_producer(running_total_schema(), RunningTotal::new(count))
            }
        })
    }
}

fn sqlstate(code: &[u8; 5]) -> [std::ffi::c_char; 5] {
    code.map(|byte| byte as std::ffi::c_char)
}

/// One ADBC statement: set a query, optionally prepare it, then execute it.
///
/// Every operation not implemented here (binding, updates, partitions and so
/// on) returns ADBC `NOT_IMPLEMENTED` through the trait's defaults.
#[derive(Debug, Default)]
pub struct HelloStatement {
    sql: Option<String>,
}

impl HelloStatement {
    fn query(&self) -> Result<Query> {
        let sql = self.sql.as_deref().ok_or_else(|| {
            Error::with_message_and_status(
                "Set a query before executing the statement",
                Status::InvalidState,
            )
        })?;
        Query::parse(sql)
    }
}

impl BackendStatement for HelloStatement {
    /// Store the query text; it is validated when prepared or executed.
    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        self.sql = Some(query.to_string());
        Ok(())
    }

    /// Validate the query; clients such as DuckDB's `adbc_scanner` prepare
    /// before executing.
    fn prepare(&mut self) -> Result<()> {
        self.query().map(drop)
    }

    /// The supported queries take no parameters.
    fn get_parameter_schema(&self) -> Result<Schema> {
        self.query()?;
        Ok(Schema::empty())
    }

    /// The result schema, without producing rows.
    fn execute_schema(&mut self) -> Result<Schema> {
        Ok(self.query()?.schema().as_ref().clone())
    }

    /// Execute the query.
    fn execute_result(&mut self) -> Result<QueryResult> {
        self.query()?.run()
    }
}

/// A client connection; each statement is independent.
#[derive(Debug, Default)]
pub struct HelloConnection;

impl BackendConnection for HelloConnection {
    fn new_statement(&mut self) -> Result<Box<dyn BackendStatement>> {
        Ok(Box::new(HelloStatement::default()))
    }
}

/// Serve the `hello` target; each client connection gets its own
/// [`HelloConnection`].
#[derive(Debug, Default)]
pub struct HelloBackend;

impl Backend for HelloBackend {
    fn open(
        &self,
        _target: &TargetConfig,
        database_options: Vec<(String, OptionValue)>,
        connection_options: Vec<(String, OptionValue)>,
    ) -> Result<Box<dyn BackendConnection>> {
        if !database_options.is_empty() || !connection_options.is_empty() {
            return Err(Error::with_message_and_status(
                "The hello-world service accepts no database or connection options",
                Status::NotImplemented,
            ));
        }
        Ok(Box::new(HelloConnection))
    }
}
