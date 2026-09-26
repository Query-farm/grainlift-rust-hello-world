// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use grainlift_rust_hello_world::{Counters, SyntheticBackend, Workload};
use grainlift_server::config::TargetConfig;
use grainlift_server::service::build_server_with_max_bind;
use grainlift_server::session::{SessionLimits, SessionManager, TargetAuthorizer};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use vgi_rpc::AuthContext;
use vgi_rpc::auth::bearer::bearer_authenticate_static;
use vgi_rpc::http::HttpState;

/// A loopback-only synthetic ADBC service. Authentication is always required.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 0)]
    port: u16,
    #[arg(long, default_value_t = 4096)]
    rows: usize,
    #[arg(long, default_value_t = 512)]
    batch_rows: usize,
    #[arg(long, default_value_t = 64)]
    payload_bytes: usize,
    #[arg(long)]
    report: PathBuf,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() {
    if run().await.is_err() {
        // Never print backend errors, query text, tokens or configuration values.
        eprintln!("Synthetic server failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let workload = Workload::new(args.rows, args.batch_rows, args.payload_bytes)?;
    let token = std::env::var("GRAINLIFT_HELLO_TOKEN")?;
    if token.len() < 16 {
        return Err("A bearer token of at least 16 bytes is required".into());
    }
    let counters = Arc::new(Counters::default());
    let target = TargetConfig {
        driver: "synthetic".into(),
        entrypoint: None,
        database_options: vec![],
        connection_options: vec![],
        allow_client_database_options: false,
        allow_client_connection_options: false,
        allowed_client_database_options: vec![],
        allowed_client_connection_options: vec!["adbc.connection.autocommit".into()],
    };
    let manager = Arc::new(SessionManager::with_limits_authorizer_and_timeout(
        Arc::new(SyntheticBackend {
            workload,
            counters: counters.clone(),
        }),
        HashMap::from([("default".into(), target)]),
        Duration::from_secs(10),
        true,
        SessionLimits {
            max_sessions: 3,
            max_sessions_per_principal: 3,
            max_statements_per_session: 32,
            max_results_per_session: 32,
        },
        TargetAuthorizer::new(HashMap::from([(
            "load-principal".into(),
            vec!["default".into()],
        )])),
        Duration::from_secs(5),
    ));
    let rpc = Arc::new(build_server_with_max_bind(
        manager.clone(),
        "synthetic-rust".into(),
        64 * 1024 * 1024,
    ));
    let state = HttpState::builder()
        .server(rpc)
        .authenticate(bearer_authenticate_static(HashMap::from([(
            token,
            AuthContext::for_principal("bearer", "load-principal"),
        )])))
        .max_body_size(2 * 1024 * 1024)
        .max_request_bytes(2 * 1024 * 1024)
        .request_timeout(Duration::from_secs(10))
        .build();
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", args.port)).await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let reaper_manager = manager.clone();
    let reaper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let _ = reaper_manager.reap_expired();
        }
    });
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        // Only a newline or EOF is needed. Never allocate for an unbounded line.
        let _ = io::stdin().lock().fill_buf();
        let _ = stop_tx.send(());
    });
    println!(
        "{}",
        serde_json::json!({"endpoint": endpoint, "sample_pid": std::process::id(),
        "transport": "authenticated loopback HTTP, native ADBC C ABI, Rust synthetic in-process backend"})
    );
    io::stdout().flush()?;
    axum::serve(listener, vgi_rpc::http::build_router(state))
        .with_graceful_shutdown(async {
            tokio::select! {
                _ = stop_rx => {},
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate_signal() => {},
            }
        })
        .await?;
    reaper.abort();
    let before = manager.resource_counts()?;
    manager.close_all()?;
    // The shared server performs driver destruction on a bounded cleanup path.
    // Wait for this finite synthetic backend's destructors to finish.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while counters.closed.load(Ordering::Relaxed) != counters.opened.load(Ordering::Relaxed) {
        if std::time::Instant::now() >= deadline {
            return Err("Cleanup deadline exceeded".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let after = manager.resource_counts()?;
    let report = serde_json::json!({
        "http_host": "rust", "worker": "direct", "runtime_threads": 1,
        "rows": args.rows, "batch_rows": args.batch_rows, "payload_bytes": args.payload_bytes,
        "connections_opened": counters.opened.load(Ordering::Relaxed),
        "connections_closed": counters.closed.load(Ordering::Relaxed),
        "queries": counters.queries.load(Ordering::Relaxed),
        "intentional_errors": counters.failures.load(Ordering::Relaxed),
        "generated_batches": counters.batches.load(Ordering::Relaxed),
        "before_shutdown": {"sessions": before.sessions, "statements": before.statements,
            "results": before.results, "bind_uploads": before.bind_uploads, "opening_sessions": before.opening_sessions},
        "after_shutdown": {"sessions": after.sessions, "statements": after.statements,
            "results": after.results, "bind_uploads": after.bind_uploads, "opening_sessions": after.opening_sessions}
    });
    std::fs::write(
        args.report,
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    Ok(())
}

async fn terminate_signal() {
    #[cfg(unix)]
    if let Ok(mut signal) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        signal.recv().await;
        return;
    }
    std::future::pending::<()>().await;
}
