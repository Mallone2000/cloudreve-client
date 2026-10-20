//! Run with `cargo test -p cloudreve-sync --test reconciliation_benchmark -- --ignored --nocapture`.
//! The fixtures use a local mock Cloudreve API and a real SQLite inventory.

mod common;

use common::{TestEnv, remote_file};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::{Mock, ResponseTemplate, matchers::{method, path}};

#[cfg(unix)]
fn peak_rss_kib() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let peak = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") { peak / 1024 } else { peak }
}

async fn recovery_fixture(files: usize) {
    let env = TestEnv::new().await;
    let mut remote = Vec::with_capacity(files);
    for index in 0..files {
        let name = format!("file-{index:05}.txt");
        env.write_local(&name, b"same");
        env.track_synced(&name, "stable-etag");
        remote.push(remote_file(&name, 4, "stable-etag"));
    }
    let remote = Arc::new(remote);
    Mock::given(method("GET"))
        .and(path("/api/v4/file"))
        .respond_with(move |request: &wiremock::Request| {
            let page = request.url.query_pairs()
                .find(|(name, _)| name == "page")
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(0);
            let start = page.saturating_mul(100);
            let end = start.saturating_add(100).min(remote.len());
            let files = if start < remote.len() { remote[start..end].to_vec() } else { vec![] };
            ResponseTemplate::new(200).set_body_json(json!({
                "code": 0, "msg": "", "data": {
                    "files": files,
                    "pagination": { "page": page, "page_size": 100, "total_items": remote.len() },
                    "props": { "max_page_size": 100, "order_by_options": ["name"], "order_direction_options": ["asc"] }
                }
            }))
        })
        .mount(&env.server).await;

    let before_operations = env.inventory.connection_checkouts();
    let started = Instant::now();
    env.full_sync().await.expect("recovery scan should succeed");
    let elapsed = started.elapsed();
    let db_connection_checkouts = env.inventory.connection_checkouts() - before_operations;
    let peak_memory_kib = peak_rss_kib();
    println!("reconciliation_benchmark files={files} scan_ms={} db_connection_checkouts={db_connection_checkouts} process_peak_rss_kib={peak_memory_kib}", elapsed.as_millis());
    assert!(env.all_tasks().is_empty(), "unchanged recovery fixture should enqueue no transfers");
    if files == 10_000 {
        assert!(elapsed < Duration::from_secs(300), "10,000-file recovery exceeded five minutes");
    }
}

#[tokio::test]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn recovery_1000_files() {
    recovery_fixture(1_000).await;
}

#[tokio::test]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn recovery_10000_files() {
    recovery_fixture(10_000).await;
}
