//! Run with `cargo test -p cloudreve-sync --test reconciliation_benchmark -- --ignored --nocapture`.
//! The fixtures use a local mock Cloudreve API and a real SQLite inventory.

mod common;

use cloudreve_sync::inventory::NewTaskRecord;
use common::{TestEnv, remote_file};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

#[cfg(unix)]
fn peak_rss_kib() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let peak = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        peak / 1024
    } else {
        peak
    }
}

async fn recovery_fixture(files: usize, changed: usize) {
    let env = TestEnv::new().await;
    let mut remote = Vec::with_capacity(files);
    for index in 0..files {
        let name = format!("file-{index:05}.txt");
        env.write_local(&name, b"same");
        env.track_synced(&name, "stable-etag");
        if index < changed {
            let path = env.local_path(&name);
            let metadata = std::fs::metadata(&path).unwrap();
            let before = filetime::FileTime::from_last_modification_time(&metadata);
            std::fs::write(&path, b"edit").unwrap();
            let (seconds, nanos) = if before.nanoseconds() == 999_999_999 {
                (before.unix_seconds() + 1, 0)
            } else {
                (before.unix_seconds(), before.nanoseconds() + 1)
            };
            filetime::set_file_mtime(&path, filetime::FileTime::from_unix_time(seconds, nanos))
                .unwrap();
        }
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
    println!(
        "reconciliation_benchmark files={files} changed={changed} scan_ms={} db_connection_checkouts={db_connection_checkouts} process_peak_rss_kib={peak_memory_kib}",
        elapsed.as_millis()
    );
    assert!(
        env.tasks_of_type("upload").len() == changed,
        "changed-file fixture should enqueue exactly the edited files"
    );
    if files == 10_000 {
        assert!(
            elapsed < Duration::from_secs(300),
            "10,000-file recovery exceeded five minutes"
        );
    }
}

#[tokio::test]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn recovery_1000_files() {
    recovery_fixture(1_000, 0).await;
}

#[tokio::test]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn recovery_10000_files() {
    recovery_fixture(10_000, 0).await;
}

#[tokio::test]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn recovery_10000_changed_files() {
    recovery_fixture(10_000, 100).await;
}

#[tokio::test]
#[cfg(unix)]
#[ignore = "performance fixture; run in the CI benchmark job"]
async fn backlog_50000_tasks_stays_bounded() {
    let mut env = TestEnv::new().await;
    let db = env.inventory.clone();
    let drive = env.drive_id.clone();
    for page in 0..200 {
        let batch: Vec<_> = (0..250)
            .map(|offset| {
                let number = page * 250 + offset;
                NewTaskRecord::new(
                    format!("task-{number}"),
                    &drive,
                    "download",
                    env.local_path(&format!("file-{number}"))
                        .to_string_lossy()
                        .into_owned(),
                )
            })
            .collect();
        assert_eq!(db.insert_task_batch(&batch).unwrap().len(), 250);
    }
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(&env.server)
        .await;
    let before = peak_rss_kib();
    env.restart_mount().await;
    for _ in 0..50 {
        if env.mount.task_queue.inflight_count() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(env.mount.task_queue.inflight_count(), 2);
    let growth = peak_rss_kib().saturating_sub(before);
    println!("backlog_50000_tasks peak_rss_growth_kib={growth}");
    assert!(
        growth < 96 * 1024,
        "dispatcher must not load the backlog into memory"
    );
}
