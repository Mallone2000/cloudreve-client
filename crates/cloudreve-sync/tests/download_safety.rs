mod common;

use cloudreve_sync::inventory::{BlockedReason, ConflictState, NewTaskRecord, TaskStatus};
use cloudreve_sync::tasks::TaskPayload;
use common::{TestEnv, remote_file};
use serde_json::json;
use std::{sync::atomic::Ordering, time::Duration};
use wiremock::{
    Mock, ResponseTemplate,
    matchers::{method, path},
};

async fn configure_download(
    env: &TestEnv,
    name: &str,
    declared: i64,
    body: Vec<u8>,
    delay: Duration,
) {
    let remote = remote_file(name, declared, "remote-new");
    Mock::given(method("GET"))
        .and(path("/api/v4/file/info"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"code":0,"msg":"","data":remote})),
        )
        .mount(&env.server)
        .await;
    Mock::given(method("POST")).and(path("/api/v4/file/url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code":0,"msg":"","data":{"urls":[{"url":format!("{}/blob/file",env.server.uri())}],"expires":"2099-01-01T00:00:00Z"}})))
        .mount(&env.server).await;
    Mock::given(method("GET"))
        .and(path("/blob/file"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(body)
                .set_delay(delay),
        )
        .mount(&env.server)
        .await;
}

async fn wait_for_blob_request(env: &TestEnv) {
    for _ in 0..100 {
        if env
            .server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .any(|request| request.url.path() == "/blob/file")
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("download request did not start");
}

async fn wait_for_terminal_download(env: &TestEnv) -> TaskStatus {
    for _ in 0..150 {
        if let Some(task) = env.tasks_of_type("download").into_iter().find(|task| {
            matches!(
                task.status,
                TaskStatus::Failed | TaskStatus::Completed | TaskStatus::Cancelled
            )
        }) {
            return task.status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "download did not finish: {:?}",
        env.tasks_of_type("download")
    );
}

#[tokio::test]
async fn declared_size_must_match_actual_bytes() {
    for (declared, body) in [(4, b"longer".to_vec()), (8, b"short".to_vec())] {
        let env = TestEnv::new().await;
        env.set_remote_files(vec![remote_file("file", declared, "remote-new")])
            .await;
        configure_download(&env, "file", declared, body, Duration::ZERO).await;
        env.full_sync().await.unwrap();
        assert_eq!(wait_for_terminal_download(&env).await, TaskStatus::Failed);
        assert!(!env.local_path("file").exists());
    }
}

#[tokio::test]
async fn local_edit_during_download_is_a_conflict() {
    let env = TestEnv::new().await;
    env.write_local("file", b"old");
    env.track_synced("file", "remote-old");
    env.set_remote_files(vec![remote_file("file", 3, "remote-new")])
        .await;
    configure_download(&env, "file", 3, b"new".to_vec(), Duration::from_millis(500)).await;
    env.full_sync().await.unwrap();
    wait_for_blob_request(&env).await;
    env.write_local("file", b"edit");
    assert_eq!(
        wait_for_terminal_download(&env).await,
        TaskStatus::Completed,
        "{:?}",
        env.tasks_of_type("download")
    );
    assert_eq!(std::fs::read(env.local_path("file")).unwrap(), b"edit");
    assert_eq!(
        env.db_entry("file").unwrap().conflict_state,
        Some(ConflictState::Pending)
    );
}

#[tokio::test]
async fn reducing_limit_cancels_running_download_before_install() {
    let env = TestEnv::with_max_file_size(2).await;
    let bytes = vec![b'x'; 1_500_000];
    env.set_remote_files(vec![remote_file("file", bytes.len() as i64, "remote-new")])
        .await;
    configure_download(
        &env,
        "file",
        bytes.len() as i64,
        bytes,
        Duration::from_millis(500),
    )
    .await;
    env.full_sync().await.unwrap();
    wait_for_blob_request(&env).await;
    env.mount
        .task_queue
        .max_file_size_mb
        .store(1, Ordering::SeqCst);
    assert_eq!(wait_for_terminal_download(&env).await, TaskStatus::Failed);
    assert!(!env.local_path("file").exists());
}

#[tokio::test]
async fn storage_consent_pending_and_revocation_survive_restart() {
    let mut env = TestEnv::new().await;
    let origin = env.server.uri();
    env.mount
        .config
        .write()
        .await
        .approved_http_storage_origins
        .clear();
    env.mount
        .task_queue
        .approved_http_storage_origins
        .write()
        .unwrap()
        .clear();
    env.set_remote_files(vec![remote_file("file", 4, "remote-new")])
        .await;
    configure_download(
        &env,
        "file",
        4,
        b"data".to_vec(),
        Duration::from_millis(500),
    )
    .await;
    env.full_sync().await.unwrap();
    for _ in 0..100 {
        if env
            .tasks_of_type("download")
            .iter()
            .any(|task| task.blocked_reason == Some(BlockedReason::StorageConsent))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let blocked = env.tasks_of_type("download");
    assert_eq!(blocked[0].blocked_origin.as_deref(), Some(origin.as_str()));
    env.restart_mount().await;
    assert_eq!(
        env.tasks_of_type("download")[0].blocked_reason,
        Some(BlockedReason::StorageConsent)
    );

    env.mount
        .config
        .write()
        .await
        .approved_http_storage_origins
        .push(origin.clone());
    env.mount
        .task_queue
        .approve_storage_origin(&origin)
        .unwrap();
    wait_for_blob_request(&env).await;
    env.mount
        .config
        .write()
        .await
        .approved_http_storage_origins
        .clear();
    env.mount
        .task_queue
        .revoke_storage_origin(&origin)
        .await
        .unwrap();
    assert_eq!(
        wait_for_terminal_download(&env).await,
        TaskStatus::Cancelled
    );
    env.restart_mount().await;
    assert_eq!(
        env.tasks_of_type("download")[0].status,
        TaskStatus::Cancelled
    );
    assert!(!env.local_path("file").exists());
}

#[tokio::test]
async fn revoking_origin_aborts_a_stalled_download() {
    let env = TestEnv::new().await;
    let origin = env.server.uri();
    env.set_remote_files(vec![remote_file("file", 4, "remote-new")])
        .await;
    configure_download(&env, "file", 4, b"data".to_vec(), Duration::from_secs(30)).await;
    env.full_sync().await.unwrap();
    wait_for_blob_request(&env).await;
    env.mount
        .task_queue
        .revoke_storage_origin(&origin)
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(1), wait_for_terminal_download(&env))
        .await
        .expect("revoked download stayed active");
    assert_eq!(status, TaskStatus::Cancelled);
    assert!(!env.local_path("file").exists());
}

#[tokio::test]
async fn approved_override_is_replayed_after_restart() {
    let mut env = TestEnv::new().await;
    env.write_local("file", b"local");
    env.track_synced("file", "remote-old");
    env.inventory
        .mark_as_conflicted(
            env.local_path("file").to_str().unwrap(),
            Some(ConflictState::Pending),
        )
        .unwrap();
    env.set_remote_files(vec![remote_file("file", 6, "remote-new")])
        .await;
    configure_download(&env, "file", 6, b"remote".to_vec(), Duration::ZERO).await;
    let task = NewTaskRecord::new(
        "approved-download",
        &env.drive_id,
        "download",
        env.local_path("file").to_str().unwrap(),
    )
    .with_custom_state(json!({"force_override": true}));
    env.inventory.insert_task_if_not_exist(&task).unwrap();
    env.restart_mount().await;
    assert_eq!(
        wait_for_terminal_download(&env).await,
        TaskStatus::Completed,
        "{:?}",
        env.tasks_of_type("download")
    );
    assert_eq!(std::fs::read(env.local_path("file")).unwrap(), b"remote");
}

#[tokio::test]
async fn negative_remote_size_and_overflowing_limit_are_rejected() {
    let env = TestEnv::new().await;
    configure_download(&env, "file", -1, b"data".to_vec(), Duration::ZERO).await;
    env.mount
        .task_queue
        .enqueue(TaskPayload::download(env.local_path("file")))
        .await
        .unwrap();
    assert_eq!(wait_for_terminal_download(&env).await, TaskStatus::Failed);
    assert!(!env.local_path("file").exists());

    let other = TestEnv::new().await;
    configure_download(&other, "file", 4, b"data".to_vec(), Duration::ZERO).await;
    other
        .mount
        .task_queue
        .max_file_size_mb
        .store(u64::MAX, Ordering::SeqCst);
    other
        .mount
        .task_queue
        .enqueue(TaskPayload::download(other.local_path("file")))
        .await
        .unwrap();
    assert_eq!(wait_for_terminal_download(&other).await, TaskStatus::Failed);
    assert!(!other.local_path("file").exists());
}
