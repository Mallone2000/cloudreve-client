use crate::drive::event_blocker::EventBlocker;
use crate::drive::ignore::IgnoreMatcher;
use crate::events::SummaryNotifier;
use crate::inventory::{
    BlockedReason, InventoryDb, NewTaskRecord, TaskRecord, TaskStatus, TaskUpdate,
};
use crate::tasks::download::DownloadTask;
use crate::tasks::types::{TaskKind, TaskPayload, TaskProgress};
use crate::tasks::upload::UploadTask;
use crate::tasks::verify_collision;
use anyhow::{Context, Result, anyhow};
use cloudreve_api::Client;
use dashmap::DashMap;
use serde_json::Value;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TaskQueueConfig {
    pub max_concurrent: usize,
}

impl Default for TaskQueueConfig {
    fn default() -> Self {
        Self { max_concurrent: 2 }
    }
}

const MAX_NETWORK_RETRIES: u32 = 5;

fn is_network_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.downcast_ref::<reqwest::Error>().is_some()
            || cause.downcast_ref::<std::io::Error>().is_some_and(|e| {
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::NotConnected
                )
            })
    })
}

pub struct TaskQueue {
    pub drive_id: String,
    pub cr_client: Arc<Client>,
    pub inventory: Arc<InventoryDb>,
    pub sync_path: PathBuf,
    pub remote_base: String,
    pub event_blocker: EventBlocker,
    pub max_file_size_mb: Arc<AtomicU64>,
    download_client: Arc<reqwest::Client>,
    pub approved_http_storage_origins: Arc<RwLock<HashSet<String>>>,
    revoked_http_storage_origins: RwLock<HashSet<String>>,
    config: TaskQueueConfig,
    semaphore: Arc<Semaphore>,
    dispatch_wake: Notify,
    ignore: IgnoreMatcher,
    dispatcher_handle: Mutex<Option<JoinHandle<()>>>,
    inflight: AtomicUsize,
    idle_notify: Notify,
    shutting_down: AtomicBool,
    cancel_requested: AtomicBool,
    progress: Arc<DashMap<String, TaskProgress>>,
    notifier: Arc<SummaryNotifier>,
    task_handles: DashMap<String, JoinHandle<()>>,
    /// Maps task_id to local_path for running tasks, used for path-based cancellation
    task_paths: DashMap<String, String>,
    active_storage_origins: Arc<DashMap<String, String>>,
}

struct InflightGuard {
    queue: Arc<TaskQueue>,
    task_id: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.queue.inflight.fetch_sub(1, Ordering::SeqCst);
        self.queue.idle_notify.notify_waiters();
        self.queue.dispatch_wake.notify_one();
        self.queue.task_handles.remove(&self.task_id);
        self.queue.active_storage_origins.remove(&self.task_id);
    }
}

impl TaskQueue {
    pub async fn new(
        drive_id: impl Into<String>,
        cr_client: Arc<Client>,
        inventory: Arc<InventoryDb>,
        config: TaskQueueConfig,
        sync_path: PathBuf,
        remote_base: String,
        event_blocker: EventBlocker,
        max_file_size_mb: u64,
        _instance_url: &str,
        _allow_insecure_http: bool,
        approved_http_storage_origins: Vec<String>,
        notifier: Arc<SummaryNotifier>,
        ignore: &IgnoreMatcher,
    ) -> Arc<Self> {
        let drive_id = drive_id.into();
        let max_concurrent = config.max_concurrent.max(1);
        let sanitized_config = TaskQueueConfig { max_concurrent };

        let approved_http_storage_origins: crate::uploader::transport::StorageApprovals = Arc::new(
            RwLock::new(approved_http_storage_origins.into_iter().collect()),
        );
        let redirect_approvals = approved_http_storage_origins.clone();
        let download_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .read_timeout(std::time::Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 10 {
                    return attempt.stop();
                }
                if attempt.previous().first().is_some_and(|initial| {
                    crate::tasks::download::allowed_download_redirect(
                        initial,
                        attempt.url(),
                        &redirect_approvals,
                    )
                }) {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()
            .expect("Failed to create download HTTP client");
        let queue = Arc::new(Self {
            drive_id,
            inventory,
            cr_client,
            sync_path,
            remote_base,
            event_blocker,
            max_file_size_mb: Arc::new(AtomicU64::new(max_file_size_mb)),
            download_client: Arc::new(download_client),
            approved_http_storage_origins,
            revoked_http_storage_origins: RwLock::new(HashSet::new()),
            config: sanitized_config,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            dispatch_wake: Notify::new(),
            ignore: ignore.clone(),
            dispatcher_handle: Mutex::new(None),
            inflight: AtomicUsize::new(0),
            idle_notify: Notify::new(),
            shutting_down: AtomicBool::new(false),
            cancel_requested: AtomicBool::new(false),
            progress: Arc::new(DashMap::new()),
            notifier,
            task_handles: DashMap::new(),
            task_paths: DashMap::new(),
            active_storage_origins: Arc::new(DashMap::new()),
        });

        if let Err(err) = queue.resume_incomplete_tasks(ignore).await {
            warn!(
                target: "tasks::queue",
                drive = %queue.drive_id,
                error = %err,
                "Failed to resume pending tasks from inventory"
            );
        }
        queue.spawn_dispatcher().await;
        queue.dispatch_wake.notify_one();
        queue
    }

    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }

    pub fn drive_id(&self) -> &str {
        &self.drive_id
    }

    pub async fn enqueue(&self, payload: TaskPayload) -> Result<String> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(anyhow!("task queue is shutting down"));
        }
        crate::drive::utils::SafeSyncPath::from_local(&payload.local_path, &self.sync_path)?
            .join_checked(&self.sync_path)?;

        // A download that permanently failed for this exact remote version
        // (same etag) will fail again: don't re-enqueue it on every sync.
        // A new etag (remote file changed) makes it eligible again, and
        // force_override (explicit user action) always bypasses this.
        if payload.kind == TaskKind::Download && !payload.force_override {
            if let Some(etag) = payload
                .custom_state
                .as_ref()
                .and_then(|s| s.get("remote_etag"))
                .and_then(|v| v.as_str())
            {
                let inventory = self.inventory.clone();
                let drive_id = self.drive_id.clone();
                let kind = payload.kind.as_str().to_string();
                let path = payload.local_path_display();
                let latest = tokio::task::spawn_blocking(move || {
                    inventory.latest_task_for_path(&drive_id, &kind, &path)
                })
                .await
                .context("Task lookup panicked")??;
                if let Some(prev) = latest {
                    let prev_etag = prev
                        .custom_state
                        .as_ref()
                        .and_then(|s| s.get("remote_etag"))
                        .and_then(|v| v.as_str());
                    if prev.status == TaskStatus::Failed && prev_etag == Some(etag) {
                        debug!(
                            target: "tasks::queue",
                            drive = %self.drive_id,
                            path = %payload.local_path_display(),
                            etag = %etag,
                            "Skipping download: already failed permanently for this remote version"
                        );
                        return Ok(prev.id);
                    }
                }
            }
        }

        let task_id = payload
            .task_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut record = NewTaskRecord::new(
            task_id.clone(),
            self.drive_id.clone(),
            payload.kind.as_str().to_string(),
            payload.local_path_display(),
        )
        .with_priority(payload.priority);

        match (payload.total_bytes, payload.processed_bytes) {
            (Some(total), Some(processed)) => {
                record = record.with_totals(total, processed);
            }
            (Some(total), None) => {
                record = record.with_totals(total, 0);
            }
            (None, Some(processed)) => {
                record = record.with_totals(0, processed);
            }
            _ => {}
        }

        let mut state = payload
            .custom_state
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if payload.force_override {
            state["force_override"] = serde_json::Value::Bool(true);
        }
        if state.as_object().is_some_and(|object| !object.is_empty()) {
            record = record.with_custom_state(state);
        }

        let inventory = self.inventory.clone();
        let inserted =
            tokio::task::spawn_blocking(move || inventory.insert_task_if_not_exist(&record))
                .await
                .context("Task insertion panicked")?
                .with_context(|| format!("Failed to persist task {}", task_id))?;

        if !inserted {
            tracing::trace!(
                target: "tasks::queue",
                task_id = %task_id,
                path = %payload.local_path_display(),
                kind = %payload.kind.as_str(),
                "Task already exists, skipping enqueue"
            );
            return Ok(task_id);
        }

        self.dispatch_wake.notify_one();
        self.notifier.notify();
        Ok(task_id)
    }

    /// Persist reconciliation work in bounded transactions before dispatching.
    pub async fn enqueue_many(&self, payloads: Vec<TaskPayload>) -> Result<usize> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(anyhow!("task queue is shutting down"));
        }
        let mut count = 0;
        let mut remaining = payloads.into_iter();
        loop {
            let batch: Vec<_> = remaining.by_ref().take(250).collect();
            if batch.is_empty() {
                break;
            }
            let mut prepared = Vec::new();
            let mut records = Vec::new();
            for payload in batch {
                if payload.force_override {
                    self.enqueue(payload).await?;
                    count += 1;
                    continue;
                }
                crate::drive::utils::SafeSyncPath::from_local(
                    &payload.local_path,
                    &self.sync_path,
                )?
                .join_checked(&self.sync_path)?;
                let id = payload
                    .task_id
                    .clone()
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let mut record = NewTaskRecord::new(
                    id.clone(),
                    self.drive_id.clone(),
                    payload.kind.as_str().to_string(),
                    payload.local_path_display(),
                )
                .with_priority(payload.priority);
                if let Some(total) = payload.total_bytes {
                    record = record.with_totals(total, payload.processed_bytes.unwrap_or(0));
                } else if let Some(processed) = payload.processed_bytes {
                    record = record.with_totals(0, processed);
                }
                if let Some(state) = payload.custom_state.clone() {
                    record = record.with_custom_state(state);
                }
                records.push(record);
                prepared.push((id, payload));
            }
            if records.is_empty() {
                continue;
            }
            let inventory = self.inventory.clone();
            let inserted =
                tokio::task::spawn_blocking(move || inventory.insert_task_batch(&records))
                    .await
                    .context("Task batch insertion panicked")??;
            for ((_id, _payload), was_inserted) in prepared.into_iter().zip(inserted) {
                if was_inserted {
                    count += 1;
                }
            }
        }
        if count > 0 {
            self.dispatch_wake.notify_one();
            self.notifier.notify();
        }
        Ok(count)
    }

    pub fn notifier(&self) -> &Arc<SummaryNotifier> {
        &self.notifier
    }

    pub fn approve_storage_origin(&self, origin: &str) -> Result<()> {
        self.revoked_http_storage_origins
            .write()
            .map_err(|_| anyhow!("Storage revocations unavailable"))?
            .remove(origin);
        self.approved_http_storage_origins
            .write()
            .map_err(|_| anyhow!("Storage approvals unavailable"))?
            .insert(origin.to_string());
        self.inventory.clear_storage_block(&self.drive_id, origin)?;
        self.dispatch_wake.notify_one();
        self.notifier.notify();
        Ok(())
    }

    pub async fn revoke_storage_origin(&self, origin: &str) -> Result<()> {
        self.revoked_http_storage_origins
            .write()
            .map_err(|_| anyhow!("Storage revocations unavailable"))?
            .insert(origin.to_string());
        self.approved_http_storage_origins
            .write()
            .map_err(|_| anyhow!("Storage approvals unavailable"))?
            .remove(origin);
        self.inventory
            .cancel_storage_block(&self.drive_id, origin)?;
        let ids: Vec<_> = self
            .task_handles
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        let mut stopped = Vec::new();
        for id in ids {
            let active_download = self
                .active_storage_origins
                .get(&id)
                .is_some_and(|active| active.value() == origin);
            let affected = active_download
                || self
                    .inventory
                    .get_upload_session(&id)?
                    .is_some_and(|session| session.uses_http_origin(origin));
            if affected {
                let local_path = self.task_paths.get(&id).map(|path| path.value().clone());
                if let Some((_, handle)) = self.task_handles.remove(&id) {
                    handle.abort();
                    stopped.push((handle, id.clone(), local_path));
                }
                if self
                    .inventory
                    .get_task_status(&id)?
                    .is_some_and(|status| status.is_active())
                {
                    self.inventory.update_task(
                        &id,
                        TaskUpdate {
                            status: Some(TaskStatus::Cancelled),
                            ..Default::default()
                        },
                    )?;
                }
                self.progress.remove(&id);
                self.task_paths.remove(&id);
                self.active_storage_origins.remove(&id);
            }
        }
        for (handle, id, local_path) in stopped {
            let _ = handle.await;
            if let Some(path) = local_path {
                let path = PathBuf::from(path);
                let staged = path.with_file_name(format!(
                    "{}{}",
                    crate::drive::utils::PARTIAL_DOWNLOAD_PREFIX,
                    id
                ));
                if let Ok(safe) =
                    crate::drive::utils::SafeSyncPath::from_local(&staged, &self.sync_path)
                {
                    let _ = safe.remove_checked(&self.sync_path);
                }
            }
        }
        self.dispatch_wake.notify_one();
        self.notifier.notify();
        Ok(())
    }

    pub fn list_active_tasks(&self) -> Result<Vec<TaskRecord>> {
        self.inventory.list_tasks(
            Some(&self.drive_id),
            Some(&[TaskStatus::Pending, TaskStatus::Running]),
        )
    }

    pub async fn ongoing_progress(&self) -> Vec<TaskProgress> {
        self.progress
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub async fn persist_progress(
        &self,
        task_id: &str,
        progress: f64,
        processed_bytes: Option<i64>,
        total_bytes: Option<i64>,
        custom_state: Option<Value>,
    ) -> Result<()> {
        let clamped = progress.clamp(0.0, 1.0);
        if let Some(mut entry) = self.progress.get_mut(task_id) {
            entry.update(clamped, processed_bytes, total_bytes, custom_state);
            Ok(())
        } else {
            Err(anyhow!("No progress entry for task {}", task_id))
        }
    }

    pub fn inflight_count(&self) -> usize {
        self.inflight.load(Ordering::SeqCst)
    }

    pub async fn shutdown(&self) {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return;
        }

        self.cancel_requested.store(true, Ordering::SeqCst);

        self.dispatch_wake.notify_waiters();

        if let Some(handle) = self.dispatcher_handle.lock().await.take() {
            handle.abort();
            let _ = handle.await;
        }

        self.cancel_running_tasks().await;
        self.task_handles.clear();
        self.task_paths.clear();
        self.progress.clear();
    }

    /// Re-enqueue all pending tasks that are waiting for network reconnection.
    /// Clears the `offline_waiting` flag and dispatches them for execution.
    /// Called when the SSE connection is restored.
    ///
    /// Parked tasks outlive the process, so this list can contain paths queued
    /// by an older version that had no ignore rules — or paths the user has
    /// excluded since. They are cancelled rather than skipped: a skipped task
    /// keeps its `offline_waiting` flag and comes back at every reconnection.
    pub fn re_enqueue_offline_tasks(&self, ignore: &IgnoreMatcher) -> Result<usize> {
        self.inventory.mark_legacy_offline_tasks(&self.drive_id)?;
        self.cancel_ignored_pending(ignore)?;
        let count = self.inventory.set_offline_block(&self.drive_id, false)?;
        self.inventory.clear_legacy_offline_flags(&self.drive_id)?;
        if count > 0 {
            self.dispatch_wake.notify_one();
            self.notifier.notify();
        }
        Ok(count)
    }

    /// Mark all pending and running tasks as offline-waiting.
    /// This cancels any currently running tasks and updates their custom_state
    /// so the UI shows "Waiting for connection...".
    /// Called when the heartbeat detects the connection is lost.
    pub async fn force_offline_waiting(&self) -> Result<usize> {
        let count = self.inventory.set_offline_block(&self.drive_id, true)?;
        self.cancel_running_tasks().await;

        if count > 0 {
            info!(
                target: "tasks::queue",
                drive = %self.drive_id,
                count = count,
                "Marked tasks as offline waiting"
            );
            self.notifier.notify();
        }

        Ok(count)
    }

    /// Cancel all tasks for a given path or its descendants.
    /// This will:
    /// 1. Mark pending tasks in inventory as cancelled
    /// 2. Abort running tasks that match the path
    /// 3. A claimed task checks its status before execution and exits if cancelled
    ///
    /// Returns the number of tasks that were cancelled.
    pub async fn cancel_by_path(&self, path: impl AsRef<std::path::Path>) -> Result<usize> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        info!(
            target: "tasks::queue",
            drive = %self.drive_id,
            path = %path_str,
            "Cancelling tasks by path"
        );

        // 1. Cancel pending tasks in inventory (this also marks running tasks as cancelled)
        let cancelled_ids = self
            .inventory
            .cancel_tasks_by_path(&self.drive_id, &path_str)
            .context("Failed to cancel tasks in inventory")?;

        let cancelled_count = cancelled_ids.len();

        // 2. Abort running task handles that match the path
        let tasks_to_abort: Vec<String> = self
            .task_paths
            .iter()
            .filter(|entry| {
                let task_path = entry.value();
                task_path == &path_str
                    || task_path.starts_with(&format!("{}{}", path_str, std::path::MAIN_SEPARATOR))
            })
            .map(|entry| entry.key().clone())
            .collect();

        for task_id in tasks_to_abort {
            if let Some((_, handle)) = self.task_handles.remove(&task_id) {
                handle.abort();
                debug!(
                    target: "tasks::queue",
                    drive = %self.drive_id,
                    task_id = %task_id,
                    "Aborted running task"
                );
            }
            self.task_paths.remove(&task_id);
            self.progress.remove(&task_id);
        }

        if cancelled_count > 0 {
            self.dispatch_wake.notify_one();
            info!(
                target: "tasks::queue",
                drive = %self.drive_id,
                path = %path_str,
                count = cancelled_count,
                "Cancelled tasks by path"
            );
            self.notifier.notify();
        }

        Ok(cancelled_count)
    }

    /// Cancel all running and pending tasks without shutting down the queue.
    /// Used when pausing a drive — the queue stays alive for later resume.
    pub async fn cancel_all(&self) {
        if let Err(e) = self.inventory.cancel_all_active_tasks(&self.drive_id) {
            warn!(target: "tasks::queue", error = %e, "Failed to cancel active tasks in inventory");
        }
        self.cancel_running_tasks().await;

        self.notifier.notify();
        info!(target: "tasks::queue", drive = %self.drive_id, "All tasks cancelled (drive paused)");
    }

    async fn spawn_dispatcher(self: &Arc<Self>) {
        let queue = Arc::clone(self);
        let handle = tokio::spawn(async move {
            queue.run_dispatch_loop().await;
        });
        *self.dispatcher_handle.lock().await = Some(handle);
    }

    async fn run_dispatch_loop(self: Arc<Self>) {
        info!(
            target: "tasks::queue",
            drive = %self.drive_id,
            concurrency = self.config.max_concurrent,
            "Task queue dispatcher started"
        );

        loop {
            let notified = self.dispatch_wake.notified();
            if self.shutting_down.load(Ordering::SeqCst) {
                break;
            }
            let available = self.semaphore.available_permits();
            if available > 0 {
                let inventory = self.inventory.clone();
                let drive = self.drive_id.clone();
                match tokio::task::spawn_blocking(move || {
                    inventory.claim_eligible_tasks(&drive, available)
                })
                .await
                {
                    Ok(Ok(records)) if !records.is_empty() => {
                        for record in records {
                            if self.ignore.is_match(&record.local_path) {
                                let _ = self.inventory.update_task(
                                    &record.id,
                                    TaskUpdate {
                                        status: Some(TaskStatus::Cancelled),
                                        ..Default::default()
                                    },
                                );
                                continue;
                            }
                            match Self::payload_from_record(&record) {
                                Ok(payload) => {
                                    self.launch_task(QueuedTask {
                                        task_id: record.id,
                                        payload,
                                    })
                                    .await
                                }
                                Err(error) => {
                                    warn!(target: "tasks::queue", error = %error, "Invalid persisted task");
                                    let _ = self.inventory.update_task(
                                        &record.id,
                                        TaskUpdate {
                                            status: Some(TaskStatus::Failed),
                                            error: Some(Some("Invalid task payload".into())),
                                            ..Default::default()
                                        },
                                    );
                                }
                            }
                        }
                        continue;
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        warn!(target: "tasks::queue", error = %error, "Failed to claim tasks")
                    }
                    Err(error) => {
                        warn!(target: "tasks::queue", error = %error, "Task claim panicked")
                    }
                }
            }
            notified.await;
        }

        info!(
            target: "tasks::queue",
            drive = %self.drive_id,
            "Task queue dispatcher stopped"
        );
    }

    async fn launch_task(self: &Arc<Self>, task: QueuedTask) {
        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(err) => {
                error!(
                    target: "tasks::queue",
                    drive = %self.drive_id,
                    error = %err,
                    "Failed to acquire semaphore permit"
                );
                if let Err(update_err) = self.inventory.update_task(
                    &task.task_id,
                    TaskUpdate {
                        status: Some(TaskStatus::Failed),
                        error: Some(Some("Failed to schedule task".to_string())),
                        ..Default::default()
                    },
                ) {
                    warn!(
                        target: "tasks::queue",
                        drive = %self.drive_id,
                        error = %update_err,
                        "Failed to persist scheduling failure"
                    );
                }
                return;
            }
        };

        self.inflight.fetch_add(1, Ordering::SeqCst);
        let queue_for_execute = Arc::clone(self);
        let queue_for_notify = Arc::clone(self);
        let task_id = task.task_id.clone();
        let handle_task_id = task_id.clone();
        let guard = InflightGuard {
            queue: Arc::clone(self),
            task_id: task_id.clone(),
        };

        let handle = tokio::spawn(async move {
            let _guard = guard;
            // Catch panics from task execution so the queue never leaks
            // permits/counters and the task doesn't stay "Running" forever.
            let result = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                queue_for_execute.execute_task(task),
            ))
            .await;

            if result.is_err() {
                error!(
                    target: "tasks::queue",
                    drive = %queue_for_notify.drive_id,
                    task_id = %handle_task_id,
                    "Task panicked during execution"
                );
                if let Err(update_err) = queue_for_notify.inventory.update_task(
                    &handle_task_id,
                    TaskUpdate {
                        status: Some(TaskStatus::Failed),
                        error: Some(Some("Task panicked during execution".to_string())),
                        ..Default::default()
                    },
                ) {
                    warn!(
                        target: "tasks::queue",
                        drive = %queue_for_notify.drive_id,
                        task_id = %handle_task_id,
                        error = %update_err,
                        "Failed to persist panic failure state"
                    );
                }
                queue_for_notify.cleanup_task_entry(&handle_task_id).await;
                queue_for_notify.notifier.notify();
            }

            drop(permit);
        });

        self.task_handles.insert(task_id, handle);
    }

    async fn execute_task(self: Arc<Self>, task: QueuedTask) {
        // Check if task was cancelled while in the channel queue
        match self.inventory.get_task_status(&task.task_id) {
            Ok(Some(TaskStatus::Cancelled)) => {
                debug!(
                    target: "tasks::queue",
                    drive = %self.drive_id,
                    task_id = %task.task_id,
                    "Task was cancelled before execution, skipping"
                );
                return;
            }
            Ok(Some(status)) if !status.is_active() => {
                debug!(
                    target: "tasks::queue",
                    drive = %self.drive_id,
                    task_id = %task.task_id,
                    status = ?status,
                    "Task is no longer active, skipping"
                );
                return;
            }
            Err(err) => {
                warn!(
                    target: "tasks::queue",
                    drive = %self.drive_id,
                    task_id = %task.task_id,
                    error = %err,
                    "Failed to check task status, proceeding with execution"
                );
            }
            _ => {}
        }

        if let Err(err) = self.inventory.update_task(
            &task.task_id,
            TaskUpdate {
                status: Some(TaskStatus::Running),
                ..Default::default()
            },
        ) {
            error!(
                target: "tasks::queue",
                drive = %self.drive_id,
                task_id = %task.task_id,
                error = %err,
                "Failed to mark task as running"
            );
            return;
        }
        self.notifier.notify();

        // Register task path for path-based cancellation
        self.task_paths
            .insert(task.task_id.clone(), task.payload.local_path_display());

        self.register_progress_entry(&task).await;

        match self.run_placeholder_task(&task).await {
            Ok(TaskRunState::Completed) => {
                if let Err(err) = self.inventory.update_task(
                    &task.task_id,
                    TaskUpdate {
                        status: Some(TaskStatus::Completed),
                        ..Default::default()
                    },
                ) {
                    warn!(
                        target: "tasks::queue",
                        drive = %self.drive_id,
                        task_id = %task.task_id,
                        error = %err,
                        "Failed to mark task as completed"
                    );
                }
                self.notifier.notify();
            }
            Ok(TaskRunState::Cancelled) => {
                if let Err(err) = self.inventory.update_task(
                    &task.task_id,
                    TaskUpdate {
                        status: Some(TaskStatus::Cancelled),
                        ..Default::default()
                    },
                ) {
                    warn!(
                        target: "tasks::queue",
                        drive = %self.drive_id,
                        task_id = %task.task_id,
                        error = %err,
                        "Failed to mark task as cancelled"
                    );
                }
                self.cleanup_task_entry(&task.task_id).await;
                self.notifier.notify();
                return;
            }
            Err(err) => {
                let consent_origin = err.chain().find_map(|cause| {
                    cause
                        .downcast_ref::<crate::uploader::transport::StorageConsentRequired>()
                        .map(|consent| consent.origin.clone())
                });
                if let Some(origin) = consent_origin {
                    let revoked = self
                        .revoked_http_storage_origins
                        .read()
                        .is_ok_and(|origins| origins.contains(&origin));
                    if revoked {
                        let _ = self.inventory.update_task(
                            &task.task_id,
                            TaskUpdate {
                                status: Some(TaskStatus::Cancelled),
                                ..Default::default()
                            },
                        );
                        self.cleanup_task_entry(&task.task_id).await;
                        self.notifier.notify();
                        return;
                    }
                    let _ = self.inventory.update_task(
                        &task.task_id,
                        TaskUpdate {
                            status: Some(TaskStatus::Pending),
                            blocked_reason: Some(Some(BlockedReason::StorageConsent)),
                            blocked_origin: Some(Some(origin)),
                            error: Some(None),
                            ..Default::default()
                        },
                    );
                    self.cleanup_task_entry(&task.task_id).await;
                    self.notifier.notify();
                    return;
                }
                if task.payload.kind == TaskKind::Upload {
                    error!(target: "tasks::queue", drive = %self.drive_id, task_id = %task.task_id,
                        diagnostic = %crate::uploader::providers::safe_upload_diagnostic(&err), "Task execution failed");
                } else {
                    error!(target: "tasks::queue", drive = %self.drive_id, task_id = %task.task_id,
                        error = ?err, "Task execution failed");
                }

                if is_network_error(&err) {
                    let retry_count = task
                        .payload
                        .custom_state
                        .as_ref()
                        .and_then(|s| s.get("network_retries"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;

                    let max_retries = if task.payload.kind == TaskKind::VerifyCollision {
                        3
                    } else {
                        MAX_NETWORK_RETRIES
                    };
                    if retry_count < max_retries {
                        warn!(
                            target: "tasks::queue",
                            drive = %self.drive_id,
                            task_id = %task.task_id,
                            retry = retry_count + 1,
                            "Network error, marking task as offline-waiting"
                        );
                        let mut retry_state = task
                            .payload
                            .custom_state
                            .clone()
                            .unwrap_or_else(|| serde_json::json!({}));
                        if !retry_state.is_object() {
                            retry_state = serde_json::json!({});
                        }
                        retry_state["network_retries"] = serde_json::json!(retry_count + 1);
                        retry_state["offline_waiting"] = serde_json::json!(true);
                        if let Err(update_err) = self.inventory.update_task(
                            &task.task_id,
                            TaskUpdate {
                                status: Some(TaskStatus::Pending),
                                blocked_reason: Some(Some(BlockedReason::Offline)),
                                custom_state: Some(Some(retry_state)),
                                ..Default::default()
                            },
                        ) {
                            warn!(
                                target: "tasks::queue",
                                drive = %self.drive_id,
                                task_id = %task.task_id,
                                error = %update_err,
                                "Failed to persist pending state for retry"
                            );
                        }
                        self.cleanup_task_entry(&task.task_id).await;
                        self.notifier.notify();
                        return;
                    }
                }

                if task.payload.kind == TaskKind::VerifyCollision {
                    verify_collision::fail_to_conflict(
                        &self.inventory,
                        &self.drive_id,
                        &task.payload.local_path,
                    );
                }

                if let Err(update_err) = self.inventory.update_task(
                    &task.task_id,
                    TaskUpdate {
                        status: Some(TaskStatus::Failed),
                        error: Some(Some(if task.payload.kind == TaskKind::Upload {
                            crate::uploader::providers::safe_upload_diagnostic(&err)
                        } else {
                            format!("{}", err.root_cause())
                        })),
                        ..Default::default()
                    },
                ) {
                    warn!(
                        target: "tasks::queue",
                        drive = %self.drive_id,
                        task_id = %task.task_id,
                        error = %update_err,
                        "Failed to persist task failure state"
                    );
                }
                self.cleanup_task_entry(&task.task_id).await;
                self.notifier.notify();
                return;
            }
        }

        self.cleanup_task_entry(&task.task_id).await;
    }

    async fn run_placeholder_task(&self, task: &QueuedTask) -> Result<TaskRunState> {
        crate::drive::utils::SafeSyncPath::from_local(&task.payload.local_path, &self.sync_path)?
            .join_checked(&self.sync_path)?;
        info!(
            target: "tasks::queue",
            drive = %self.drive_id,
            task_id = %task.task_id,
            kind = task.payload.kind.as_str(),
            path = %task.payload.local_path_display(),
            "Executing task"
        );

        match &task.payload.kind {
            TaskKind::Upload => {
                let mut task_executor = UploadTask::new(
                    self.inventory.clone(),
                    self.cr_client.clone(),
                    self.drive_id.as_str(),
                    &task,
                    self.sync_path.clone(),
                    self.remote_base.clone(),
                    Arc::clone(&self.progress),
                    Arc::clone(&self.notifier),
                    self.approved_http_storage_origins.clone(),
                );

                task_executor.execute().await?;
            }
            TaskKind::Download => {
                let mut task_executor = DownloadTask::new(
                    self.inventory.clone(),
                    self.cr_client.clone(),
                    self.drive_id.as_str(),
                    &task,
                    self.sync_path.clone(),
                    self.remote_base.clone(),
                    Arc::clone(&self.progress),
                    Arc::clone(&self.notifier),
                    self.event_blocker.clone(),
                    self.max_file_size_mb.clone(),
                    self.download_client.clone(),
                    self.approved_http_storage_origins.clone(),
                    self.active_storage_origins.clone(),
                );

                task_executor.execute().await?;
            }
            TaskKind::VerifyCollision => {
                verify_collision::execute(
                    task,
                    self.inventory.clone(),
                    self.cr_client.clone(),
                    self.download_client.clone(),
                    &self.approved_http_storage_origins,
                    &self.active_storage_origins,
                    &self.drive_id,
                    &self.sync_path,
                    &self.remote_base,
                )
                .await?;
            }
        }

        // for step in 0..PLACEHOLDER_STEPS {
        //     if self.cancel_requested.load(Ordering::SeqCst) {
        //         return Ok(TaskRunState::Cancelled);
        //     }
        //     sleep(Duration::from_millis(250)).await;
        //     let progress = (step + 1) as f64 / PLACEHOLDER_STEPS as f64;
        //     let direction = match task.payload.kind {
        //         TaskKind::Upload => "upload",
        //         TaskKind::Download => "download",
        //     };
        //     let state = json!({
        //         "step": step + 1,
        //         "total_steps": PLACEHOLDER_STEPS,
        //         "local_path": task.payload.local_path_display(),
        //         "kind": direction,
        //     });
        //     if let Err(err) = self
        //         .persist_progress(
        //             &task.task_id,
        //             progress,
        //             task.payload.processed_bytes,
        //             task.payload.total_bytes,
        //             Some(state),
        //         )
        //         .await
        //     {
        //         warn!(
        //             target: "tasks::queue",
        //             drive = %self.drive_id,
        //             task_id = %task.task_id,
        //             error = %err,
        //             "Failed to persist placeholder progress"
        //         );
        //     }
        // }

        Ok(TaskRunState::Completed)
    }

    #[allow(dead_code)]
    async fn wait_for_idle(&self) {
        while self.inflight.load(Ordering::SeqCst) > 0 {
            self.idle_notify.notified().await;
        }
    }

    async fn register_progress_entry(&self, task: &QueuedTask) {
        self.progress.insert(
            task.task_id.clone(),
            TaskProgress::from_payload(&task.task_id, &task.payload),
        );
    }

    #[allow(dead_code)]
    async fn clear_progress_entry(&self, task_id: &str) {
        self.progress.remove(task_id);
    }

    async fn cleanup_task_entry(&self, task_id: &str) {
        self.progress.remove(task_id);
        self.task_paths.remove(task_id);
    }

    /// Replay the tasks the previous run left behind, at startup.
    ///
    /// This is the twin of `re_enqueue_offline_tasks`, one boot earlier: an
    /// upload interrupted by quitting the app comes back through here, before
    /// any reconnection event ever fires. It needs the same filter — a junk
    /// task queued by an older version would otherwise be pushed to the
    /// server at the very next launch. Cancelled, not skipped, so it does not
    /// return at every boot.
    async fn resume_incomplete_tasks(self: &Arc<Self>, ignore: &IgnoreMatcher) -> Result<()> {
        let queue = Arc::clone(self);
        let ignore = ignore.clone();
        tokio::task::spawn_blocking(move || {
            queue.inventory.reset_running_tasks(&queue.drive_id)?;
            queue.inventory.mark_legacy_offline_tasks(&queue.drive_id)?;
            queue.cancel_ignored_pending(&ignore)
        })
        .await
        .context("Startup task recovery panicked")??;
        self.dispatch_wake.notify_one();
        Ok(())
    }

    fn cancel_ignored_pending(&self, ignore: &IgnoreMatcher) -> Result<()> {
        let mut after = String::new();
        loop {
            let page = self
                .inventory
                .pending_paths_page(&self.drive_id, &after, 250)?;
            if page.is_empty() {
                break;
            }
            for (id, path) in page {
                after = id.clone();
                if ignore.is_match(&path) {
                    self.inventory.update_task(
                        &id,
                        TaskUpdate {
                            status: Some(TaskStatus::Cancelled),
                            ..Default::default()
                        },
                    )?;
                }
            }
        }
        Ok(())
    }

    async fn cancel_running_tasks(&self) {
        let running: Vec<String> = self
            .task_handles
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        let mut handles = Vec::new();
        for task_id in running {
            if let Some((_, handle)) = self.task_handles.remove(&task_id) {
                handle.abort();
                handles.push(handle);
            }

            self.progress.remove(&task_id);
            self.task_paths.remove(&task_id);
        }
        for handle in handles {
            let _ = handle.await;
        }
        self.dispatch_wake.notify_one();
    }

    fn payload_from_record(record: &TaskRecord) -> Result<TaskPayload> {
        let kind = TaskKind::from_str(&record.task_type)
            .ok_or_else(|| anyhow!("Unknown task type {}", record.task_type))?;

        let mut payload = TaskPayload::new(kind, PathBuf::from(&record.local_path))
            .with_priority(record.priority)
            .with_task_id(record.id.clone());

        let total_bytes = record.total_bytes;
        let processed_bytes = record.processed_bytes;
        if total_bytes != 0 || processed_bytes != 0 {
            payload = payload.with_totals(processed_bytes, total_bytes);
        }

        if let Some(state) = &record.custom_state {
            payload = payload.with_custom_state(state.clone());
            payload.force_override = state
                .get("force_override")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
        }

        Ok(payload)
    }
}

#[allow(dead_code)]
pub enum TaskRunState {
    Completed,
    Cancelled,
}

pub struct QueuedTask {
    pub task_id: String,
    pub payload: TaskPayload,
}
