//! Download task: downloads a remote file to its local path.
//! On macOS/Linux all files are real (no virtual placeholders).

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use cloudreve_api::{Client, api::ExplorerApi, models::explorer::FileURLService};
use dashmap::DashMap;
use futures::StreamExt;
use notify_debouncer_full::notify::event::{CreateKind, EventKind, ModifyKind};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    drive::{event_blocker::EventBlocker, utils::local_path_to_cr_uri},
    events::SummaryNotifier,
    inventory::{ConflictState, FileMetadata, InventoryDb, MetadataEntry, local_mtime_secs},
    tasks::queue::QueuedTask,
    utils::{disk_space, toast::send_conflict_toast},
};

use super::types::TaskProgress;

pub struct DownloadProgressTracker {
    total_size: u64,
    downloaded_bytes: AtomicU64,
    samples: std::sync::Mutex<Vec<(Instant, u64)>>,
    window_duration: Duration,
}

impl DownloadProgressTracker {
    pub fn new(total_size: u64) -> Self {
        Self {
            total_size,
            downloaded_bytes: AtomicU64::new(0),
            samples: std::sync::Mutex::new(Vec::with_capacity(32)),
            window_duration: Duration::from_secs(10),
        }
    }

    pub fn add_bytes(&self, bytes: u64) {
        self.downloaded_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn downloaded(&self) -> u64 {
        self.downloaded_bytes.load(Ordering::SeqCst)
    }

    pub fn create_update(&self) -> DownloadProgressUpdate {
        let downloaded = self.downloaded();
        let now = Instant::now();

        let speed = {
            let mut samples = self.samples.lock().expect("DownloadProgressTracker mutex poisoned — another thread panicked while holding the lock");
            samples.push((now, downloaded));
            let cutoff = now - self.window_duration;
            samples.retain(|(t, _)| *t >= cutoff);
            if let Some((oldest_time, oldest_bytes)) = samples.first() {
                let elapsed = now.duration_since(*oldest_time);
                if elapsed.as_millis() > 0 {
                    let bytes_diff = downloaded.saturating_sub(*oldest_bytes);
                    (bytes_diff as f64 / elapsed.as_secs_f64()) as u64
                } else {
                    0
                }
            } else {
                0
            }
        };

        let progress = if self.total_size > 0 {
            (downloaded as f64 / self.total_size as f64).clamp(0.0, 1.0)
        } else {
            1.0
        };

        let eta_seconds = if speed > 0 && downloaded < self.total_size {
            Some((self.total_size - downloaded) / speed)
        } else {
            None
        };

        DownloadProgressUpdate {
            total_size: self.total_size,
            downloaded,
            progress,
            speed_bytes_per_sec: speed,
            eta_seconds,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownloadProgressUpdate {
    pub total_size: u64,
    pub downloaded: u64,
    pub progress: f64,
    pub speed_bytes_per_sec: u64,
    pub eta_seconds: Option<u64>,
}

pub struct InMemoryDownloadProgressReporter {
    task_id: String,
    progress_map: Arc<DashMap<String, TaskProgress>>,
    notifier: Arc<SummaryNotifier>,
}

impl InMemoryDownloadProgressReporter {
    pub fn new(
        task_id: String,
        progress_map: Arc<DashMap<String, TaskProgress>>,
        notifier: Arc<SummaryNotifier>,
    ) -> Self {
        Self {
            task_id,
            progress_map,
            notifier,
        }
    }

    pub fn on_progress(&self, update: &DownloadProgressUpdate) {
        if let Some(mut entry) = self.progress_map.get_mut(&self.task_id) {
            entry.progress = update.progress;
            entry.processed_bytes = Some(update.downloaded as i64);
            entry.total_bytes = Some(update.total_size as i64);
            entry.speed_bytes_per_sec = update.speed_bytes_per_sec;
            entry.eta_seconds = update.eta_seconds;
            drop(entry);
            self.notifier.notify_throttled();
        }
    }
}

pub struct DownloadTask<'a> {
    inventory: Arc<InventoryDb>,
    cr_client: Arc<Client>,
    drive_id: &'a str,
    sync_path: PathBuf,
    remote_base: String,
    task: &'a QueuedTask,
    inventory_meta: Option<FileMetadata>,
    remote_file_info: Option<cloudreve_api::models::explorer::FileResponse>,
    cancel_token: CancellationToken,
    progress_map: Arc<DashMap<String, TaskProgress>>,
    notifier: Arc<SummaryNotifier>,
    event_blocker: EventBlocker,
    max_file_size_mb: u64,
    download_client: Arc<reqwest::Client>,
    approved_http_origin: Option<Url>,
}

impl<'a> DownloadTask<'a> {
    pub fn new(
        inventory: Arc<InventoryDb>,
        cr_client: Arc<Client>,
        drive_id: &'a str,
        task: &'a QueuedTask,
        sync_path: PathBuf,
        remote_base: String,
        progress_map: Arc<DashMap<String, TaskProgress>>,
        notifier: Arc<SummaryNotifier>,
        event_blocker: EventBlocker,
        max_file_size_mb: u64,
        download_client: Arc<reqwest::Client>,
        approved_http_origin: Option<Url>,
    ) -> Self {
        Self {
            inventory,
            cr_client,
            drive_id,
            inventory_meta: None,
            remote_file_info: None,
            task,
            sync_path,
            remote_base,
            cancel_token: CancellationToken::new(),
            progress_map,
            notifier,
            event_blocker,
            max_file_size_mb,
            download_client,
            approved_http_origin,
        }
    }

    #[allow(dead_code)]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    pub async fn execute(&mut self) -> Result<()> {
        let local_path = &self.task.payload.local_path;

        // Skip directories
        if local_path.is_dir() {
            info!(target: "tasks::download", "Skipping directory: {}", local_path.display());
            return Ok(());
        }

        // Get inventory metadata if available
        let path_str = local_path.to_str().context("invalid path")?;
        self.inventory_meta = self
            .inventory
            .query_by_path(path_str)
            .context("failed to query inventory")?;

        self.download_file().await
    }

    async fn download_file(&mut self) -> Result<()> {
        let local_path = self.task.payload.local_path.clone();
        info!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Starting download");

        // Compute remote URI
        let uri = local_path_to_cr_uri(
            local_path.clone(),
            self.sync_path.clone(),
            self.remote_base.clone(),
        )
        .context("failed to compute remote URI")?
        .to_string();

        // Get remote file info
        let file_info = match self
            .cr_client
            .get_file_info(&cloudreve_api::models::explorer::GetFileInfoService {
                uri: Some(uri.clone()),
                id: None,
                extended: None,
                folder_summary: None,
            })
            .await
        {
            Ok(info) => info,
            Err(cloudreve_api::ApiError::ApiError {
                code, message: _, ..
            }) if code == 40016 || code == 404 => {
                // A stale download must never delete a local copy. The next
                // reconciliation will forget its inventory row if remote is gone.
                info!(
                    target: "tasks::download",
                    task_id = %self.task.task_id,
                    path = %local_path.display(),
                    code = code,
                    "Remote file no longer exists, keeping local copy"
                );
                return Ok(());
            }
            Err(e) => return Err(anyhow::Error::from(e).context("failed to get remote file info")),
        };

        // Check if the remote file exceeds the drive's size limit
        if self.max_file_size_mb > 0 {
            let max_bytes = self.max_file_size_mb * 1024 * 1024;
            if file_info.size > 0 && (file_info.size as u64) > max_bytes {
                info!(
                    target: "tasks::download",
                    task_id = %self.task.task_id,
                    path = %local_path.display(),
                    size = file_info.size,
                    limit_mb = self.max_file_size_mb,
                    "Skipping download: file exceeds size limit"
                );
                return Ok(());
            }
        }

        // If the local inventory already tracks this exact remote entity (same etag),
        // the file is already in sync — skip the download to avoid a needless
        // re-download that would generate FS events and trigger a re-upload loop.
        if !self.task.payload.force_override {
            if let Some(ref meta) = self.inventory_meta {
                if let Some(ref remote_etag) = file_info.primary_entity {
                    if !meta.etag.is_empty() && meta.etag == *remote_etag {
                        debug!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Local file already matches remote etag, skipping download");
                        return Ok(());
                    }
                }
            }
        }

        // Conflict guard: this is the single choke point for every download
        // (SSE, full sync, heartbeat). Never overwrite local changes silently.
        // `force_override` is set when the user explicitly resolved the conflict.
        if !self.task.payload.force_override {
            if let Some(ref meta) = self.inventory_meta {
                if matches!(meta.conflict_state, Some(ConflictState::Pending)) {
                    info!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Skipping download: file has an unresolved conflict");
                    return Ok(());
                }
                if meta.is_locally_modified(&local_path) {
                    warn!(
                        target: "tasks::download",
                        task_id = %self.task.task_id,
                        path = %local_path.display(),
                        "Conflict detected: local file modified while remote also changed"
                    );
                    if let Some(path_str) = local_path.to_str() {
                        if let Err(e) = self
                            .inventory
                            .mark_as_conflicted(path_str, Some(ConflictState::Pending))
                        {
                            warn!(target: "tasks::download", error = ?e, "Failed to mark conflict");
                        }
                    }
                    send_conflict_toast(self.drive_id, &local_path, meta.id);
                    return Ok(());
                }
            }
        }

        let file_size = file_info.size as u64;

        // Never let a mirror of a remote larger than the local volume fill the
        // disk. Failing the task keeps the error visible instead of writing
        // until the machine breaks.
        let available = disk_space::available_space_for(&local_path)
            .context("failed to query available disk space")?;
        if !disk_space::fits_on_volume(file_size, available) {
            anyhow::bail!(
                "not enough disk space for {}: needs {} bytes, {} available (keeping {} bytes reserved)",
                local_path.display(),
                file_size,
                available,
                disk_space::RESERVED_BYTES
            );
        }

        self.remote_file_info = Some(file_info);

        // Get download URL
        let mut request = FileURLService::default();
        request.uris.push(uri.clone());
        if let Some(entity) = self
            .remote_file_info
            .as_ref()
            .and_then(|f| f.primary_entity.clone())
        {
            request.entity = Some(entity);
        }

        let url_res = self
            .cr_client
            .get_file_url(&request)
            .await
            .context("failed to get download URL")?;
        let download_url = {
            let raw = url_res.urls.first().context("no download URL")?.url.clone();
            // Correct only a server's loopback SiteURL. A storage provider or
            // CDN host is part of the signed URL and must be preserved.
            self.cr_client.rewrite_url_origin(&raw)
        };

        validate_download_url(&download_url, &self.approved_http_origin)?;
        debug!(target: "tasks::download", task_id = %self.task.task_id, size = file_size, "Got download URL");

        let destination =
            crate::drive::utils::SafeSyncPath::from_local(&local_path, &self.sync_path)?;
        destination.join_checked(&self.sync_path)?;

        // Stage the download next to its destination rather than in the system
        // temp dir: a sync folder on another volume (external drive, second SSD,
        // separate /home partition) cannot be reached by a rename from there.
        // The `.cloudreve-part-` prefix keeps the local scan and the FS watcher
        // from mistaking a half-written file for a user change.
        let temp_path = local_path.with_file_name(format!(
            "{}{}",
            crate::drive::utils::PARTIAL_DOWNLOAD_PREFIX,
            self.task.task_id
        ));
        let staged = crate::drive::utils::SafeSyncPath::from_local(&temp_path, &self.sync_path)?;
        let _ = staged.remove_checked(&self.sync_path);

        let tracker = Arc::new(DownloadProgressTracker::new(file_size));
        let reporter = InMemoryDownloadProgressReporter::new(
            self.task.task_id.clone(),
            Arc::clone(&self.progress_map),
            Arc::clone(&self.notifier),
        );

        let result = self
            .download_to_temp(&download_url, &temp_path, tracker.clone(), &reporter)
            .await;

        match result {
            Ok(()) => {
                reporter.on_progress(&tracker.create_update());
                // Block FS events the rename will generate so we don't re-upload the file.
                // macOS FSEvents can emit an unpredictable number of Create/Modify events
                // per rename, so we use a time-based block (5 s) instead of a fixed count.
                let block_duration = Duration::from_secs(5);
                self.event_blocker.register_for_duration(
                    &EventKind::Create(CreateKind::Any),
                    local_path.clone(),
                    block_duration,
                );
                self.event_blocker.register_for_duration(
                    &EventKind::Modify(ModifyKind::Any),
                    local_path.clone(),
                    block_duration,
                );
                // Atomic move temp → final path
                staged
                    .rename_checked(&self.sync_path, &destination)
                    .context("failed to move downloaded file")?;
                info!(target: "tasks::download", task_id = %self.task.task_id, path = %local_path.display(), "Download complete");
                // Update inventory so 3-way sync knows this file was last synced now
                if let Some(file_info) = &self.remote_file_info {
                    let parse_ts = |s: &str| -> i64 {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.timestamp())
                            .unwrap_or(0)
                    };
                    if let Ok(drive_uuid) = Uuid::parse_str(self.drive_id) {
                        let path_str = local_path.to_str().unwrap_or("");
                        let entry = MetadataEntry::new(drive_uuid, path_str, false)
                            .with_created_at(parse_ts(&file_info.created_at))
                            .with_updated_at(parse_ts(&file_info.updated_at))
                            .with_etag(file_info.primary_entity.clone().unwrap_or_default())
                            .with_size(file_info.size)
                            .with_local_mtime(local_mtime_secs(&local_path));
                        if let Err(e) = self.inventory.upsert(&entry) {
                            warn!(target: "tasks::download", task_id = %self.task.task_id, error = %e, "Failed to update inventory after download");
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                let _ = staged.remove_checked(&self.sync_path);
                Err(e)
            }
        }
    }

    async fn download_to_temp(
        &self,
        url: &str,
        temp_path: &PathBuf,
        tracker: Arc<DownloadProgressTracker>,
        reporter: &InMemoryDownloadProgressReporter,
    ) -> Result<()> {
        let response = self
            .download_client
            .get(url)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("Download request failed")?;

        if !response.status().is_success() {
            anyhow::bail!("Download failed with status: {}", response.status());
        }

        let staged = crate::drive::utils::SafeSyncPath::from_local(temp_path, &self.sync_path)?;
        let root = self.sync_path.clone();
        let file = tokio::task::spawn_blocking(move || staged.create_new_checked(&root))
            .await
            .context("staged file open task panicked")??;
        let mut file = tokio::fs::File::from_std(file);
        let mut stream = response.bytes_stream();
        let mut last_report = Instant::now();
        const REPORT_INTERVAL: Duration = Duration::from_millis(200);

        while let Some(chunk_result) = stream.next().await {
            if self.cancel_token.is_cancelled() {
                anyhow::bail!("Download cancelled");
            }
            let chunk = chunk_result
                .map_err(reqwest::Error::without_url)
                .context("Failed to read download chunk")?;
            file.write_all(&chunk)
                .await
                .context("failed to write chunk")?;
            tracker.add_bytes(chunk.len() as u64);
            if last_report.elapsed() >= REPORT_INTERVAL {
                reporter.on_progress(&tracker.create_update());
                last_report = Instant::now();
            }
        }

        file.flush().await.context("failed to flush temp file")?;
        Ok(())
    }
}

pub(crate) fn validate_download_url(raw: &str, approved_http_origin: &Option<Url>) -> Result<()> {
    let url = Url::parse(raw).context("Invalid signed download URL")?;
    if url.scheme() == "https" {
        return Ok(());
    }
    if url.scheme() == "http"
        && approved_http_origin
            .as_ref()
            .is_some_and(|allowed| url.origin() == allowed.origin())
    {
        return Ok(());
    }
    anyhow::bail!("Signed download URL uses an unapproved insecure origin")
}

pub(crate) fn allowed_download_redirect(
    initial: &Url,
    next: &Url,
    approved_http_origin: &Option<Url>,
) -> bool {
    initial.origin() == next.origin()
        && (next.scheme() == "https"
            || (next.scheme() == "http"
                && approved_http_origin
                    .as_ref()
                    .is_some_and(|allowed| next.origin() == allowed.origin())))
}

#[cfg(test)]
mod url_policy_tests {
    use super::*;

    #[tokio::test]
    async fn reqwest_errors_do_not_expose_signed_query_values() {
        let error = reqwest::Client::new()
            .get("http://127.0.0.1:0/download?signature=SIGNED_SENTINEL")
            .send()
            .await
            .unwrap_err()
            .without_url();
        assert!(!format!("{error:?}").contains("SIGNED_SENTINEL"));
    }

    #[test]
    fn signed_cdn_https_is_allowed_but_insecure_cross_origin_is_rejected() {
        let approved = Some(Url::parse("http://cloud.example:5212").unwrap());
        assert!(
            validate_download_url("https://cdn.example.net/file?signature=abc", &approved).is_ok()
        );
        assert!(validate_download_url("http://cloud.example:5212/file", &approved).is_ok());
        assert!(
            validate_download_url("http://cdn.example.net/file?signature=abc", &approved).is_err()
        );
        assert!(validate_download_url("file:///tmp/secret", &approved).is_err());
        let signed = Url::parse("https://cdn.example.net/file?signature=abc").unwrap();
        assert!(allowed_download_redirect(
            &signed,
            &Url::parse("https://cdn.example.net/next").unwrap(),
            &approved
        ));
        assert!(!allowed_download_redirect(
            &signed,
            &Url::parse("https://other.example.net/next").unwrap(),
            &approved
        ));
        assert!(!allowed_download_redirect(
            &signed,
            &Url::parse("http://cdn.example.net/next").unwrap(),
            &approved
        ));
        let insecure = Url::parse("http://cloud.example:5212/file").unwrap();
        assert!(allowed_download_redirect(
            &insecure,
            &Url::parse("http://cloud.example:5212/next").unwrap(),
            &approved
        ));
    }
}
