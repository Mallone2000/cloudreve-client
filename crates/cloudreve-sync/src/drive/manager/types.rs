use crate::drive::mounts::DriveConfig;
use crate::inventory::TaskRecord;
use crate::tasks::TaskProgress;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DriveState {
    pub drives: Vec<DriveConfig>,
}

/// Explicitly whitelisted fields safe to send to a WebView.
#[derive(Debug, Clone, Serialize)]
pub struct DriveSummary {
    pub id: String,
    pub name: String,
    pub instance_url: String,
    pub remote_path: String,
    pub sync_path: std::path::PathBuf,
    pub icon_path: Option<String>,
    pub raw_icon_path: Option<String>,
    pub enabled: bool,
    pub user_id: String,
    pub ignore_patterns: Vec<String>,
    pub max_file_size_mb: u64,
    pub allow_insecure_http: bool,
    pub transport_consent_required: bool,
}

impl From<&DriveConfig> for DriveSummary {
    fn from(config: &DriveConfig) -> Self {
        Self {
            id: config.id.clone(),
            name: config.name.clone(),
            instance_url: config.instance_url.clone(),
            remote_path: config.remote_path.clone(),
            sync_path: config.sync_path.clone(),
            icon_path: config.icon_path.clone(),
            raw_icon_path: config.raw_icon_path.clone(),
            enabled: config.enabled,
            user_id: config.user_id.clone(),
            ignore_patterns: config.ignore_patterns.clone(),
            max_file_size_mb: config.max_file_size_mb,
            allow_insecure_http: false,
            transport_consent_required: url::Url::parse(&config.instance_url)
                .is_ok_and(|url| url.scheme() == "http"),
        }
    }
}

/// Summary of the current status including drives and recent tasks
#[derive(Debug, Clone, Serialize)]
pub struct StatusSummary {
    /// All configured drives, without credentials (unfiltered).
    pub drives: Vec<DriveSummary>,
    /// Active tasks (pending/running) with optional live progress info
    pub active_tasks: Vec<TaskWithProgress>,
    /// Recently finished tasks (completed/failed/cancelled)
    pub finished_tasks: Vec<TaskRecord>,
    /// Whether at least one drive has completed its initial full sync
    pub has_ever_synced: bool,
    /// Files with an unresolved conflict, pending user action
    pub conflicts: Vec<ConflictInfo>,
    /// Drive IDs that are currently paused
    pub paused_drives: Vec<String>,
}

#[cfg(test)]
mod summary_tests {
    use super::*;

    #[test]
    fn ui_summary_never_serializes_credentials() {
        let config = DriveConfig {
            id: "drive-1".into(),
            credentials: crate::drive::mounts::Credentials {
                access_token: Some("ACCESS_SENTINEL".into()),
                refresh_token: "REFRESH_SENTINEL".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        let json = serde_json::to_string(&DriveSummary::from(&config)).unwrap();
        assert!(!json.contains("ACCESS_SENTINEL"));
        assert!(!json.contains("REFRESH_SENTINEL"));
        assert!(!json.contains("credentials"));
        let status = StatusSummary {
            drives: vec![DriveSummary::from(&config)],
            active_tasks: vec![],
            finished_tasks: vec![],
            has_ever_synced: false,
            conflicts: vec![],
            paused_drives: vec![],
        };
        let status_json = serde_json::to_string(&status).unwrap();
        assert!(!status_json.contains("ACCESS_SENTINEL"));
        assert!(!status_json.contains("REFRESH_SENTINEL"));
    }

    #[test]
    fn old_http_config_requires_consent_after_deserialization() {
        let mut serialized = serde_json::to_value(DriveConfig {
            instance_url: "http://cloud.example".into(),
            ..Default::default()
        })
        .unwrap();
        serialized
            .as_object_mut()
            .unwrap()
            .remove("allow_insecure_http");
        let config: DriveConfig = serde_json::from_value(serialized).unwrap();
        let summary = DriveSummary::from(&config);
        assert!(!summary.allow_insecure_http);
        assert!(summary.transport_consent_required);
    }

    #[test]
    fn previously_approved_http_config_is_still_dormant() {
        let config = DriveConfig {
            instance_url: "http://cloud.example".into(),
            allow_insecure_http: true,
            ..Default::default()
        };
        let summary = DriveSummary::from(&config);
        assert!(!summary.allow_insecure_http);
        assert!(summary.transport_consent_required);
    }
}

/// A file conflict awaiting user resolution
#[derive(Debug, Clone, Serialize)]
pub struct ConflictInfo {
    /// Inventory row id
    pub id: i64,
    /// Drive this file belongs to
    pub drive_id: String,
    /// Drive display name
    pub drive_name: String,
    /// Absolute local path
    pub local_path: String,
    /// Last synced size (from inventory)
    pub synced_size: i64,
    /// Current local size (None if the local file is missing)
    pub local_size: Option<i64>,
    /// Current local modification time (unix seconds)
    pub local_modified_at: Option<i64>,
}

/// How the user wants to resolve a conflict
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictResolution {
    /// Keep the local version: overwrite the remote file
    KeepLocal,
    /// Keep the remote version: overwrite the local file
    KeepRemote,
    /// Keep both: rename the local copy, then download the remote version
    KeepBoth,
}

/// A task record with optional live progress information
#[derive(Debug, Clone, Serialize)]
pub struct TaskWithProgress {
    /// The task record from the database
    #[serde(flatten)]
    pub task: TaskRecord,
    /// Live progress information for running tasks (None if task is not currently running)
    pub live_progress: Option<TaskProgress>,
}

/// Capacity summary for UI display
#[derive(Debug, Clone, Serialize)]
pub struct CapacitySummary {
    /// Total capacity in bytes
    pub total: i64,
    /// Used capacity in bytes
    pub used: i64,
    /// Formatted label for display (e.g., "152.1 MB / 1.0 GB (14.9%)")
    pub label: String,
}

/// Sync status for UI display
#[derive(Debug, Clone, Serialize)]
pub enum SyncStatus {
    /// All files are in sync
    InSync,
    /// Currently syncing files
    Syncing,
    /// Sync is paused
    Paused,
    /// There was an error during sync
    Error,
}

/// Drive status information for the Windows Shell UI
#[derive(Debug, Clone, Serialize)]
pub struct DriveStatusUI {
    /// Drive display name
    pub name: String,
    /// Path to the raw (non-ICO) icon image
    pub raw_icon_path: Option<String>,
    /// Capacity summary (None if not available)
    pub capacity: Option<CapacitySummary>,
    /// URL to user profile page
    pub profile_url: String,
    /// URL to settings page
    pub settings_url: String,
    pub storage_url: String,
    /// Current sync status
    pub sync_status: SyncStatus,
    /// Number of active (pending/running) tasks
    pub active_task_count: usize,
}

/// Drive information for the settings UI
#[derive(Debug, Clone, Serialize)]
pub struct DriveInfo {
    /// Drive ID
    pub id: String,
    /// Drive display name
    pub name: String,
    /// Instance URL
    pub instance_url: String,
    pub remote_path: String,
    /// Local sync path
    pub sync_path: String,
    /// Path to the ICO icon
    pub icon_path: Option<String>,
    /// Path to the raw (non-ICO) icon image
    pub raw_icon_path: Option<String>,
    /// Whether the drive is enabled
    pub enabled: bool,
    /// Whether the drive is currently paused
    pub paused: bool,
    /// User ID
    pub user_id: String,
    /// Current drive status
    pub status: DriveInfoStatus,
    /// Capacity summary (None if not available)
    pub capacity: Option<CapacitySummary>,
    pub approved_http_storage_origins: Vec<String>,
    pub pending_http_storage_origins: Vec<String>,
}

/// Drive status for the settings UI
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DriveInfoStatus {
    /// Drive is active and synced
    Active,
    // Event push subscription is lost
    EventPushLost,
    /// Credentials have expired
    CredentialExpired,
    /// Network connection is offline
    Offline,
    /// Existing HTTP drive is dormant until the user explicitly accepts it.
    TransportConsentRequired,
}

/// Format bytes into a human-readable string (e.g., "1.5 GB")
pub fn format_bytes(bytes: i64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    const TB: f64 = GB * 1024.0;

    let bytes_f = bytes as f64;

    if bytes_f >= TB {
        format!("{:.1} TB", bytes_f / TB)
    } else if bytes_f >= GB {
        format!("{:.1} GB", bytes_f / GB)
    } else if bytes_f >= MB {
        format!("{:.1} MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.1} KB", bytes_f / KB)
    } else {
        format!("{} B", bytes)
    }
}
