mod db;
mod models;
pub(crate) mod schema;

pub use db::{InventoryDb, RecentTasks};
pub use models::{
    BlockedReason, ConflictState, DriveProps, DrivePropsUpdate, FileMetadata,
    LOCAL_FINGERPRINT_KEY, LocalFingerprint, MetadataEntry, NewTaskRecord, TaskRecord, TaskStatus,
    TaskUpdate, local_mtime_secs,
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
