pub mod config;
pub mod drive;
pub mod events;
pub mod inventory;
pub mod logging;
pub mod tasks;
pub mod uploader;
pub mod utils;

// Re-export commonly used types
pub use config::{AppConfig, ConfigManager};
pub use drive::manager::{
    ConflictInfo, ConflictResolution, DriveInfo, DriveInfoStatus, DriveManager, DriveSummary,
    StatusSummary, TaskWithProgress,
};
pub use drive::mounts::{Credentials, DriveConfig};
pub use events::{Event, EventBroadcaster, SummaryNotifier};
pub use logging::{LogConfig, LogGuard};

/// User agent string for HTTP requests
pub const USER_AGENT: &str = concat!("cloudreve-desktop/", env!("CARGO_PKG_VERSION"));

#[macro_use]
extern crate rust_i18n;

i18n!("../../locales");

#[cfg(test)]
mod locale_tests {
    #[test]
    fn migrated_locale_parser_loads_existing_translations() {
        assert_eq!(t!("viewOnline", locale = "en-US"), "View online");
    }
}
