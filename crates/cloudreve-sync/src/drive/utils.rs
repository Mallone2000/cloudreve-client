use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result};
use cloudreve_api::models::uri::CrUri;
use percent_encoding::percent_decode_str;
use url::Url;

use crate::drive::mounts::DriveConfig;

/// Prefix of a partial download. Downloads are staged next to their destination
/// so the final move stays on one filesystem, which means the sync folder holds
/// these files while they are being written.
pub const PARTIAL_DOWNLOAD_PREFIX: &str = ".cloudreve-part-";

/// Whether `path` is a partial download staged by the client. Such a file is
/// half-written by definition and must never be scanned, tracked or uploaded.
pub fn is_partial_download(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with(PARTIAL_DOWNLOAD_PREFIX))
}

pub fn local_path_to_cr_uri(path: PathBuf, root: PathBuf, remote_base: String) -> Result<CrUri> {
    let mut base = CrUri::new(&remote_base)?;

    if path == root {
        return Ok(base);
    }

    // Strip the root from path to get the relative path
    let relative = SafeSyncPath::from_local(&path, &root)?;

    // Convert to string with forward slashes (for URI compatibility)
    let relative_str = relative.as_path()
        .to_str()
        .context("Path contains invalid UTF-8")?
        .replace("\\", "/");

    // Join the relative path to the base URI if not empty
    if !relative_str.is_empty() {
        base.join(&relative_str.split("/").collect::<Vec<&str>>());
    }

    Ok(base)
}

pub fn remote_path_to_local_relative_path(
    remote_path: &CrUri,
    remote_base: &CrUri,
) -> Result<PathBuf> {
    Ok(SafeSyncPath::from_remote_uri(remote_path, remote_base)?.0)
}

/// A relative file path whose components cannot escape the sync root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeSyncPath(PathBuf);

impl SafeSyncPath {
    fn from_decoded(relative: &str) -> Result<Self> {
        if relative.is_empty() || relative.starts_with('/') || relative.contains('\\') || relative.contains('\0') {
            anyhow::bail!("Unsafe sync path");
        }
        let mut path = PathBuf::new();
        for segment in relative.split('/') {
            if segment.is_empty() || segment == "." || segment == ".." || segment.contains('/') {
                anyhow::bail!("Unsafe sync path component");
            }
            // Do not permit a second URL decode to turn a filename into a separator.
            let lower = segment.to_ascii_lowercase();
            if lower.contains("%2f") || lower.contains("%5c") || lower.contains("%2e") || lower.contains("%00") {
                anyhow::bail!("Nested encoded sync path component");
            }
            path.push(segment);
        }
        if path.components().any(|c| !matches!(c, Component::Normal(_))) {
            anyhow::bail!("Unsafe sync path component");
        }
        Ok(Self(path))
    }

    pub fn from_event_path(path: &str) -> Result<Self> {
        let raw = path.strip_prefix('/').unwrap_or(path);
        reject_encoded_separators(raw)?;
        let decoded = percent_decode_str(raw).decode_utf8().context("Invalid event path UTF-8")?;
        Self::from_decoded(&decoded)
    }

    pub fn from_remote_uri(remote: &CrUri, base: &CrUri) -> Result<Self> {
        let remote_url = Url::parse(&remote.to_string().replacen("cloudreve://", "http://", 1))?;
        let base_url = Url::parse(&base.to_string().replacen("cloudreve://", "http://", 1))?;
        if remote_url.host_str() != base_url.host_str()
            || remote_url.port() != base_url.port()
            || remote_url.username() != base_url.username()
            || remote_url.password() != base_url.password()
            || remote_url.query().is_some() || remote_url.fragment().is_some()
            || base_url.query().is_some() || base_url.fragment().is_some()
        {
            anyhow::bail!("Remote URI is outside the configured filesystem");
        }
        reject_encoded_separators(remote_url.path())?;
        let remote_path = remote.path();
        let base_path = base.path();
        let prefix = format!("{}/", base_path.trim_end_matches('/'));
        let relative = remote_path.strip_prefix(&prefix).context("Path is not under remote base")?;
        Self::from_decoded(relative)
    }

    pub fn from_local(path: &Path, root: &Path) -> Result<Self> {
        let relative = path.strip_prefix(root).context("Path is not under root")?;
        let relative = relative.to_str().context("Path contains invalid UTF-8")?;
        Self::from_decoded(&relative.replace(std::path::MAIN_SEPARATOR, "/"))
    }

    pub fn from_relative_path(path: &Path) -> Result<Self> {
        let relative = path.to_str().context("Path contains invalid UTF-8")?;
        Self::from_decoded(&relative.replace(std::path::MAIN_SEPARATOR, "/"))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }

    /// Check every existing component. Call immediately before filesystem I/O
    /// as well as when scheduling a task; missing leaf components are allowed.
    pub fn join_checked(&self, root: &Path) -> Result<PathBuf> {
        let root_meta = std::fs::symlink_metadata(root).context("Sync root is missing")?;
        if root_meta.file_type().is_symlink() || !root_meta.is_dir() {
            anyhow::bail!("Sync root must be a real directory");
        }
        let mut result = root.to_path_buf();
        for segment in self.0.components() {
            result.push(segment.as_os_str());
            match std::fs::symlink_metadata(&result) {
                Ok(meta) if meta.file_type().is_symlink() => anyhow::bail!("Symlink in sync path"),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(result)
    }

    /// Open each directory relative to an already-open parent. The resulting
    /// descriptor remains inside the original root even if a path is swapped
    /// between the scan and this read.
    #[cfg(unix)]
    pub fn open_read_checked(&self, root: &Path) -> Result<std::fs::File> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::ffi::OsStrExt;

        let mut directory = std::fs::OpenOptions::new().read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root).context("Failed to open sync root safely")?;
        let components: Vec<_> = self.0.components().collect();
        for (index, component) in components.iter().enumerate() {
            let name = CString::new(component.as_os_str().as_bytes())?;
            let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC
                | if index + 1 == components.len() { 0 } else { libc::O_DIRECTORY };
            let descriptor = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
            if descriptor < 0 {
                return Err(std::io::Error::last_os_error()).context("Unsafe or missing sync path");
            }
            directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
        }
        if !directory.metadata()?.is_file() {
            anyhow::bail!("Sync read target is not a regular file");
        }
        Ok(directory)
    }

    #[cfg(not(unix))]
    pub fn open_read_checked(&self, root: &Path) -> Result<std::fs::File> {
        Ok(std::fs::File::open(self.join_checked(root)?)?)
    }

    #[cfg(unix)]
    fn open_parent_checked(&self, root: &Path, create: bool) -> Result<(std::fs::File, std::ffi::CString)> {
        use std::ffi::CString;
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::ffi::OsStrExt;

        let mut directory = std::fs::OpenOptions::new().read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root).context("Failed to open sync root safely")?;
        let mut components = self.0.components().peekable();
        while let Some(component) = components.next() {
            let name = CString::new(component.as_os_str().as_bytes())?;
            if components.peek().is_none() {
                return Ok((directory, name));
            }
            if create {
                let result = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o755) };
                if result < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists {
                    return Err(std::io::Error::last_os_error()).context("Failed to create sync directory");
                }
            }
            let descriptor = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC) };
            if descriptor < 0 {
                return Err(std::io::Error::last_os_error()).context("Unsafe sync directory");
            }
            directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
        }
        anyhow::bail!("Empty sync path")
    }

    /// Create a new regular file without following any directory or leaf symlink.
    #[cfg(unix)]
    pub fn create_new_checked(&self, root: &Path) -> Result<std::fs::File> {
        use std::os::fd::{AsRawFd, FromRawFd};
        let (directory, name) = self.open_parent_checked(root, true)?;
        let descriptor = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC, 0o600) };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error()).context("Failed to create staged download safely");
        }
        Ok(unsafe { std::fs::File::from_raw_fd(descriptor) })
    }

    #[cfg(not(unix))]
    pub fn create_new_checked(&self, root: &Path) -> Result<std::fs::File> {
        let path = self.join_checked(root)?;
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        Ok(std::fs::OpenOptions::new().write(true).create_new(true).open(path)?)
    }

    #[cfg(unix)]
    pub fn remove_checked(&self, root: &Path) -> Result<()> {
        use std::os::fd::AsRawFd;
        let (directory, name) = self.open_parent_checked(root, false)?;
        if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } < 0 {
            return Err(std::io::Error::last_os_error()).context("Failed to remove staged download safely");
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn remove_checked(&self, root: &Path) -> Result<()> {
        Ok(std::fs::remove_file(self.join_checked(root)?)?)
    }

    #[cfg(unix)]
    pub fn rename_checked(&self, root: &Path, destination: &SafeSyncPath) -> Result<()> {
        use std::os::fd::AsRawFd;
        let (source_dir, source_name) = self.open_parent_checked(root, false)?;
        let (destination_dir, destination_name) = destination.open_parent_checked(root, false)?;
        if unsafe { libc::renameat(source_dir.as_raw_fd(), source_name.as_ptr(),
            destination_dir.as_raw_fd(), destination_name.as_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error()).context("Failed to move staged download safely");
        }
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn rename_checked(&self, root: &Path, destination: &SafeSyncPath) -> Result<()> {
        Ok(std::fs::rename(self.join_checked(root)?, destination.join_checked(root)?)?)
    }
}

fn reject_encoded_separators(raw: &str) -> Result<()> {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("%2f") || lower.contains("%5c") || lower.contains("%00") {
        anyhow::bail!("Encoded separator or NUL in sync path");
    }
    Ok(())
}

/// Generate a URL to view a folder or file online.
///
/// For folders: pass the folder path as `folder_path` and None for `open_file`
/// For files: pass the parent folder path as `folder_path` and the file path as `open_file`
pub fn view_online_url(
    folder_path: &str,
    open_file: Option<&str>,
    config: &DriveConfig,
) -> Result<String> {
    let mut base = config.instance_url.parse::<Url>()?;
    base.set_path("/home");

    {
        let mut query = base.query_pairs_mut();
        query.append_pair("path", folder_path);

        if let Some(file) = open_file {
            query.append_pair("open", file);
        }

        query.append_pair("user_hint", config.user_id.as_str());
    }

    Ok(base.to_string())
}

pub fn recycle_bin_url(config: &DriveConfig) -> Result<String> {
    let mut base = config.instance_url.parse::<Url>()?;
    base.set_path("/home");

    {
        let mut query = base.query_pairs_mut();
        query.append_pair("user_hint", config.user_id.as_str());
        query.append_pair("path", "cloudreve://trash");
    }

    Ok(base.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_escaping_remote_paths_and_events() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        for candidate in [
            "cloudreve://my/data/%2F..%2Foutside",
            "cloudreve://my/data/%5c..%5coutside",
            "cloudreve://my/data/../outside",
            "cloudreve://my/data/%2e%2e/outside",
            "cloudreve://other/data/file",
            "cloudreve://my:444/data/file",
            "cloudreve://my/data/file?unexpected=1",
        ] {
            assert!(CrUri::new(candidate)
                .and_then(|remote| remote_path_to_local_relative_path(&remote, &base).map(|_| remote))
                .is_err(), "{candidate}");
        }
        for event in ["/../outside", "//outside", "/safe/%2foutside", "/safe/%5coutside"] {
            assert!(SafeSyncPath::from_event_path(event).is_err(), "{event}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_components() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
        let safe = SafeSyncPath::from_event_path("/link/file.txt").unwrap();
        assert!(safe.join_checked(root.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn refuses_parent_swap_after_scan_for_reads_and_writes() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("folder")).unwrap();
        std::fs::write(root.path().join("folder/file"), b"inside").unwrap();
        std::fs::write(outside.path().join("file"), b"outside").unwrap();
        let source = SafeSyncPath::from_event_path("/folder/file").unwrap();
        let staged = SafeSyncPath::from_event_path("/folder/.cloudreve-part-test").unwrap();
        source.join_checked(root.path()).unwrap();
        staged.join_checked(root.path()).unwrap();
        std::fs::rename(root.path().join("folder"), root.path().join("old-folder")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("folder")).unwrap();
        assert!(source.open_read_checked(root.path()).is_err());
        assert!(staged.create_new_checked(root.path()).is_err());
        assert!(staged.rename_checked(root.path(), &source).is_err());
        assert_eq!(std::fs::read(outside.path().join("file")).unwrap(), b"outside");
    }

    fn config() -> DriveConfig {
        DriveConfig {
            instance_url: "https://cloud.example.com".to_string(),
            user_id: "user-42".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn local_path_maps_to_remote_uri() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/home/me/sync/docs/report.txt");
        let uri = local_path_to_cr_uri(path, root, "cloudreve://my/data".to_string()).unwrap();
        assert_eq!(uri.path(), "/data/docs/report.txt");
    }

    #[test]
    fn local_path_at_root_returns_base() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/home/me/sync");
        let uri = local_path_to_cr_uri(path, root, "cloudreve://my/data".to_string()).unwrap();
        assert_eq!(uri.path(), "/data");
    }

    #[test]
    fn local_path_outside_root_errors() {
        let root = PathBuf::from("/home/me/sync");
        let path = PathBuf::from("/etc/passwd");
        assert!(local_path_to_cr_uri(path, root, "cloudreve://my".to_string()).is_err());
    }

    #[test]
    fn remote_path_maps_to_local_relative() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        let remote = CrUri::new("cloudreve://my/data/docs/report.txt").unwrap();
        let rel = remote_path_to_local_relative_path(&remote, &base).unwrap();
        assert_eq!(rel, PathBuf::from("docs/report.txt"));
    }

    #[test]
    fn remote_path_outside_base_errors() {
        let base = CrUri::new("cloudreve://my/data").unwrap();
        let remote = CrUri::new("cloudreve://my/other/file").unwrap();
        assert!(remote_path_to_local_relative_path(&remote, &base).is_err());
    }

    #[test]
    fn view_online_url_for_folder() {
        let url = view_online_url("cloudreve://my/docs", None, &config()).unwrap();
        assert!(url.starts_with("https://cloud.example.com/home?"));
        assert!(url.contains("user_hint=user-42"));
        assert!(url.contains("path=cloudreve"));
        assert!(!url.contains("open="));
    }

    #[test]
    fn view_online_url_for_file_includes_open() {
        let url = view_online_url(
            "cloudreve://my/docs",
            Some("cloudreve://my/docs/a.txt"),
            &config(),
        )
        .unwrap();
        assert!(url.contains("open=cloudreve"));
    }

    #[test]
    fn recycle_bin_url_points_at_trash() {
        let url = recycle_bin_url(&config()).unwrap();
        assert!(url.starts_with("https://cloud.example.com/home?"));
        assert!(url.contains("user_hint=user-42"));
        assert!(url.contains("trash"));
    }
}
