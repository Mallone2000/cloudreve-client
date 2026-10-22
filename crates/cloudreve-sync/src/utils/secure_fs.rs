//! Helpers for persisting sensitive data (OAuth tokens, credentials) with
//! restrictive filesystem permissions so other local users cannot read them.

use anyhow::{Context, Result};
use std::io::Read;
use std::path::Path;

/// Read a private file through the same descriptor whose type and permissions
/// are checked. Reopening by path would allow a symlink swap between checks.
pub fn read_private<P: AsRef<Path>>(path: P) -> Result<String> {
    let path = path.as_ref();
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(path)
            .with_context(|| format!("Failed to open private file {}", path.display()))?;
        if !file.metadata()?.is_file() {
            anyhow::bail!("Private file must be a regular file: {}", path.display());
        }
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file
    };
    #[cfg(not(unix))]
    let file = std::fs::File::open(path)?;
    const MAX_PRIVATE_FILE: u64 = 16 * 1024 * 1024;
    let mut contents = String::new();
    file.take(MAX_PRIVATE_FILE + 1)
        .read_to_string(&mut contents)
        .with_context(|| format!("Failed to read private file {}", path.display()))?;
    if contents.len() as u64 > MAX_PRIVATE_FILE {
        anyhow::bail!("Private file exceeds 16 MiB: {}", path.display());
    }
    Ok(contents)
}

/// Write `contents` to `path`, ensuring the file is only readable and writable
/// by the current user (mode `0600` on Unix).
///
/// Each save replaces the old file with an owner-only temporary file, so an
/// existing symlink cannot redirect the credential write.
pub fn write_private<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> Result<()> {
    let path = path.as_ref();

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let parent = path
            .parent()
            .context("Credential file has no parent directory")?;
        let temp = parent.join(format!(".credentials-{}.tmp", uuid::Uuid::new_v4()));
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)
            .with_context(|| format!("Failed to create private file in {}", parent.display()))?;
        let result = (|| -> Result<()> {
            f.write_all(contents.as_ref())
                .context("Failed to write private file")?;
            f.sync_all().context("Failed to sync private file")?;
            drop(f);
            std::fs::rename(&temp, path).context("Failed to replace credential file")?;
            Ok(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        result?;
        return Ok(());
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)
            .with_context(|| format!("Failed to write file {}", path.display()))?;
        Ok(())
    }
}

/// Restrict an existing file to owner-only read/write (`0600`) on Unix.
/// No-op on non-Unix platforms.
pub fn restrict_file<P: AsRef<Path>>(path: P) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::fs::PermissionsExt;
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("Failed to open private file {}", path.display()))?;
        if !file.metadata()?.is_file() {
            anyhow::bail!("Private file must be a regular file: {}", path.display());
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn credential_file_is_written_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("drives.json");
        write_private(&file, b"{\"token\":\"secret\"}").unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "credential file must not be group/world readable"
        );
    }

    #[test]
    fn rewriting_tightens_existing_world_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("drives.json");
        std::fs::write(&file, b"old").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_private(&file, b"new").unwrap();
        let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn credential_write_replaces_symlink_without_touching_target() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside.txt");
        let file = dir.path().join("drives.json");
        std::fs::write(&outside, b"keep").unwrap();
        std::os::unix::fs::symlink(&outside, &file).unwrap();
        write_private(&file, b"new").unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
        assert_eq!(std::fs::read(&file).unwrap(), b"new");
        assert!(
            !std::fs::symlink_metadata(&file)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn restrict_file_rejects_symlink_without_changing_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("drives.json");
        std::fs::write(&target, b"secret").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(restrict_file(&link).is_err());
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o644
        );
    }

    #[test]
    fn private_read_rejects_symlinks_and_tightens_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("drives.json");
        let link = dir.path().join("link.json");
        std::fs::write(&file, b"secret").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert!(read_private(&link).is_err());
        assert_eq!(read_private(&file).unwrap(), "secret");
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn private_directory_rejects_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        let link = dir.path().join("link");
        std::fs::create_dir(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(restrict_dir(&link).is_err());
    }
}

/// Restrict a directory to owner-only access (`0700`) on Unix so its contents
/// (config, credentials) are not enumerable by other local users.
/// No-op on non-Unix platforms.
pub fn restrict_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::fs::PermissionsExt;
        let path = path.as_ref();
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("Failed to open private directory {}", path.display()))?;
        directory
            .set_permissions(std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}
