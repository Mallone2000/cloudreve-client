use anyhow::{Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{io::Write, path::PathBuf};

/// Manifest.json structure
#[derive(Debug, Deserialize)]
struct ManifestIcon {
    sizes: String,
    src: String,
    #[serde(rename = "type")]
    icon_type: String,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    icons: Vec<ManifestIcon>,
}

/// Result containing paths to both the ICO icon and raw image
#[derive(Debug, Clone)]
pub struct FaviconResult {
    /// Path to the ICO file (for Windows shell integration)
    pub ico_path: String,
    /// Path to the raw image file (PNG/JPG/etc, before ICO conversion)
    pub raw_path: String,
}

/// Icon type for download and fallback handling
#[derive(Debug, Clone, Copy)]
enum IconType {
    /// Small icon for ICO conversion (Windows shell integration)
    Small,
    /// Large icon for raw display (status UI)
    Large,
}

/// Get the icons directory path
fn get_icons_dir() -> Result<PathBuf> {
    let home_dir = dirs::home_dir().context("Failed to get user home directory")?;
    let app_dir = home_dir.join(".cloudreve");
    let icons_dir = app_dir.join("icos");

    for directory in [&app_dir, &icons_dir] {
        match std::fs::symlink_metadata(directory) {
            Ok(meta) if !meta.is_dir() || meta.file_type().is_symlink() => {
                anyhow::bail!("Icon cache directory is not a real directory");
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(directory).context("Failed to create icons directory")?;
            }
            Err(error) => return Err(error).context("Failed to inspect icons directory"),
        }
    }

    Ok(icons_dir)
}

pub(super) fn cached_icon_is_safe(path: &str) -> bool {
    let Ok(cache_dir) = get_icons_dir() else {
        return false;
    };
    icon_path_is_safe(std::path::Path::new(path), &cache_dir)
}

fn icon_path_is_safe(path: &std::path::Path, cache_dir: &std::path::Path) -> bool {
    path.parent() == Some(cache_dir)
        && std::fs::symlink_metadata(path)
            .is_ok_and(|meta| meta.is_file() && !meta.file_type().is_symlink())
}

/// Parse icon size from sizes string (e.g., "192x192" or "64x64 32x32")
/// Returns the first (typically largest for multi-size) dimension
fn parse_icon_size(sizes: &str) -> Option<u32> {
    sizes
        .split_whitespace()
        .filter_map(|size| size.split('x').next().and_then(|s| s.parse::<u32>().ok()))
        .next()
}

/// Build full URL from icon src and base instance URL
fn build_icon_url(icon: &ManifestIcon, instance_url: &str) -> Result<String> {
    let base = url::Url::parse(instance_url)?;
    if base.scheme() != "https"
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
    {
        anyhow::bail!("Site icons require HTTPS");
    }
    let url = base.join(&icon.src)?;
    if url.origin() != base.origin() || !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Manifest icon points outside the Cloudreve site");
    }
    Ok(url.to_string())
}

/// Determine file extension from icon type and URL
fn get_extension_from_icon(icon: &ManifestIcon, url: &str) -> &'static str {
    if icon.icon_type.contains("png") {
        "png"
    } else if icon.icon_type.contains("jpeg") || icon.icon_type.contains("jpg") {
        "jpg"
    } else if icon.icon_type.contains("x-icon") || icon.icon_type.contains("ico") {
        "ico"
    } else if url.ends_with(".png") {
        "png"
    } else if url.ends_with(".jpg") || url.ends_with(".jpeg") {
        "jpg"
    } else if url.ends_with(".ico") {
        "ico"
    } else {
        "png" // Default to PNG
    }
}

/// Download an icon from URL
async fn download_icon(client: &reqwest::Client, url: &str) -> Result<bytes::Bytes> {
    let parsed = url::Url::parse(url).context("Invalid icon URL")?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        anyhow::bail!("Site icons require HTTPS");
    }
    let response = client
        .get(url)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to download icon")?;

    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("Failed to download icon: HTTP {}", status);
    }

    read_limited(response, 2 * 1024 * 1024).await
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<bytes::Bytes> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        anyhow::bail!("Remote image or manifest is too large");
    }
    let mut body = bytes::BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk
            .map_err(reqwest::Error::without_url)
            .context("Failed to read remote image")?;
        if chunk.len() > limit.saturating_sub(body.len()) {
            anyhow::bail!("Remote image or manifest is too large");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

/// Get fallback icon bytes for the given icon type
fn get_fallback_icon(_icon_type: IconType) -> Result<Vec<u8>> {
    anyhow::bail!("No bundled fallback icon available on this platform")
}

/// Save icon bytes to destination, converting to ICO if needed
fn save_icon(bytes: &[u8], dest_path: &PathBuf, convert_to_ico: bool) -> Result<()> {
    let output = if convert_to_ico {
        // Convert image to ICO format
        let mut reader = image::io::Reader::new(std::io::Cursor::new(bytes))
            .with_guessed_format()
            .context("Failed to identify icon format")?;
        let mut limits = image::io::Limits::default();
        limits.max_image_width = Some(4096);
        limits.max_image_height = Some(4096);
        limits.max_alloc = Some(64 * 1024 * 1024);
        reader.limits(limits);
        let img = reader.decode().context("Failed to load image")?;
        // Resize to 64x64 for ICO
        let resized = img.resize(64, 64, image::imageops::FilterType::Lanczos3);
        let mut encoded = std::io::Cursor::new(Vec::new());
        resized
            .write_to(&mut encoded, image::ImageOutputFormat::Ico)
            .context("Failed to encode ICO")?;
        encoded.into_inner()
    } else {
        bytes.to_vec()
    };
    // A rename replaces an existing symlink instead of writing through it.
    let temp_path = dest_path.with_file_name(format!(".icon-{}", uuid::Uuid::new_v4()));
    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .context("Failed to create icon cache file")?;
    temp.write_all(&output)
        .context("Failed to write icon cache file")?;
    drop(temp);
    std::fs::rename(&temp_path, dest_path).context("Failed to install icon cache file")?;
    Ok(())
}

/// Fetch and save favicon from instance_url
/// Returns both the ICO path and the raw image path
/// For ICO: downloads the smallest icon for Windows shell integration
/// For raw: downloads the largest icon for status UI display
/// Falls back to bundled icons if download fails
pub async fn fetch_and_save_favicon(instance_url: &str) -> Result<FaviconResult> {
    tracing::info!(target: "drive::favicon", "Fetching favicon");

    // Parse the URL to get hostname and port
    let parsed_url = url::Url::parse(instance_url).context("Failed to parse instance URL")?;

    let host_with_port = if let Some(port) = parsed_url.port() {
        format!("{}:{}", parsed_url.host_str().unwrap_or(""), port)
    } else {
        parsed_url.host_str().unwrap_or("").to_string()
    };

    // Generate SHA256 hash of hostname:port
    let mut hasher = Sha256::new();
    hasher.update(host_with_port.as_bytes());
    let hash_hex = format!("{:x}", hasher.finalize());
    let hash = &hash_hex[..16];

    // Get icons directory
    let icons_dir = get_icons_dir()?;
    let ico_path = icons_dir.join(format!("{}.ico", hash));
    let raw_path = icons_dir.join(format!("{}_raw.png", hash));

    // Try to fetch and process icons from remote
    match fetch_icons_from_remote(instance_url, &icons_dir, hash).await {
        Ok(result) => return Ok(result),
        Err(e) => {
            tracing::warn!(target: "drive::favicon", error = %e, "Failed to fetch favicon from remote, using fallback");
        }
    }

    // Fallback: use bundled icons
    // Small icon fallback
    let small_bytes = get_fallback_icon(IconType::Small)?;
    save_icon(&small_bytes, &ico_path, true)?;
    tracing::debug!(target: "drive::favicon", path = %ico_path.display(), "Fallback ICO saved");

    // Large icon fallback
    let large_bytes = get_fallback_icon(IconType::Large)?;
    std::fs::write(&raw_path, &large_bytes).context("Failed to save fallback raw icon")?;
    tracing::debug!(target: "drive::favicon", path = %raw_path.display(), "Fallback raw icon saved");

    tracing::info!(target: "drive::favicon", ico_path = %ico_path.display(), raw_path = %raw_path.display(), "Fallback favicon saved successfully");

    Ok(FaviconResult {
        ico_path: ico_path.to_string_lossy().to_string(),
        raw_path: raw_path.to_string_lossy().to_string(),
    })
}

#[cfg(all(test, unix))]
mod file_safety_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn manifest_icons_cannot_downgrade_to_http() {
        let icon = ManifestIcon {
            sizes: "64x64".into(),
            src: "http://cloud.example/icon.png".into(),
            icon_type: "image/png".into(),
        };
        assert!(build_icon_url(&icon, "https://cloud.example").is_err());
        assert!(build_icon_url(&icon, "http://cloud.example").is_err());
    }

    #[test]
    fn icon_install_replaces_symlink_without_writing_through_it() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside.txt");
        std::fs::write(&outside, b"keep").unwrap();
        let cache = temp.path().join("icon.png");
        symlink(&outside, &cache).unwrap();
        save_icon(b"new icon", &cache, false).unwrap();
        assert_eq!(std::fs::read(&outside).unwrap(), b"keep");
        assert_eq!(std::fs::read(&cache).unwrap(), b"new icon");
        assert!(
            !std::fs::symlink_metadata(&cache)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn only_regular_files_directly_inside_icon_cache_can_be_removed() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("icos");
        std::fs::create_dir(&cache).unwrap();
        let inside = cache.join("icon.png");
        let outside = temp.path().join("outside.txt");
        let link = cache.join("link.png");
        std::fs::write(&inside, b"icon").unwrap();
        std::fs::write(&outside, b"keep").unwrap();
        symlink(&outside, &link).unwrap();
        assert!(icon_path_is_safe(&inside, &cache));
        assert!(!icon_path_is_safe(&outside, &cache));
        assert!(!icon_path_is_safe(&link, &cache));
    }

    #[test]
    fn oversized_icon_dimensions_are_rejected_before_conversion() {
        let temp = tempfile::tempdir().unwrap();
        let image = image::DynamicImage::new_rgba8(4097, 1);
        let mut bytes = std::io::Cursor::new(Vec::new());
        image
            .write_to(&mut bytes, image::ImageOutputFormat::Png)
            .unwrap();
        assert!(save_icon(&bytes.into_inner(), &temp.path().join("icon.ico"), true).is_err());
    }
}

/// Fetch icons from remote server
async fn fetch_icons_from_remote(
    instance_url: &str,
    icons_dir: &PathBuf,
    hash: &str,
) -> Result<FaviconResult> {
    let parsed = url::Url::parse(instance_url).context("Invalid Cloudreve site URL")?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        anyhow::bail!("Site icons require HTTPS");
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("Failed to create HTTP client")?;

    // Fetch manifest.json
    let manifest_url = format!("{}/manifest.json", instance_url.trim_end_matches('/'));
    tracing::debug!(target: "drive::favicon", "Fetching manifest.json");

    let manifest_response = client
        .get(&manifest_url)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("Failed to fetch manifest.json")?;
    if !manifest_response.status().is_success() {
        anyhow::bail!(
            "Manifest request failed: HTTP {}",
            manifest_response.status()
        );
    }
    let manifest: Manifest =
        serde_json::from_slice(&read_limited(manifest_response, 1024 * 1024).await?)
            .context("Failed to parse manifest.json")?;

    // Find the smallest icon for ICO (Windows shell integration)
    let smallest_icon = manifest
        .icons
        .iter()
        .filter_map(|icon| parse_icon_size(&icon.sizes).map(|size| (size, icon)))
        .min_by_key(|(size, _)| *size)
        .map(|(_, icon)| icon)
        .context("No valid icons found in manifest")?;

    // Find the largest icon for raw image (status UI display)
    let largest_icon = manifest
        .icons
        .iter()
        .filter_map(|icon| parse_icon_size(&icon.sizes).map(|size| (size, icon)))
        .max_by_key(|(size, _)| *size)
        .map(|(_, icon)| icon)
        .unwrap_or(smallest_icon);

    tracing::debug!(target: "drive::favicon", smallest_sizes = %smallest_icon.sizes, "Selected smallest icon for ICO");
    tracing::debug!(target: "drive::favicon", largest_sizes = %largest_icon.sizes, "Selected largest icon for raw");

    let smallest_icon_url = build_icon_url(smallest_icon, instance_url)?;
    let largest_icon_url = build_icon_url(largest_icon, instance_url)?;
    let same_icon = largest_icon.src == smallest_icon.src;

    // Download smallest icon for ICO, with fallback
    let small_bytes = match download_icon(&client, &smallest_icon_url).await {
        Ok(bytes) => bytes.to_vec(),
        Err(e) => {
            tracing::warn!(target: "drive::favicon", error = %e, "Failed to download small icon, using fallback");
            get_fallback_icon(IconType::Small)?
        }
    };

    // Download largest icon for raw (or reuse if same), with fallback
    let large_bytes = if same_icon {
        small_bytes.clone()
    } else {
        match download_icon(&client, &largest_icon_url).await {
            Ok(bytes) => bytes.to_vec(),
            Err(e) => {
                tracing::warn!(target: "drive::favicon", error = %e, "Failed to download large icon, using fallback");
                get_fallback_icon(IconType::Large)?
            }
        }
    };

    // Determine paths and extensions
    let ico_path = icons_dir.join(format!("{}.ico", hash));
    let raw_extension = get_extension_from_icon(largest_icon, &largest_icon_url);
    let raw_path = icons_dir.join(format!("{}_raw.{}", hash, raw_extension));

    // Save small icon as ICO
    save_icon(&small_bytes, &ico_path, true)?;
    tracing::debug!(target: "drive::favicon", path = %ico_path.display(), "ICO saved");

    // Save large icon as raw
    save_icon(&large_bytes, &raw_path, false)?;
    tracing::debug!(target: "drive::favicon", path = %raw_path.display(), "Raw icon saved");

    tracing::info!(target: "drive::favicon", ico_path = %ico_path.display(), raw_path = %raw_path.display(), "Favicon saved successfully");

    Ok(FaviconResult {
        ico_path: ico_path.to_string_lossy().to_string(),
        raw_path: raw_path.to_string_lossy().to_string(),
    })
}
