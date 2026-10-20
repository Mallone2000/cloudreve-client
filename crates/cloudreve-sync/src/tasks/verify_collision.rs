use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use cloudreve_api::{Client, api::ExplorerApi, models::explorer::{FileURLService, GetFileInfoService}};
use futures::StreamExt;
use tokio::io::AsyncReadExt;
use url::Url;

use crate::{
    drive::utils::{SafeSyncPath, local_path_to_cr_uri},
    inventory::{ConflictState, InventoryDb},
    tasks::{download::validate_download_url, queue::QueuedTask},
    utils::toast::send_conflict_toast,
};

fn same_local_file(before: &std::fs::Metadata, after: &std::fs::Metadata) -> bool {
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != after.dev() || before.ino() != after.ino()
            || before.ctime() != after.ctime() || before.ctime_nsec() != after.ctime_nsec() {
            return false;
        }
    }
    true
}

fn mark_conflict(inventory: &InventoryDb, drive_id: &str, path: &PathBuf) -> Result<()> {
    let path_str = path.to_str().context("Invalid local path")?;
    inventory.mark_as_conflicted(path_str, Some(ConflictState::Pending))?;
    let id = inventory.query_by_path(path_str)?.map(|row| row.id).unwrap_or(0);
    send_conflict_toast(drive_id, path, id);
    Ok(())
}

pub async fn execute(
    task: &QueuedTask,
    inventory: Arc<InventoryDb>,
    client: Arc<Client>,
    download_client: Arc<reqwest::Client>,
    approved_http_origin: &Option<Url>,
    drive_id: &str,
    sync_root: &PathBuf,
    remote_base: &str,
) -> Result<()> {
    let path = SafeSyncPath::from_local(&task.payload.local_path, sync_root)?
        .join_checked(sync_root)?;
    let expected_etag = task.payload.custom_state.as_ref()
        .and_then(|state| state.get("remote_etag"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty());
    let before = std::fs::symlink_metadata(&path).context("Collision local file is missing")?;
    if !before.is_file() || expected_etag.is_none() {
        mark_conflict(&inventory, drive_id, &path)?;
        return Ok(());
    }
    let uri = local_path_to_cr_uri(path.clone(), sync_root.clone(), remote_base.to_string())?.to_string();
    let info_request = GetFileInfoService {
        uri: Some(uri.clone()), id: None, extended: None, folder_summary: None,
    };
    let remote = client.get_file_info(&info_request).await?;
    if remote.primary_entity.as_deref() != expected_etag || remote.size < 0 || remote.size as u64 != before.len() {
        mark_conflict(&inventory, drive_id, &path)?;
        return Ok(());
    }
    let mut request = FileURLService::default();
    request.uris.push(uri);
    request.entity = remote.primary_entity.clone();
    let signed = client.get_file_url(&request).await?
        .urls.first().context("No collision comparison URL")?.url.clone();
    let signed = client.rewrite_url_origin(&signed);
    validate_download_url(&signed, approved_http_origin)?;
    let response = download_client.get(signed).send().await
        .map_err(reqwest::Error::without_url).context("Collision comparison download failed")?;
    if !response.status().is_success() {
        anyhow::bail!("Collision comparison download failed: HTTP {}", response.status());
    }

    let local = SafeSyncPath::from_local(&path, sync_root)?.open_read_checked(sync_root)?;
    if !same_local_file(&before, &local.metadata()?) {
        mark_conflict(&inventory, drive_id, &path)?;
        return Ok(());
    }
    let mut local = tokio::fs::File::from_std(local);
    let mut stream = response.bytes_stream();
    let mut compared = 0u64;
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(reqwest::Error::without_url)
            .context("Failed to read collision comparison")?;
        for piece in chunk.chunks(1024 * 1024) {
            buffer.resize(piece.len(), 0);
            if local.read_exact(&mut buffer).await.is_err() || buffer.as_slice() != piece {
                mark_conflict(&inventory, drive_id, &path)?;
                return Ok(());
            }
            compared = compared.saturating_add(piece.len() as u64);
        }
    }
    let mut trailing = [0u8; 1];
    if compared != before.len() || local.read(&mut trailing).await? != 0 {
        mark_conflict(&inventory, drive_id, &path)?;
        return Ok(());
    }
    let after = std::fs::symlink_metadata(&path)?;
    let remote_after = client.get_file_info(&info_request).await?;
    if !same_local_file(&before, &after) || remote_after.size != remote.size
        || remote_after.primary_entity.as_deref() != expected_etag {
        mark_conflict(&inventory, drive_id, &path)?;
        return Ok(());
    }
    inventory.mark_as_conflicted(path.to_str().context("Invalid local path")?, None)?;
    Ok(())
}

pub fn fail_to_conflict(inventory: &InventoryDb, drive_id: &str, path: &PathBuf) {
    if let Err(error) = mark_conflict(inventory, drive_id, path) {
        tracing::warn!(target: "tasks::verify_collision", error = %error, "Failed to surface unverified collision");
    }
}
