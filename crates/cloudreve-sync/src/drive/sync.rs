use crate::drive::mounts::Mount;
use crate::drive::utils::{SafeSyncPath, is_partial_download, remote_path_to_local_relative_path};
use crate::inventory::{FileMetadata, MetadataEntry};
use crate::tasks::TaskPayload;
use crate::utils::disk_space;
use crate::utils::toast::send_low_disk_space_toast;
use anyhow::{Context, Result};
use cloudreve_api::{
    api::explorer::ExplorerApiExt,
    models::{
        common::ListAllRes,
        explorer::{FileResponse, ListResponse, file_type},
        uri::CrUri,
    },
};
use futures::{StreamExt, stream};
use notify_debouncer_full::DebouncedEvent;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use uuid::Uuid;

/// Sync direction / trigger mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Local file changed — upload to remote
    LocalChanged,
    /// Remote file changed — download to local
    RemoteChanged,
    /// Full bidirectional sync
    Full,
}

/// Groups of filesystem events reduced to affected paths
pub struct GroupedFsEvents {
    pub changed: Vec<PathBuf>,
    pub deleted: Vec<PathBuf>,
}

impl GroupedFsEvents {
    pub fn all_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.changed.clone();
        paths.extend(self.deleted.iter().cloned());
        paths
    }
}

/// Reduce a list of debounced FS events into changed/deleted path sets
pub fn group_fs_events(events: Vec<DebouncedEvent>) -> GroupedFsEvents {
    use notify_debouncer_full::notify::event::EventKind;
    let mut changed = HashSet::new();
    let mut deleted = HashSet::new();

    for event in events {
        for path in &event.paths {
            // Our own partial downloads live in the sync folder; reacting to
            // them would upload a half-written file back to the server.
            if is_partial_download(path) {
                continue;
            }
            match event.kind {
                EventKind::Remove(_) => {
                    changed.remove(path);
                    deleted.insert(path.clone());
                }
                EventKind::Create(_) | EventKind::Modify(_) => {
                    deleted.remove(path);
                    changed.insert(path.clone());
                }
                _ => {}
            }
        }
    }

    GroupedFsEvents {
        changed: changed.into_iter().collect(),
        deleted: deleted.into_iter().collect(),
    }
}

/// Perform a full bidirectional sync between local and remote using 3-way comparison.
///
/// State sources:
/// - **DB** : last known synced state (populated after each upload/download)
/// - **Local** : current local filesystem state
/// - **Remote** : current remote server state
///
/// Decision table (DB | Local | Remote):
/// - `(true, false, true)` : deleted locally → delete from server
/// - `(true, true, false)` : deleted from server → delete locally
/// - `(true, false, false)` : deleted from both → clean DB entry
/// - `(false, true, false)` : new local file → upload
/// - `(false, false, true)` : new remote file → download
/// - `(false, true, true)` : exists on both sides, not yet tracked → verify bytes before adoption
/// - `(true, true, true)` : already in sync, SSE handles live changes
/// Total size of the remote files that have no local copy yet — the bytes a
/// sync would have to write to disk. Saturates rather than wrapping so an
/// absurd remote size can never look small.
fn bytes_to_download(
    remote_map: &HashMap<PathBuf, &FileResponse>,
    local_set: &HashSet<PathBuf>,
) -> u64 {
    remote_map
        .iter()
        .filter(|(rel, _)| !local_set.contains(*rel))
        .fold(0u64, |acc, (_, f)| acc.saturating_add(f.size.max(0) as u64))
}

/// Notify the user when `needed` bytes will not fit on the sync volume.
async fn warn_if_sync_will_not_fit(mount: &Mount, local_root: &PathBuf, needed: u64) {
    if needed == 0 {
        return;
    }
    match disk_space::available_space_for(local_root) {
        Ok(available) if !disk_space::fits_on_volume(needed, available) => {
            let name = mount.get_config().await.name;
            send_low_disk_space_toast(&mount.id, &name, needed, available);
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(
                target: "drive::sync",
                id = %mount.id,
                error = %e,
                "Failed to query available disk space"
            );
        }
    }
}

pub async fn full_sync(mount: &Mount, local_root: &PathBuf, remote_path: &str) -> Result<()> {
    if mount.is_paused() {
        tracing::info!(target: "drive::sync", id = %mount.id, "Sync skipped: drive is paused");
        return Ok(());
    }
    tracing::info!(target: "drive::sync", id = %mount.id, remote = remote_path, "Starting 3-way full sync");

    let remote_base = CrUri::new(remote_path)?;

    // 1. Remote state: relative_path → FileResponse
    let remote_files = list_remote_recursive(mount, &remote_base).await?;
    let remote_map: HashMap<PathBuf, &FileResponse> = remote_files
        .iter()
        .filter(|f| f.file_type != file_type::FOLDER)
        .filter_map(|f| {
            let uri = CrUri::new(&f.path).ok()?;
            let rel = remote_path_to_local_relative_path(&uri, &remote_base).ok()?;
            Some((rel, f))
        })
        .collect();

    // 2. Local state
    let local_files = walk_local(local_root.clone()).await?;
    let local_set: HashSet<PathBuf> = local_files
        .iter()
        .filter_map(|p| p.strip_prefix(local_root).ok().map(|r| r.to_path_buf()))
        .collect();

    // 3. DB state (last known synced state)
    let db_entries = load_db_entries(mount.inventory.clone(), mount.id.clone()).await?;
    let db_set: HashSet<PathBuf> = db_entries
        .iter()
        .filter(|e| !e.is_folder)
        .filter_map(|e| {
            PathBuf::from(&e.local_path)
                .strip_prefix(local_root)
                .ok()
                .map(|r| r.to_path_buf())
        })
        .collect();
    let db_map: HashMap<PathBuf, &FileMetadata> = db_entries
        .iter()
        .filter(|e| !e.is_folder)
        .filter_map(|e| {
            PathBuf::from(&e.local_path)
                .strip_prefix(local_root)
                .ok()
                .map(|r| (r.to_path_buf(), e))
        })
        .collect();

    // Warn up front if the whole sync cannot fit. This does not stop the sync:
    // whatever fits is still worth downloading, and the per-file guard in the
    // download task is the hard stop. Without this warning the user would only
    // see files failing one by one with no explanation.
    warn_if_sync_will_not_fit(
        mount,
        local_root,
        bytes_to_download(&remote_map, &local_set),
    )
    .await;

    // 4. Union of all known paths
    let all_paths: HashSet<PathBuf> = remote_map
        .keys()
        .chain(local_set.iter())
        .chain(db_set.iter())
        .cloned()
        .collect();

    let drive_uuid = Uuid::parse_str(&mount.id).ok();
    let mut inventory_deletes: Vec<String> = Vec::new();
    let mut collision_entries: Vec<MetadataEntry> = Vec::new();
    let mut collision_verifications: Vec<(PathBuf, String)> = Vec::new();
    let mut scheduled_tasks: Vec<TaskPayload> = Vec::new();

    // One snapshot for the whole merge: a Settings save landing mid-loop must
    // not apply the new rules to half the tree and the old ones to the rest.
    let ignore = mount.ignore_matcher.read().await.clone();

    // A file with an active upload session has no downloadable entity yet
    // (the listing shows it, but downloading it fails with "Entity not exist").
    let is_uploading = |f: &FileResponse| {
        f.metadata.as_ref().is_some_and(|m| {
            m.contains_key(cloudreve_api::models::explorer::metadata::UPLOAD_SESSION_ID)
        })
    };

    for rel in &all_paths {
        let local_path = match SafeSyncPath::from_relative_path(rel)
            .and_then(|safe| safe.join_checked(local_root))
        {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(target: "drive::sync", path = %rel.display(), error = %error, "Unsafe path skipped");
                continue;
            }
        };
        let in_remote = remote_map.contains_key(rel);
        let in_local = local_set.contains(rel);
        let in_db = db_set.contains(rel);
        let path_str = local_path.to_str().unwrap_or("").to_string();

        // Ignored paths are excluded from the merge entirely, whatever their
        // state. Checking only the "new file" branches would leave anything
        // already in the inventory churning forever: the (true, true, true)
        // branch below compares etags and would keep pulling down a `.DS_Store`
        // every time another machine's Finder rewrote it.
        //
        // A tracked row is left in place, dormant — NOT deleted. It is the
        // only memory of what was last synced: without it, a file edited
        // while a pattern covered it comes back through the
        // (false, true, true) arm once the pattern is removed and gets
        // stamped "in sync" as-is, silently dropping the edit (and any
        // pending conflict_state with it).
        if ignore.is_match(&local_path) {
            continue;
        }

        match (in_db, in_local, in_remote) {
            // Deleted locally (still on server) → forget it, don't re-download, don't touch server
            (true, false, true) => {
                inventory_deletes.push(path_str.clone());
            }
            // Deleted from server (still local) → keep local copy, forget tracking
            (true, true, false) => {
                inventory_deletes.push(path_str.clone());
            }
            // Deleted from both → clean DB
            (true, false, false) => {
                inventory_deletes.push(path_str.clone());
            }
            // New local file → upload
            (false, true, false) => {
                if let Ok(meta) = std::fs::metadata(&local_path) {
                    if !mount.is_file_size_allowed(meta.len()).await {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            size = meta.len(),
                            "Skipping upload: file exceeds size limit"
                        );
                        continue;
                    }
                }
                scheduled_tasks.push(TaskPayload::upload(local_path));
            }
            // New remote file → download
            (false, false, true) => {
                if let Some(rf) = remote_map.get(rel) {
                    if is_uploading(rf) {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            "Skipping download: remote file is still being uploaded"
                        );
                        continue;
                    }
                    if !mount.is_file_size_allowed(rf.size as u64).await {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            size = rf.size,
                            "Skipping download: file exceeds size limit"
                        );
                        continue;
                    }
                    let mut payload = TaskPayload::download(local_path).with_totals(0, rf.size);
                    if let Some(etag) = &rf.primary_entity {
                        payload =
                            payload.with_custom_state(serde_json::json!({ "remote_etag": etag }));
                    }
                    scheduled_tasks.push(payload);
                }
            }
            // Existing files at the same path are not evidence of equal content.
            (false, true, true) => {
                if let (Some(rf), Some(uuid)) = (remote_map.get(rel), drive_uuid) {
                    let parse_ts = |s: &str| -> i64 {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .map(|dt| dt.timestamp())
                            .unwrap_or(0)
                    };
                    let mut entry = MetadataEntry::new(uuid, &path_str, false)
                        .with_created_at(parse_ts(&rf.created_at))
                        .with_updated_at(parse_ts(&rf.updated_at))
                        .with_etag(rf.primary_entity.clone().unwrap_or_default())
                        .with_size(rf.size)
                        .with_local_mtime(crate::inventory::local_mtime_secs(&local_path));
                    let local_size = std::fs::metadata(&local_path)
                        .map(|m| m.len())
                        .unwrap_or(u64::MAX);
                    if local_size != rf.size.max(0) as u64
                        || rf.primary_entity.as_deref().is_none_or(str::is_empty)
                    {
                        entry.conflict_state = Some(crate::inventory::ConflictState::Pending);
                    } else {
                        entry.conflict_state = Some(crate::inventory::ConflictState::Verifying);
                        collision_verifications
                            .push((local_path, rf.primary_entity.clone().unwrap_or_default()));
                    }
                    collision_entries.push(entry);
                }
            }
            // Already tracked and present everywhere → check for modifications
            (true, true, true) => {
                if let (Some(db_entry), Some(rf)) = (db_map.get(rel), remote_map.get(rel)) {
                    if db_entry.conflict_state == Some(crate::inventory::ConflictState::Verifying) {
                        if rf.primary_entity.as_deref().is_none_or(str::is_empty) {
                            mount.inventory.mark_as_conflicted(
                                &path_str,
                                Some(crate::inventory::ConflictState::Pending),
                            )?;
                        } else {
                            scheduled_tasks.push(
                                TaskPayload::verify_collision(local_path).with_custom_state(
                                    serde_json::json!({"remote_etag": rf.primary_entity.clone()}),
                                ),
                            );
                        }
                        continue;
                    }
                    // Files with an unresolved conflict are frozen: no transfer
                    // until the user resolves them.
                    if db_entry.conflict_state.is_some() {
                        tracing::debug!(
                            target: "drive::sync",
                            path = %rel.display(),
                            "Skipping transfer: file has an unresolved conflict"
                        );
                        continue;
                    }

                    // Check if remote was modified (etag changed)
                    let remote_etag = rf.primary_entity.clone().unwrap_or_default();
                    let remote_changed = !remote_etag.is_empty()
                        && !db_entry.etag.is_empty()
                        && remote_etag != db_entry.etag;
                    let local_changed = db_entry.is_locally_modified(&local_path);

                    // Both sides modified since last sync → conflict, let the user decide
                    if remote_changed && local_changed {
                        tracing::warn!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_etag = %db_entry.etag,
                            remote_etag = %remote_etag,
                            "Conflict detected: both local and remote modified since last sync"
                        );
                        mount
                            .inventory
                            .mark_as_conflicted(
                                &path_str,
                                Some(crate::inventory::ConflictState::Pending),
                            )
                            .context("Failed to persist conflict state in inventory")?;
                        crate::utils::toast::send_conflict_toast(
                            &mount.id,
                            &local_path,
                            db_entry.id,
                        );
                        mount.task_queue.notifier().notify();
                        continue;
                    }

                    if remote_changed {
                        if is_uploading(rf) {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                "Skipping download: remote file is still being uploaded"
                            );
                            continue;
                        }
                        if !mount.is_file_size_allowed(rf.size as u64).await {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                size = rf.size,
                                "Skipping download: file exceeds size limit"
                            );
                            continue;
                        }
                        tracing::info!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_etag = %db_entry.etag,
                            remote_etag = %remote_etag,
                            "Remote file modified since last sync, downloading"
                        );
                        let mut payload = TaskPayload::download(local_path).with_totals(0, rf.size);
                        if !remote_etag.is_empty() {
                            payload = payload.with_custom_state(
                                serde_json::json!({ "remote_etag": remote_etag }),
                            );
                        }
                        scheduled_tasks.push(payload);
                    } else if local_changed {
                        let local_size = std::fs::metadata(&local_path)
                            .map(|m| m.len() as i64)
                            .unwrap_or(0);
                        if !mount.is_file_size_allowed(local_size as u64).await {
                            tracing::debug!(
                                target: "drive::sync",
                                path = %rel.display(),
                                size = local_size,
                                "Skipping upload: file exceeds size limit"
                            );
                            continue;
                        }
                        tracing::info!(
                            target: "drive::sync",
                            path = %rel.display(),
                            db_size = db_entry.size,
                            local_size = local_size,
                            "Local file modified since last sync, uploading"
                        );
                        scheduled_tasks.push(TaskPayload::upload(local_path));
                    }
                }
            }
            (false, false, false) => {}
        }
    }

    if !collision_entries.is_empty() {
        let inventory = mount.inventory.clone();
        tokio::task::spawn_blocking(move || inventory.batch_upsert(&collision_entries))
            .await
            .context("Collision inventory write panicked")??;
        for (path, etag) in collision_verifications {
            scheduled_tasks.push(
                TaskPayload::verify_collision(path)
                    .with_custom_state(serde_json::json!({"remote_etag": etag})),
            );
        }
        mount.task_queue.notifier().notify();
    }

    mount.task_queue.enqueue_many(scheduled_tasks).await?;

    if !inventory_deletes.is_empty() {
        let inventory = mount.inventory.clone();
        tokio::task::spawn_blocking(move || {
            let paths: Vec<&str> = inventory_deletes.iter().map(String::as_str).collect();
            inventory.batch_delete_by_path(paths)
        })
        .await
        .context("Inventory cleanup task panicked")??;
    }

    tracing::info!(target: "drive::sync", id = %mount.id, "Full sync complete");
    Ok(())
}

/// Page size used for the very first request. The server advertises its own
/// limit in `props.max_page_size`, but that only arrives *with* a response, so
/// the first call has to guess conservatively.
const INITIAL_PAGE_SIZE: i32 = 100;

/// List all remote files recursively starting from `base`.
///
/// Every directory is read to its last page. Cloudreve paginates either by
/// offset or by cursor depending on the storage policy; `list_files_all` handles
/// both and reports whether more remains. Stopping early would not just miss
/// files: an already-synced file that fell past the boundary would look deleted
/// from the server, lose its inventory row, and get re-uploaded on the next pass.
async fn list_remote_recursive(mount: &Mount, base: &CrUri) -> Result<Vec<FileResponse>> {
    let mut all = Vec::new();
    let mut dirs = vec![base.clone()];
    // Learned from the first response, then reused for every later directory.
    let mut server_page_size: Option<i32> = None;

    while !dirs.is_empty() {
        let page_size = server_page_size.unwrap_or(INITIAL_PAGE_SIZE);
        let client = mount.cr_client.clone();
        let base = base.clone();
        let batch = std::mem::take(&mut dirs);
        let mut listings = stream::iter(batch.into_iter().map(|dir| {
            let client = client.clone();
            let base = base.clone();
            async move { list_one_remote_directory(client, dir, base, page_size).await }
        }))
        .buffer_unordered(4);
        while let Some(result) = listings.next().await {
            let (files, children, learned_page_size) = result?;
            if server_page_size.is_none() {
                server_page_size = learned_page_size;
            }
            all.extend(files);
            dirs.extend(children);
        }
    }

    Ok(all)
}

async fn list_one_remote_directory(
    client: std::sync::Arc<cloudreve_api::Client>,
    dir: CrUri,
    base: CrUri,
    page_size: i32,
) -> Result<(Vec<FileResponse>, Vec<CrUri>, Option<i32>)> {
    let uri = dir.to_string();
    let mut files = Vec::new();
    let mut children = Vec::new();
    let mut learned_page_size = None;
    let mut previous: Option<ListAllRes<ListResponse>> = None;
    let mut last_position: Option<(i32, Option<String>)> = None;
    loop {
        // Page size is frozen within a directory to keep offset windows stable.
        let mut response = client
            .list_files_all(previous.as_ref(), &uri, page_size)
            .await
            .context("Failed to list remote directory")?;
        if learned_page_size.is_none() && response.res.props.max_page_size > 0 {
            learned_page_size = Some(response.res.props.max_page_size);
        }
        for item in std::mem::take(&mut response.res.files) {
            if item.file_type == file_type::FOLDER {
                if let Ok(child) = CrUri::new(&item.path) {
                    if remote_path_to_local_relative_path(&child, &base).is_ok() {
                        children.push(child);
                    } else {
                        tracing::warn!(target: "drive::sync", "Unsafe remote folder URI skipped");
                    }
                }
            }
            files.push(item);
        }
        if !response.more {
            break;
        }
        let position = (
            response.res.pagination.page,
            response.res.pagination.next_token.clone(),
        );
        if last_position.as_ref() == Some(&position) {
            anyhow::bail!(
                "Remote listing of {uri} is not advancing past page {}",
                position.0
            );
        }
        last_position = Some(position);
        previous = Some(response);
    }
    Ok((files, children, learned_page_size))
}

/// Read the last-known synced state for a drive, off the runtime thread.
///
/// Diesel is a synchronous driver and this query loads every row for the drive,
/// so on a large inventory it holds whichever thread calls it for the duration.
async fn load_db_entries(
    inventory: std::sync::Arc<crate::inventory::InventoryDb>,
    drive_id: String,
) -> Result<Vec<FileMetadata>> {
    tokio::task::spawn_blocking(move || inventory.query_all_for_drive(&drive_id))
        .await
        .context("Inventory read panicked")?
}

/// Walk local directory and collect all file paths (excluding directories).
///
/// Refuses to scan a root that is not a reachable directory. An ejected volume,
/// an unmounted share or a deleted folder would otherwise scan as empty, and the
/// 3-way merge would read that as the user having deleted every file: the whole
/// inventory gets purged, local-only files are re-uploaded when the volume comes
/// back, and edits made in the meantime are re-registered against the remote
/// etag and never uploaded. An empty *reachable* folder stays legitimate.
///
/// Runs on the blocking pool. Every `read_dir`/`is_dir` here is a synchronous
/// syscall, and on a large sync folder the walk takes long enough that doing it
/// on a runtime thread stalls every other task on that thread — downloads,
/// uploads and the UI's status polling included.
async fn walk_local(root: PathBuf) -> Result<Vec<PathBuf>> {
    tokio::task::spawn_blocking(move || {
        let root_is_real_dir = std::fs::symlink_metadata(&root)
            .is_ok_and(|meta| meta.is_dir() && !meta.file_type().is_symlink());
        if !root_is_real_dir {
            anyhow::bail!(
                "Sync folder is not reachable: {} (deleted, or its volume is not mounted)",
                root.display()
            );
        }
        let mut files = Vec::new();
        walk_dir_recursive(&root, &root, &mut files)?;
        Ok(files)
    })
    .await
    .context("Local walk panicked")?
}

fn walk_dir_recursive(root: &Path, dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    // Only the root is a hard requirement; a subdirectory removed mid-walk is
    // ordinary churn, not a reason to abort the whole sync.
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir).context("Failed to read directory")? {
        let entry = entry?;
        let path = entry.path();
        let metadata = match std::fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() {
            tracing::warn!(target: "drive::sync", path = %path.display(), "Symlink skipped during local scan");
            crate::utils::toast::send_skipped_symlink_toast(root, &path);
            continue;
        }
        if metadata.is_dir() {
            walk_dir_recursive(root, &path, files)?;
        } else if !is_partial_download(&path) {
            // A download in flight is not a local file the user created.
            files.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify_debouncer_full::DebouncedEvent;
    use notify_debouncer_full::notify::event::{
        CreateKind, Event, EventKind, ModifyKind, RemoveKind,
    };
    use std::time::Instant;

    fn ev(kind: EventKind, path: &str) -> DebouncedEvent {
        DebouncedEvent::new(
            Event::new(kind).add_path(PathBuf::from(path)),
            Instant::now(),
        )
    }

    /// Partial downloads live inside the sync folder while they are being
    /// written. Treating one as a local change would upload a half-written
    /// file back to the server.
    #[test]
    fn partial_downloads_are_not_reported_as_local_changes() {
        let grouped = group_fs_events(vec![
            ev(
                EventKind::Create(CreateKind::File),
                "/sync/.cloudreve-part-abc123",
            ),
            ev(
                EventKind::Modify(ModifyKind::Any),
                "/sync/.cloudreve-part-abc123",
            ),
            ev(EventKind::Create(CreateKind::File), "/sync/real.txt"),
        ]);

        assert_eq!(grouped.changed, vec![PathBuf::from("/sync/real.txt")]);
    }

    /// Nor may a partial download be picked up by the local scan: full_sync
    /// would see an untracked local file and upload it.
    #[tokio::test]
    async fn the_local_scan_skips_partial_downloads() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("real.txt"), b"x").unwrap();
        std::fs::write(dir.path().join(".cloudreve-part-abc123"), b"x").unwrap();

        let found = walk_local(dir.path().to_path_buf()).await.unwrap();

        assert_eq!(found, vec![dir.path().join("real.txt")]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_local_scan_skips_symlink_files_and_directories() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"secret").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("linked-dir")).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("secret"),
            root.path().join("linked-file"),
        )
        .unwrap();
        std::fs::write(root.path().join("real"), b"real").unwrap();
        assert_eq!(
            walk_local(root.path().to_path_buf()).await.unwrap(),
            vec![root.path().join("real")]
        );
    }

    fn sized(size: i64) -> FileResponse {
        FileResponse {
            size,
            ..Default::default()
        }
    }

    /// Only files missing locally have to be written to disk.
    #[test]
    fn files_already_present_locally_do_not_count_towards_the_download_size() {
        let here = sized(300);
        let missing = sized(700);
        let remote = HashMap::from([
            (PathBuf::from("here.bin"), &here),
            (PathBuf::from("missing.bin"), &missing),
        ]);
        let local = HashSet::from([PathBuf::from("here.bin")]);

        assert_eq!(bytes_to_download(&remote, &local), 700);
    }

    /// A remote far larger than u64 can express must not wrap around to a
    /// small number and defeat the guard.
    #[test]
    fn absurd_remote_sizes_saturate_instead_of_overflowing() {
        let a = sized(i64::MAX);
        let b = sized(i64::MAX);
        let c = sized(i64::MAX);
        let remote = HashMap::from([
            (PathBuf::from("a"), &a),
            (PathBuf::from("b"), &b),
            (PathBuf::from("c"), &c),
        ]);

        assert_eq!(bytes_to_download(&remote, &HashSet::new()), u64::MAX);
    }

    /// An editor that saves via "write temp + delete + recreate" must end up
    /// as a change, not a deletion — otherwise the file would vanish.
    #[test]
    fn delete_then_recreate_is_a_change() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Remove(RemoveKind::File), "/sync/doc.txt"),
            ev(EventKind::Create(CreateKind::File), "/sync/doc.txt"),
        ]);

        assert_eq!(grouped.changed, vec![PathBuf::from("/sync/doc.txt")]);
        assert!(grouped.deleted.is_empty());
    }

    /// A file created then deleted within the debounce window is a deletion:
    /// it must not be uploaded.
    #[test]
    fn create_then_delete_is_a_deletion() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Create(CreateKind::File), "/sync/tmp.bin"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/tmp.bin"),
            ev(EventKind::Remove(RemoveKind::File), "/sync/tmp.bin"),
        ]);

        assert!(grouped.changed.is_empty());
        assert_eq!(grouped.deleted, vec![PathBuf::from("/sync/tmp.bin")]);
    }

    /// Repeated modifications of the same file collapse into a single change.
    #[test]
    fn repeated_modifications_are_deduplicated() {
        let grouped = group_fs_events(vec![
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
            ev(EventKind::Modify(ModifyKind::Any), "/sync/log.txt"),
        ]);

        assert_eq!(grouped.changed.len(), 1);
        assert!(grouped.deleted.is_empty());
    }

    /// The local walk must not run on the runtime thread.
    ///
    /// A single-threaded runtime only polls other tasks when the current one
    /// yields. So a task spawned just before the walk can only have run if the
    /// walk reached an await point — which is exactly what offloading to the
    /// blocking pool provides. Called inline, the walk never yields and the flag
    /// stays false however long it takes. No timing involved: the assertion
    /// distinguishes the two implementations outright.
    #[tokio::test(flavor = "current_thread")]
    async fn the_local_walk_yields_to_the_runtime() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.txt"), b"x").unwrap();

        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        tokio::spawn(async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let files = walk_local(dir.path().to_path_buf()).await.unwrap();

        assert_eq!(files.len(), 1);
        assert!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            "the local walk blocked the runtime: a concurrently spawned task never got to run"
        );
    }

    /// Same requirement for the inventory read. Diesel is a synchronous driver:
    /// `query_all_for_drive` loads every row for the drive and holds the calling
    /// thread for the whole query.
    #[tokio::test(flavor = "current_thread")]
    async fn the_inventory_read_yields_to_the_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(
            crate::inventory::InventoryDb::with_path(dir.path().join("meta.db")).unwrap(),
        );

        let ran = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();
        tokio::spawn(async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        let entries = load_db_entries(db, Uuid::new_v4().to_string())
            .await
            .unwrap();

        assert!(entries.is_empty());
        assert!(
            ran.load(std::sync::atomic::Ordering::SeqCst),
            "the inventory read blocked the runtime: a concurrently spawned task never got to run"
        );
    }
}
