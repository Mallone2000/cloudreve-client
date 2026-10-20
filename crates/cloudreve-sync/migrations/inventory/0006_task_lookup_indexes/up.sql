CREATE INDEX IF NOT EXISTS idx_task_queue_drive_type_path_created
ON task_queue(drive_id, task_type, local_path, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_task_queue_active_path
ON task_queue(drive_id, task_type, local_path, status);

CREATE INDEX IF NOT EXISTS idx_task_queue_drive_status_updated
ON task_queue(drive_id, status, updated_at DESC);
