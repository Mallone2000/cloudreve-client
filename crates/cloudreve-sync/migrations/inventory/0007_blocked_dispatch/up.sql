ALTER TABLE task_queue ADD COLUMN blocked_reason TEXT;
ALTER TABLE task_queue ADD COLUMN blocked_origin TEXT;
UPDATE task_queue SET blocked_reason = 'offline'
WHERE status = 'pending' AND json_extract(custom_state, '$.offline_waiting') = 1;
CREATE INDEX idx_task_queue_dispatch ON task_queue(drive_id, status, blocked_reason, priority DESC, created_at, id);
CREATE INDEX idx_task_queue_blocked_origin ON task_queue(drive_id, blocked_reason, blocked_origin);
