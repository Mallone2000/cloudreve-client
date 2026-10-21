DROP INDEX IF EXISTS idx_task_queue_blocked_origin;
DROP INDEX IF EXISTS idx_task_queue_dispatch;
ALTER TABLE task_queue DROP COLUMN blocked_origin;
ALTER TABLE task_queue DROP COLUMN blocked_reason;
