-- Inbound provider syncs are durable work. The API only enqueues one of these
-- rows; a background task claims it and continuously records progress.
CREATE TABLE media_tracker_sync_jobs (
    id                    BLOB PRIMARY KEY NOT NULL,
    user_media_tracker_id BLOB NOT NULL
                          REFERENCES user_media_trackers(id) ON DELETE CASCADE,
    -- queued | running | completed | failed
    status                TEXT NOT NULL DEFAULT 'queued',
    queued_at             DATETIME NOT NULL,
    started_at            DATETIME,
    finished_at           DATETIME,
    received              INTEGER NOT NULL DEFAULT 0,
    processed             INTEGER NOT NULL DEFAULT 0,
    matched               INTEGER NOT NULL DEFAULT 0,
    applied               INTEGER NOT NULL DEFAULT 0,
    payload_bytes         INTEGER NOT NULL DEFAULT 0,
    latest_error          TEXT,
    updated_at            DATETIME NOT NULL
);

-- Repeated Sync Now clicks (and an initial-connect enqueue racing one) must all
-- resolve to the same active job.
CREATE UNIQUE INDEX idx_media_tracker_sync_jobs_one_active
    ON media_tracker_sync_jobs(user_media_tracker_id)
    WHERE status IN ('queued', 'running');

CREATE INDEX idx_media_tracker_sync_jobs_latest
    ON media_tracker_sync_jobs(user_media_tracker_id, queued_at DESC, id DESC);

CREATE INDEX idx_media_tracker_sync_jobs_queue
    ON media_tracker_sync_jobs(status, queued_at, id);

-- Verification is distinct from successful sync/outbox activity. Keeping its
-- own timestamp lets the dashboard show exactly when Verify last succeeded.
ALTER TABLE user_media_trackers ADD COLUMN last_verified_at DATETIME;

-- Older workers copied a single dead-letter event onto connection health.
-- Preserve the failed outbox row for display, but stop presenting that
-- item-specific mismatch as a broken provider connection.
UPDATE user_media_trackers
SET status = 'connected',
    last_error_at = NULL,
    last_error = NULL,
    last_error_kind = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE status IN ('connected', 'error')
  AND last_error IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM media_tracker_outbox AS outbox
      WHERE outbox.user_media_tracker_id = user_media_trackers.id
        AND outbox.status IN ('failed_retryable', 'failed_permanent')
        AND outbox.last_error = user_media_trackers.last_error
  );

-- Deployments upgrading from the synchronous importer may have a connected
-- tracker but no cursor because that importer was interrupted. Queue the
-- baseline durably so the rollout resumes it without requiring another click.
INSERT INTO media_tracker_sync_jobs
    (id, user_media_tracker_id, status, queued_at, updated_at)
SELECT randomblob(16), tracker.id, 'queued', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM user_media_trackers AS tracker
LEFT JOIN media_tracker_sync_state AS sync_state
       ON sync_state.user_media_tracker_id = tracker.id
WHERE tracker.status = 'connected'
  AND (sync_state.cursor IS NULL OR trim(sync_state.cursor) = '');

-- The direct API poke gives low latency. These triggers recover queued/running
-- work after a crash and cover the tiny race where a job is queued as a worker
-- is finishing.
INSERT OR IGNORE INTO task_triggers (id, task_id, kind, time_limit_hours, cron)
VALUES ('default-mediatrackerinboundsync-startup', 'MediaTrackerInboundSync',
        'StartupTrigger', NULL, NULL);

INSERT OR IGNORE INTO task_triggers (id, task_id, kind, time_limit_hours, cron)
VALUES ('default-mediatrackerinboundsync-interval', 'MediaTrackerInboundSync',
        'IntervalTrigger', NULL, '*/30 * * * * *');
