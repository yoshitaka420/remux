ALTER TABLE user_media_trackers
    ADD COLUMN sync_role TEXT NOT NULL DEFAULT 'mirror'
        CHECK (sync_role IN ('primary', 'mirror'));

ALTER TABLE user_media_trackers
    ADD COLUMN authority_version INTEGER NOT NULL DEFAULT 0;

-- Preserve the existing single-tracker behaviour. If an experimental database
-- already has several connections, the oldest connected row wins deterministically.
UPDATE user_media_trackers AS tracker
SET sync_role = 'primary'
WHERE tracker.id = (
    SELECT candidate.id
    FROM user_media_trackers AS candidate
    WHERE candidate.user_id = tracker.user_id
    ORDER BY CASE WHEN candidate.status = 'connected' THEN 0 ELSE 1 END,
             candidate.created_at ASC,
             candidate.id ASC
    LIMIT 1
);

CREATE UNIQUE INDEX idx_user_media_trackers_one_primary
    ON user_media_trackers(user_id)
    WHERE sync_role = 'primary';

CREATE INDEX idx_media_kind_external_kitsu
    ON media(kind, CAST(json_extract(external_ids, '$.kitsu') AS INTEGER));

CREATE INDEX idx_media_kind_external_mal
    ON media(kind, CAST(json_extract(external_ids, '$.mal') AS INTEGER));

CREATE INDEX idx_media_kind_external_anilist
    ON media(kind, CAST(json_extract(external_ids, '$.anilist') AS INTEGER));

ALTER TABLE media_tracker_outbox
    ADD COLUMN origin_connection_id BLOB
        REFERENCES user_media_trackers(id) ON DELETE SET NULL;

CREATE INDEX idx_media_tracker_outbox_origin
    ON media_tracker_outbox(origin_connection_id, created_at);
