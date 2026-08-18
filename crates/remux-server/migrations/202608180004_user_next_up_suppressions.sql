-- A user can dismiss a series from Jellyfin's Next Up shelf without changing
-- watched state (and therefore without sending an unwatch event to a tracker).
CREATE TABLE user_next_up_suppressions (
    user_id       BLOB     NOT NULL,
    series_id     BLOB     NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    suppressed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, series_id)
);

-- Required for efficient foreign-key cleanup when a series is removed.
CREATE INDEX idx_user_next_up_suppressions_series
    ON user_next_up_suppressions(series_id);
