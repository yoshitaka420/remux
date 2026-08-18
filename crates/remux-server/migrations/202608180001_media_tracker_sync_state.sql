-- Independent inbound cursor. user_media_trackers.last_success_at also moves
-- when an outbound scrobble succeeds, so it cannot safely delimit provider
-- history imports.
CREATE TABLE media_tracker_sync_state (
    user_media_tracker_id BLOB PRIMARY KEY NOT NULL
                          REFERENCES user_media_trackers(id) ON DELETE CASCADE,
    -- Provider-owned opaque watermark. Simkl explicitly requires clients to
    -- send the exact /sync/activities value back as date_from.
    cursor                TEXT,
    updated_at            DATETIME NOT NULL
);
