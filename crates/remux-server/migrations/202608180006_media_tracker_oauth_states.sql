CREATE TABLE media_tracker_oauth_states (
    state TEXT PRIMARY KEY NOT NULL,
    user_id BLOB NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    addon_id BLOB NOT NULL REFERENCES addons(id) ON DELETE CASCADE,
    redirect_uri TEXT NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_media_tracker_oauth_states_user_addon
    ON media_tracker_oauth_states(user_id, addon_id);

CREATE INDEX idx_media_tracker_oauth_states_expiry
    ON media_tracker_oauth_states(expires_at);
