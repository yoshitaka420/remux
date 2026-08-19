use anyhow::Result;
use chrono::{NaiveDateTime, Utc};
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MediaTrackerSyncState {
    pub user_media_tracker_id: Uuid,
    pub cursor: Option<String>,
    pub updated_at: NaiveDateTime,
}

impl MediaTrackerSyncState {
    pub async fn get(
        db: &SqlitePool,
        user_media_tracker_id: Uuid,
    ) -> Result<Option<Self>> {
        Ok(sqlx::query_as::<_, Self>(
            "SELECT user_media_tracker_id, cursor, updated_at \
             FROM media_tracker_sync_state WHERE user_media_tracker_id = ?1",
        )
        .bind(user_media_tracker_id)
        .fetch_optional(db)
        .await?)
    }

    pub async fn set_cursor(
        db: &SqlitePool,
        user_media_tracker_id: Uuid,
        cursor: &str,
    ) -> Result<()> {
        let now = Utc::now().naive_utc();
        sqlx::query(
            "INSERT INTO media_tracker_sync_state \
             (user_media_tracker_id, cursor, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(user_media_tracker_id) DO UPDATE SET \
                 cursor = excluded.cursor, updated_at = excluded.updated_at",
        )
        .bind(user_media_tracker_id)
        .bind(cursor)
        .bind(now)
        .execute(db)
        .await?;
        Ok(())
    }
}
