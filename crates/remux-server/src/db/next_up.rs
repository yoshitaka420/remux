use anyhow::Result;
use chrono::NaiveDateTime;
use sqlx::SqlitePool;
use uuid::Uuid;

use super::{Media, MediaKind};

/// A user-local dismissal of one series from Jellyfin's Next Up shelf.
///
/// This is deliberately separate from [`super::UserMediaState`]: dismissing a
/// card must not alter playback history or emit an outbound tracking event.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UserNextUpSuppression {
    pub user_id: Uuid,
    pub series_id: Uuid,
    pub suppressed_at: NaiveDateTime,
}

impl UserNextUpSuppression {
    /// Resolve the containing series for any TV hierarchy item accepted by the
    /// suppression API.
    pub fn series_id_for(media: &Media) -> Option<Uuid> {
        match media.kind {
            MediaKind::Series => Some(media.id),
            MediaKind::Season => media.parent_id,
            MediaKind::Episode => media.grandparent_id,
            _ => None,
        }
    }

    pub async fn suppress(
        db: &SqlitePool,
        user_id: Uuid,
        series_id: Uuid,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO user_next_up_suppressions \
             (user_id, series_id, suppressed_at) \
             VALUES (?1, ?2, CURRENT_TIMESTAMP) \
             ON CONFLICT(user_id, series_id) DO UPDATE SET \
             suppressed_at = excluded.suppressed_at",
        )
        .bind(user_id)
        .bind(series_id)
        .execute(db)
        .await?;
        Ok(())
    }

    pub async fn restore(
        db: &SqlitePool,
        user_id: Uuid,
        series_id: Uuid,
    ) -> Result<bool> {
        let deleted = sqlx::query(
            "DELETE FROM user_next_up_suppressions \
             WHERE user_id = ?1 AND series_id = ?2",
        )
        .bind(user_id)
        .bind(series_id)
        .execute(db)
        .await?;
        Ok(deleted.rows_affected() > 0)
    }

    /// Restore the containing show after intentional local activity. Provider
    /// imports do not call this helper, so a routine inbound sync cannot undo a
    /// user's dismissal.
    pub async fn restore_for_media(
        db: &SqlitePool,
        user_id: Uuid,
        media: &Media,
    ) -> Result<bool> {
        let Some(series_id) = Self::series_id_for(media) else {
            return Ok(false);
        };
        Self::restore(db, user_id, series_id).await
    }

    pub async fn is_suppressed(
        db: &SqlitePool,
        user_id: Uuid,
        series_id: Uuid,
    ) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM user_next_up_suppressions \
             WHERE user_id = ?1 AND series_id = ?2)",
        )
        .bind(user_id)
        .bind(series_id)
        .fetch_one(db)
        .await?)
    }
}
