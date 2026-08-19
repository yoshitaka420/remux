use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(
    strum_macros::EnumString,
    strum_macros::Display,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    sqlx::Type,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
pub enum MediaTrackerSyncJobStatus {
    #[default]
    Queued,
    Running,
    Completed,
    Failed,
}

impl MediaTrackerSyncJobStatus {
    pub fn active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MediaTrackerSyncJob {
    pub id: Uuid,
    pub user_media_tracker_id: Uuid,
    pub status: MediaTrackerSyncJobStatus,
    pub queued_at: NaiveDateTime,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub received: i64,
    pub processed: i64,
    pub matched: i64,
    pub applied: i64,
    pub payload_bytes: i64,
    pub latest_error: Option<String>,
    pub updated_at: NaiveDateTime,
}

const COLS: &str = "id, user_media_tracker_id, status, queued_at, started_at, \
    finished_at, received, processed, matched, applied, payload_bytes, latest_error, updated_at";

impl MediaTrackerSyncJob {
    pub fn active(&self) -> bool {
        self.status
            .active()
    }

    /// Enqueue one sync per connection. The partial unique index makes this
    /// atomic even when two browser requests arrive together.
    pub async fn enqueue(db: &SqlitePool, user_media_tracker_id: Uuid) -> Result<Self> {
        let id = crate::common::get_uuid();
        let now = Utc::now().naive_utc();
        let mut tx = db
            .begin()
            .await?;
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO media_tracker_sync_jobs \
             (id, user_media_tracker_id, status, queued_at, started_at, finished_at, \
              received, processed, matched, applied, payload_bytes, latest_error, updated_at) \
             VALUES (?1, ?2, 'queued', ?3, NULL, NULL, 0, 0, 0, 0, 0, NULL, ?3)",
        )
        .bind(id)
        .bind(user_media_tracker_id)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let job = if inserted == 1 {
            sqlx::query_as::<_, Self>(&format!(
                "SELECT {COLS} FROM media_tracker_sync_jobs WHERE id = ?1"
            ))
            .bind(id)
            .fetch_one(&mut *tx)
            .await?
        } else {
            sqlx::query_as::<_, Self>(&format!(
                "SELECT {COLS} FROM media_tracker_sync_jobs \
                 WHERE user_media_tracker_id = ?1 AND status IN ('queued', 'running') \
                 ORDER BY queued_at DESC, id DESC LIMIT 1"
            ))
            .bind(user_media_tracker_id)
            .fetch_optional(&mut *tx)
            .await?
            .context("active tracking sync job disappeared while enqueueing")?
        };
        tx.commit()
            .await?;
        Ok(job)
    }

    pub async fn get(db: &SqlitePool, id: Uuid) -> Result<Option<Self>> {
        Ok(sqlx::query_as::<_, Self>(&format!(
            "SELECT {COLS} FROM media_tracker_sync_jobs WHERE id = ?1"
        ))
        .bind(id)
        .fetch_optional(db)
        .await?)
    }

    pub async fn latest_for_connection(
        db: &SqlitePool,
        user_media_tracker_id: Uuid,
    ) -> Result<Option<Self>> {
        Ok(sqlx::query_as::<_, Self>(&format!(
            "SELECT {COLS} FROM media_tracker_sync_jobs \
             WHERE user_media_tracker_id = ?1 \
             ORDER BY queued_at DESC, id DESC LIMIT 1"
        ))
        .bind(user_media_tracker_id)
        .fetch_optional(db)
        .await?)
    }

    /// A running row can only survive between task invocations when the prior
    /// worker was aborted or the process exited. TaskService serialises one task
    /// key, so recovering all such rows at task start cannot steal live work.
    pub async fn recover_interrupted(db: &SqlitePool) -> Result<u64> {
        let now = Utc::now().naive_utc();
        Ok(sqlx::query(
            "UPDATE media_tracker_sync_jobs \
             SET status = 'queued', finished_at = NULL, \
                 latest_error = 'Previous sync worker was interrupted; retrying', \
                 updated_at = ?1 \
             WHERE status = 'running'",
        )
        .bind(now)
        .execute(db)
        .await?
        .rows_affected())
    }

    pub async fn claim_next(db: &SqlitePool) -> Result<Option<Self>> {
        let now = Utc::now().naive_utc();
        Ok(sqlx::query_as::<_, Self>(&format!(
            "UPDATE media_tracker_sync_jobs \
             SET status = 'running', started_at = COALESCE(started_at, ?1), \
                 finished_at = NULL, latest_error = NULL, updated_at = ?1 \
             WHERE id = (SELECT id FROM media_tracker_sync_jobs \
                         WHERE status = 'queued' ORDER BY queued_at, id LIMIT 1) \
               AND status = 'queued' \
             RETURNING {COLS}"
        ))
        .bind(now)
        .fetch_optional(db)
        .await?)
    }

    pub async fn update_progress(
        db: &SqlitePool,
        id: Uuid,
        received: usize,
        processed: usize,
        matched: usize,
        applied: usize,
        payload_bytes: usize,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE media_tracker_sync_jobs \
             SET received = ?2, processed = ?3, matched = ?4, applied = ?5, \
                 payload_bytes = ?6, updated_at = ?7 \
             WHERE id = ?1 AND status = 'running'",
        )
        .bind(id)
        .bind(i64::try_from(received).unwrap_or(i64::MAX))
        .bind(i64::try_from(processed).unwrap_or(i64::MAX))
        .bind(i64::try_from(matched).unwrap_or(i64::MAX))
        .bind(i64::try_from(applied).unwrap_or(i64::MAX))
        .bind(i64::try_from(payload_bytes).unwrap_or(i64::MAX))
        .bind(Utc::now().naive_utc())
        .execute(db)
        .await?;
        Ok(())
    }

    pub async fn complete(
        db: &SqlitePool,
        id: Uuid,
        received: usize,
        matched: usize,
        applied: usize,
        payload_bytes: usize,
    ) -> Result<()> {
        let now = Utc::now().naive_utc();
        sqlx::query(
            "UPDATE media_tracker_sync_jobs \
             SET status = 'completed', finished_at = ?2, received = ?3, processed = ?3, \
                 matched = ?4, applied = ?5, payload_bytes = ?6, latest_error = NULL, \
                 updated_at = ?2 \
             WHERE id = ?1",
        )
        .bind(id)
        .bind(now)
        .bind(i64::try_from(received).unwrap_or(i64::MAX))
        .bind(i64::try_from(matched).unwrap_or(i64::MAX))
        .bind(i64::try_from(applied).unwrap_or(i64::MAX))
        .bind(i64::try_from(payload_bytes).unwrap_or(i64::MAX))
        .execute(db)
        .await?;
        Ok(())
    }

    pub async fn fail(db: &SqlitePool, id: Uuid, error: &str) -> Result<()> {
        let now = Utc::now().naive_utc();
        sqlx::query(
            "UPDATE media_tracker_sync_jobs \
             SET status = 'failed', finished_at = ?2, latest_error = ?3, updated_at = ?2 \
             WHERE id = ?1",
        )
        .bind(id)
        .bind(now)
        .bind(error)
        .execute(db)
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        addons::tracking::TrackingCredentials, integration_test::new_test_server,
    };

    async fn seed_connection(db: &SqlitePool) -> Uuid {
        let addon = crate::common::get_uuid();
        sqlx::query(
            "INSERT INTO addons (id, name, preset, resources, types, enabled, \
             priority, created_at, updated_at, system, is_default) \
             VALUES (?1, 'sync-test', '{\"kind\":\"simkl\",\"config\":{\"client_id\":\"x\"}}', \
             '[\"tracking\"]', '[\"movie\"]', 1, 0, datetime('now'), datetime('now'), 0, 1)",
        )
        .bind(addon)
        .execute(db)
        .await
        .unwrap();
        let mut user = crate::db::User::new_with_password(
            String::new(),
            "sync-job-user".into(),
            "pw",
            None,
        )
        .unwrap();
        user.save(db)
            .await
            .unwrap();
        let connection = crate::db::UserMediaTracker::new(
            user.id,
            addon,
            TrackingCredentials::default(),
            Vec::new(),
        );
        connection
            .upsert(db)
            .await
            .unwrap();
        connection.id
    }

    #[tokio::test]
    async fn duplicate_enqueues_share_one_active_job() {
        let (_server, guard) = new_test_server()
            .await
            .unwrap();
        let connection = seed_connection(
            &guard
                .0
                .db,
        )
        .await;
        let (first, second) = tokio::join!(
            MediaTrackerSyncJob::enqueue(
                &guard
                    .0
                    .db,
                connection
            ),
            MediaTrackerSyncJob::enqueue(
                &guard
                    .0
                    .db,
                connection
            ),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(first.id, second.id);
        assert!(first.active());

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_tracker_sync_jobs \
             WHERE user_media_tracker_id = ?1 AND status IN ('queued', 'running')",
        )
        .bind(connection)
        .fetch_one(
            &guard
                .0
                .db,
        )
        .await
        .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn interrupted_running_job_is_requeued_and_keeps_progress() {
        let (_server, guard) = new_test_server()
            .await
            .unwrap();
        let connection = seed_connection(
            &guard
                .0
                .db,
        )
        .await;
        let queued = MediaTrackerSyncJob::enqueue(
            &guard
                .0
                .db,
            connection,
        )
        .await
        .unwrap();
        let running = MediaTrackerSyncJob::claim_next(
            &guard
                .0
                .db,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(running.id, queued.id);
        MediaTrackerSyncJob::update_progress(
            &guard
                .0
                .db,
            running.id,
            100,
            50,
            40,
            30,
            4096,
        )
        .await
        .unwrap();

        assert_eq!(
            MediaTrackerSyncJob::recover_interrupted(
                &guard
                    .0
                    .db
            )
            .await
            .unwrap(),
            1
        );
        let recovered = MediaTrackerSyncJob::get(
            &guard
                .0
                .db,
            running.id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered.status, MediaTrackerSyncJobStatus::Queued);
        assert_eq!(
            (
                recovered.received,
                recovered.processed,
                recovered.matched,
                recovered.applied
            ),
            (100, 50, 40, 30)
        );
    }
}
