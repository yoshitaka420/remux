use anyhow::Result;
use async_trait::async_trait;
use std::{sync::Arc, time::Instant};
use tracing::{info, warn};

use super::{ProgressReporter, Task, TaskCategory, TaskService};
use crate::{AppContext, api::tracking::sync_connection_from_provider, db};

pub struct MediaTrackerInboundSyncTask;

#[async_trait]
impl Task for MediaTrackerInboundSyncTask {
    fn key(&self) -> &str {
        "MediaTrackerInboundSync"
    }

    fn name(&self) -> &str {
        "Media Tracker Inbound Sync"
    }

    fn description(&self) -> &str {
        "Imports provider watch history, resume positions, and ratings in durable background jobs."
    }

    fn short_description(&self) -> &str {
        "Imports tracking-provider state"
    }

    fn category(&self) -> TaskCategory {
        TaskCategory::Maintenance
    }

    async fn run(
        &self,
        ctx: AppContext,
        _tasks: Arc<TaskService>,
        progress: ProgressReporter,
    ) -> Result<()> {
        let recovered = db::MediaTrackerSyncJob::recover_interrupted(&ctx.db).await?;
        if recovered > 0 {
            warn!(recovered, "requeued interrupted inbound tracking sync jobs");
        }

        let mut processed = 0usize;
        while let Some(job) = db::MediaTrackerSyncJob::claim_next(&ctx.db).await? {
            processed += 1;
            let started = Instant::now();
            let Some(connection) =
                db::UserMediaTracker::get(&ctx.db, job.user_media_tracker_id).await?
            else {
                // The FK cascade normally removes this job with its connection.
                // If a non-FK test database leaves it behind, make it terminal.
                db::MediaTrackerSyncJob::fail(
                    &ctx.db,
                    job.id,
                    "Tracking connection no longer exists",
                )
                .await?;
                continue;
            };
            let Some(user) = db::User::get_by_id(&ctx.db, &connection.user_id).await?
            else {
                db::MediaTrackerSyncJob::fail(
                    &ctx.db,
                    job.id,
                    "Tracking connection user no longer exists",
                )
                .await?;
                continue;
            };

            info!(
                job_id = %job.id,
                user_id = %user.id,
                addon_id = %connection.addon_id,
                "inbound tracking sync job started"
            );
            match sync_connection_from_provider(&ctx, &user, &connection, job.id).await
            {
                Ok(result) => {
                    db::MediaTrackerSyncJob::complete(
                        &ctx.db,
                        job.id,
                        result.received,
                        result.matched,
                        result.applied,
                        result.payload_bytes,
                    )
                    .await?;
                    info!(
                        job_id = %job.id,
                        addon_id = %connection.addon_id,
                        received = result.received,
                        matched = result.matched,
                        applied = result.applied,
                        payload_bytes = result.payload_bytes,
                        duration_ms = started.elapsed().as_millis(),
                        "inbound tracking sync job completed"
                    );
                }
                Err(error) => {
                    db::MediaTrackerSyncJob::fail(&ctx.db, job.id, &error.to_string())
                        .await?;
                    if let Err(mark_error) = db::UserMediaTracker::mark_failure(
                        &ctx.db,
                        connection.id,
                        &error,
                    )
                    .await
                    {
                        warn!(
                            job_id = %job.id,
                            error = %mark_error,
                            "failed to record inbound tracking connection error"
                        );
                    }
                    warn!(
                        job_id = %job.id,
                        addon_id = %connection.addon_id,
                        duration_ms = started.elapsed().as_millis(),
                        error = %error,
                        "inbound tracking sync job failed"
                    );
                }
            }
        }

        info!(processed, "inbound tracking sync queue drained");
        progress.set(100.0);
        Ok(())
    }
}
