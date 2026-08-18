use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use axum_anyhow::ApiResult as Result;
use dashmap::DashMap;
use remux_macros::{delete, get, post};
use remux_sdks::tracking::{
    TrackingConnectionDto, TrackingFailedEventDto, TrackingFiltersRequest,
    TrackingPinPollDto, TrackingPinPollRequest, TrackingPinStartDto, TrackingPinStatus,
    TrackingSyncJobDto, TrackingSyncJobStatus, TrackingSyncResultDto,
};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    AppContext, AppState, IntoApiError, OptionExt, ResultExt,
    addons::{
        AddonRuntime,
        tracking::{
            AuthFlow, DeviceAuthPoll, RemoteWatch, SyncDirection, TrackingCtx,
            TrackingError, TrackingEventKind, open_credentials, seal_credentials,
        },
    },
    db::{self, auth},
};

#[derive(Clone)]
struct PendingPin {
    user_id: Uuid,
    addon_id: Uuid,
    expires_at: Instant,
    state: Arc<tokio::sync::Mutex<PendingPinState>>,
}

enum PendingPinState {
    Awaiting {
        provider_token: String,
        interval: Duration,
        next_poll_at: Instant,
    },
    Approved,
    Denied,
}

static PENDING_PINS: LazyLock<DashMap<String, PendingPin>> =
    LazyLock::new(DashMap::new);

const MATCH_PROGRESS_INTERVAL: usize = 250;
const MATCH_PROGRESS_LOG_INTERVAL: usize = 5_000;

fn runtime_for(state: &AppState, addon_id: Uuid) -> Result<AddonRuntime> {
    state
        .ctx
        .addons
        .tracking_addons()
        .into_iter()
        .find(|runtime| {
            runtime
                .row
                .id
                == addon_id
        })
        .context_not_found("Tracking addon not found or disabled")
}

fn tracking_context(ctx: &AppContext) -> TrackingCtx {
    TrackingCtx {
        config: std::sync::Arc::new(
            ctx.config
                .clone(),
        ),
    }
}

fn auth_flow_name(flow: &AuthFlow) -> &'static str {
    match flow {
        AuthFlow::Token => "token",
        AuthFlow::OAuthDeviceCode => "oauth_device_code",
        AuthFlow::OAuthRedirect => "oauth_redirect",
    }
}

fn direction_name(direction: SyncDirection) -> &'static str {
    match direction {
        SyncDirection::None => "none",
        SyncDirection::Push => "push",
        SyncDirection::Pull => "pull",
        SyncDirection::Both => "both",
    }
}

async fn connection_dto(
    ctx: &AppContext,
    runtime: &AddonRuntime,
    connection: Option<&db::UserMediaTracker>,
) -> anyhow::Result<TrackingConnectionDto> {
    let provider = runtime
        .tracking
        .as_ref()
        .expect("tracking_addons returned a runtime without tracking");
    let capabilities = provider.capabilities();
    let (pending_events, failed_events, latest_failed_event) = if let Some(connection) =
        connection
    {
        let pending = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_tracker_outbox \
             WHERE user_media_tracker_id = ?1 AND status = 'pending'",
        )
        .bind(connection.id)
        .fetch_one(&ctx.db)
        .await?;
        let failed = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM media_tracker_outbox \
             WHERE user_media_tracker_id = ?1 \
               AND status IN ('failed_retryable', 'failed_permanent')",
        )
        .bind(connection.id)
        .fetch_one(&ctx.db)
        .await?;
        let latest =
            sqlx::query_as::<_, (String, Option<String>, chrono::NaiveDateTime)>(
                "SELECT event_kind, last_error, updated_at FROM media_tracker_outbox \
             WHERE user_media_tracker_id = ?1 \
               AND status IN ('failed_retryable', 'failed_permanent') \
             ORDER BY updated_at DESC, id DESC LIMIT 1",
            )
            .bind(connection.id)
            .fetch_optional(&ctx.db)
            .await?
            .map(|(event_kind, error, failed_at)| TrackingFailedEventDto {
                event_kind,
                error: error
                    .unwrap_or_else(|| "Provider rejected this event".to_string()),
                failed_at,
            });
        (pending as usize, failed as usize, latest)
    } else {
        (0, 0, None)
    };

    Ok(TrackingConnectionDto {
        addon_id: runtime
            .row
            .id,
        addon_name: runtime
            .row
            .name
            .clone(),
        provider: provider
            .id()
            .to_string(),
        connected: connection.is_some(),
        status: connection.map(|connection| {
            connection
                .status
                .to_string()
        }),
        event_filters: connection
            .map(|connection| {
                connection
                    .event_filters
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        supported_events: capabilities
            .supported_events
            .iter()
            .map(ToString::to_string)
            .collect(),
        default_event_filter: capabilities
            .default_event_filter
            .iter()
            .map(ToString::to_string)
            .collect(),
        auth_flow: auth_flow_name(&capabilities.auth_flow).to_string(),
        history_import: capabilities.history_import,
        progress_import: capabilities.progress_import,
        watch_state_sync: direction_name(capabilities.watch_state_sync).to_string(),
        ratings_sync: direction_name(capabilities.ratings).to_string(),
        last_success_at: connection.and_then(|connection| connection.last_success_at),
        last_verified_at: connection.and_then(|connection| connection.last_verified_at),
        last_error_at: connection.and_then(|connection| connection.last_error_at),
        last_error: connection.and_then(|connection| {
            connection
                .last_error
                .clone()
        }),
        pending_events,
        failed_events,
        latest_failed_event,
    })
}

fn sync_job_dto(job: db::MediaTrackerSyncJob) -> TrackingSyncJobDto {
    TrackingSyncJobDto {
        id: job.id,
        status: match job.status {
            db::MediaTrackerSyncJobStatus::Queued => TrackingSyncJobStatus::Queued,
            db::MediaTrackerSyncJobStatus::Running => TrackingSyncJobStatus::Running,
            db::MediaTrackerSyncJobStatus::Completed => {
                TrackingSyncJobStatus::Completed
            }
            db::MediaTrackerSyncJobStatus::Failed => TrackingSyncJobStatus::Failed,
        },
        queued_at: job.queued_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        received: usize::try_from(job.received).unwrap_or(usize::MAX),
        processed: usize::try_from(job.processed).unwrap_or(usize::MAX),
        matched: usize::try_from(job.matched).unwrap_or(usize::MAX),
        applied: usize::try_from(job.applied).unwrap_or(usize::MAX),
        payload_bytes: usize::try_from(job.payload_bytes).unwrap_or(usize::MAX),
        latest_error: job.latest_error,
    }
}

#[get("/remux/tracking/addons")]
pub async fn list_tracking_addons(
    State(state): State<AppState>,
    session: auth::AuthSession,
) -> Result<Json<Vec<TrackingConnectionDto>>> {
    let mut result = Vec::new();
    for runtime in state
        .ctx
        .addons
        .tracking_addons()
    {
        let connection = db::UserMediaTracker::get_for_user_and_addon(
            &state
                .ctx
                .db,
            session
                .user
                .id,
            runtime
                .row
                .id,
        )
        .await?;
        result.push(connection_dto(&state.ctx, &runtime, connection.as_ref()).await?);
    }
    Ok(Json(result))
}

#[post("/remux/tracking/addons/{addon_id}/pin")]
pub async fn begin_tracking_pin(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
) -> Result<Json<TrackingPinStartDto>> {
    let runtime = runtime_for(&state, addon_id)?;
    let provider = runtime
        .tracking
        .as_ref()
        .unwrap();
    if provider
        .capabilities()
        .auth_flow
        != AuthFlow::OAuthDeviceCode
    {
        return Err(anyhow::anyhow!("addon does not use PIN authentication")
            .context_bad_request("Unsupported authentication flow"));
    }
    let started = provider
        .begin_device_auth(&tracking_context(&state.ctx))
        .await
        .map_err(tracking_api_error)?;
    let user_id = session
        .user
        .id;
    let now = Instant::now();
    PENDING_PINS.retain(|_, pending| {
        pending.expires_at > now
            && !(pending.user_id == user_id && pending.addon_id == addon_id)
    });
    let public_token = Uuid::new_v4().to_string();
    PENDING_PINS.insert(
        public_token.clone(),
        PendingPin {
            user_id,
            addon_id,
            expires_at: now + started.expires_in,
            state: Arc::new(tokio::sync::Mutex::new(PendingPinState::Awaiting {
                provider_token: started.poll_token,
                interval: started.interval,
                next_poll_at: now + started.interval,
            })),
        },
    );
    Ok(Json(TrackingPinStartDto {
        verification_url: started.verification_url,
        user_code: started.user_code,
        poll_token: public_token,
        interval_seconds: started
            .interval
            .as_secs(),
        expires_in_seconds: started
            .expires_in
            .as_secs(),
    }))
}

#[post("/remux/tracking/addons/{addon_id}/pin/poll")]
pub async fn poll_tracking_pin(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
    Json(payload): Json<TrackingPinPollRequest>,
) -> Result<Json<TrackingPinPollDto>> {
    let now = Instant::now();
    let pending = match PENDING_PINS.get(&payload.poll_token) {
        Some(pending) => pending.clone(),
        None => {
            return Err(anyhow::anyhow!("unknown PIN session")
                .context_bad_request("Unknown or expired PIN session"));
        }
    };
    if pending.user_id
        != session
            .user
            .id
        || pending.addon_id != addon_id
    {
        return Err(anyhow::anyhow!("PIN session owner mismatch")
            .context_forbidden("PIN session belongs to another user or addon"));
    }
    if now >= pending.expires_at {
        PENDING_PINS.remove(&payload.poll_token);
        return Ok(Json(TrackingPinPollDto {
            status: TrackingPinStatus::Denied,
            connection: None,
        }));
    }

    let runtime = runtime_for(&state, addon_id)?;
    let mut pin_state = pending
        .state
        .lock()
        .await;
    let provider_token = match &mut *pin_state {
        PendingPinState::Approved => {
            let connection = db::UserMediaTracker::get_for_user_and_addon(
                &state
                    .ctx
                    .db,
                session
                    .user
                    .id,
                addon_id,
            )
            .await?
            .context_internal("Approved tracking connection disappeared")?;
            return Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Approved,
                connection: Some(
                    connection_dto(&state.ctx, &runtime, Some(&connection)).await?,
                ),
            }));
        }
        PendingPinState::Denied => {
            return Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Denied,
                connection: None,
            }));
        }
        PendingPinState::Awaiting {
            provider_token,
            interval,
            next_poll_at,
        } => {
            if now < *next_poll_at {
                return Ok(Json(TrackingPinPollDto {
                    status: TrackingPinStatus::Pending,
                    connection: None,
                }));
            }
            *next_poll_at = now + *interval;
            provider_token.clone()
        }
    };

    let provider = runtime
        .tracking
        .as_ref()
        .unwrap();
    let polled = provider
        .poll_device_auth(&provider_token, &tracking_context(&state.ctx))
        .await
        .map_err(tracking_api_error)?;
    match polled {
        DeviceAuthPoll::Pending => Ok(Json(TrackingPinPollDto {
            status: TrackingPinStatus::Pending,
            connection: None,
        })),
        DeviceAuthPoll::Denied => {
            // Keep terminal state until the original expiry. A duplicate browser
            // request must see the same answer instead of asking Simkl to reuse a
            // single-use code.
            *pin_state = PendingPinState::Denied;
            Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Denied,
                connection: None,
            }))
        }
        DeviceAuthPoll::Approved(credentials) => {
            // Receiving an access token is Simkl's successful PIN result. Save it
            // before doing any follow-up work so a lost browser response can be
            // reconciled through the integrations endpoint.
            let sealed = seal_credentials(
                &credentials,
                &state
                    .ctx
                    .config,
            )
            .map_err(tracking_api_error)?;
            let capabilities = provider.capabilities();
            let event_filters = db::UserMediaTracker::get_for_user_and_addon(
                &state
                    .ctx
                    .db,
                session
                    .user
                    .id,
                addon_id,
            )
            .await?
            .map(|connection| connection.event_filters)
            .unwrap_or(capabilities.default_event_filter);
            let row = db::UserMediaTracker::new(
                session
                    .user
                    .id,
                addon_id,
                sealed,
                event_filters,
            );
            row.upsert(
                &state
                    .ctx
                    .db,
            )
            .await?;
            let connection = db::UserMediaTracker::get_for_user_and_addon(
                &state
                    .ctx
                    .db,
                session
                    .user
                    .id,
                addon_id,
            )
            .await?
            .context_internal("Connected tracker disappeared")?;
            // A reconnect can authorize a different provider account. Never
            // reuse its predecessor's inbound watermark, and give events that
            // previously failed for expired credentials another chance.
            sqlx::query(
                "DELETE FROM media_tracker_sync_state \
                 WHERE user_media_tracker_id = ?1",
            )
            .bind(connection.id)
            .execute(
                &state
                    .ctx
                    .db,
            )
            .await?;
            let requeued = sqlx::query(
                "UPDATE media_tracker_outbox SET status = 'pending', attempts = 0, \
                     next_attempt_at = CURRENT_TIMESTAMP, last_error = NULL, \
                     updated_at = CURRENT_TIMESTAMP \
                 WHERE user_media_tracker_id = ?1 \
                   AND status IN ('failed_retryable', 'failed_permanent')",
            )
            .bind(connection.id)
            .execute(
                &state
                    .ctx
                    .db,
            )
            .await?
            .rows_affected();
            let sync_job = db::MediaTrackerSyncJob::enqueue(
                &state
                    .ctx
                    .db,
                connection.id,
            )
            .await?;
            *pin_state = PendingPinState::Approved;

            if let Err(error) = state
                .tasks
                .run_task("MediaTrackerInboundSync")
                .await
            {
                warn!(job_id = %sync_job.id, error = %error, "failed to wake inbound tracker queue");
            }
            if requeued > 0 {
                if let Err(error) = state
                    .tasks
                    .run_task("MediaTrackerSync")
                    .await
                {
                    warn!(error = %error, "failed to wake reconnected tracker queue");
                }
            }

            info!(
                addon_id = %addon_id,
                user_id = %session.user.id,
                sync_job_id = %sync_job.id,
                "tracking PIN approved and initial sync queued"
            );

            Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Approved,
                connection: Some(
                    connection_dto(&state.ctx, &runtime, Some(&connection)).await?,
                ),
            }))
        }
    }
}

#[post("/remux/tracking/addons/{addon_id}/verify")]
pub async fn verify_tracking_addon(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
) -> Result<Json<TrackingConnectionDto>> {
    let runtime = runtime_for(&state, addon_id)?;
    let connection = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        session
            .user
            .id,
        addon_id,
    )
    .await?
    .context_not_found("Tracking connection not found")?;
    let provider = runtime
        .tracking
        .as_ref()
        .unwrap();
    let credentials = open_credentials(
        &connection.credentials,
        &state
            .ctx
            .config,
    )
    .map_err(tracking_api_error)?;
    match provider
        .verify(&credentials, &tracking_context(&state.ctx))
        .await
    {
        Ok(()) => {
            db::UserMediaTracker::mark_verified(
                &state
                    .ctx
                    .db,
                connection.id,
            )
            .await?
        }
        Err(error) => {
            db::UserMediaTracker::mark_failure(
                &state
                    .ctx
                    .db,
                connection.id,
                &error,
            )
            .await?;
            return Err(tracking_api_error(error));
        }
    }
    let connection = db::UserMediaTracker::get(
        &state
            .ctx
            .db,
        connection.id,
    )
    .await?
    .context_internal("Tracking connection disappeared")?;
    Ok(Json(
        connection_dto(&state.ctx, &runtime, Some(&connection)).await?,
    ))
}

#[delete("/remux/tracking/addons/{addon_id}")]
pub async fn disconnect_tracking_addon(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
) -> Result<StatusCode> {
    let runtime = runtime_for(&state, addon_id)?;
    let Some(connection) = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        session
            .user
            .id,
        addon_id,
    )
    .await?
    else {
        return Ok(StatusCode::NO_CONTENT);
    };
    if let Ok(credentials) = open_credentials(
        &connection.credentials,
        &state
            .ctx
            .config,
    ) {
        if let Err(error) = runtime
            .tracking
            .as_ref()
            .unwrap()
            .disconnect(&credentials, &tracking_context(&state.ctx))
            .await
        {
            warn!(addon_id = %addon_id, error = %error, "remote tracking disconnect failed");
        }
    }
    db::UserMediaTracker::delete(
        &state
            .ctx
            .db,
        connection.id,
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[post("/remux/tracking/addons/{addon_id}/filters")]
pub async fn set_tracking_filters(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
    Json(payload): Json<TrackingFiltersRequest>,
) -> Result<Json<TrackingConnectionDto>> {
    let runtime = runtime_for(&state, addon_id)?;
    let provider = runtime
        .tracking
        .as_ref()
        .unwrap();
    let capabilities = provider.capabilities();
    let mut filters = Vec::new();
    for value in payload.event_filters {
        let parsed = TrackingEventKind::from_str(&value)
            .map_err(|_| anyhow::anyhow!("unknown tracking event: {value}"))
            .context_bad_request("Invalid tracking event filter")?;
        if !capabilities.supports(parsed) {
            return Err(anyhow::anyhow!("unsupported tracking event: {value}")
                .context_bad_request("Unsupported tracking event filter"));
        }
        if !filters.contains(&parsed) {
            filters.push(parsed);
        }
    }
    let connection = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        session
            .user
            .id,
        addon_id,
    )
    .await?
    .context_not_found("Tracking connection not found")?;
    db::UserMediaTracker::set_event_filters(
        &state
            .ctx
            .db,
        connection.id,
        &filters,
    )
    .await?;
    let connection = db::UserMediaTracker::get(
        &state
            .ctx
            .db,
        connection.id,
    )
    .await?
    .context_internal("Tracking connection disappeared")?;
    Ok(Json(
        connection_dto(&state.ctx, &runtime, Some(&connection)).await?,
    ))
}

#[post("/remux/tracking/addons/{addon_id}/sync")]
pub async fn sync_tracking_addon(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
) -> Result<(StatusCode, Json<TrackingSyncJobDto>)> {
    let _runtime = runtime_for(&state, addon_id)?;
    let connection = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        session
            .user
            .id,
        addon_id,
    )
    .await?
    .context_not_found("Tracking connection not found")?;
    let job = db::MediaTrackerSyncJob::enqueue(
        &state
            .ctx
            .db,
        connection.id,
    )
    .await?;
    if let Err(error) = state
        .tasks
        .run_task("MediaTrackerInboundSync")
        .await
    {
        warn!(job_id = %job.id, error = %error, "failed to wake inbound tracker queue");
    }
    Ok((StatusCode::ACCEPTED, Json(sync_job_dto(job))))
}

#[get("/remux/tracking/addons/{addon_id}/sync/status")]
pub async fn tracking_sync_status(
    State(state): State<AppState>,
    session: auth::AuthSession,
    Path(addon_id): Path<Uuid>,
) -> Result<Json<Option<TrackingSyncJobDto>>> {
    let _runtime = runtime_for(&state, addon_id)?;
    let connection = db::UserMediaTracker::get_for_user_and_addon(
        &state
            .ctx
            .db,
        session
            .user
            .id,
        addon_id,
    )
    .await?
    .context_not_found("Tracking connection not found")?;
    let job = db::MediaTrackerSyncJob::latest_for_connection(
        &state
            .ctx
            .db,
        connection.id,
    )
    .await?
    .map(sync_job_dto);
    Ok(Json(job))
}

/// Pull provider state into local user data. Local writes here bypass the
/// HTTP mutation handlers, so they do not echo back into the outbound outbox.
pub(crate) async fn sync_connection_from_provider(
    ctx: &AppContext,
    user: &db::User,
    connection: &db::UserMediaTracker,
    job_id: Uuid,
) -> std::result::Result<TrackingSyncResultDto, TrackingError> {
    let provider = ctx
        .addons
        .tracking_for(connection.addon_id)
        .ok_or_else(|| TrackingError::permanent("tracking addon is disabled"))?;
    let credentials = open_credentials(&connection.credentials, &ctx.config)?;
    let sync_state = db::MediaTrackerSyncState::get(&ctx.db, connection.id)
        .await
        .map_err(|error| {
            TrackingError::retryable(format!("loading sync cursor: {error}"))
        })?;
    let cursor = sync_state.and_then(|state| state.cursor);
    let tracking_ctx = tracking_context(ctx);
    let remote = if cursor.is_none()
        && provider
            .capabilities()
            .history_import
    {
        provider
            .import_history(&credentials, &tracking_ctx)
            .await?
    } else {
        provider
            .pull_changes(cursor, &credentials, &tracking_ctx)
            .await?
    };

    let mut result = TrackingSyncResultDto {
        received: remote
            .items
            .len(),
        payload_bytes: remote.payload_bytes,
        ..Default::default()
    };
    info!(
        job_id = %job_id,
        addon_id = %connection.addon_id,
        received = result.received,
        payload_bytes = result.payload_bytes,
        "tracking provider payload received"
    );
    db::MediaTrackerSyncJob::update_progress(
        &ctx.db,
        job_id,
        result.received,
        0,
        result.matched,
        result.applied,
        result.payload_bytes,
    )
    .await
    .map_err(|error| {
        TrackingError::retryable(format!("saving sync progress: {error}"))
    })?;

    let server_config = db::Settings::get_config_or_default(&ctx.db).await;
    let mut series_cache: HashMap<crate::addons::tracking::TrackingIds, Option<Uuid>> =
        HashMap::new();
    for (index, change) in remote
        .items
        .into_iter()
        .enumerate()
    {
        let media = find_remote_media(&ctx.db, &change, &mut series_cache)
            .await
            .map_err(|error| {
                TrackingError::retryable(format!("matching library item: {error}"))
            })?;
        if let Some(media) = media {
            result.matched += 1;
            let mut changed = false;

            if let Some(watched) = change.watched {
                if watched {
                    let mut state = media
                        .mark_played(
                            &ctx.db,
                            user,
                            true,
                            server_config.release_date_threshold(),
                        )
                        .await
                        .map_err(|error| {
                            TrackingError::retryable(format!(
                                "applying watched state: {error}"
                            ))
                        })?;
                    if let Some(watched_at) = change.watched_at {
                        state.played_at = Some(watched_at);
                        state
                            .save(&ctx.db)
                            .await
                            .map_err(|error| {
                                TrackingError::retryable(format!(
                                    "saving watched timestamp: {error}"
                                ))
                            })?;
                    }
                } else {
                    media
                        .mark_unplayed(&ctx.db, user, true)
                        .await
                        .map_err(|error| {
                            TrackingError::retryable(format!(
                                "clearing watched state: {error}"
                            ))
                        })?;
                }
                changed = true;
            }
            let position_ticks = change
                .position_ticks
                .or_else(|| {
                    change
                        .position_percent
                        .and_then(|percent| {
                            media
                                .runtime
                                .map(|runtime_seconds| {
                                    (percent.clamp(0.0, 100.0) / 100.0
                                        * runtime_seconds as f64
                                        * 10_000_000.0)
                                        .round()
                                        as i64
                                })
                        })
                });
            if let Some(position_ticks) = position_ticks {
                db::UserMediaState::update_playback(
                    &ctx.db,
                    user,
                    &media,
                    position_ticks.max(0),
                    None,
                    None,
                    None,
                )
                .await
                .map_err(|error| {
                    TrackingError::retryable(format!(
                        "applying playback progress: {error}"
                    ))
                })?;
                changed = true;
            }
            if let Some(favorite) = change.favorite {
                if favorite {
                    media
                        .mark_favorite(&ctx.db, user)
                        .await
                } else {
                    media
                        .unmark_favorite(&ctx.db, user)
                        .await
                }
                .map_err(|error| {
                    TrackingError::retryable(format!(
                        "applying favorite state: {error}"
                    ))
                })?;
                changed = true;
            }
            if let Some(rating) = change.rating {
                let rating = rating
                    .map(|rating| {
                        db::UserRating::try_from(rating as f64).map_err(|error| {
                            TrackingError::permanent(format!(
                                "provider returned invalid rating: {error}"
                            ))
                        })
                    })
                    .transpose()?;
                db::UserMediaState::set_rating(&ctx.db, user, &media, rating)
                    .await
                    .map_err(|error| {
                        TrackingError::retryable(format!("applying rating: {error}"))
                    })?;
                changed = true;
            }
            if changed {
                result.applied += 1;
            }
        }

        let processed = index + 1;
        if processed % MATCH_PROGRESS_INTERVAL == 0 || processed == result.received {
            db::MediaTrackerSyncJob::update_progress(
                &ctx.db,
                job_id,
                result.received,
                processed,
                result.matched,
                result.applied,
                result.payload_bytes,
            )
            .await
            .map_err(|error| {
                TrackingError::retryable(format!("saving sync progress: {error}"))
            })?;
        }
        if processed % MATCH_PROGRESS_LOG_INTERVAL == 0 || processed == result.received
        {
            info!(
                job_id = %job_id,
                processed,
                received = result.received,
                matched = result.matched,
                applied = result.applied,
                "tracking import matching progress"
            );
        }
    }

    db::MediaTrackerSyncState::set_cursor(&ctx.db, connection.id, &remote.cursor)
        .await
        .map_err(|error| {
            TrackingError::retryable(format!("saving sync cursor: {error}"))
        })?;
    db::UserMediaTracker::mark_success(&ctx.db, connection.id)
        .await
        .map_err(|error| {
            TrackingError::retryable(format!("saving tracker health: {error}"))
        })?;
    Ok(result)
}

async fn find_remote_media(
    db_pool: &sqlx::SqlitePool,
    change: &RemoteWatch,
    series_cache: &mut HashMap<crate::addons::tracking::TrackingIds, Option<Uuid>>,
) -> anyhow::Result<Option<db::Media>> {
    if change
        .ids
        .is_empty()
    {
        return Ok(None);
    }
    if change.kind == db::MediaKind::Episode {
        // Simkl episode rows carry their parent series IDs. Looking for those
        // IDs on episode rows first turns every item into a kind-wide JSON scan.
        // Resolve/cache the series, then use the hierarchy coordinate indexes.
        let series_id = if let Some(cached) = series_cache.get(&change.ids) {
            *cached
        } else {
            let resolved = find_by_ids(db_pool, db::MediaKind::Series, &change.ids)
                .await?
                .map(|series| series.id);
            series_cache.insert(
                change
                    .ids
                    .clone(),
                resolved,
            );
            resolved
        };
        let Some(series_id) = series_id else {
            return Ok(None);
        };
        let Some(season) = change.season else {
            return Ok(None);
        };
        let Some(episode) = change.episode else {
            return Ok(None);
        };
        if let Some(episode_row) = sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media INDEXED BY idx_media_grandparent_kind_parent_idx_idx \
             WHERE grandparent_id = ?1 AND kind = 'episode' \
             AND parent_idx = ?2 AND idx = ?3 LIMIT 1",
        )
        .bind(series_id)
        .bind(season)
        .bind(episode)
        .fetch_optional(db_pool)
        .await?
        {
            return Ok(Some(episode_row));
        }

        // Older imports may only have parent_id populated. Resolve the season
        // and then the episode with two exact idx_media_parent_kind_idx probes.
        let season_id = sqlx::query_scalar::<_, Uuid>(
            "SELECT id FROM media INDEXED BY idx_media_parent_kind_idx \
             WHERE parent_id = ?1 AND kind = 'season' \
             AND idx = ?2 LIMIT 1",
        )
        .bind(series_id)
        .bind(season)
        .fetch_optional(db_pool)
        .await?;
        let Some(season_id) = season_id else {
            return Ok(None);
        };
        return Ok(sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media INDEXED BY idx_media_parent_kind_idx \
             WHERE parent_id = ?1 AND kind = 'episode' \
             AND idx = ?2 LIMIT 1",
        )
        .bind(season_id)
        .bind(episode)
        .fetch_optional(db_pool)
        .await?);
    }
    find_by_ids(
        db_pool,
        change
            .kind
            .clone(),
        &change.ids,
    )
    .await
}

async fn find_by_ids(
    db_pool: &sqlx::SqlitePool,
    kind: db::MediaKind,
    ids: &crate::addons::tracking::TrackingIds,
) -> anyhow::Result<Option<db::Media>> {
    if let Some(imdb) = ids
        .imdb
        .as_deref()
    {
        if let Some(media) = sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media INDEXED BY idx_media_kind_external_imdb WHERE kind = ?1 \
             AND json_extract(external_ids, '$.imdb') = ?2 LIMIT 1",
        )
        .bind(kind.clone())
        .bind(imdb)
        .fetch_optional(db_pool)
        .await?
        {
            return Ok(Some(media));
        }
    }
    if let Some(tmdb) = ids.tmdb {
        if let Some(media) = sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media INDEXED BY idx_media_kind_external_tmdb WHERE kind = ?1 \
             AND CAST(json_extract(external_ids, '$.tmdb') AS INTEGER) = ?2 LIMIT 1",
        )
        .bind(kind.clone())
        .bind(tmdb)
        .fetch_optional(db_pool)
        .await?
        {
            return Ok(Some(media));
        }
    }
    if let Some(tvdb) = ids.tvdb {
        if let Some(media) = sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media INDEXED BY idx_media_kind_external_tvdb WHERE kind = ?1 \
             AND CAST(json_extract(external_ids, '$.tvdb') AS INTEGER) = ?2 LIMIT 1",
        )
        .bind(kind)
        .bind(tvdb)
        .fetch_optional(db_pool)
        .await?
        {
            return Ok(Some(media));
        }
    }
    Ok(None)
}

fn tracking_api_error(error: TrackingError) -> axum_anyhow::ApiError {
    let retryable = error.is_retryable();
    let detail = error.to_string();
    let error = anyhow::anyhow!(error);
    if retryable {
        error.context_bad_gateway(&detail)
    } else {
        error.context_bad_request(&detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        addons::tracking::{TrackingCredentials, TrackingIds},
        integration_test::{
            AUTH_HEADER, TestGuard, auth_header_with_token, new_test_server,
            new_test_server_with_config,
        },
    };
    use axum_test::TestServer;
    use http::header::HeaderValue;

    fn auth_value(token: &str) -> HeaderValue {
        HeaderValue::from_str(&auth_header_with_token(token)).unwrap()
    }

    async fn mocked_tracking_server(
        provider: &httpmock::MockServer,
    ) -> (TestServer, TestGuard, String, Uuid) {
        let (server, guard) = new_test_server_with_config(crate::Config {
            database_url: Some("sqlite::memory:".into()),
            torrent_http_port: None,
            disable_dht: true,
            simkl_base_url: provider.base_url(),
            simkl_connect_timeout_seconds: 1,
            simkl_request_timeout_seconds: 3,
            ..Default::default()
        })
        .await
        .unwrap();
        let login = server
            .post("/users/authenticatebyname")
            .add_header(
                http::header::AUTHORIZATION,
                HeaderValue::from_static(AUTH_HEADER),
            )
            .json(&serde_json::json!({ "Username": "test", "Pw": "test" }))
            .await;
        let login_body: serde_json::Value = login.json();
        let token = login_body["AccessToken"]
            .as_str()
            .unwrap()
            .to_string();
        let created = server
            .post("/addons")
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .json(&serde_json::json!({
                "preset": {
                    "kind": "simkl",
                    "config": { "client_id": "test-client" }
                },
                "name": "Mock Simkl",
                "resources": ["tracking"],
                "types": ["movie", "series", "season", "episode"]
            }))
            .await;
        created.assert_status(StatusCode::CREATED);
        let created_body: serde_json::Value = created.json();
        let addon_id = Uuid::parse_str(
            created_body["id"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        (server, guard, token, addon_id)
    }

    async fn save_media(
        db_pool: &sqlx::SqlitePool,
        title: &str,
        kind: db::MediaKind,
        parent_id: Option<Uuid>,
        grandparent_id: Option<Uuid>,
        idx: Option<i64>,
        parent_idx: Option<i64>,
        imdb: Option<&str>,
        tmdb: Option<i64>,
        tvdb: Option<i64>,
    ) -> db::Media {
        let mut media = db::Media {
            title: title.to_string(),
            kind,
            parent_id,
            grandparent_id,
            idx,
            parent_idx,
            external_ids: db::ExternalIds {
                imdb: imdb.and_then(|value| {
                    remux_utils::NonEmptyString::try_new(value.to_string()).ok()
                }),
                tmdb,
                tvdb,
                ..Default::default()
            },
            ..Default::default()
        };
        sqlx::query(
            "INSERT INTO media \
             (id, title, kind, parent_id, grandparent_id, idx, parent_idx, external_ids, \
              created_at, updated_at, locked_fields) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, '[]')",
        )
        .bind(media.id)
        .bind(&media.title)
        .bind(media.kind.clone())
        .bind(media.parent_id)
        .bind(media.grandparent_id)
        .bind(media.idx)
        .bind(media.parent_idx)
        .bind(sqlx::types::Json(&media.external_ids))
        .bind(media.created_at)
        .bind(media.updated_at)
        .execute(db_pool)
        .await
        .unwrap();
        media
    }

    fn remote_episode(ids: TrackingIds, season: i64, episode: i64) -> RemoteWatch {
        RemoteWatch {
            kind: db::MediaKind::Episode,
            ids,
            season: Some(season),
            episode: Some(episode),
            watched: Some(true),
            position_ticks: None,
            position_percent: None,
            watched_at: None,
            favorite: None,
            rating: None,
        }
    }

    async fn explain(db_pool: &sqlx::SqlitePool, query: &str) -> String {
        sqlx::query_as::<_, (i64, i64, i64, String)>(&format!(
            "EXPLAIN QUERY PLAN {query}"
        ))
        .fetch_all(db_pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(_, _, _, detail)| detail)
        .collect::<Vec<_>>()
        .join("\n")
    }

    #[tokio::test]
    async fn external_id_and_episode_queries_use_tracking_indexes() {
        let (_server, guard) = new_test_server()
            .await
            .unwrap();
        let db_pool = &guard
            .0
            .db;
        let cases = [
            (
                "SELECT * FROM media INDEXED BY idx_media_kind_external_imdb \
                 WHERE kind = 'series' \
                 AND json_extract(external_ids, '$.imdb') = 'tt123' LIMIT 1",
                "idx_media_kind_external_imdb",
            ),
            (
                "SELECT * FROM media INDEXED BY idx_media_kind_external_tmdb \
                 WHERE kind = 'movie' \
                 AND CAST(json_extract(external_ids, '$.tmdb') AS INTEGER) = 123 LIMIT 1",
                "idx_media_kind_external_tmdb",
            ),
            (
                "SELECT * FROM media INDEXED BY idx_media_kind_external_tvdb \
                 WHERE kind = 'series' \
                 AND CAST(json_extract(external_ids, '$.tvdb') AS INTEGER) = 123 LIMIT 1",
                "idx_media_kind_external_tvdb",
            ),
            (
                "SELECT * FROM media INDEXED BY idx_media_grandparent_kind_parent_idx_idx \
                 WHERE grandparent_id = x'00000000000000000000000000000000' \
                 AND kind = 'episode' AND parent_idx = 1 AND idx = 2 LIMIT 1",
                "idx_media_grandparent_kind_parent_idx_idx",
            ),
        ];
        for (query, expected_index) in cases {
            let plan = explain(db_pool, query).await;
            assert!(
                plan.contains(expected_index),
                "expected {expected_index} in query plan:\n{plan}"
            );
            assert!(
                !plan.contains("SCAN media"),
                "tracking lookup regressed to a media scan:\n{plan}"
            );
        }
    }

    #[tokio::test]
    async fn episode_matching_resolves_series_before_hierarchy_coordinates() {
        let (_server, guard) = new_test_server()
            .await
            .unwrap();
        let db_pool = &guard
            .0
            .db;
        let wanted_series = save_media(
            db_pool,
            "Wanted",
            db::MediaKind::Series,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(9001),
        )
        .await;
        let wanted_season = save_media(
            db_pool,
            "Season 2",
            db::MediaKind::Season,
            Some(wanted_series.id),
            None,
            Some(2),
            None,
            None,
            None,
            None,
        )
        .await;
        let wanted_episode = save_media(
            db_pool,
            "Wanted episode",
            db::MediaKind::Episode,
            Some(wanted_season.id),
            Some(wanted_series.id),
            Some(3),
            Some(2),
            None,
            None,
            None,
        )
        .await;

        // This row mimics the dangerous old path: an episode happens to carry
        // the same external ID as the remote row's parent series.
        let other_series = save_media(
            db_pool,
            "Other",
            db::MediaKind::Series,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(9002),
        )
        .await;
        let _distractor = save_media(
            db_pool,
            "Wrong episode",
            db::MediaKind::Episode,
            Some(other_series.id),
            Some(other_series.id),
            Some(3),
            Some(2),
            None,
            None,
            Some(9001),
        )
        .await;

        let change = remote_episode(
            TrackingIds {
                tvdb: Some(9001),
                ..Default::default()
            },
            2,
            3,
        );
        let mut cache = HashMap::new();
        let matched = find_remote_media(db_pool, &change, &mut cache)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(matched.id, wanted_episode.id);
        assert_eq!(cache.len(), 1, "series resolution should be cached");
    }

    #[tokio::test]
    async fn sequential_id_lookup_preserves_imdb_precedence() {
        let (_server, guard) = new_test_server()
            .await
            .unwrap();
        let db_pool = &guard
            .0
            .db;
        let imdb_match = save_media(
            db_pool,
            "IMDb",
            db::MediaKind::Movie,
            None,
            None,
            None,
            None,
            Some("tt0000123"),
            None,
            None,
        )
        .await;
        let _tmdb_match = save_media(
            db_pool,
            "TMDB",
            db::MediaKind::Movie,
            None,
            None,
            None,
            None,
            None,
            Some(456),
            None,
        )
        .await;
        let matched = find_by_ids(
            db_pool,
            db::MediaKind::Movie,
            &TrackingIds {
                imdb: Some("tt0000123".to_string()),
                tmdb: Some(456),
                tvdb: None,
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(matched.id, imdb_match.id);
    }

    #[tokio::test]
    async fn lost_pin_response_is_idempotent_and_sync_clicks_deduplicate() {
        let provider = httpmock::MockServer::start();
        let begin = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin");
            then.status(200)
                .json_body(serde_json::json!({
                    "result": "OK",
                    "device_code": "device",
                    "user_code": "ONEUSE",
                    "verification_uri": "https://simkl.com/pin",
                    "expires_in": 900,
                    "interval": 5
                }));
        });
        let poll = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin/ONEUSE");
            then.status(200)
                .json_body(serde_json::json!({
                    "result": "OK",
                    "access_token": "approved-token"
                }));
        });
        // Keep the initial job active while the duplicate buttons are tested.
        let _shows = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/shows");
            then.status(200)
                .delay(Duration::from_secs(2))
                .json_body(serde_json::json!({ "shows": [] }));
        });
        let _movies = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/movies");
            then.status(200)
                .json_body(serde_json::json!({ "movies": [] }));
        });
        let _anime = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/anime");
            then.status(200)
                .json_body(serde_json::json!({ "anime": [] }));
        });
        let _playback = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/playback");
            then.status(200)
                .json_body(serde_json::json!([]));
        });
        let _activities = provider.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({ "all": "cursor" }));
        });
        let (server, _guard, token, addon_id) = mocked_tracking_server(&provider).await;
        let pin_start: TrackingPinStartDto = server
            .post(&format!("/remux/tracking/addons/{addon_id}/pin"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .await
            .json();
        begin.assert_hits(1);

        // Tests do not wait five seconds for the documented provider interval.
        // Moving this one test session's next poll time does not alter production.
        let pending = PENDING_PINS
            .get(&pin_start.poll_token)
            .unwrap()
            .clone();
        let mut pending_state = pending
            .state
            .lock()
            .await;
        if let PendingPinState::Awaiting { next_poll_at, .. } = &mut *pending_state {
            *next_poll_at = Instant::now();
        }
        drop(pending_state);

        // The browser loses this successful response. The durable connection and
        // retained terminal PIN state must still let the retry reconcile.
        let lost_response = server
            .post(&format!("/remux/tracking/addons/{addon_id}/pin/poll"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .json(&TrackingPinPollRequest {
                poll_token: pin_start
                    .poll_token
                    .clone(),
            })
            .await;
        lost_response.assert_status_ok();
        let retry: TrackingPinPollDto = server
            .post(&format!("/remux/tracking/addons/{addon_id}/pin/poll"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .json(&TrackingPinPollRequest {
                poll_token: pin_start
                    .poll_token
                    .clone(),
            })
            .await
            .json();
        assert_eq!(retry.status, TrackingPinStatus::Approved);
        assert!(
            retry
                .connection
                .is_some_and(|connection| connection.connected)
        );
        poll.assert_hits(1);

        let connections: Vec<TrackingConnectionDto> = server
            .get("/remux/tracking/addons")
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .await
            .json();
        assert!(
            connections
                .iter()
                .any(|connection| {
                    connection.addon_id == addon_id && connection.connected
                })
        );

        let first = server
            .post(&format!("/remux/tracking/addons/{addon_id}/sync"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .await;
        first.assert_status(StatusCode::ACCEPTED);
        let first: TrackingSyncJobDto = first.json();
        let second = server
            .post(&format!("/remux/tracking/addons/{addon_id}/sync"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .await;
        second.assert_status(StatusCode::ACCEPTED);
        let second: TrackingSyncJobDto = second.json();
        assert_eq!(first.id, second.id);
        assert!(
            second
                .status
                .active()
        );

        // A refreshed page has no in-memory job handle. The durable status
        // endpoint is sufficient for it to resume progress polling.
        let resumed: Option<TrackingSyncJobDto> = server
            .get(&format!("/remux/tracking/addons/{addon_id}/sync/status"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .await
            .json();
        assert_eq!(resumed.map(|job| job.id), Some(first.id));

        PENDING_PINS.remove(&pin_start.poll_token);
    }

    #[tokio::test]
    async fn verification_failure_returns_non_success_and_persists_provider_error() {
        let provider = httpmock::MockServer::start();
        let settings = provider.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/users/settings")
                .json_body(serde_json::json!({}));
            then.status(401)
                .json_body(serde_json::json!({ "message": "invalid test token" }));
        });
        let (server, guard, token, addon_id) = mocked_tracking_server(&provider).await;
        let user = db::User::get_by_username(
            &guard
                .0
                .db,
            "test",
        )
        .await
        .unwrap()
        .unwrap();
        let credentials = seal_credentials(
            &TrackingCredentials::new(
                serde_json::json!({ "access_token": "bad-token" }),
            ),
            &guard
                .0
                .config,
        )
        .unwrap();
        let connection =
            db::UserMediaTracker::new(user.id, addon_id, credentials, vec![]);
        connection
            .upsert(
                &guard
                    .0
                    .db,
            )
            .await
            .unwrap();

        let response = server
            .post(&format!("/remux/tracking/addons/{addon_id}/verify"))
            .add_header(http::header::AUTHORIZATION, auth_value(&token))
            .expect_failure()
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
        assert!(
            response
                .text()
                .contains("Simkl rejected the access token")
        );
        settings.assert_hits(1);
        let saved = db::UserMediaTracker::get_for_user_and_addon(
            &guard
                .0
                .db,
            user.id,
            addon_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(saved.status, db::MediaTrackerStatus::AuthExpired);
        assert!(
            saved
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("Simkl rejected the access token"))
        );
    }
}
