use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use axum_anyhow::ApiResult as Result;
use dashmap::DashMap;
use remux_macros::{delete, get, post};
use remux_sdks::tracking::{
    TrackingConnectionDto, TrackingFiltersRequest, TrackingPinPollDto,
    TrackingPinPollRequest, TrackingPinStartDto, TrackingPinStatus,
    TrackingSyncResultDto,
};
use std::{
    str::FromStr,
    sync::LazyLock,
    time::{Duration, Instant},
};
use tracing::warn;
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
    provider_token: String,
    interval: Duration,
    next_poll_at: Instant,
    expires_at: Instant,
}

static PENDING_PINS: LazyLock<DashMap<String, PendingPin>> =
    LazyLock::new(DashMap::new);
static SYNC_LOCKS: LazyLock<DashMap<Uuid, std::sync::Arc<tokio::sync::Mutex<()>>>> =
    LazyLock::new(DashMap::new);

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
    let (pending_events, failed_events) = if let Some(connection) = connection {
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
        (pending as usize, failed as usize)
    } else {
        (0, 0)
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
        last_error_at: connection.and_then(|connection| connection.last_error_at),
        last_error: connection.and_then(|connection| {
            connection
                .last_error
                .clone()
        }),
        pending_events,
        failed_events,
    })
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
            provider_token: started.poll_token,
            interval: started.interval,
            next_poll_at: now + started.interval,
            expires_at: now + started.expires_in,
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
    let pending = {
        let Some(mut pending) = PENDING_PINS.get_mut(&payload.poll_token) else {
            return Err(anyhow::anyhow!("unknown PIN session")
                .context_bad_request("Unknown or expired PIN session"));
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
            drop(pending);
            PENDING_PINS.remove(&payload.poll_token);
            return Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Denied,
                connection: None,
            }));
        }
        if now < pending.next_poll_at {
            return Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Pending,
                connection: None,
            }));
        }
        pending.next_poll_at = now + pending.interval;
        pending.clone()
    };

    let runtime = runtime_for(&state, addon_id)?;
    let provider = runtime
        .tracking
        .as_ref()
        .unwrap();
    let polled = provider
        .poll_device_auth(&pending.provider_token, &tracking_context(&state.ctx))
        .await
        .map_err(tracking_api_error)?;
    match polled {
        DeviceAuthPoll::Pending => Ok(Json(TrackingPinPollDto {
            status: TrackingPinStatus::Pending,
            connection: None,
        })),
        DeviceAuthPoll::Denied => {
            PENDING_PINS.remove(&payload.poll_token);
            Ok(Json(TrackingPinPollDto {
                status: TrackingPinStatus::Denied,
                connection: None,
            }))
        }
        DeviceAuthPoll::Approved(credentials) => {
            provider
                .verify(&credentials, &tracking_context(&state.ctx))
                .await
                .map_err(tracking_api_error)?;
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
            PENDING_PINS.remove(&payload.poll_token);

            let sync_ctx = state
                .ctx
                .clone();
            let sync_user = session
                .user
                .clone();
            let sync_connection = connection.clone();
            tokio::spawn(async move {
                if let Err(error) = sync_connection_from_provider(
                    &sync_ctx,
                    &sync_user,
                    &sync_connection,
                )
                .await
                {
                    if let Err(mark_error) = db::UserMediaTracker::mark_failure(
                        &sync_ctx.db,
                        sync_connection.id,
                        &error,
                    )
                    .await
                    {
                        warn!(
                            addon_id = %sync_connection.addon_id,
                            error = %mark_error,
                            "failed to record initial tracking import error"
                        );
                    }
                    warn!(
                        addon_id = %sync_connection.addon_id,
                        user_id = %sync_user.id,
                        error = %error,
                        "initial tracking history import failed"
                    );
                }
            });
            if requeued > 0 {
                if let Err(error) = state
                    .tasks
                    .run_task("MediaTrackerSync")
                    .await
                {
                    warn!(error = %error, "failed to wake reconnected tracker queue");
                }
            }

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
            db::UserMediaTracker::mark_success(
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
) -> Result<Json<TrackingSyncResultDto>> {
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
    match sync_connection_from_provider(&state.ctx, &session.user, &connection).await {
        Ok(result) => Ok(Json(result)),
        Err(error) => {
            db::UserMediaTracker::mark_failure(
                &state
                    .ctx
                    .db,
                connection.id,
                &error,
            )
            .await?;
            Err(tracking_api_error(error))
        }
    }
}

/// Pull provider state into local user data. Local writes here bypass the
/// HTTP mutation handlers, so they do not echo back into the outbound outbox.
pub(crate) async fn sync_connection_from_provider(
    ctx: &AppContext,
    user: &db::User,
    connection: &db::UserMediaTracker,
) -> std::result::Result<TrackingSyncResultDto, TrackingError> {
    let sync_lock = SYNC_LOCKS
        .entry(connection.id)
        .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _sync_guard = sync_lock
        .lock()
        .await;
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
        ..Default::default()
    };
    let server_config = db::Settings::get_config_or_default(&ctx.db).await;
    for change in remote.items {
        let Some(media) = find_remote_media(&ctx.db, &change)
            .await
            .map_err(|error| {
                TrackingError::retryable(format!("matching library item: {error}"))
            })?
        else {
            continue;
        };
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
                                    .round() as i64
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
                TrackingError::retryable(format!("applying playback progress: {error}"))
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
                TrackingError::retryable(format!("applying favorite state: {error}"))
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
) -> anyhow::Result<Option<db::Media>> {
    if change
        .ids
        .is_empty()
    {
        return Ok(None);
    }
    if change.kind == db::MediaKind::Episode {
        if let Some(direct) =
            find_by_ids(db_pool, db::MediaKind::Episode, &change.ids).await?
        {
            if change
                .episode
                .is_none()
                || direct.idx == change.episode
            {
                return Ok(Some(direct));
            }
        }
        let Some(series) =
            find_by_ids(db_pool, db::MediaKind::Series, &change.ids).await?
        else {
            return Ok(None);
        };
        let Some(season) = change.season else {
            return Ok(None);
        };
        let Some(episode) = change.episode else {
            return Ok(None);
        };
        return Ok(sqlx::query_as::<_, db::Media>(
            "SELECT * FROM media WHERE kind = 'episode' AND parent_idx = ?2 AND idx = ?3 \
             AND (grandparent_id = ?1 OR parent_id IN (\
                 SELECT id FROM media WHERE kind = 'season' AND parent_id = ?1\
             )) LIMIT 1",
        )
        .bind(series.id)
        .bind(season)
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
    Ok(sqlx::query_as::<_, db::Media>(
        "SELECT * FROM media WHERE kind = ?1 AND (\
             (?2 IS NOT NULL AND json_extract(external_ids, '$.imdb') = ?2) OR \
             (?3 IS NOT NULL AND CAST(json_extract(external_ids, '$.tmdb') AS INTEGER) = ?3) OR \
             (?4 IS NOT NULL AND CAST(json_extract(external_ids, '$.tvdb') AS INTEGER) = ?4)\
         ) ORDER BY \
             CASE WHEN ?2 IS NOT NULL AND json_extract(external_ids, '$.imdb') = ?2 \
                  THEN 0 ELSE 1 END \
         LIMIT 1",
    )
    .bind(kind)
    .bind(&ids.imdb)
    .bind(ids.tmdb)
    .bind(ids.tvdb)
    .fetch_optional(db_pool)
    .await?)
}

fn tracking_api_error(error: TrackingError) -> axum_anyhow::ApiError {
    let retryable = error.is_retryable();
    let error = anyhow::anyhow!(error);
    if retryable {
        error.context_bad_gateway("Tracking provider is temporarily unavailable")
    } else {
        error.context_bad_request("Tracking provider rejected the request")
    }
}
