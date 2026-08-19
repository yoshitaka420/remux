use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use dashmap::DashMap;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

use super::{
    AddonCapabilities, AddonKind, AddonMetadata, AddonOption, AddonOptionType,
    AddonPreset, AddonPresetRegistration, MediaKind, ResourceType,
    tracking::{
        AuthFlow, DeviceAuthPoll, DeviceAuthStart, RemoteSync, RemoteWatch,
        SyncDirection, TrackingAddon, TrackingCapabilities, TrackingCredentials,
        TrackingCtx, TrackingError, TrackingEvent, TrackingEventKind, TrackingIds,
        TrackingResult, TrackingTarget,
    },
};
use crate::sdks::{self, simkl as api};

const WRITE_INTERVAL: Duration = Duration::from_secs(1);
const EMPTY_ACTIVITY_CURSOR: &str = "1970-01-01T00:00:00Z";

pub struct SimklPreset;

impl AddonPreset for SimklPreset {
    fn id(&self) -> &'static str {
        "simkl"
    }

    fn metadata(&self) -> AddonMetadata {
        AddonMetadata {
            id: "simkl".to_string(),
            display_name: "Simkl".to_string(),
            description: "Sync playback, watched history, and ratings with Simkl."
                .to_string(),
            icon: None,
            supported_resources: vec![AddonMetadata::simple_resource(ResourceType::Tracking)],
            supported_types: vec![
                MediaKind::Movie,
                MediaKind::Series,
                MediaKind::Season,
                MediaKind::Episode,
            ],
            // The addon instance (and its Simkl client ID) is global; each
            // Remux user authorizes their own account on Integrations.
            supported_resources_user: vec![],
            supported_types_user: vec![],
            options: vec![AddonOption {
                id: "client_id".to_string(),
                name: "Simkl Client ID".to_string(),
                description: Some(
                    "Create an application at simkl.com/settings/developer/ and paste its Client ID."
                        .to_string(),
                ),
                required: true,
                default: None,
                kind: AddonOptionType::String,
            }],
        }
    }

    fn from_cfg(
        &self,
        _addon_id: Uuid,
        cfg: &serde_json::Value,
        config: &crate::Config,
    ) -> Result<AddonCapabilities> {
        let client_id = cfg
            .get("client_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("Simkl Client ID is required"))?
            .to_string();
        let addon = Arc::new(SimklAddon {
            client_id,
            base_url: config
                .simkl_base_url
                .clone(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            connect_timeout: Duration::from_secs(
                config
                    .simkl_connect_timeout_seconds
                    .max(1),
            ),
            request_timeout: Duration::from_secs(
                config
                    .simkl_request_timeout_seconds
                    .max(1),
            ),
            last_write: Mutex::new(None),
            user_requests: DashMap::new(),
        });
        Ok(AddonCapabilities {
            kind: Some(addon.clone()),
            tracking: Some(addon),
            ..Default::default()
        })
    }
}

inventory::submit! {
    AddonPresetRegistration(|| Box::new(SimklPreset))
}

pub struct SimklAddon {
    client_id: String,
    base_url: String,
    app_version: String,
    connect_timeout: Duration,
    request_timeout: Duration,
    /// Simkl documents one POST per second. The same addon instance serves all
    /// user connections, so serialising here is conservative and predictable.
    last_write: Mutex<Option<tokio::time::Instant>>,
    /// Simkl asks clients to keep user-state calls sequential per access token.
    /// This covers an inbound sync racing a scrobble for the same account while
    /// still allowing independent users to make GET requests concurrently.
    user_requests: DashMap<u64, Arc<Mutex<()>>>,
}

impl SimklAddon {
    fn client(
        &self,
        access_token: Option<&str>,
    ) -> TrackingResult<sdks::RestClient<api::SimklAuth>> {
        api::client_with_timeouts(
            &self.client_id,
            access_token,
            &self.base_url,
            &self.app_version,
            self.connect_timeout,
            self.request_timeout,
        )
        .map_err(|error| {
            TrackingError::permanent(format!("invalid Simkl API URL: {error}"))
        })
    }

    fn token<'a>(
        &self,
        credentials: &'a TrackingCredentials,
    ) -> TrackingResult<&'a str> {
        credentials
            .get_str("access_token")
            .ok_or_else(|| TrackingError::reauth("Simkl access token is missing"))
    }

    async fn lock_user_request(&self, token: &str) -> OwnedMutexGuard<()> {
        let mut hasher = DefaultHasher::new();
        token.hash(&mut hasher);
        let lock = self
            .user_requests
            .entry(hasher.finish())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned()
            .await
    }

    async fn wait_for_write(&self) {
        let mut last = self
            .last_write
            .lock()
            .await;
        if let Some(instant) = *last {
            let ready_at = instant + WRITE_INTERVAL;
            if ready_at > tokio::time::Instant::now() {
                tokio::time::sleep_until(ready_at).await;
            }
        }
        *last = Some(tokio::time::Instant::now());
    }

    async fn execute_write<E: sdks::Endpoint + Clone>(
        &self,
        client: &sdks::RestClient<api::SimklAuth>,
        endpoint: E,
    ) -> TrackingResult<E::Output> {
        self.wait_for_write()
            .await;
        client
            .execute(endpoint)
            .await
            .map_err(map_client_error)
    }

    async fn execute_scrobble(
        &self,
        client: &sdks::RestClient<api::SimklAuth>,
        endpoint: api::ScrobbleEndpoint,
    ) -> TrackingResult<serde_json::Value> {
        let duplicate_stop = matches!(endpoint.action, api::ScrobbleAction::Stop);
        self.wait_for_write()
            .await;
        match client
            .execute(endpoint)
            .await
        {
            Ok(response) => Ok(response),
            // A repeated stop is idempotent; Simkl documents it as 409 for one
            // hour after the original session was finalized.
            Err(sdks::ClientError::Http { status: 409, .. }) if duplicate_stop => {
                Ok(serde_json::Value::Null)
            }
            Err(error) => Err(map_client_error(error)),
        }
    }

    fn history_body(
        target: &TrackingTarget,
        watched_at: Option<String>,
    ) -> TrackingResult<api::SyncWriteBody> {
        let mut body = api::SyncWriteBody::default();
        match target
            .kind
            .clone()
        {
            crate::db::MediaKind::Movie => body
                .movies
                .push(api::SyncItem {
                    title: Some(
                        target
                            .title
                            .clone(),
                    ),
                    year: target.year,
                    ids: api_ids(&target.ids),
                    watched_at,
                    ..Default::default()
                }),
            crate::db::MediaKind::Series => body
                .shows
                .push(api::SyncItem {
                    title: Some(
                        target
                            .title
                            .clone(),
                    ),
                    year: target.year,
                    ids: api_ids(&target.ids),
                    watched_at,
                    status: Some("completed".to_string()),
                    use_tvdb_anime_seasons: Some(true),
                    ..Default::default()
                }),
            crate::db::MediaKind::Season => {
                let series = target
                    .series
                    .as_deref()
                    .ok_or_else(|| {
                        TrackingError::permanent("season has no series target")
                    })?;
                let season = target
                    .season
                    .ok_or_else(|| {
                        TrackingError::permanent("season has no season number")
                    })?;
                body.shows
                    .push(api::SyncItem {
                        title: Some(
                            series
                                .title
                                .clone(),
                        ),
                        year: series.year,
                        ids: api_ids(&series.ids),
                        use_tvdb_anime_seasons: Some(true),
                        seasons: vec![api::SeasonRef {
                            number: season,
                            episodes: Vec::new(),
                        }],
                        ..Default::default()
                    });
            }
            crate::db::MediaKind::Episode => {
                let series = target
                    .series
                    .as_deref()
                    .ok_or_else(|| {
                        TrackingError::permanent("episode has no series target")
                    })?;
                let season = target
                    .season
                    .ok_or_else(|| {
                        TrackingError::permanent("episode has no season number")
                    })?;
                let episode = target
                    .episode
                    .ok_or_else(|| {
                        TrackingError::permanent("episode has no episode number")
                    })?;
                body.shows
                    .push(api::SyncItem {
                        title: Some(
                            series
                                .title
                                .clone(),
                        ),
                        year: series.year,
                        ids: api_ids(&series.ids),
                        use_tvdb_anime_seasons: Some(true),
                        seasons: vec![api::SeasonRef {
                            number: season,
                            episodes: vec![api::EpisodeRef {
                                season: None,
                                number: Some(episode),
                                title: None,
                                watched_at,
                                ids: episode_api_ids(&target.ids),
                                tvdb: None,
                                tvdb_season: None,
                                tvdb_number: None,
                            }],
                        }],
                        ..Default::default()
                    });
            }
            _ => return Err(TrackingError::unsupported("history for this media type")),
        }
        Ok(body)
    }

    fn rating_body(
        target: &TrackingTarget,
        rating: Option<i32>,
    ) -> TrackingResult<api::SyncWriteBody> {
        let item = api::SyncItem {
            title: Some(
                target
                    .title
                    .clone(),
            ),
            year: target.year,
            ids: api_ids(&target.ids),
            rating,
            ..Default::default()
        };
        let mut body = api::SyncWriteBody::default();
        match target
            .kind
            .clone()
        {
            crate::db::MediaKind::Movie => body
                .movies
                .push(item),
            crate::db::MediaKind::Series => body
                .shows
                .push(item),
            _ => return Err(TrackingError::unsupported("ratings for this media type")),
        }
        Ok(body)
    }

    async fn fetch_remote(
        &self,
        since: Option<String>,
        credentials: &TrackingCredentials,
    ) -> TrackingResult<RemoteSync> {
        let token = self.token(credentials)?;
        let _request_guard = self
            .lock_user_request(token)
            .await;
        let client = self.client(Some(token))?;
        let incremental = since.is_some();
        let mut payload_bytes = 0usize;

        // Simkl requires every incremental sync to check the cheap activities
        // endpoint first and to reuse its exact watermark on the next pull.
        let incremental_cursor = if let Some(previous) = since.as_deref() {
            let (activities, response_bytes) = client
                .execute_with_response_size(api::ActivitiesEndpoint)
                .await
                .map_err(map_client_error)?;
            payload_bytes = payload_bytes.saturating_add(response_bytes);
            let current = activities_cursor(&activities);
            if current == previous {
                return Ok(RemoteSync {
                    items: Vec::new(),
                    cursor: current,
                    payload_bytes,
                });
            }
            Some(current)
        } else {
            None
        };

        let date_from = since;
        let params = api::AllItemsParams {
            // Remux uses TVDB/TMDB-style seasons for every series. This is a
            // superset of `full` that adds Simkl's anime-to-TVDB coordinates.
            extended: Some("full_anime_seasons".to_string()),
            date_from: date_from.clone(),
            episode_watched_at: Some("yes".to_string()),
            include_all_episodes: Some("yes".to_string()),
        };
        let items = if incremental {
            // Continuous multi-type sync is one delta request. Simkl requires
            // the exact prior activities watermark in `date_from`.
            let (items, response_bytes) = client
                .execute_with_response_size(api::AllItemsEndpoint {
                    media_type: None,
                    status: None,
                    params,
                })
                .await
                .map_err(map_client_error)?;
            payload_bytes = payload_bytes.saturating_add(response_bytes);
            items
        } else {
            // Simkl's API rules require a multi-type baseline to fetch these
            // large payloads sequentially rather than as one combined burst.
            let mut combined = api::AllItemsResponse::default();
            for media_type in ["shows", "movies", "anime"] {
                let (mut response, response_bytes) = client
                    .execute_with_response_size(api::AllItemsEndpoint {
                        media_type: Some(media_type.to_string()),
                        status: None,
                        params: params.clone(),
                    })
                    .await
                    .map_err(map_client_error)?;
                payload_bytes = payload_bytes.saturating_add(response_bytes);
                combined
                    .shows
                    .append(&mut response.shows);
                combined
                    .movies
                    .append(&mut response.movies);
                combined
                    .anime
                    .append(&mut response.anime);
            }
            combined
        };
        let (playback, response_bytes) = client
            .execute_with_response_size(api::PlaybackEndpoint {
                media_type: None,
                params: api::PlaybackParams {
                    date_from,
                    limit: Some(10_000),
                },
            })
            .await
            .map_err(map_client_error)?;
        payload_bytes = payload_bytes.saturating_add(response_bytes);

        // The initial full pull takes its watermark afterwards, matching the
        // two-phase sync sequence in Simkl's integration guide.
        let cursor = match incremental_cursor {
            Some(cursor) => cursor,
            None => {
                let (activities, response_bytes) = client
                    .execute_with_response_size(api::ActivitiesEndpoint)
                    .await
                    .map_err(map_client_error)?;
                payload_bytes = payload_bytes.saturating_add(response_bytes);
                activities_cursor(&activities)
            }
        };

        let mut remote = Vec::new();
        let mut movie_entries = items.movies;
        let mut series_entries = items
            .shows
            .into_iter()
            .map(|entry| (entry, false))
            .collect::<Vec<_>>();
        for entry in items.anime {
            if entry
                .movie
                .is_some()
            {
                movie_entries.push(entry);
            } else {
                series_entries.push((entry, true));
            }
        }

        for entry in movie_entries {
            let Some(movie) = entry
                .movie
                .as_ref()
            else {
                continue;
            };
            remote.push(RemoteWatch {
                kind: crate::db::MediaKind::Movie,
                ids: tracking_ids(&movie.ids),
                season: None,
                episode: None,
                watched: if incremental {
                    Some(
                        entry
                            .last_watched_at
                            .is_some()
                            || entry
                                .status
                                .as_deref()
                                == Some("completed"),
                    )
                } else {
                    (entry
                        .last_watched_at
                        .is_some()
                        || entry
                            .status
                            .as_deref()
                            == Some("completed"))
                    .then_some(true)
                },
                position_ticks: None,
                position_percent: None,
                watched_at: entry
                    .last_watched_at
                    .as_deref()
                    .and_then(parse_datetime),
                favorite: None,
                // The primary provider is authoritative. An explicit null in
                // Simkl's full or delta row clears a stale local rating.
                rating: Some(
                    entry
                        .user_rating
                        .map(|value| value as f32),
                ),
            });
        }
        for (entry, is_anime) in series_entries {
            let Some(show) = entry
                .show
                .as_ref()
                .or(entry
                    .anime
                    .as_ref())
            else {
                continue;
            };
            let ids = tracking_ids(&show.ids);
            // Clearing a series first, then replaying its returned episode
            // rows, also handles a remote episode being marked unwatched.
            // One TVDB series can map to several Simkl anime cour records.
            // Never clear or complete the whole local series from one cour;
            // the synthesized per-episode rows below are the authoritative
            // TVDB-shaped baseline.
            if !is_anime {
                remote.push(RemoteWatch {
                    kind: crate::db::MediaKind::Series,
                    ids: ids.clone(),
                    season: None,
                    episode: None,
                    watched: if incremental {
                        Some(
                            entry
                                .status
                                .as_deref()
                                == Some("completed"),
                        )
                    } else {
                        (entry
                            .status
                            .as_deref()
                            == Some("completed"))
                        .then_some(true)
                    },
                    position_ticks: None,
                    position_percent: None,
                    watched_at: entry
                        .last_watched_at
                        .as_deref()
                        .and_then(parse_datetime),
                    favorite: None,
                    rating: Some(
                        entry
                            .user_rating
                            .map(|value| value as f32),
                    ),
                });
            } else {
                // Anime episode rows carry the granular watch history, but the
                // user's score belongs to the AniList/Simkl title itself. Keep a
                // separate series-level assertion so ratings are not discarded.
                remote.push(RemoteWatch {
                    kind: crate::db::MediaKind::Series,
                    ids: ids.clone(),
                    season: None,
                    episode: None,
                    watched: None,
                    position_ticks: None,
                    position_percent: None,
                    watched_at: None,
                    favorite: None,
                    rating: Some(
                        entry
                            .user_rating
                            .map(|value| value as f32),
                    ),
                });
            }
            for season in entry.seasons {
                for episode in season.episodes {
                    let Some((mapped_season, mapped_episode)) =
                        episode_coordinates(is_anime, &episode, season.number)
                    else {
                        continue;
                    };
                    remote.push(RemoteWatch {
                        kind: crate::db::MediaKind::Episode,
                        ids: ids.clone(),
                        season: Some(mapped_season),
                        episode: Some(mapped_episode),
                        watched: Some(true),
                        position_ticks: None,
                        position_percent: None,
                        watched_at: episode
                            .watched_at
                            .as_deref()
                            .and_then(parse_datetime)
                            .or_else(|| {
                                entry
                                    .last_watched_at
                                    .as_deref()
                                    .and_then(parse_datetime)
                            }),
                        favorite: None,
                        rating: None,
                    });
                }
            }
        }
        for session in playback {
            let (media, season, episode) = match (
                session
                    .movie
                    .as_ref(),
                session
                    .show
                    .as_ref()
                    .or(session
                        .anime
                        .as_ref()),
                session
                    .episode
                    .as_ref(),
            ) {
                (Some(movie), _, _) => (movie, None, None),
                (_, Some(show), Some(episode)) => {
                    let coordinates = episode
                        .tvdb_season
                        .zip(episode.tvdb_number);
                    (
                        show,
                        coordinates
                            .map(|(season, _)| season)
                            .or(episode.season),
                        coordinates
                            .map(|(_, episode)| episode)
                            .or(episode.number),
                    )
                }
                _ => continue,
            };
            remote.push(RemoteWatch {
                kind: if episode.is_some() {
                    crate::db::MediaKind::Episode
                } else {
                    crate::db::MediaKind::Movie
                },
                ids: tracking_ids(&media.ids),
                season,
                episode,
                watched: None,
                position_ticks: None,
                position_percent: session.progress,
                watched_at: session
                    .watched_at
                    .as_deref()
                    .and_then(parse_datetime),
                favorite: None,
                rating: None,
            });
        }
        Ok(RemoteSync {
            items: remote,
            cursor,
            payload_bytes,
        })
    }
}

#[async_trait]
impl AddonKind for SimklAddon {
    fn id(&self) -> &'static str {
        "simkl"
    }
}

#[async_trait]
impl TrackingAddon for SimklAddon {
    fn capabilities(&self) -> TrackingCapabilities {
        let supported_events = vec![
            TrackingEventKind::PlaybackStart,
            TrackingEventKind::PlaybackProgress,
            TrackingEventKind::PlaybackStop,
            TrackingEventKind::MarkPlayed,
            TrackingEventKind::MarkUnplayed,
            TrackingEventKind::Rating,
        ];
        TrackingCapabilities {
            auth_flow: AuthFlow::OAuthDeviceCode,
            supported_events: supported_events.clone(),
            default_event_filter: supported_events,
            history_import: true,
            progress_import: true,
            watch_state_sync: SyncDirection::Both,
            ratings: SyncDirection::Both,
            ..Default::default()
        }
    }

    fn supports_event(&self, event: &TrackingEvent, target: &TrackingTarget) -> bool {
        if !self
            .capabilities()
            .supports(event.kind())
        {
            return false;
        }
        if matches!(
            target.kind,
            crate::db::MediaKind::Season | crate::db::MediaKind::Episode
        ) && target
            .series
            .as_deref()
            .map_or(true, |series| {
                series
                    .ids
                    .is_empty()
            })
        {
            return false;
        }
        match event {
            TrackingEvent::Rating { .. } => matches!(
                target
                    .kind
                    .clone(),
                crate::db::MediaKind::Movie | crate::db::MediaKind::Series
            ),
            TrackingEvent::PlaybackStart { .. }
            | TrackingEvent::PlaybackProgress { .. }
            | TrackingEvent::PlaybackStop { .. } => matches!(
                target
                    .kind
                    .clone(),
                crate::db::MediaKind::Movie | crate::db::MediaKind::Episode
            ),
            _ => true,
        }
    }

    async fn begin_device_auth(
        &self,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<DeviceAuthStart> {
        let response = self
            .client(None)?
            .execute(api::BeginPinEndpoint)
            .await
            .map_err(map_client_error)?;
        let verification_url = response
            .verification_url()
            .ok_or_else(|| {
                TrackingError::permanent("Simkl PIN response omitted its URL")
            })?
            .to_string();
        Ok(DeviceAuthStart {
            verification_url,
            user_code: response
                .user_code
                .clone(),
            poll_token: response.user_code,
            interval: Duration::from_secs(
                response
                    .interval
                    .max(1),
            ),
            expires_in: Duration::from_secs(
                response
                    .expires_in
                    .max(1),
            ),
        })
    }

    async fn poll_device_auth(
        &self,
        poll_token: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<DeviceAuthPoll> {
        let response = self
            .client(None)?
            .execute(api::PollPinEndpoint {
                user_code: poll_token.to_string(),
            })
            .await
            .map_err(map_client_error)?;
        Ok(device_auth_poll(response))
    }

    async fn verify(
        &self,
        credentials: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        let token = self.token(credentials)?;
        let _request_guard = self
            .lock_user_request(token)
            .await;
        let client = self.client(Some(token))?;
        self.execute_write(&client, api::UserSettingsEndpoint)
            .await
            .map(|_| ())
    }

    async fn on_event(
        &self,
        event: &TrackingEvent,
        target: &TrackingTarget,
        credentials: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        let token = self.token(credentials)?;
        let _request_guard = self
            .lock_user_request(token)
            .await;
        let client = self.client(Some(token))?;
        match event {
            TrackingEvent::PlaybackStart { position_ticks } => {
                let body = scrobble_body(target, *position_ticks, false)?;
                self.execute_scrobble(
                    &client,
                    api::ScrobbleEndpoint {
                        action: api::ScrobbleAction::Start,
                        body,
                    },
                )
                .await?;
            }
            TrackingEvent::PlaybackProgress {
                position_ticks,
                is_paused,
            } => {
                let body = scrobble_body(target, *position_ticks, false)?;
                self.execute_scrobble(
                    &client,
                    api::ScrobbleEndpoint {
                        action: if *is_paused {
                            api::ScrobbleAction::Pause
                        } else {
                            api::ScrobbleAction::Start
                        },
                        body,
                    },
                )
                .await?;
            }
            TrackingEvent::PlaybackStop {
                position_ticks,
                played,
            } => {
                let body = scrobble_body(target, *position_ticks, *played)?;
                self.execute_scrobble(
                    &client,
                    api::ScrobbleEndpoint {
                        // Simkl treats stop at >=80% as watched. A local abandon
                        // is therefore a pause, preserving Remux's own decision.
                        action: if *played {
                            api::ScrobbleAction::Stop
                        } else {
                            api::ScrobbleAction::Pause
                        },
                        body,
                    },
                )
                .await?;
            }
            TrackingEvent::MarkPlayed | TrackingEvent::MarkUnplayed => {
                let body = Self::history_body(
                    target,
                    matches!(event, TrackingEvent::MarkPlayed)
                        .then(|| Utc::now().to_rfc3339()),
                )?;
                let response = self
                    .execute_write(
                        &client,
                        api::SyncWriteEndpoint {
                            action: if matches!(event, TrackingEvent::MarkPlayed) {
                                api::SyncWriteAction::AddHistory
                            } else {
                                api::SyncWriteAction::RemoveHistory
                            },
                            body,
                        },
                    )
                    .await?;
                ensure_sync_write_accepted(&response)?;
            }
            TrackingEvent::Rating { rating } => {
                let value = simkl_rating(*rating)?;
                let body = Self::rating_body(target, value)?;
                let response = self
                    .execute_write(
                        &client,
                        api::SyncWriteEndpoint {
                            action: if value.is_some() {
                                api::SyncWriteAction::AddRatings
                            } else {
                                api::SyncWriteAction::RemoveRatings
                            },
                            body,
                        },
                    )
                    .await?;
                ensure_sync_write_accepted(&response)?;
            }
            TrackingEvent::Favorite { .. } => {
                return Err(TrackingError::unsupported("favorites"));
            }
        }
        Ok(())
    }

    async fn import_history(
        &self,
        credentials: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RemoteSync> {
        self.fetch_remote(None, credentials)
            .await
    }

    async fn pull_changes(
        &self,
        since: Option<String>,
        credentials: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RemoteSync> {
        self.fetch_remote(since, credentials)
            .await
    }
}

fn device_auth_poll(response: api::PinPollResponse) -> DeviceAuthPoll {
    if let Some(access_token) = response
        .access_token
        .filter(|token| !token.is_empty())
    {
        return DeviceAuthPoll::Approved(TrackingCredentials::new(
            serde_json::json!({ "access_token": access_token }),
        ));
    }
    // Polling a deleted or unknown code creates a fresh PIN-shaped
    // response. It is not authorization pending for the original code.
    if response
        .device_code
        .is_some()
    {
        return DeviceAuthPoll::Denied;
    }
    let message = response
        .message
        .unwrap_or_default()
        .to_ascii_lowercase();
    if message.contains("expired")
        || message.contains("denied")
        || message.contains("cancel")
    {
        DeviceAuthPoll::Denied
    } else {
        DeviceAuthPoll::Pending
    }
}

fn simkl_rating(rating: Option<f32>) -> TrackingResult<Option<i32>> {
    let Some(rating) = rating else {
        return Ok(None);
    };
    if !rating.is_finite() {
        return Err(TrackingError::permanent(
            "Simkl cannot store a non-finite rating",
        ));
    }
    if !(0.0..=10.0).contains(&rating) {
        return Err(TrackingError::permanent(format!(
            "Remux ratings must be between 0 and 10 (got {rating})"
        )));
    }
    if rating == 0.0 {
        return Ok(None);
    }
    Ok(Some((rating.round() as i32).clamp(1, 10)))
}

/// Simkl deliberately returns 2xx for sync items it could not resolve (and
/// for out-of-range ratings). The authoritative failure is the response's
/// `not_found` object, so treating every 2xx as delivered would silently lose
/// user activity.
fn ensure_sync_write_accepted(response: &serde_json::Value) -> TrackingResult<()> {
    let rejected = response
        .get("not_found")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|groups| {
            groups
                .values()
                .any(|items| {
                    items
                        .as_array()
                        .is_some_and(|items| !items.is_empty())
                })
        });
    if rejected {
        Err(TrackingError::permanent(
            "Simkl could not match the media item in this tracking event",
        ))
    } else {
        Ok(())
    }
}

fn activities_cursor(activities: &api::Activities) -> String {
    activities
        .all
        .clone()
        .unwrap_or_else(|| EMPTY_ACTIVITY_CURSOR.to_string())
}

fn api_ids(ids: &TrackingIds) -> api::Ids {
    api::Ids {
        imdb: ids
            .imdb
            .clone(),
        tmdb: ids
            .tmdb
            .map(api::FlexibleId::Number),
        tvdb: ids
            .tvdb
            .map(api::FlexibleId::Number),
        kitsu: ids
            .kitsu
            .map(api::FlexibleId::Number),
        mal: ids
            .mal
            .map(api::FlexibleId::Number),
        anilist: ids
            .anilist
            .map(api::FlexibleId::Number),
        ..Default::default()
    }
}

/// Simkl accepts TVDB/AniDB episode IDs for scrobbling. Remux currently stores
/// TVDB, IMDb, and TMDB; sending an IMDb/TMDB episode ID would take precedence
/// over season/number even though Simkl cannot resolve it.
fn episode_api_ids(ids: &TrackingIds) -> Option<api::Ids> {
    ids.tvdb
        .map(|tvdb| api::Ids {
            tvdb: Some(api::FlexibleId::Number(tvdb)),
            ..Default::default()
        })
}

fn tracking_ids(ids: &api::Ids) -> TrackingIds {
    TrackingIds {
        imdb: ids
            .imdb
            .clone(),
        tmdb: ids
            .tmdb
            .as_ref()
            .and_then(api::FlexibleId::as_i64),
        tvdb: ids
            .tvdb
            .as_ref()
            .and_then(api::FlexibleId::as_i64),
        kitsu: ids
            .kitsu
            .as_ref()
            .and_then(api::FlexibleId::as_i64),
        mal: ids
            .mal
            .as_ref()
            .and_then(api::FlexibleId::as_i64),
        anilist: ids
            .anilist
            .as_ref()
            .and_then(api::FlexibleId::as_i64),
    }
}

fn episode_coordinates(
    is_anime: bool,
    episode: &api::EpisodeRef,
    containing_season: i64,
) -> Option<(i64, i64)> {
    if is_anime {
        // Falling back to Simkl's native anime numbering would silently update
        // the wrong TVDB episode for absolute-numbered and split-cour shows.
        episode
            .tvdb
            .as_ref()
            .map(|coordinate| (coordinate.season, coordinate.episode))
    } else {
        Some((
            episode
                .season
                .unwrap_or(containing_season),
            episode.number?,
        ))
    }
}

fn media_ref(target: &TrackingTarget) -> api::MediaRef {
    api::MediaRef {
        title: Some(
            target
                .title
                .clone(),
        ),
        year: target.year,
        ids: api_ids(&target.ids),
        // Runtime appears in extended sync responses but is not part of a
        // scrobble request's media reference.
        runtime: None,
    }
}

fn progress(
    position_ticks: i64,
    runtime_ticks: Option<i64>,
    force_watched: bool,
) -> f64 {
    let computed = runtime_ticks
        .filter(|runtime| *runtime > 0)
        .map(|runtime| position_ticks.max(0) as f64 / runtime as f64 * 100.0)
        .unwrap_or(0.0)
        .clamp(0.0, 100.0);
    if force_watched {
        computed.max(80.0)
    } else {
        computed
    }
}

fn scrobble_body(
    target: &TrackingTarget,
    position_ticks: i64,
    force_watched: bool,
) -> TrackingResult<api::ScrobbleBody> {
    let mut body = api::ScrobbleBody {
        progress: progress(position_ticks, target.runtime_ticks, force_watched),
        ..Default::default()
    };
    match target
        .kind
        .clone()
    {
        crate::db::MediaKind::Movie => body.movie = Some(media_ref(target)),
        crate::db::MediaKind::Episode => {
            let series = target
                .series
                .as_deref()
                .ok_or_else(|| {
                    TrackingError::permanent("episode has no series target")
                })?;
            body.show = Some(media_ref(series));
            body.episode = Some(api::EpisodeRef {
                season: target.season,
                number: target.episode,
                title: None,
                watched_at: None,
                ids: episode_api_ids(&target.ids),
                tvdb: None,
                tvdb_season: None,
                tvdb_number: None,
            });
        }
        _ => return Err(TrackingError::unsupported("scrobbling for this media type")),
    }
    Ok(body)
}

fn parse_datetime(value: &str) -> Option<NaiveDateTime> {
    DateTime::parse_from_rfc3339(value)
        .map(|date| date.naive_utc())
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .ok()
}

fn map_client_error(error: sdks::ClientError) -> TrackingError {
    match error {
        sdks::ClientError::Unauthorized => {
            TrackingError::reauth("Simkl rejected the access token")
        }
        sdks::ClientError::RateLimited { retry_after_secs } => {
            TrackingError::retry_after(
                "Simkl rate limit reached",
                Duration::from_secs(retry_after_secs.max(1)),
            )
        }
        sdks::ClientError::Transport(error) => {
            TrackingError::retryable(format!("Simkl network error: {error}"))
        }
        sdks::ClientError::Http {
            status, message, ..
        } if status == 408 || status == 425 || status >= 500 => {
            TrackingError::retryable(format!("Simkl HTTP {status}: {message}"))
        }
        sdks::ClientError::Http {
            status, message, ..
        } => TrackingError::permanent(format!("Simkl HTTP {status}: {message}")),
        sdks::ClientError::Url(error) => {
            TrackingError::permanent(format!("invalid Simkl URL: {error}"))
        }
        other => TrackingError::retryable(format!("Simkl API error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addon(
        server: &httpmock::MockServer,
        request_timeout: Duration,
    ) -> SimklAddon {
        SimklAddon {
            client_id: "test-client".into(),
            base_url: server.base_url(),
            app_version: "test".into(),
            connect_timeout: request_timeout,
            request_timeout,
            last_write: Mutex::new(None),
            user_requests: DashMap::new(),
        }
    }

    fn test_context() -> TrackingCtx {
        TrackingCtx {
            config: Arc::new(crate::Config::default()),
            db: sqlx::sqlite::SqlitePoolOptions::new()
                .connect_lazy("sqlite::memory:")
                .unwrap(),
        }
    }

    fn test_credentials() -> TrackingCredentials {
        TrackingCredentials::new(serde_json::json!({ "access_token": "token" }))
    }

    fn movie() -> TrackingTarget {
        TrackingTarget {
            media_id: None,
            kind: crate::db::MediaKind::Movie,
            title: "Arrival".to_string(),
            year: Some(2016),
            ids: TrackingIds {
                imdb: Some("tt2543164".to_string()),
                tmdb: Some(329865),
                tvdb: None,
                ..Default::default()
            },
            is_anime: false,
            series: None,
            season: None,
            episode: None,
            runtime_ticks: Some(6_960_000_000),
        }
    }

    #[test]
    fn abandoned_stop_never_forces_simkl_watched_threshold() {
        let body = scrobble_body(&movie(), 1_000_000_000, false).unwrap();
        assert!(body.progress < 80.0);
    }

    #[test]
    fn played_stop_is_at_least_simkl_watched_threshold() {
        let body = scrobble_body(&movie(), 1_000_000_000, true).unwrap();
        assert_eq!(body.progress, 80.0);
    }

    #[test]
    fn history_payload_uses_remote_ids() {
        let body =
            SimklAddon::history_body(&movie(), Some("2026-08-18T00:00:00Z".into()))
                .unwrap();
        assert_eq!(
            body.movies
                .len(),
            1
        );
        assert_eq!(
            body.movies[0]
                .ids
                .imdb
                .as_deref(),
            Some("tt2543164")
        );
    }

    #[test]
    fn ratings_outside_simkls_range_are_not_silently_coerced() {
        assert_eq!(simkl_rating(Some(7.6)).unwrap(), Some(8));
        assert_eq!(simkl_rating(None).unwrap(), None);
        assert_eq!(simkl_rating(Some(0.0)).unwrap(), None);
        assert_eq!(simkl_rating(Some(0.1)).unwrap(), Some(1));
        assert!(simkl_rating(Some(f32::NAN)).is_err());
    }

    #[tokio::test]
    async fn rating_event_reaches_simkls_rating_endpoint_with_rounded_score() {
        let server = httpmock::MockServer::start();
        let request = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/sync/ratings")
                .header("authorization", "Bearer token")
                .json_body(serde_json::json!({
                    "movies": [{
                        "title": "Arrival",
                        "year": 2016,
                        "ids": { "imdb": "tt2543164", "tmdb": 329865 },
                        "rating": 8
                    }]
                }));
            then.status(200)
                .json_body(serde_json::json!({
                    "added": { "movies": 1 },
                    "not_found": { "movies": [], "shows": [] }
                }));
        });
        let addon = test_addon(&server, Duration::from_secs(2));

        addon
            .on_event(
                &TrackingEvent::Rating { rating: Some(7.6) },
                &movie(),
                &test_credentials(),
                &test_context(),
            )
            .await
            .unwrap();

        request.assert_hits(1);
    }

    #[tokio::test]
    async fn clearing_or_zeroing_a_rating_uses_simkls_remove_endpoint() {
        for rating in [None, Some(0.0)] {
            let server = httpmock::MockServer::start();
            let request = server.mock(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/sync/ratings/remove")
                    .json_body(serde_json::json!({
                        "movies": [{
                            "title": "Arrival",
                            "year": 2016,
                            "ids": { "imdb": "tt2543164", "tmdb": 329865 }
                        }]
                    }));
                then.status(200)
                    .json_body(serde_json::json!({
                        "removed": { "movies": 1 },
                        "not_found": { "movies": [], "shows": [] }
                    }));
            });
            let addon = test_addon(&server, Duration::from_secs(2));

            addon
                .on_event(
                    &TrackingEvent::Rating { rating },
                    &movie(),
                    &test_credentials(),
                    &test_context(),
                )
                .await
                .unwrap();

            request.assert_hits(1);
        }
    }

    #[tokio::test]
    async fn anime_rating_with_only_a_kitsu_id_survives_inbound_conversion() {
        let server = httpmock::MockServer::start();
        for media_type in ["shows", "movies"] {
            server.mock(|when, then| {
                when.method(httpmock::Method::GET)
                    .path(format!("/sync/all-items/{media_type}"));
                then.status(200)
                    .json_body(serde_json::json!({}));
            });
        }
        let anime = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/anime");
            then.status(200)
                .json_body(serde_json::json!({
                    "anime": [{
                        "status": "watching",
                        "user_rating": 9,
                        "anime": {
                            "title": "Kitsu-only anime",
                            "ids": { "kitsu": 42 }
                        },
                        "seasons": []
                    }]
                }));
        });
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/playback");
            then.status(200)
                .json_body(serde_json::json!([]));
        });
        server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({ "all": "cursor" }));
        });
        let addon = test_addon(&server, Duration::from_secs(2));

        let remote = addon
            .import_history(&test_credentials(), &test_context())
            .await
            .unwrap();
        let rating = remote
            .items
            .into_iter()
            .find(|item| {
                item.kind == crate::db::MediaKind::Series
                    && item
                        .ids
                        .kitsu
                        == Some(42)
            });
        assert!(
            rating.is_some_and(|item| item.rating == Some(Some(9.0))),
            "anime title-level ratings must not be discarded"
        );
        anime.assert_hits(1);
    }

    #[test]
    fn a_sync_2xx_with_not_found_is_a_delivery_failure() {
        let rejected = serde_json::json!({
            "added": { "movies": 0 },
            "not_found": { "movies": [{ "ids": { "tmdb": 123 } }], "shows": [] }
        });
        assert!(ensure_sync_write_accepted(&rejected).is_err());

        let accepted = serde_json::json!({
            "added": { "movies": 1 },
            "not_found": { "movies": [], "shows": [] }
        });
        assert!(ensure_sync_write_accepted(&accepted).is_ok());
    }

    #[test]
    fn whole_season_history_uses_an_empty_episode_list() {
        let target = TrackingTarget {
            media_id: None,
            kind: crate::db::MediaKind::Season,
            title: "Season 2".into(),
            year: None,
            ids: TrackingIds::default(),
            is_anime: false,
            series: Some(Box::new(TrackingTarget {
                media_id: None,
                kind: crate::db::MediaKind::Series,
                title: "Example".into(),
                year: Some(2024),
                ids: TrackingIds {
                    tvdb: Some(123),
                    ..Default::default()
                },
                is_anime: false,
                series: None,
                season: None,
                episode: None,
                runtime_ticks: None,
            })),
            season: Some(2),
            episode: None,
            runtime_ticks: None,
        };
        let body = SimklAddon::history_body(&target, None).unwrap();
        assert_eq!(body.shows[0].seasons[0].number, 2);
        assert_eq!(body.shows[0].use_tvdb_anime_seasons, Some(true));
        assert!(
            body.shows[0].seasons[0]
                .episodes
                .is_empty()
        );
    }

    #[test]
    fn anime_episode_coordinates_use_the_tvdb_cross_map() {
        let episode = api::EpisodeRef {
            season: Some(1),
            number: Some(1),
            tvdb: Some(api::TvdbEpisodeRef {
                season: 3,
                episode: 13,
            }),
            ..Default::default()
        };
        assert_eq!(episode_coordinates(true, &episode, 1), Some((3, 13)));
        assert_eq!(episode_coordinates(false, &episode, 1), Some((1, 1)));
    }

    #[test]
    fn episode_ids_only_send_identifiers_simkl_accepts() {
        let ids = TrackingIds {
            imdb: Some("tt0000001".into()),
            tmdb: Some(55),
            tvdb: Some(66),
            ..Default::default()
        };
        let ids = episode_api_ids(&ids).unwrap();
        assert!(
            ids.imdb
                .is_none()
        );
        assert!(
            ids.tmdb
                .is_none()
        );
        assert_eq!(
            ids.tvdb
                .and_then(|id| id.as_i64()),
            Some(66)
        );
    }

    #[test]
    fn activity_watermark_is_preserved_verbatim() {
        let activities = api::Activities {
            all: Some("2026-08-18T12:34:56.789Z".into()),
            ..Default::default()
        };
        assert_eq!(activities_cursor(&activities), "2026-08-18T12:34:56.789Z");
    }

    #[test]
    fn a_fresh_pin_shape_terminates_polling_for_the_old_code() {
        let response = api::PinPollResponse {
            result: "OK".into(),
            device_code: Some("DEVICE_CODE".into()),
            user_code: Some("NEW01".into()),
            ..Default::default()
        };
        assert!(matches!(device_auth_poll(response), DeviceAuthPoll::Denied));
    }

    #[test]
    fn authorization_pending_keeps_the_pin_poll_alive() {
        let response = api::PinPollResponse {
            result: "KO".into(),
            message: Some("Authorization pending".into()),
            ..Default::default()
        };
        assert!(matches!(
            device_auth_poll(response),
            DeviceAuthPoll::Pending
        ));
    }

    #[tokio::test]
    async fn slow_provider_requests_end_at_the_configured_deadline() {
        let server = httpmock::MockServer::start();
        let request = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/oauth/pin");
            then.status(200)
                .delay(Duration::from_millis(250))
                .json_body(serde_json::json!({
                    "result": "OK",
                    "user_code": "SLOW",
                    "verification_uri": "https://simkl.com/pin",
                    "expires_in": 900,
                    "interval": 5
                }));
        });
        let addon = test_addon(&server, Duration::from_millis(50));
        let started = std::time::Instant::now();

        let error = addon
            .begin_device_auth(&test_context())
            .await
            .expect_err("the delayed request should time out");

        assert!(error.is_retryable());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "provider deadline was not enforced"
        );
        request.assert_hits(1);
    }

    #[tokio::test]
    async fn verification_posts_an_empty_json_body_and_surfaces_failure() {
        let server = httpmock::MockServer::start();
        let request = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/users/settings")
                .header("content-type", "application/json")
                .json_body(serde_json::json!({}));
            then.status(401)
                .json_body(serde_json::json!({ "message": "invalid token" }));
        });
        let addon = test_addon(&server, Duration::from_secs(2));

        let error = addon
            .verify(&test_credentials(), &test_context())
            .await
            .expect_err("provider verification failure must propagate");

        assert!(error.requires_reauth());
        request.assert_hits(1);
    }

    #[tokio::test]
    async fn large_baseline_is_received_completely_before_its_cursor() {
        const MOVIES: usize = 10_000;
        let server = httpmock::MockServer::start();
        let movies = (0..MOVIES)
            .map(|id| {
                serde_json::json!({
                    "status": "completed",
                    "movie": {
                        "title": format!("Movie {id}"),
                        "ids": { "tmdb": id as i64 + 1 }
                    }
                })
            })
            .collect::<Vec<_>>();
        let shows = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/shows");
            then.status(200)
                .json_body(serde_json::json!({ "shows": [] }));
        });
        let movies_request = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/movies");
            then.status(200)
                .json_body(serde_json::json!({ "movies": movies }));
        });
        let anime = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/all-items/anime");
            then.status(200)
                .json_body(serde_json::json!({ "anime": [] }));
        });
        let playback = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/playback");
            then.status(200)
                .json_body(serde_json::json!([]));
        });
        let activities = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/sync/activities");
            then.status(200)
                .json_body(serde_json::json!({
                    "all": "2026-08-18T12:34:56.789Z"
                }));
        });
        let addon = test_addon(&server, Duration::from_secs(10));

        let result = addon
            .import_history(&test_credentials(), &test_context())
            .await
            .unwrap();

        assert_eq!(
            result
                .items
                .len(),
            MOVIES
        );
        assert_eq!(result.cursor, "2026-08-18T12:34:56.789Z");
        assert!(result.payload_bytes > MOVIES * 40);
        shows.assert_hits(1);
        movies_request.assert_hits(1);
        anime.assert_hits(1);
        playback.assert_hits(1);
        activities.assert_hits(1);
    }
}
