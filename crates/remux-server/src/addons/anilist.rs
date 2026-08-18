use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use uuid::Uuid;

use super::{
    AddonCapabilities, AddonKind, AddonMetadata, AddonOption, AddonOptionType,
    AddonPreset, AddonPresetRegistration, MediaKind, ResourceType,
    tracking::{
        AuthFlow, RedirectAuthStart, RemoteSync, RemoteWatch, SyncDirection,
        TrackingAddon, TrackingCapabilities, TrackingCredentials, TrackingCtx,
        TrackingError, TrackingEvent, TrackingEventKind, TrackingIds, TrackingResult,
        TrackingTarget,
    },
};
use crate::sdks::{self, anilist as api};

/// AniList currently documents a degraded 30 requests/minute budget. Keeping
/// starts just over two seconds apart also avoids its separate burst limiter.
const REQUEST_INTERVAL: Duration = Duration::from_millis(2_100);
const MAX_LIST_PAGES: i64 = 250;

pub struct AniListPreset;

impl AddonPreset for AniListPreset {
    fn id(&self) -> &'static str {
        "anilist"
    }

    fn metadata(&self) -> AddonMetadata {
        AddonMetadata {
            id: "anilist".to_string(),
            display_name: "AniList".to_string(),
            description: "Sync anime watch progress and ratings with AniList."
                .to_string(),
            icon: None,
            supported_resources: vec![AddonMetadata::simple_resource(ResourceType::Tracking)],
            supported_types: vec![
                MediaKind::Movie,
                MediaKind::Series,
                MediaKind::Episode,
            ],
            supported_resources_user: vec![],
            supported_types_user: vec![],
            options: vec![
                AddonOption {
                    id: "client_id".to_string(),
                    name: "AniList Client ID".to_string(),
                    description: Some(
                        "Create an AniList application and enter its numeric Client ID."
                            .to_string(),
                    ),
                    required: true,
                    default: None,
                    kind: AddonOptionType::Number {
                        min: Some(1),
                        max: None,
                    },
                },
                AddonOption {
                    id: "client_secret".to_string(),
                    name: "AniList Client Secret".to_string(),
                    description: Some(
                        "The server-side secret for the same AniList application. Set its redirect URL to https://YOUR-REMUX-HOST/remux/tracking/oauth/callback."
                            .to_string(),
                    ),
                    required: true,
                    default: None,
                    kind: AddonOptionType::Password,
                },
            ],
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
            .and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| {
                        value
                            .as_str()
                            .and_then(|value| {
                                value
                                    .parse()
                                    .ok()
                            })
                    })
            })
            .filter(|value| *value > 0)
            .ok_or_else(|| anyhow!("AniList Client ID must be a positive integer"))?;
        let client_secret = cfg
            .get("client_secret")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("AniList Client Secret is required"))?;
        let client = api::Client::new(
            client_id,
            client_secret,
            &config.anilist_graphql_url,
            &config.anilist_oauth_base_url,
            Duration::from_secs(
                config
                    .anilist_connect_timeout_seconds
                    .max(1),
            ),
            Duration::from_secs(
                config
                    .anilist_request_timeout_seconds
                    .max(1),
            ),
        )?;
        let addon = Arc::new(AniListAddon {
            client,
            request_gate: Mutex::new(None),
            mappings: DashMap::new(),
        });
        Ok(AddonCapabilities {
            kind: Some(addon.clone()),
            tracking: Some(addon),
            ..Default::default()
        })
    }
}

inventory::submit! {
    AddonPresetRegistration(|| Box::new(AniListPreset))
}

pub struct AniListAddon {
    client: api::Client,
    request_gate: Mutex<Option<tokio::time::Instant>>,
    mappings: DashMap<TrackingIds, api::Media>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct AniListCursor {
    updated_at: i64,
    #[serde(default)]
    entry_ids: Vec<i64>,
}

impl AniListCursor {
    fn contains(&self, entry: &api::MediaListEntry) -> bool {
        let updated_at = entry
            .updated_at
            .unwrap_or_default();
        updated_at < self.updated_at
            || (updated_at == self.updated_at
                && self
                    .entry_ids
                    .contains(&entry.id))
    }

    fn observe(&mut self, entry: &api::MediaListEntry) {
        let updated_at = entry
            .updated_at
            .unwrap_or_default();
        if updated_at > self.updated_at {
            self.updated_at = updated_at;
            self.entry_ids
                .clear();
            self.entry_ids
                .push(entry.id);
        } else if updated_at == self.updated_at
            && !self
                .entry_ids
                .contains(&entry.id)
        {
            self.entry_ids
                .push(entry.id);
        }
    }
}

impl AniListAddon {
    fn access_token<'a>(
        &self,
        credentials: &'a TrackingCredentials,
    ) -> TrackingResult<&'a str> {
        if credentials
            .0
            .get("expires_at")
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|expires_at| expires_at <= Utc::now().timestamp())
        {
            return Err(TrackingError::reauth("AniList access token expired"));
        }
        credentials
            .get_str("access_token")
            .ok_or_else(|| TrackingError::reauth("AniList access token is missing"))
    }

    async fn wait_for_budget(&self) {
        let mut last = self
            .request_gate
            .lock()
            .await;
        if let Some(previous) = *last {
            let elapsed = previous.elapsed();
            if elapsed < REQUEST_INTERVAL {
                tokio::time::sleep(REQUEST_INTERVAL - elapsed).await;
            }
        }
        *last = Some(tokio::time::Instant::now());
    }

    async fn viewer(&self, token: &str) -> TrackingResult<api::Viewer> {
        self.wait_for_budget()
            .await;
        self.client
            .viewer(token)
            .await
            .map_err(map_api_error)
    }

    fn ids_for_media(media: &api::Media) -> TrackingIds {
        TrackingIds {
            anilist: Some(media.id),
            mal: media.id_mal,
            ..Default::default()
        }
    }

    fn target_media(target: &TrackingTarget) -> &TrackingTarget {
        target
            .series
            .as_deref()
            .unwrap_or(target)
    }

    fn target_ids(target: &TrackingTarget) -> &TrackingIds {
        &Self::target_media(target).ids
    }

    fn can_search_by_title(target: &TrackingTarget) -> bool {
        let media = Self::target_media(target);
        media.is_anime
            && media.year.is_some()
            && !media
                .title
                .trim()
                .is_empty()
            // AniList models seasons as separate media entries. Until Remux
            // carries a season-specific title/ID, a root-series search is safe
            // only for movies and first-season episodes. Direct AniList/MAL/
            // Kitsu IDs continue to support whole-series events.
            && (matches!(target.kind, crate::db::MediaKind::Movie)
                || (matches!(target.kind, crate::db::MediaKind::Episode)
                    && target.season == Some(1)))
    }

    async fn search_media(
        &self,
        target: &TrackingTarget,
        token: &str,
    ) -> TrackingResult<Option<api::Media>> {
        let reference = Self::target_media(target);
        let Some(year) = reference.year else {
            return Ok(None);
        };
        self.wait_for_budget()
            .await;
        let candidates = self
            .client
            .search_media(&reference.title, year, token)
            .await
            .map_err(map_api_error)?;
        let mut matches = candidates
            .into_iter()
            .filter(|candidate| media_matches_target(candidate, target))
            .collect::<Vec<_>>();
        matches.sort_by_key(|candidate| candidate.id);
        matches.dedup_by_key(|candidate| candidate.id);
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            count => Err(TrackingError::permanent(format!(
                "AniList title lookup for '{}' was ambiguous ({count} exact matches)",
                reference.title
            ))),
        }
    }

    async fn persist_mapping(
        target: &TrackingTarget,
        media: &api::Media,
        ctx: &TrackingCtx,
    ) {
        let reference = Self::target_media(target);
        let Some(media_id) = reference.media_id else {
            return;
        };
        if let Err(error) = crate::db::Media::merge_anime_tracking_ids(
            &ctx.db,
            &media_id,
            media.id,
            media.id_mal,
        )
        .await
        {
            tracing::warn!(
                local_media_id = %media_id,
                anilist_id = media.id,
                error = %error,
                "could not persist verified AniList media mapping"
            );
        }
    }

    async fn resolve_media(
        &self,
        target: &TrackingTarget,
        token: &str,
        ctx: &TrackingCtx,
    ) -> TrackingResult<api::Media> {
        let ids = Self::target_ids(target);
        if let Some(cached) = self
            .mappings
            .get(ids)
        {
            return Ok(cached
                .value()
                .clone());
        }

        let media = if let Some(id) = ids.anilist {
            self.wait_for_budget()
                .await;
            self.client
                .media_by_id(id, token)
                .await
                .map_err(map_api_error)?
        } else {
            let mal_id = if let Some(id) = ids.mal {
                Some(id)
            } else if let Some(kitsu_id) = ids.kitsu {
                sdks::kitsu::client()
                    .execute(sdks::kitsu::MappingsEndpoint { kitsu_id })
                    .await
                    .map_err(|error| {
                        TrackingError::retryable(format!(
                            "Kitsu mapping request for AniList failed: {error}"
                        ))
                    })?
                    .mal_id()
            } else {
                None
            };
            if let Some(mal_id) = mal_id {
                self.wait_for_budget()
                    .await;
                self.client
                    .media_by_mal_id(mal_id, token)
                    .await
                    .map_err(map_api_error)?
            } else if Self::can_search_by_title(target) {
                self.search_media(target, token)
                    .await?
            } else {
                None
            }
        };
        let Some(media) = media else {
            return Err(TrackingError::permanent(format!(
                "AniList could not safely map '{}'",
                Self::target_media(target).title
            )));
        };
        self.mappings
            .insert(ids.clone(), media.clone());
        let mut enriched_ids = ids.clone();
        enriched_ids
            .anilist
            .get_or_insert(media.id);
        if let Some(mal_id) = media.id_mal {
            enriched_ids
                .mal
                .get_or_insert(mal_id);
        }
        if enriched_ids != *ids {
            self.mappings
                .insert(enriched_ids, media.clone());
        }
        Self::persist_mapping(target, &media, ctx).await;
        Ok(media)
    }

    async fn save(
        &self,
        input: api::SaveMediaListEntry,
        token: &str,
    ) -> TrackingResult<()> {
        self.wait_for_budget()
            .await;
        self.client
            .save_media_list_entry(&input, token)
            .await
            .map(|_| ())
            .map_err(map_api_error)
    }

    async fn fetch_remote(
        &self,
        since: Option<String>,
        credentials: &TrackingCredentials,
    ) -> TrackingResult<RemoteSync> {
        let token = self.access_token(credentials)?;
        let viewer_id = credentials
            .0
            .get("viewer_id")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| {
                TrackingError::reauth("AniList account identity is missing")
            })?;
        let previous = since
            .as_deref()
            .map(serde_json::from_str::<AniListCursor>)
            .transpose()
            .map_err(|error| {
                TrackingError::permanent(format!(
                    "invalid AniList sync cursor: {error}"
                ))
            })?;
        let mut next = previous
            .clone()
            .unwrap_or_default();
        let mut items = Vec::new();
        let mut payload_bytes = 0usize;

        for page_number in 1..=MAX_LIST_PAGES {
            self.wait_for_budget()
                .await;
            let page = self
                .client
                .media_list_page(viewer_id, page_number, token)
                .await
                .map_err(map_api_error)?;
            payload_bytes = payload_bytes.saturating_add(
                page.media_list
                    .len()
                    .saturating_mul(192),
            );
            let mut reached_old_data = false;
            for entry in &page.media_list {
                next.observe(entry);
                if previous
                    .as_ref()
                    .is_some_and(|cursor| cursor.contains(entry))
                {
                    if entry
                        .updated_at
                        .unwrap_or_default()
                        < cursor_timestamp(previous.as_ref())
                    {
                        reached_old_data = true;
                    }
                    continue;
                }
                append_remote_entry(&mut items, entry);
            }
            if reached_old_data
                || !page
                    .page_info
                    .has_next_page
            {
                break;
            }
            if page_number == MAX_LIST_PAGES {
                return Err(TrackingError::retryable(
                    "AniList list exceeded the safe pagination limit",
                ));
            }
        }

        let cursor = serde_json::to_string(&next).map_err(|error| {
            TrackingError::permanent(format!("encoding AniList sync cursor: {error}"))
        })?;
        Ok(RemoteSync {
            items,
            cursor,
            payload_bytes,
        })
    }
}

fn normalize_title(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn media_matches_target(media: &api::Media, target: &TrackingTarget) -> bool {
    let reference = AniListAddon::target_media(target);
    let Some(year) = reference.year else {
        return false;
    };
    if media
        .season_year
        .or_else(|| {
            media
                .start_date
                .as_ref()
                .and_then(|date| date.year)
        })
        != Some(year)
    {
        return false;
    }
    let format_matches = match target.kind {
        crate::db::MediaKind::Movie => media.format == Some(api::MediaFormat::Movie),
        crate::db::MediaKind::Series | crate::db::MediaKind::Episode => matches!(
            media.format,
            Some(
                api::MediaFormat::Tv
                    | api::MediaFormat::TvShort
                    | api::MediaFormat::Special
                    | api::MediaFormat::Ova
                    | api::MediaFormat::Ona
            )
        ),
        _ => false,
    };
    if !format_matches {
        return false;
    }
    if matches!(target.kind, crate::db::MediaKind::Episode)
        && target
            .episode
            .zip(media.episodes)
            .is_some_and(|(episode, total)| episode > total)
    {
        return false;
    }
    let expected = normalize_title(&reference.title);
    !expected.is_empty()
        && [
            media
                .title
                .user_preferred
                .as_deref(),
            media
                .title
                .romaji
                .as_deref(),
            media
                .title
                .english
                .as_deref(),
            media
                .title
                .native
                .as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|title| normalize_title(title) == expected)
}

fn cursor_timestamp(cursor: Option<&AniListCursor>) -> i64 {
    cursor
        .map(|cursor| cursor.updated_at)
        .unwrap_or_default()
}

#[async_trait]
impl AddonKind for AniListAddon {
    fn id(&self) -> &'static str {
        "anilist"
    }
}

#[async_trait]
impl TrackingAddon for AniListAddon {
    fn capabilities(&self) -> TrackingCapabilities {
        let supported_events = vec![
            TrackingEventKind::PlaybackStop,
            TrackingEventKind::MarkPlayed,
            TrackingEventKind::MarkUnplayed,
            TrackingEventKind::Rating,
        ];
        TrackingCapabilities {
            auth_flow: AuthFlow::OAuthRedirect,
            supported_events: supported_events.clone(),
            default_event_filter: supported_events,
            history_import: true,
            progress_import: false,
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
        let ids = Self::target_ids(target);
        if ids
            .anilist
            .is_none()
            && ids
                .mal
                .is_none()
            && ids
                .kitsu
                .is_none()
            && !Self::can_search_by_title(target)
        {
            return false;
        }
        match event {
            TrackingEvent::PlaybackStop { played, .. } => {
                *played
                    && matches!(
                        target.kind,
                        crate::db::MediaKind::Movie | crate::db::MediaKind::Episode
                    )
            }
            TrackingEvent::MarkPlayed | TrackingEvent::MarkUnplayed => matches!(
                target.kind,
                crate::db::MediaKind::Movie
                    | crate::db::MediaKind::Series
                    | crate::db::MediaKind::Episode
            ),
            TrackingEvent::Rating { .. } => matches!(
                target.kind,
                crate::db::MediaKind::Movie | crate::db::MediaKind::Series
            ),
            _ => false,
        }
    }

    async fn begin_redirect_auth(
        &self,
        state: &str,
        redirect_uri: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RedirectAuthStart> {
        Ok(RedirectAuthStart {
            authorization_url: self
                .client
                .authorization_url(redirect_uri, state)
                .map_err(map_api_error)?,
            expires_in: Duration::from_secs(10 * 60),
        })
    }

    async fn complete_redirect_auth(
        &self,
        code: &str,
        redirect_uri: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<TrackingCredentials> {
        let token = self
            .client
            .exchange_code(code, redirect_uri)
            .await
            .map_err(map_api_error)?;
        let viewer = self
            .viewer(&token.access_token)
            .await?;
        let expires_in = token
            .expires_in
            .unwrap_or(365 * 24 * 60 * 60)
            .max(1);
        Ok(TrackingCredentials::new(serde_json::json!({
            "access_token": token.access_token,
            "viewer_id": viewer.id,
            "viewer_name": viewer.name,
            "expires_at": Utc::now().timestamp().saturating_add(expires_in),
        })))
    }

    async fn verify(
        &self,
        credentials: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        let token = self.access_token(credentials)?;
        self.viewer(token)
            .await
            .map(|_| ())
    }

    async fn on_event(
        &self,
        event: &TrackingEvent,
        target: &TrackingTarget,
        credentials: &TrackingCredentials,
        ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        let token = self.access_token(credentials)?;
        // Ratings need only the stable AniList media id. Avoid spending one of
        // the provider's scarce requests on a metadata lookup when it is direct.
        if let TrackingEvent::Rating { rating } = event {
            if let Some(media_id) = Self::target_ids(target).anilist {
                return self
                    .save(
                        api::SaveMediaListEntry {
                            media_id,
                            status: None,
                            progress: None,
                            score_raw: Some(anilist_rating(*rating)?),
                        },
                        token,
                    )
                    .await;
            }
        }
        let media = self
            .resolve_media(target, token, ctx)
            .await?;
        let mut input = api::SaveMediaListEntry {
            media_id: media.id,
            status: None,
            progress: None,
            score_raw: None,
        };
        match event {
            TrackingEvent::PlaybackStop { played: true, .. }
            | TrackingEvent::MarkPlayed => {
                if matches!(target.kind, crate::db::MediaKind::Episode) {
                    let progress = target
                        .episode
                        .unwrap_or(1)
                        .max(1);
                    input.progress = Some(progress);
                    input.status = Some(
                        if media
                            .episodes
                            .is_some_and(|total| progress >= total)
                        {
                            api::MediaListStatus::Completed
                        } else {
                            api::MediaListStatus::Current
                        },
                    );
                } else {
                    input.progress = Some(
                        media
                            .episodes
                            .unwrap_or(1)
                            .max(1),
                    );
                    input.status = Some(api::MediaListStatus::Completed);
                }
            }
            TrackingEvent::PlaybackStop { played: false, .. } => return Ok(()),
            TrackingEvent::MarkUnplayed => {
                let progress = if matches!(target.kind, crate::db::MediaKind::Episode) {
                    target
                        .episode
                        .unwrap_or(1)
                        .saturating_sub(1)
                } else {
                    0
                };
                input.progress = Some(progress);
                input.status = Some(if progress == 0 {
                    api::MediaListStatus::Planning
                } else {
                    api::MediaListStatus::Current
                });
            }
            TrackingEvent::Rating { rating } => {
                input.score_raw = Some(anilist_rating(*rating)?);
            }
            _ => return Err(TrackingError::unsupported("this AniList event")),
        }
        self.save(input, token)
            .await
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

fn append_remote_entry(items: &mut Vec<RemoteWatch>, entry: &api::MediaListEntry) {
    let ids = AniListAddon::ids_for_media(&entry.media);
    let completed = entry
        .status
        .is_some_and(api::MediaListStatus::is_completed);
    let watched_at = completed
        .then(|| {
            fuzzy_datetime(
                entry
                    .completed_at
                    .as_ref(),
            )
        })
        .flatten();
    let rating = Some(
        if entry
            .score
            .unwrap_or_default()
            > 0.0
        {
            Some(
                entry
                    .score
                    .unwrap_or_default()
                    .clamp(0.0, 10.0),
            )
        } else {
            None
        },
    );
    if entry
        .media
        .format
        .is_some_and(api::MediaFormat::is_movie)
    {
        items.push(RemoteWatch {
            kind: crate::db::MediaKind::Movie,
            ids,
            season: None,
            episode: None,
            watched: Some(
                completed
                    || entry
                        .progress
                        .unwrap_or_default()
                        > 0,
            ),
            position_ticks: None,
            position_percent: None,
            watched_at,
            favorite: None,
            rating,
        });
        return;
    }

    items.push(RemoteWatch {
        kind: crate::db::MediaKind::Series,
        ids: ids.clone(),
        season: None,
        episode: None,
        watched: completed.then_some(true),
        position_ticks: None,
        position_percent: None,
        watched_at,
        favorite: None,
        rating,
    });
    let progress = entry
        .progress
        .unwrap_or_default()
        .max(0);
    for episode in 1..=progress {
        items.push(RemoteWatch {
            kind: crate::db::MediaKind::Episode,
            ids: ids.clone(),
            season: Some(1),
            episode: Some(episode),
            watched: Some(true),
            position_ticks: None,
            position_percent: None,
            watched_at,
            favorite: None,
            rating: None,
        });
    }
    let first_unwatched = progress.saturating_add(1);
    let last_unwatched = entry
        .media
        .episodes
        .unwrap_or(first_unwatched)
        .max(first_unwatched);
    if !completed {
        for episode in first_unwatched..=last_unwatched {
            items.push(RemoteWatch {
                kind: crate::db::MediaKind::Episode,
                ids: ids.clone(),
                season: Some(1),
                episode: Some(episode),
                watched: Some(false),
                position_ticks: None,
                position_percent: None,
                watched_at: None,
                favorite: None,
                rating: None,
            });
        }
    }
}

fn fuzzy_datetime(date: Option<&api::FuzzyDate>) -> Option<NaiveDateTime> {
    let date = date?;
    NaiveDate::from_ymd_opt(
        date.year?,
        date.month
            .unwrap_or(1),
        date.day
            .unwrap_or(1),
    )
    .and_then(|date| date.and_hms_opt(0, 0, 0))
}

fn anilist_rating(rating: Option<f32>) -> TrackingResult<i32> {
    let Some(rating) = rating else {
        return Ok(0);
    };
    if !rating.is_finite() || !(0.0..=10.0).contains(&rating) {
        return Err(TrackingError::permanent(
            "Remux ratings sent to AniList must be finite and between 0 and 10",
        ));
    }
    Ok((rating * 10.0).round() as i32)
}

fn map_api_error(error: api::Error) -> TrackingError {
    match error {
        api::Error::Unauthorized => {
            TrackingError::reauth("AniList rejected the access token")
        }
        api::Error::RateLimited { retry_after_secs } => TrackingError::retry_after(
            "AniList rate limit reached",
            Duration::from_secs(retry_after_secs.max(1)),
        ),
        api::Error::Transport(error) => {
            TrackingError::retryable(format!("AniList network error: {error}"))
        }
        api::Error::Http { status, message }
            if status == 408 || status == 425 || status >= 500 =>
        {
            TrackingError::retryable(format!("AniList HTTP {status}: {message}"))
        }
        api::Error::GraphQl {
            status: Some(status),
            message,
        } if status == 408 || status == 425 || status >= 500 => {
            TrackingError::retryable(format!("AniList GraphQL {status}: {message}"))
        }
        other => TrackingError::permanent(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_addon(server: &httpmock::MockServer) -> AniListAddon {
        AniListAddon {
            client: api::Client::new(
                42,
                "secret",
                server.url("/graphql"),
                server.url("/oauth"),
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
            .unwrap(),
            request_gate: Mutex::new(None),
            mappings: DashMap::new(),
        }
    }

    fn credentials() -> TrackingCredentials {
        TrackingCredentials::new(serde_json::json!({
            "access_token": "token",
            "viewer_id": 77,
            "expires_at": Utc::now().timestamp() + 3600,
        }))
    }

    fn movie() -> TrackingTarget {
        TrackingTarget {
            media_id: None,
            kind: crate::db::MediaKind::Movie,
            title: "Your Name".to_string(),
            year: Some(2016),
            ids: TrackingIds {
                anilist: Some(21519),
                ..Default::default()
            },
            is_anime: true,
            series: None,
            season: None,
            episode: None,
            runtime_ticks: None,
        }
    }

    fn idless_anime_episode(season: i64) -> TrackingTarget {
        TrackingTarget {
            media_id: None,
            kind: crate::db::MediaKind::Episode,
            title: "S1E7 - All Eyes on Arata".to_string(),
            year: Some(2026),
            ids: TrackingIds {
                tmdb: Some(7_451_680),
                ..Default::default()
            },
            is_anime: true,
            series: Some(Box::new(TrackingTarget {
                media_id: None,
                kind: crate::db::MediaKind::Series,
                title: "KAIJU GIRL CARAMELISE".to_string(),
                year: Some(2026),
                ids: TrackingIds {
                    imdb: Some("tt39246964".to_string()),
                    tmdb: Some(308_874),
                    tvdb: Some(471_878),
                    ..Default::default()
                },
                is_anime: true,
                series: None,
                season: None,
                episode: None,
                runtime_ticks: None,
            })),
            season: Some(season),
            episode: Some(7),
            runtime_ticks: Some(24 * 60 * 10_000_000),
        }
    }

    fn kaiju_candidate() -> api::Media {
        api::Media {
            id: 204_466,
            id_mal: Some(63_150),
            format: Some(api::MediaFormat::Tv),
            episodes: Some(12),
            duration: Some(24),
            season_year: Some(2026),
            start_date: Some(api::FuzzyDate {
                year: Some(2026),
                month: Some(7),
                day: Some(3),
            }),
            title: api::MediaTitle {
                user_preferred: Some("Otome Kaijuu Caraméliser".to_string()),
                romaji: Some("Otome Kaijuu Caraméliser".to_string()),
                english: Some("KAIJU GIRL CARAMELISE".to_string()),
                native: Some("乙女怪獣キャラメリゼ".to_string()),
            },
        }
    }

    #[test]
    fn title_fallback_requires_an_explicit_anime_first_season_target() {
        let target = idless_anime_episode(1);
        assert!(AniListAddon::can_search_by_title(&target));

        let mut non_anime = target.clone();
        non_anime
            .series
            .as_mut()
            .unwrap()
            .is_anime = false;
        assert!(!AniListAddon::can_search_by_title(&non_anime));

        assert!(!AniListAddon::can_search_by_title(&idless_anime_episode(2)));

        let series = *target
            .series
            .unwrap();
        assert!(!AniListAddon::can_search_by_title(&series));
    }

    #[test]
    fn title_fallback_requires_exact_title_year_format_and_episode_bounds() {
        let target = idless_anime_episode(1);
        let candidate = kaiju_candidate();
        assert!(media_matches_target(&candidate, &target));

        let mut wrong_year = candidate.clone();
        wrong_year.season_year = Some(2025);
        assert!(!media_matches_target(&wrong_year, &target));

        let mut wrong_format = candidate.clone();
        wrong_format.format = Some(api::MediaFormat::Movie);
        assert!(!media_matches_target(&wrong_format, &target));

        let mut wrong_title = candidate.clone();
        wrong_title
            .title
            .english = Some("Kaiju No. 8".to_string());
        wrong_title
            .title
            .user_preferred = None;
        wrong_title
            .title
            .romaji = None;
        wrong_title
            .title
            .native = None;
        assert!(!media_matches_target(&wrong_title, &target));

        let mut too_short = candidate;
        too_short.episodes = Some(6);
        assert!(!media_matches_target(&too_short, &target));
    }

    #[test]
    fn rating_uses_anilist_raw_hundred_point_scale() {
        assert_eq!(anilist_rating(Some(7.25)).unwrap(), 73);
        assert_eq!(anilist_rating(Some(0.0)).unwrap(), 0);
        assert_eq!(anilist_rating(None).unwrap(), 0);
        assert!(anilist_rating(Some(f32::NAN)).is_err());
        assert!(anilist_rating(Some(10.1)).is_err());
    }

    #[test]
    fn cursor_orders_equal_second_updates_by_entry_id() {
        let cursor = AniListCursor {
            updated_at: 50,
            entry_ids: vec![10],
        };
        let entry = api::MediaListEntry {
            id: 11,
            status: None,
            score: None,
            progress: None,
            updated_at: Some(50),
            completed_at: None,
            media: api::Media {
                id: 1,
                id_mal: None,
                format: None,
                episodes: None,
                duration: None,
                season_year: None,
                start_date: None,
                title: api::MediaTitle::default(),
            },
        };
        assert!(!cursor.contains(&entry));
        let seen = api::MediaListEntry {
            id: 10,
            ..entry.clone()
        };
        assert!(cursor.contains(&seen));
    }

    #[tokio::test]
    async fn rating_uses_score_raw_without_a_metadata_lookup() {
        let server = httpmock::MockServer::start();
        let mutation = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .header("authorization", "Bearer token")
                .body_contains("SaveMediaListEntry")
                .body_contains("\"mediaId\":21519")
                .body_contains("\"scoreRaw\":73");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "SaveMediaListEntry": {
                            "id": 1,
                            "status": null,
                            "score": 7.3,
                            "progress": 0,
                            "updatedAt": 1,
                            "media": {
                                "id": 21519,
                                "idMal": 32281,
                                "format": "MOVIE",
                                "episodes": 1,
                                "duration": 106,
                                "title": { "userPreferred": "Your Name" }
                            }
                        }
                    }
                }));
        });
        let addon = test_addon(&server);

        addon
            .on_event(
                &TrackingEvent::Rating { rating: Some(7.25) },
                &movie(),
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: sqlx::sqlite::SqlitePoolOptions::new()
                        .connect_lazy("sqlite::memory:")
                        .unwrap(),
                },
            )
            .await
            .unwrap();

        mutation.assert_hits(1);
    }

    #[tokio::test]
    async fn episode_watch_resolves_a_series_mal_id_before_saving_progress() {
        let server = httpmock::MockServer::start();
        let lookup = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .header("authorization", "Bearer token")
                .body_contains("Media(idMal:")
                .body_contains("\"idMal\":62080");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "Media": {
                            "id": 196219,
                            "idMal": 62080,
                            "format": "TV",
                            "episodes": 12,
                            "duration": 24,
                            "title": {
                                "userPreferred": "The Oblivious Saint Can't Contain Her Power"
                            }
                        }
                    }
                }));
        });
        let mutation = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .header("authorization", "Bearer token")
                .body_contains("SaveMediaListEntry")
                .body_contains("\"mediaId\":196219")
                .body_contains("\"progress\":8")
                .body_contains("\"status\":\"CURRENT\"");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "SaveMediaListEntry": {
                            "id": 1,
                            "status": "CURRENT",
                            "score": null,
                            "progress": 8,
                            "updatedAt": 1,
                            "media": {
                                "id": 196219,
                                "idMal": 62080,
                                "format": "TV",
                                "episodes": 12,
                                "duration": 24,
                                "title": {
                                    "userPreferred": "The Oblivious Saint Can't Contain Her Power"
                                }
                            }
                        }
                    }
                }));
        });
        let addon = test_addon(&server);
        let target = TrackingTarget {
            media_id: None,
            kind: crate::db::MediaKind::Episode,
            title: "Episode 8".to_string(),
            year: Some(2026),
            ids: TrackingIds::default(),
            is_anime: true,
            series: Some(Box::new(TrackingTarget {
                media_id: None,
                kind: crate::db::MediaKind::Series,
                title: "The Oblivious Saint Can't Contain Her Power".to_string(),
                year: Some(2026),
                ids: TrackingIds {
                    mal: Some(62080),
                    ..Default::default()
                },
                is_anime: true,
                series: None,
                season: None,
                episode: None,
                runtime_ticks: None,
            })),
            season: Some(1),
            episode: Some(8),
            runtime_ticks: Some(24 * 60 * 10_000_000),
        };

        addon
            .on_event(
                &TrackingEvent::MarkPlayed,
                &target,
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: sqlx::sqlite::SqlitePoolOptions::new()
                        .connect_lazy("sqlite::memory:")
                        .unwrap(),
                },
            )
            .await
            .unwrap();

        lookup.assert_hits(1);
        mutation.assert_hits(1);
    }

    #[tokio::test]
    async fn tmdb_only_anime_episode_is_searched_saved_and_persisted() {
        let server = httpmock::MockServer::start();
        let search = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .header("authorization", "Bearer token")
                .body_contains("seasonYear")
                .body_contains("\"search\":\"KAIJU GIRL CARAMELISE\"")
                .body_contains("\"year\":2026");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "Page": {
                            "media": [{
                                "id": 204466,
                                "idMal": 63150,
                                "format": "TV",
                                "episodes": 12,
                                "duration": 24,
                                "seasonYear": 2026,
                                "startDate": { "year": 2026, "month": 7, "day": 3 },
                                "title": {
                                    "userPreferred": "Otome Kaijuu Caraméliser",
                                    "romaji": "Otome Kaijuu Caraméliser",
                                    "english": "KAIJU GIRL CARAMELISE",
                                    "native": "乙女怪獣キャラメリゼ"
                                }
                            }]
                        }
                    }
                }));
        });
        let mutation = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .header("authorization", "Bearer token")
                .body_contains("SaveMediaListEntry")
                .body_contains("\"mediaId\":204466")
                .body_contains("\"progress\":7")
                .body_contains("\"status\":\"CURRENT\"");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "SaveMediaListEntry": {
                            "id": 1,
                            "status": "CURRENT",
                            "score": null,
                            "progress": 7,
                            "updatedAt": 1,
                            "media": {
                                "id": 204466,
                                "idMal": 63150,
                                "format": "TV",
                                "episodes": 12,
                                "duration": 24,
                                "title": { "userPreferred": "KAIJU GIRL CARAMELISE" }
                            }
                        }
                    }
                }));
        });

        let db_pool = crate::db::connect("sqlite::memory:", 10_000)
            .await
            .unwrap();
        crate::db::migrate(&db_pool)
            .await
            .unwrap();
        let released_at = chrono::NaiveDate::from_ymd_opt(2026, 7, 2)
            .unwrap()
            .and_hms_opt(16, 15, 0)
            .unwrap();
        let mut series = crate::db::Media {
            title: "KAIJU GIRL CARAMELISE".to_string(),
            kind: crate::db::MediaKind::Series,
            released_at: Some(released_at),
            original_language: Some("ja".to_string()),
            country: Some("JP".to_string()),
            external_ids: crate::db::ExternalIds {
                imdb: remux_utils::NonEmptyString::try_new("tt39246964".to_string())
                    .ok(),
                tmdb: Some(308_874),
                tvdb: Some(471_878),
                ..Default::default()
            },
            ..Default::default()
        };
        series.id = uuid::Uuid::from(&series.media_id_raw());
        let mut genre = crate::db::Media {
            title: "Anime".to_string(),
            kind: crate::db::MediaKind::Genre,
            ..Default::default()
        };
        genre.id = uuid::Uuid::from(&genre.media_id_raw());
        let mut episode = crate::db::Media {
            title: "S1E7 - All Eyes on Arata".to_string(),
            kind: crate::db::MediaKind::Episode,
            parent_id: Some(series.id),
            grandparent_id: Some(series.id),
            parent_idx: Some(1),
            idx: Some(7),
            external_ids: crate::db::ExternalIds {
                tmdb: Some(7_451_680),
                custom_stremio_id: Some("tt39246964:1:7".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        episode.id = uuid::Uuid::from(&episode.media_id_raw());
        crate::db::Media::insert(
            &db_pool,
            &[genre.clone(), series.clone(), episode.clone()],
        )
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO media_relations \
             (relation_id, left_media_id, right_media_id) VALUES (?1, ?2, ?3)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(series.id)
        .bind(genre.id)
        .execute(&db_pool)
        .await
        .unwrap();

        let target = crate::addons::tracking::resolve_target(&db_pool, &episode)
            .await
            .unwrap()
            .unwrap();
        let target_series = target
            .series
            .as_deref()
            .unwrap();
        assert!(target_series.is_anime);
        assert_eq!(
            target_series
                .ids
                .tmdb,
            Some(308_874)
        );
        assert_eq!(
            target_series
                .ids
                .mal,
            None
        );

        let addon = test_addon(&server);
        assert!(addon.supports_event(&TrackingEvent::MarkPlayed, &target));
        addon
            .on_event(
                &TrackingEvent::MarkPlayed,
                &target,
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: db_pool.clone(),
                },
            )
            .await
            .unwrap();

        let mapped = crate::db::Media::get_by_id(&db_pool, &series.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            mapped
                .external_ids
                .anilist,
            Some(204_466)
        );
        assert_eq!(
            mapped
                .external_ids
                .mal,
            Some(63_150)
        );
        assert_eq!(
            mapped
                .external_ids
                .tmdb,
            Some(308_874)
        );

        // A later TMDB-only refresh must not erase the tracker mapping, while
        // unrelated identifiers still retain the provider's replacement
        // semantics rather than becoming stale forever.
        let refreshed = crate::db::Media {
            external_ids: crate::db::ExternalIds {
                imdb: series
                    .external_ids
                    .imdb
                    .clone(),
                tmdb: Some(308_874),
                ..Default::default()
            },
            ..series
        };
        crate::db::Media::upsert(&db_pool, &[refreshed])
            .await
            .unwrap();
        let after_refresh = crate::db::Media::get_by_id(&db_pool, &mapped.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after_refresh
                .external_ids
                .anilist,
            Some(204_466)
        );
        assert_eq!(
            after_refresh
                .external_ids
                .mal,
            Some(63_150)
        );
        assert_eq!(
            after_refresh
                .external_ids
                .imdb
                .as_deref()
                .map(String::as_str),
            Some("tt39246964")
        );
        assert_eq!(
            after_refresh
                .external_ids
                .tvdb,
            None
        );

        search.assert_hits(1);
        mutation.assert_hits(1);
    }

    #[tokio::test]
    async fn ambiguous_exact_title_search_is_not_allowed_to_mutate_anilist() {
        let server = httpmock::MockServer::start();
        let search = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .body_contains("seasonYear");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "Page": {
                            "media": [
                                {
                                    "id": 204466,
                                    "idMal": 63150,
                                    "format": "TV",
                                    "episodes": 12,
                                    "duration": 24,
                                    "seasonYear": 2026,
                                    "startDate": { "year": 2026 },
                                    "title": { "english": "KAIJU GIRL CARAMELISE" }
                                },
                                {
                                    "id": 999999,
                                    "idMal": null,
                                    "format": "TV",
                                    "episodes": 12,
                                    "duration": 24,
                                    "seasonYear": 2026,
                                    "startDate": { "year": 2026 },
                                    "title": { "english": "KAIJU GIRL CARAMELISE" }
                                }
                            ]
                        }
                    }
                }));
        });
        let mutation = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .body_contains("SaveMediaListEntry");
            then.status(500);
        });
        let addon = test_addon(&server);

        let error = addon
            .on_event(
                &TrackingEvent::MarkPlayed,
                &idless_anime_episode(1),
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: sqlx::sqlite::SqlitePoolOptions::new()
                        .connect_lazy("sqlite::memory:")
                        .unwrap(),
                },
            )
            .await
            .expect_err("ambiguous title matches must be rejected");

        assert!(!error.is_retryable());
        assert!(
            error
                .to_string()
                .contains("ambiguous")
        );
        search.assert_hits(1);
        mutation.assert_hits(0);
    }

    #[tokio::test]
    async fn media_list_import_preserves_movie_rating_and_direct_ids() {
        let server = httpmock::MockServer::start();
        let list = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .body_contains("mediaList");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": {
                        "Page": {
                            "pageInfo": { "currentPage": 1, "hasNextPage": false },
                            "mediaList": [{
                                "id": 9,
                                "status": "COMPLETED",
                                "score": 8.5,
                                "progress": 1,
                                "updatedAt": 100,
                                "completedAt": { "year": 2026, "month": 8, "day": 18 },
                                "media": {
                                    "id": 21519,
                                    "idMal": 32281,
                                    "format": "MOVIE",
                                    "episodes": 1,
                                    "duration": 106,
                                    "title": { "userPreferred": "Your Name" }
                                }
                            }]
                        }
                    }
                }));
        });
        let addon = test_addon(&server);

        let remote = addon
            .import_history(
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: sqlx::sqlite::SqlitePoolOptions::new()
                        .connect_lazy("sqlite::memory:")
                        .unwrap(),
                },
            )
            .await
            .unwrap();

        assert_eq!(
            remote
                .items
                .len(),
            1
        );
        let item = &remote.items[0];
        assert_eq!(item.kind, crate::db::MediaKind::Movie);
        assert_eq!(
            item.ids
                .anilist,
            Some(21519)
        );
        assert_eq!(
            item.ids
                .mal,
            Some(32281)
        );
        assert_eq!(item.watched, Some(true));
        assert_eq!(item.rating, Some(Some(8.5)));
        list.assert_hits(1);
    }

    #[test]
    fn series_progress_clears_every_later_episode() {
        let entry = api::MediaListEntry {
            id: 9,
            status: Some(api::MediaListStatus::Current),
            score: None,
            progress: Some(2),
            updated_at: Some(100),
            completed_at: None,
            media: api::Media {
                id: 1,
                id_mal: Some(2),
                format: Some(api::MediaFormat::Tv),
                episodes: Some(4),
                duration: Some(24),
                season_year: Some(2026),
                start_date: None,
                title: api::MediaTitle::default(),
            },
        };
        let mut items = Vec::new();

        append_remote_entry(&mut items, &entry);

        let episode_states = items
            .iter()
            .filter(|item| item.kind == crate::db::MediaKind::Episode)
            .map(|item| (item.episode, item.watched))
            .collect::<Vec<_>>();
        assert_eq!(
            episode_states,
            vec![
                (Some(1), Some(true)),
                (Some(2), Some(true)),
                (Some(3), Some(false)),
                (Some(4), Some(false)),
            ]
        );
    }

    #[tokio::test]
    async fn graphql_errors_inside_http_200_are_not_treated_as_success() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/graphql")
                .body_contains("Viewer");
            then.status(200)
                .json_body(serde_json::json!({
                    "data": null,
                    "errors": [{ "message": "Invalid token", "status": 401 }]
                }));
        });
        let addon = test_addon(&server);

        let error = addon
            .verify(
                &credentials(),
                &TrackingCtx {
                    config: Arc::new(crate::Config::default()),
                    db: sqlx::sqlite::SqlitePoolOptions::new()
                        .connect_lazy("sqlite::memory:")
                        .unwrap(),
                },
            )
            .await
            .expect_err("GraphQL errors must fail verification");

        assert!(error.requires_reauth());
    }
}
