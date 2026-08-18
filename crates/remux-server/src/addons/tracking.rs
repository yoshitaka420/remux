//! Tracking capability: syncing a user's watch activity with an external
//! service (Simkl, Trakt, Yamtrack). Unlike other capabilities this is per-user —
//! the operator configures the addon, each user connects it separately.

use crate::{AppContext, db};
use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Datelike;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use super::AddonKind;
use async_trait::async_trait;

/// Split by what the dispatcher should do next, not by cause.
#[derive(Debug)]
pub enum TrackingError {
    /// Rate limited, 5xx, network. `retry_after` is a provider hint; the
    /// dispatcher waits the longer of it and its own backoff.
    Retryable {
        message: String,
        retry_after: Option<Duration>,
    },
    /// `reauth_required` drives the Reconnect prompt in the UI.
    Permanent {
        message: String,
        reauth_required: bool,
    },
}

impl TrackingError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable {
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn retry_after(message: impl Into<String>, after: Duration) -> Self {
        Self::Retryable {
            message: message.into(),
            retry_after: Some(after),
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent {
            message: message.into(),
            reauth_required: false,
        }
    }

    pub fn reauth(message: impl Into<String>) -> Self {
        Self::Permanent {
            message: message.into(),
            reauth_required: true,
        }
    }

    /// Backstop for a capability the provider never declared. Core gates on
    /// `TrackingCapabilities` first, so this firing means the two disagree.
    pub fn unsupported(what: &str) -> Self {
        Self::permanent(format!("provider does not support {what}"))
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }

    pub fn requires_reauth(&self) -> bool {
        matches!(
            self,
            Self::Permanent {
                reauth_required: true,
                ..
            }
        )
    }
}

impl std::fmt::Display for TrackingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable {
                message,
                retry_after: Some(after),
            } => {
                write!(f, "{message} (retry after {}s)", after.as_secs())
            }
            Self::Retryable { message, .. } => write!(f, "{message}"),
            Self::Permanent {
                message,
                reauth_required: true,
            } => {
                write!(f, "{message} (reconnect required)")
            }
            Self::Permanent { message, .. } => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for TrackingError {}

pub type TrackingResult<T> = std::result::Result<T, TrackingError>;

/// Unit a per-user event filter is expressed in. Serialised into
/// `user_media_trackers.event_filters`, so renaming a variant is a migration.
#[derive(
    strum_macros::EnumString,
    strum_macros::Display,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[sqlx(type_name = "TEXT", rename_all = "snake_case")]
pub enum TrackingEventKind {
    PlaybackStart,
    PlaybackProgress,
    PlaybackStop,
    MarkPlayed,
    MarkUnplayed,
    Favorite,
    Rating,
}

/// One thing that happened to one item, for one user.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TrackingEvent {
    PlaybackStart {
        position_ticks: i64,
    },
    PlaybackProgress {
        position_ticks: i64,
        is_paused: bool,
    },
    PlaybackStop {
        position_ticks: i64,
        /// Passed the watched threshold. Providers scrobble a finish
        /// differently from an abandon.
        played: bool,
    },
    MarkPlayed,
    MarkUnplayed,
    Favorite {
        is_favorite: bool,
    },
    Rating {
        /// The user's own 0-10 rating, or `None` when they cleared it.
        /// Providers using a different scale rescale on the way out.
        rating: Option<f32>,
    },
}

impl TrackingEvent {
    pub fn kind(&self) -> TrackingEventKind {
        match self {
            Self::PlaybackStart { .. } => TrackingEventKind::PlaybackStart,
            Self::PlaybackProgress { .. } => TrackingEventKind::PlaybackProgress,
            Self::PlaybackStop { .. } => TrackingEventKind::PlaybackStop,
            Self::MarkPlayed => TrackingEventKind::MarkPlayed,
            Self::MarkUnplayed => TrackingEventKind::MarkUnplayed,
            Self::Favorite { .. } => TrackingEventKind::Favorite,
            Self::Rating { .. } => TrackingEventKind::Rating,
        }
    }

    pub fn position_ticks(&self) -> Option<i64> {
        match self {
            Self::PlaybackStart { position_ticks }
            | Self::PlaybackProgress { position_ticks, .. }
            | Self::PlaybackStop { position_ticks, .. } => Some(*position_ticks),
            Self::MarkPlayed
            | Self::MarkUnplayed
            | Self::Favorite { .. }
            | Self::Rating { .. } => None,
        }
    }
}

/// A media item resolved into what a provider needs to identify it remotely.
/// Core walks to the series via `Media::get_ancestors` once so addons never
/// need a DB handle. No `Default`: there is no meaningful default `MediaKind`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackingTarget {
    pub kind: db::MediaKind,
    pub title: String,
    pub year: Option<i32>,
    pub ids: TrackingIds,
    /// Set for episodes: the parent series' title, year and ids.
    pub series: Option<Box<TrackingTarget>>,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// Local runtime in Jellyfin ticks (100 ns). Tracking providers use this
    /// to convert event positions into provider-specific progress values.
    pub runtime_ticks: Option<i64>,
}

/// The durable representation written to the media-tracker outbox. Resolving
/// the local media tree happens before the request returns, so retries do not
/// depend on the library item still existing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaTrackerOutboxPayload {
    pub event: TrackingEvent,
    pub target: TrackingTarget,
}

/// The ids tracking services key on — narrower than `db::ExternalIds`, which
/// also carries music, IPTV, and addon-private identifiers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackingIds {
    pub imdb: Option<String>,
    pub tmdb: Option<i64>,
    pub tvdb: Option<i64>,
    pub kitsu: Option<i64>,
    pub mal: Option<i64>,
    pub anilist: Option<i64>,
}

impl TrackingIds {
    /// Nothing to match on. Core drops the action rather than queueing
    /// something no provider can act on.
    pub fn is_empty(&self) -> bool {
        self.imdb
            .is_none()
            && self
                .tmdb
                .is_none()
            && self
                .tvdb
                .is_none()
            && self
                .kitsu
                .is_none()
            && self
                .mal
                .is_none()
            && self
                .anilist
                .is_none()
    }
}

/// Opaque to core: a static webhook token and an OAuth triple look the same.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrackingCredentials(pub serde_json::Value);

impl TrackingCredentials {
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.0
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

static CREDENTIAL_KEYS: LazyLock<Mutex<HashMap<PathBuf, [u8; 32]>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn read_credential_key(path: &Path) -> TrackingResult<[u8; 32]> {
    let encoded = std::fs::read_to_string(path).map_err(|e| {
        TrackingError::permanent(format!("reading credential key: {e}"))
    })?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded.trim())
        .map_err(|e| {
            TrackingError::permanent(format!("decoding credential key: {e}"))
        })?;
    bytes
        .try_into()
        .map_err(|_| {
            TrackingError::permanent(
                "tracking credential key must contain exactly 32 bytes",
            )
        })
}

fn credential_key(config: &crate::Config) -> TrackingResult<[u8; 32]> {
    let path = config
        .data_dir
        .join("tracking-credentials.key");
    if let Some(key) = CREDENTIAL_KEYS
        .lock()
        .map_err(|_| TrackingError::permanent("credential key cache is poisoned"))?
        .get(&path)
        .copied()
    {
        return Ok(key);
    }

    let key = if path.exists() {
        read_credential_key(&path)?
    } else {
        std::fs::create_dir_all(&config.data_dir).map_err(|e| {
            TrackingError::permanent(format!(
                "creating data directory for credential key: {e}"
            ))
        })?;
        let mut generated = [0u8; 32];
        OsRng.fill_bytes(&mut generated);
        let encoded = URL_SAFE_NO_PAD.encode(generated);
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(mut file) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    file.set_permissions(std::fs::Permissions::from_mode(0o600))
                        .map_err(|e| {
                            TrackingError::permanent(format!(
                                "securing tracking credential key: {e}"
                            ))
                        })?;
                }
                file.write_all(encoded.as_bytes())
                    .map_err(|e| {
                        TrackingError::permanent(format!(
                            "writing tracking credential key: {e}"
                        ))
                    })?;
                generated
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                read_credential_key(&path)?
            }
            Err(e) => {
                return Err(TrackingError::permanent(format!(
                    "creating tracking credential key: {e}"
                )));
            }
        }
    };

    CREDENTIAL_KEYS
        .lock()
        .map_err(|_| TrackingError::permanent("credential key cache is poisoned"))?
        .insert(path, key);
    Ok(key)
}

/// Encrypt credentials before they enter SQLite. The key is generated once in
/// the server data directory with owner-only permissions and must be backed up
/// with the database.
pub fn seal_credentials(
    credentials: &TrackingCredentials,
    config: &crate::Config,
) -> TrackingResult<TrackingCredentials> {
    if credentials
        .0
        .get("ciphertext")
        .is_some()
    {
        return Ok(credentials.clone());
    }
    let key = credential_key(config)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|_| TrackingError::permanent("invalid tracking credential key"))?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let plaintext = serde_json::to_vec(&credentials.0)
        .map_err(|e| TrackingError::permanent(format!("encoding credentials: {e}")))?;
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce_bytes), plaintext.as_ref())
        .map_err(|_| {
            TrackingError::permanent("encrypting tracking credentials failed")
        })?;
    let mut envelope = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&ciphertext);
    Ok(TrackingCredentials::new(serde_json::json!({
        "version": 1,
        "ciphertext": URL_SAFE_NO_PAD.encode(envelope),
    })))
}

/// Decrypt credentials loaded from SQLite. Plain JSON remains readable for
/// compatibility with development rows created before encryption landed.
pub fn open_credentials(
    credentials: &TrackingCredentials,
    config: &crate::Config,
) -> TrackingResult<TrackingCredentials> {
    let Some(encoded) = credentials.get_str("ciphertext") else {
        return Ok(credentials.clone());
    };
    let envelope = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| {
            TrackingError::permanent("stored tracking credentials are corrupt")
        })?;
    if envelope.len() <= 12 {
        return Err(TrackingError::permanent(
            "stored tracking credentials are truncated",
        ));
    }
    let (nonce, ciphertext) = envelope.split_at(12);
    let key = credential_key(config)?;
    let cipher = Aes256Gcm::new_from_slice(&key)
        .map_err(|_| TrackingError::permanent("invalid tracking credential key"))?;
    let plaintext = cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| {
            TrackingError::permanent(
                "tracking credentials could not be decrypted; restore tracking-credentials.key",
            )
        })?;
    let value = serde_json::from_slice(&plaintext).map_err(|_| {
        TrackingError::permanent("decrypted tracking credentials are invalid")
    })?;
    Ok(TrackingCredentials::new(value))
}

/// Drives which connect UI the dashboard renders, without it knowing the
/// provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthFlow {
    /// User pastes a value, described by `connect_fields`.
    Token,
    /// User enters a code on the provider's site; server polls. Suits TV
    /// clients with no browser.
    OAuthDeviceCode,
    /// Redirect plus callback. Needs a publicly reachable server.
    OAuthRedirect,
}

#[derive(Debug, Clone)]
pub struct DeviceAuthStart {
    pub verification_url: String,
    pub user_code: String,
    /// Opaque handle for `poll_device_auth`.
    pub poll_token: String,
    /// Providers reject polling faster than this.
    pub interval: Duration,
    pub expires_in: Duration,
}

#[derive(Debug, Clone)]
pub enum DeviceAuthPoll {
    Pending,
    Approved(TrackingCredentials),
    /// Declined or expired. Start over.
    Denied,
}

#[derive(Debug, Clone)]
pub struct RedirectAuthStart {
    pub authorization_url: String,
    pub expires_in: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncDirection {
    #[default]
    None,
    /// Remux to provider.
    Push,
    /// Provider to remux.
    Pull,
    Both,
}

impl SyncDirection {
    pub fn pushes(self) -> bool {
        matches!(self, Self::Push | Self::Both)
    }

    pub fn pulls(self) -> bool {
        matches!(self, Self::Pull | Self::Both)
    }
}

/// Static, no-I/O declaration of what a provider can do. Core reads these to
/// decide what to offer and what to call, so it never matches on provider id.
#[derive(Debug, Clone)]
pub struct TrackingCapabilities {
    pub auth_flow: AuthFlow,
    /// `Token` only. Reuses the addon option schema so the dashboard
    /// renders it with the same generic form code as addon settings.
    pub connect_fields: Vec<remux_sdks::remux::AddonOption>,
    pub supported_events: Vec<TrackingEventKind>,
    /// Must be a subset of `supported_events`.
    pub default_event_filter: Vec<TrackingEventKind>,
    pub history_import: bool,
    /// Partial playback positions, carried on `RemoteWatch` by
    /// `import_history` and `pull_changes` rather than by a method of its own.
    pub progress_import: bool,
    pub watch_state_sync: SyncDirection,
    pub favorites: SyncDirection,
    /// Personal 0-10 ratings. Separate from `favorites` because a provider can
    /// take one without the other.
    pub ratings: SyncDirection,
    pub watchlist: SyncDirection,
}

impl Default for TrackingCapabilities {
    /// Providers opt in, so a new capability never silently turns on for an
    /// existing addon.
    fn default() -> Self {
        Self {
            auth_flow: AuthFlow::Token,
            connect_fields: Vec::new(),
            supported_events: Vec::new(),
            default_event_filter: Vec::new(),
            history_import: false,
            progress_import: false,
            watch_state_sync: SyncDirection::None,
            favorites: SyncDirection::None,
            ratings: SyncDirection::None,
            watchlist: SyncDirection::None,
        }
    }
}

impl TrackingCapabilities {
    pub fn supports(&self, kind: TrackingEventKind) -> bool {
        self.supported_events
            .contains(&kind)
    }
}

/// Per-call context. No DB handle, matching `MetricsCtx`.
#[derive(Clone)]
pub struct TrackingCtx {
    pub config: Arc<crate::Config>,
}

/// One external tracking service. Bulk-sync methods default to `unsupported`
/// so a provider implements only what its capabilities advertise.
#[async_trait]
pub trait TrackingAddon: AddonKind + Send + Sync {
    /// Must be cheap and do no I/O — called while rendering pages.
    fn capabilities(&self) -> TrackingCapabilities;

    /// Providers can narrow an otherwise-supported event for a particular
    /// media shape (for example, Simkl has episode scrobbling but no episode
    /// ratings). Returning false keeps an impossible action out of the outbox.
    fn supports_event(&self, event: &TrackingEvent, _target: &TrackingTarget) -> bool {
        self.capabilities()
            .supports(event.kind())
    }

    /// Should hit the provider so a bad token is rejected while the user is
    /// still on the form, not later as a failed scrobble.
    async fn connect_with_token(
        &self,
        _fields: &serde_json::Value,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<TrackingCredentials> {
        Err(TrackingError::unsupported("token authentication"))
    }

    async fn begin_device_auth(
        &self,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<DeviceAuthStart> {
        Err(TrackingError::unsupported("device-code authentication"))
    }

    async fn poll_device_auth(
        &self,
        _poll_token: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<DeviceAuthPoll> {
        Err(TrackingError::unsupported("device-code authentication"))
    }

    async fn begin_redirect_auth(
        &self,
        _state: &str,
        _redirect_uri: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RedirectAuthStart> {
        Err(TrackingError::unsupported("redirect authentication"))
    }

    async fn complete_redirect_auth(
        &self,
        _code: &str,
        _redirect_uri: &str,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<TrackingCredentials> {
        Err(TrackingError::unsupported("redirect authentication"))
    }

    /// Default suits providers whose credentials do not expire.
    async fn refresh(
        &self,
        creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<TrackingCredentials> {
        Ok(creds.clone())
    }

    /// Backs the connection health indicator. Should be cheap.
    async fn verify(
        &self,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        Ok(())
    }

    /// Core deletes its row regardless: disconnecting must succeed locally
    /// even when the provider is down.
    async fn disconnect(
        &self,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        Ok(())
    }

    async fn on_event(
        &self,
        event: &TrackingEvent,
        target: &TrackingTarget,
        creds: &TrackingCredentials,
        ctx: &TrackingCtx,
    ) -> TrackingResult<()>;

    async fn import_history(
        &self,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RemoteSync> {
        Err(TrackingError::unsupported("history import"))
    }

    /// `since` is the provider's opaque cursor from the previous successful
    /// pull. Providers return the replacement cursor alongside the changes so
    /// core never has to manufacture a timestamp on their behalf.
    async fn pull_changes(
        &self,
        _since: Option<String>,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<RemoteSync> {
        Err(TrackingError::unsupported("external change sync"))
    }

    async fn pull_watchlist(
        &self,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<Vec<TrackingIds>> {
        Err(TrackingError::unsupported("watchlist sync"))
    }

    async fn push_watchlist(
        &self,
        _target: &TrackingTarget,
        _add: bool,
        _creds: &TrackingCredentials,
        _ctx: &TrackingCtx,
    ) -> TrackingResult<()> {
        Err(TrackingError::unsupported("watchlist sync"))
    }
}

/// One successful provider pull. The cursor is deliberately opaque: some
/// services require their exact watermark string to be sent back unchanged.
#[derive(Debug, Clone, Default)]
pub struct RemoteSync {
    pub items: Vec<RemoteWatch>,
    pub cursor: String,
    pub payload_bytes: usize,
}

/// One item's user data read back from a provider, by `import_history` and
/// `pull_changes`.
#[derive(Debug, Clone)]
pub struct RemoteWatch {
    pub kind: db::MediaKind,
    pub ids: TrackingIds,
    pub season: Option<i64>,
    pub episode: Option<i64>,
    /// `None` means the provider returned only progress/rating data and made no
    /// assertion about watched state. This prevents an unwatched rating from
    /// accidentally clearing a local play state.
    pub watched: Option<bool>,
    /// Absolute progress when the provider reports its native value in ticks.
    pub position_ticks: Option<i64>,
    /// Percentage progress when the provider does not return a runtime. Core
    /// converts this with the matched local item's runtime.
    pub position_percent: Option<f64>,
    pub watched_at: Option<chrono::NaiveDateTime>,
    /// `None` when the provider does not report favourites. Without this a
    /// provider could declare `favorites: Pull` that core had no way to act on.
    pub favorite: Option<bool>,
    /// Outer `None`: provider made no rating assertion. `Some(None)`: the user
    /// explicitly has no rating. `Some(Some(value))`: remote 0-10 rating.
    pub rating: Option<Option<f32>>,
}

fn ids_for(media: &db::Media) -> TrackingIds {
    TrackingIds {
        imdb: media
            .external_ids
            .imdb
            .as_ref()
            .map(ToString::to_string),
        tmdb: media
            .external_ids
            .tmdb,
        tvdb: media
            .external_ids
            .tvdb,
        kitsu: media
            .external_ids
            .kitsu,
        mal: media
            .external_ids
            .mal,
        anilist: media
            .external_ids
            .anilist,
    }
}

fn basic_target(media: &db::Media) -> TrackingTarget {
    TrackingTarget {
        kind: media
            .kind
            .clone(),
        title: media
            .title
            .clone(),
        year: media
            .released_at
            .map(|date| date.year()),
        ids: ids_for(media),
        series: None,
        season: None,
        episode: None,
        runtime_ticks: media
            .runtime
            .and_then(|seconds| seconds.checked_mul(10_000_000)),
    }
}

/// Convert a local media row into the stable identifiers understood by
/// tracking providers. Unsupported kinds and rows with no usable remote IDs
/// are deliberately ignored.
pub async fn resolve_target(
    db_pool: &sqlx::SqlitePool,
    media: &db::Media,
) -> anyhow::Result<Option<TrackingTarget>> {
    let mut target = basic_target(media);
    match media
        .kind
        .clone()
    {
        db::MediaKind::Movie | db::MediaKind::Series => Ok((!target
            .ids
            .is_empty())
        .then_some(target)),
        db::MediaKind::Season | db::MediaKind::Episode => {
            let ancestors = db::Media::get_ancestors(db_pool, &media.id).await?;
            let Some(series) = ancestors
                .iter()
                .find(|ancestor| ancestor.kind == db::MediaKind::Series)
            else {
                return Ok(None);
            };
            let series_target = basic_target(series);
            if series_target
                .ids
                .is_empty()
                && target
                    .ids
                    .is_empty()
            {
                return Ok(None);
            }
            target.series = Some(Box::new(series_target));
            target.season = if media.kind == db::MediaKind::Season {
                media.idx
            } else {
                media
                    .parent_idx
                    .or_else(|| {
                        ancestors
                            .iter()
                            .find(|ancestor| ancestor.kind == db::MediaKind::Season)
                            .and_then(|season| season.idx)
                    })
            };
            target.episode = (media.kind == db::MediaKind::Episode)
                .then_some(media.idx)
                .flatten();
            if target
                .season
                .is_none()
                || (media.kind == db::MediaKind::Episode
                    && target
                        .episode
                        .is_none())
            {
                return Ok(None);
            }
            Ok(Some(target))
        }
        _ => Ok(None),
    }
}

/// Fan one local event out to every connected tracking addon that supports it.
/// Rows are inserted before the handler returns; the task poke only reduces
/// latency and is not required for durability.
pub async fn enqueue_event(
    ctx: &AppContext,
    user_id: uuid::Uuid,
    media: &db::Media,
    event: TrackingEvent,
) -> anyhow::Result<usize> {
    enqueue_event_with_origin(ctx, user_id, media, event, None).await
}

/// Fan out an event created by a primary provider. Only mirrors receive it and
/// the source connection is excluded, preventing an inbound/outbound echo.
pub async fn enqueue_event_from_provider(
    ctx: &AppContext,
    user_id: uuid::Uuid,
    media: &db::Media,
    event: TrackingEvent,
    origin_connection_id: uuid::Uuid,
) -> anyhow::Result<usize> {
    enqueue_event_with_origin(ctx, user_id, media, event, Some(origin_connection_id))
        .await
}

async fn enqueue_event_with_origin(
    ctx: &AppContext,
    user_id: uuid::Uuid,
    media: &db::Media,
    event: TrackingEvent,
    origin_connection_id: Option<uuid::Uuid>,
) -> anyhow::Result<usize> {
    let Some(target) = resolve_target(&ctx.db, media).await? else {
        return Ok(0);
    };
    let event_kind = event.kind();
    let mut inserted = 0;

    for runtime in ctx
        .addons
        .tracking_addons()
        .into_iter()
        .filter(|runtime| runtime.supports_type(&media.kind))
    {
        let Some(provider) = runtime
            .tracking
            .as_ref()
        else {
            continue;
        };
        if !provider.supports_event(&event, &target) {
            continue;
        }
        let Some(connection) = db::UserMediaTracker::get_for_user_and_addon(
            &ctx.db,
            user_id,
            runtime
                .row
                .id,
        )
        .await?
        else {
            continue;
        };
        if !connection.wants(event_kind) {
            continue;
        }
        if let Some(origin) = origin_connection_id {
            if connection.id == origin
                || connection.sync_role != db::MediaTrackerRole::Mirror
            {
                continue;
            }
        }

        let payload = serde_json::to_string(&MediaTrackerOutboxPayload {
            event: event.clone(),
            target: target.clone(),
        })?;
        let row = db::MediaTrackerOutbox::new(connection.id, event_kind, payload);
        let row = match origin_connection_id {
            Some(origin) => row.with_origin(origin),
            None => row,
        };
        row.insert(&ctx.db)
            .await?;
        inserted += 1;
    }

    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_and_permanent_are_distinguishable() {
        assert!(TrackingError::retryable("boom").is_retryable());
        assert!(!TrackingError::permanent("nope").is_retryable());
        assert!(!TrackingError::reauth("bad token").is_retryable());
    }

    #[test]
    fn only_reauth_errors_ask_the_user_to_reconnect() {
        assert!(TrackingError::reauth("401").requires_reauth());
        assert!(!TrackingError::permanent("400 bad request").requires_reauth());
        assert!(!TrackingError::retryable("timeout").requires_reauth());
    }

    #[test]
    fn unsupported_is_permanent_and_never_retried() {
        let err = TrackingError::unsupported("watchlist sync");
        assert!(!err.is_retryable());
        assert!(!err.requires_reauth());
        assert!(
            err.to_string()
                .contains("watchlist sync")
        );
    }

    #[test]
    fn retry_after_is_carried_and_shown() {
        let err = TrackingError::retry_after("429", Duration::from_secs(30));
        match &err {
            TrackingError::Retryable {
                retry_after: Some(d),
                ..
            } => assert_eq!(d.as_secs(), 30),
            other => panic!("expected a retry-after hint, got {other:?}"),
        }
        assert!(
            err.to_string()
                .contains("30")
        );
    }

    /// Filters are stored as strings; a variant that does not round-trip would
    /// silently drop a user's filter on reload.
    #[test]
    fn event_kinds_round_trip_through_their_string_form() {
        for kind in [
            TrackingEventKind::PlaybackStart,
            TrackingEventKind::PlaybackProgress,
            TrackingEventKind::PlaybackStop,
            TrackingEventKind::MarkPlayed,
            TrackingEventKind::MarkUnplayed,
            TrackingEventKind::Favorite,
            TrackingEventKind::Rating,
        ] {
            let s = kind.to_string();
            assert_eq!(
                s.parse::<TrackingEventKind>()
                    .unwrap(),
                kind
            );
            let json = serde_json::to_string(&kind).unwrap();
            assert_eq!(
                serde_json::from_str::<TrackingEventKind>(&json).unwrap(),
                kind
            );
            // serde and strum must agree, or a filter written by the API would
            // not match one read by the dispatcher.
            assert_eq!(json, format!("\"{s}\""));
        }
    }

    #[test]
    fn every_event_reports_its_own_kind() {
        let cases = [
            (
                TrackingEvent::PlaybackStart { position_ticks: 0 },
                TrackingEventKind::PlaybackStart,
            ),
            (
                TrackingEvent::PlaybackProgress {
                    position_ticks: 1,
                    is_paused: true,
                },
                TrackingEventKind::PlaybackProgress,
            ),
            (
                TrackingEvent::PlaybackStop {
                    position_ticks: 2,
                    played: true,
                },
                TrackingEventKind::PlaybackStop,
            ),
            (TrackingEvent::MarkPlayed, TrackingEventKind::MarkPlayed),
            (TrackingEvent::MarkUnplayed, TrackingEventKind::MarkUnplayed),
            (
                TrackingEvent::Favorite { is_favorite: true },
                TrackingEventKind::Favorite,
            ),
            (
                TrackingEvent::Rating { rating: Some(7.0) },
                TrackingEventKind::Rating,
            ),
            (
                TrackingEvent::Rating { rating: None },
                TrackingEventKind::Rating,
            ),
        ];
        for (event, want) in cases {
            assert_eq!(event.kind(), want, "wrong kind for {event:?}");
        }
    }

    #[test]
    fn only_playback_events_carry_a_position() {
        assert_eq!(
            TrackingEvent::PlaybackStop {
                position_ticks: 99,
                played: false,
            }
            .position_ticks(),
            Some(99)
        );
        assert_eq!(TrackingEvent::MarkPlayed.position_ticks(), None);
        assert_eq!(
            TrackingEvent::Favorite { is_favorite: false }.position_ticks(),
            None
        );
        assert_eq!(
            TrackingEvent::Rating { rating: Some(7.0) }.position_ticks(),
            None
        );
    }

    #[test]
    fn ids_are_empty_only_when_nothing_is_matchable() {
        assert!(TrackingIds::default().is_empty());
        assert!(
            !TrackingIds {
                imdb: Some("tt123".into()),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !TrackingIds {
                tmdb: Some(603),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !TrackingIds {
                tvdb: Some(1),
                ..Default::default()
            }
            .is_empty()
        );
    }

    #[test]
    fn sync_direction_resolves_both_ways() {
        assert!(!SyncDirection::None.pushes() && !SyncDirection::None.pulls());
        assert!(SyncDirection::Push.pushes() && !SyncDirection::Push.pulls());
        assert!(!SyncDirection::Pull.pushes() && SyncDirection::Pull.pulls());
        assert!(SyncDirection::Both.pushes() && SyncDirection::Both.pulls());
    }

    /// A provider that forgets to declare an event should send nothing.
    #[test]
    fn default_capabilities_grant_nothing() {
        let caps = TrackingCapabilities::default();
        assert!(
            caps.supported_events
                .is_empty()
        );
        assert!(!caps.history_import);
        assert!(!caps.progress_import);
        assert_eq!(caps.watch_state_sync, SyncDirection::None);
        assert_eq!(caps.favorites, SyncDirection::None);
        assert_eq!(caps.ratings, SyncDirection::None);
        assert_eq!(caps.watchlist, SyncDirection::None);
        assert!(!caps.supports(TrackingEventKind::PlaybackStop));
    }

    /// A provider declaring `favorites: Pull` needs somewhere to put one, and a
    /// favourite is independent of watched state.
    #[test]
    fn remote_user_data_can_carry_a_favourite_on_its_own() {
        let watch = RemoteWatch {
            kind: db::MediaKind::Movie,
            ids: TrackingIds {
                tmdb: Some(603),
                ..Default::default()
            },
            season: None,
            episode: None,
            watched: None,
            position_ticks: None,
            position_percent: None,
            watched_at: None,
            favorite: Some(true),
            rating: None,
        };
        assert_eq!(watch.favorite, Some(true));
        assert_eq!(watch.watched, None);
    }

    /// Same reasoning for `ratings: Pull`: a rating is independent of both
    /// watched state and favourite, so it has to survive on its own.
    #[test]
    fn remote_user_data_can_carry_a_rating_on_its_own() {
        let watch = RemoteWatch {
            kind: db::MediaKind::Movie,
            ids: TrackingIds {
                tmdb: Some(603),
                ..Default::default()
            },
            season: None,
            episode: None,
            watched: None,
            position_ticks: None,
            position_percent: None,
            watched_at: None,
            favorite: None,
            rating: Some(Some(7.0)),
        };
        assert_eq!(watch.rating, Some(Some(7.0)));
        assert_eq!(watch.favorite, None);
        assert_eq!(watch.watched, None);
    }

    #[test]
    fn credentials_are_encrypted_at_rest_and_round_trip() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = crate::Config::default();
        config.data_dir = temp
            .path()
            .to_path_buf();
        let plaintext = TrackingCredentials::new(serde_json::json!({
            "access_token": "secret-token"
        }));

        let sealed = seal_credentials(&plaintext, &config).unwrap();
        assert_eq!(
            sealed
                .0
                .get("version")
                .and_then(|value| value.as_i64()),
            Some(1)
        );
        assert!(
            !sealed
                .0
                .to_string()
                .contains("secret-token")
        );
        assert_eq!(
            open_credentials(&sealed, &config)
                .unwrap()
                .0,
            plaintext.0
        );

        let key_path = temp
            .path()
            .join("tracking-credentials.key");
        assert!(key_path.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(key_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn legacy_plaintext_credentials_remain_readable() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = crate::Config::default();
        config.data_dir = temp
            .path()
            .to_path_buf();
        let plaintext = TrackingCredentials::new(serde_json::json!({
            "access_token": "old-row"
        }));
        assert_eq!(
            open_credentials(&plaintext, &config)
                .unwrap()
                .0,
            plaintext.0
        );
        assert!(
            !temp
                .path()
                .join("tracking-credentials.key")
                .exists()
        );
    }
}
