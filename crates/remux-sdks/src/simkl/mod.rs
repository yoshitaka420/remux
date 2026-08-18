use crate::{Auth, Body, ClientError, Endpoint, RestClient};
use http::Method;
use serde::{Deserialize, Serialize};

pub const DEFAULT_BASE_URL: &str = "https://api.simkl.com";

#[derive(Clone, Debug)]
pub struct SimklAuth {
    pub client_id: String,
    pub app_name: String,
    pub app_version: String,
    pub access_token: Option<String>,
}

impl Auth for SimklAuth {
    fn apply(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let req = req
            .query(&[
                (
                    "client_id",
                    self.client_id
                        .as_str(),
                ),
                (
                    "app-name",
                    self.app_name
                        .as_str(),
                ),
                (
                    "app-version",
                    self.app_version
                        .as_str(),
                ),
            ])
            .header("Accept", "application/json")
            .header(
                "User-Agent",
                format!("{}/{}", self.app_name, self.app_version),
            );
        match self
            .access_token
            .as_deref()
        {
            Some(token) => req.bearer_auth(token),
            None => req,
        }
    }
}

fn simkl_error(status: u16, endpoint: &str, body: &str) -> ClientError {
    let parsed = serde_json::from_str::<serde_json::Value>(body).ok();
    let code = parsed
        .as_ref()
        .and_then(|v| {
            v.get("error")
                .or_else(|| v.get("code"))
        })
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let message = parsed
        .as_ref()
        .and_then(|v| {
            v.get("message")
                .or_else(|| v.get("error_description"))
        })
        .and_then(|v| v.as_str())
        .unwrap_or(code)
        .trim();

    // Simkl's per-user scrobble lock is reported as HTTP 400 RATE_LIMIT.
    if status == 400 && code.eq_ignore_ascii_case("RATE_LIMIT") {
        return ClientError::RateLimited {
            retry_after_secs: 20,
        };
    }

    ClientError::Http {
        status,
        message: if message.is_empty() {
            format!("Simkl request failed ({status})")
        } else {
            message.to_string()
        },
        endpoint: Some(endpoint.to_string()),
        body: Some(body.to_string()),
    }
}

pub fn client(
    client_id: &str,
    access_token: Option<&str>,
    base_url: &str,
    app_version: &str,
) -> Result<RestClient<SimklAuth>, url::ParseError> {
    Ok(RestClient::new(base_url)?
        .with_auth(SimklAuth {
            client_id: client_id.to_string(),
            app_name: "remux".to_string(),
            app_version: app_version.to_string(),
            access_token: access_token.map(str::to_string),
        })
        .with_error_mapper(simkl_error))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FlexibleId {
    Number(i64),
    String(String),
}

impl FlexibleId {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::String(value) => value
                .parse()
                .ok(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Ids {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub simkl: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imdb: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tmdb: Option<FlexibleId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvdb: Option<FlexibleId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kitsu: Option<FlexibleId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MediaRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<i32>,
    #[serde(default)]
    pub ids: Ids,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EpisodeRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub season: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none", alias = "episode")]
    pub number: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watched_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Ids>,
    /// TVDB coordinates attached by `extended=full_anime_seasons`. Simkl's
    /// native anime episode number can otherwise be absolute or cour-local.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvdb: Option<TvdbEpisodeRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvdb_season: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tvdb_number: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TvdbEpisodeRef {
    pub season: i64,
    pub episode: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScrobbleBody {
    pub progress: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub movie: Option<MediaRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show: Option<MediaRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anime: Option<MediaRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode: Option<EpisodeRef>,
}

#[derive(Debug, Clone, Copy)]
pub enum ScrobbleAction {
    Start,
    Pause,
    Stop,
}

impl ScrobbleAction {
    fn path(self) -> &'static str {
        match self {
            Self::Start => "scrobble/start",
            Self::Pause => "scrobble/pause",
            Self::Stop => "scrobble/stop",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScrobbleEndpoint {
    pub action: ScrobbleAction,
    pub body: ScrobbleBody,
}

impl Endpoint for ScrobbleEndpoint {
    type Output = serde_json::Value;

    fn path(&self) -> String {
        self.action
            .path()
            .to_string()
    }
    fn method(&self) -> Method {
        Method::POST
    }
    fn body(&self) -> Body {
        Body::Json(serde_json::to_value(&self.body).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinStartResponse {
    pub result: String,
    pub device_code: Option<String>,
    pub user_code: String,
    pub verification_uri: Option<String>,
    pub verification_url: Option<String>,
    pub expires_in: u64,
    pub interval: u64,
}

impl PinStartResponse {
    pub fn verification_url(&self) -> Option<&str> {
        self.verification_uri
            .as_deref()
            .or(self
                .verification_url
                .as_deref())
    }
}

#[derive(Debug, Clone, Default)]
pub struct BeginPinEndpoint;

impl Endpoint for BeginPinEndpoint {
    type Output = PinStartResponse;
    fn path(&self) -> String {
        "oauth/pin".to_string()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinPollResponse {
    pub result: String,
    pub access_token: Option<String>,
    pub message: Option<String>,
    pub device_code: Option<String>,
    pub user_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PollPinEndpoint {
    pub user_code: String,
}

impl Endpoint for PollPinEndpoint {
    type Output = PinPollResponse;
    fn path(&self) -> String {
        format!("oauth/pin/{}", self.user_code)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserSettings {
    #[serde(default)]
    pub user: serde_json::Value,
    #[serde(default)]
    pub account: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct UserSettingsEndpoint;

impl Endpoint for UserSettingsEndpoint {
    type Output = UserSettings;
    fn path(&self) -> String {
        "users/settings".to_string()
    }
    fn method(&self) -> Method {
        Method::POST
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SeasonRef {
    pub number: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub episodes: Vec<EpisodeRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<i32>,
    #[serde(default)]
    pub ids: Ids,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watched_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rating: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub seasons: Vec<SeasonRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_tvdb_anime_seasons: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncWriteBody {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub movies: Vec<SyncItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shows: Vec<SyncItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub anime: Vec<SyncItem>,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncWriteAction {
    AddHistory,
    RemoveHistory,
    AddRatings,
    RemoveRatings,
}

impl SyncWriteAction {
    fn path(self) -> &'static str {
        match self {
            Self::AddHistory => "sync/history",
            Self::RemoveHistory => "sync/history/remove",
            Self::AddRatings => "sync/ratings",
            Self::RemoveRatings => "sync/ratings/remove",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SyncWriteEndpoint {
    pub action: SyncWriteAction,
    pub body: SyncWriteBody,
}

impl Endpoint for SyncWriteEndpoint {
    type Output = serde_json::Value;
    fn path(&self) -> String {
        self.action
            .path()
            .to_string()
    }
    fn method(&self) -> Method {
        Method::POST
    }
    fn body(&self) -> Body {
        Body::Json(serde_json::to_value(&self.body).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Activities {
    pub all: Option<String>,
    #[serde(default)]
    pub settings: serde_json::Value,
    #[serde(default)]
    pub tv_shows: serde_json::Value,
    #[serde(default)]
    pub anime: serde_json::Value,
    #[serde(default)]
    pub movies: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct ActivitiesEndpoint;

impl Endpoint for ActivitiesEndpoint {
    type Output = Activities;
    fn path(&self) -> String {
        "sync/activities".to_string()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AllItemsEntry {
    pub added_to_watchlist_at: Option<String>,
    pub last_watched_at: Option<String>,
    pub user_rated_at: Option<String>,
    pub user_rating: Option<i32>,
    pub status: Option<String>,
    pub show: Option<MediaRef>,
    /// Some anime-specific responses use `anime` instead of the cross-mapped
    /// `show` envelope. Accept both shapes.
    pub anime: Option<MediaRef>,
    pub movie: Option<MediaRef>,
    #[serde(default)]
    pub mapped_tvdb_seasons: Vec<i64>,
    #[serde(default)]
    pub seasons: Vec<SeasonRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AllItemsResponse {
    #[serde(default)]
    pub shows: Vec<AllItemsEntry>,
    #[serde(default)]
    pub movies: Vec<AllItemsEntry>,
    #[serde(default)]
    pub anime: Vec<AllItemsEntry>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AllItemsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode_watched_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_all_episodes: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct AllItemsEndpoint {
    pub media_type: Option<String>,
    pub status: Option<String>,
    pub params: AllItemsParams,
}

impl Endpoint for AllItemsEndpoint {
    type Output = AllItemsResponse;
    fn path(&self) -> String {
        match (&self.media_type, &self.status) {
            (Some(kind), Some(status)) => format!("sync/all-items/{kind}/{status}"),
            (Some(kind), None) => format!("sync/all-items/{kind}"),
            _ => "sync/all-items".to_string(),
        }
    }
    fn query_params(&self) -> impl Serialize + '_ {
        &self.params
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlaybackSession {
    pub id: Option<i64>,
    pub progress: Option<f64>,
    #[serde(alias = "paused_at")]
    pub watched_at: Option<String>,
    pub movie: Option<MediaRef>,
    pub show: Option<MediaRef>,
    pub anime: Option<MediaRef>,
    pub episode: Option<EpisodeRef>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PlaybackParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default)]
pub struct PlaybackEndpoint {
    pub media_type: Option<String>,
    pub params: PlaybackParams,
}

impl Endpoint for PlaybackEndpoint {
    type Output = Vec<PlaybackSession>;
    fn path(&self) -> String {
        self.media_type
            .as_ref()
            .map(|kind| format!("sync/playback/{kind}"))
            .unwrap_or_else(|| "sync/playback".to_string())
    }
    fn query_params(&self) -> impl Serialize + '_ {
        &self.params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrobble_body_omits_unused_media_shapes() {
        let body = ScrobbleBody {
            progress: 42.25,
            movie: Some(MediaRef {
                ids: Ids {
                    imdb: Some("tt1375666".into()),
                    tmdb: Some(FlexibleId::Number(27205)),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        let value = serde_json::to_value(body).unwrap();
        assert!(
            value
                .get("movie")
                .is_some()
        );
        assert!(
            value
                .get("show")
                .is_none()
        );
        assert_eq!(value["progress"], 42.25);
    }

    #[test]
    fn flexible_ids_accept_numbers_and_strings() {
        let ids: Ids = serde_json::from_value(serde_json::json!({
            "tmdb": "27205",
            "tvdb": 113
        }))
        .unwrap();
        assert_eq!(
            ids.tmdb
                .and_then(|id| id.as_i64()),
            Some(27205)
        );
        assert_eq!(
            ids.tvdb
                .and_then(|id| id.as_i64()),
            Some(113)
        );
    }

    #[test]
    fn pin_response_accepts_both_documented_url_fields() {
        let uri: PinStartResponse = serde_json::from_value(serde_json::json!({
            "result": "OK",
            "user_code": "ABCD",
            "verification_uri": "https://simkl.com/pin",
            "expires_in": 900,
            "interval": 5
        }))
        .unwrap();
        assert_eq!(uri.verification_url(), Some("https://simkl.com/pin"));

        let url: PinStartResponse = serde_json::from_value(serde_json::json!({
            "result": "OK",
            "user_code": "EFGH",
            "verification_url": "https://simkl.com/pin",
            "expires_in": 900,
            "interval": 5
        }))
        .unwrap();
        assert_eq!(url.verification_url(), Some("https://simkl.com/pin"));
    }

    #[test]
    fn history_episode_uses_the_documented_nested_show_shape() {
        let body = SyncWriteBody {
            shows: vec![SyncItem {
                ids: Ids {
                    imdb: Some("tt0903747".into()),
                    ..Default::default()
                },
                seasons: vec![SeasonRef {
                    number: 2,
                    episodes: vec![EpisodeRef {
                        number: Some(3),
                        watched_at: Some("2026-08-18T12:00:00Z".into()),
                        ..Default::default()
                    }],
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["shows"][0]["seasons"][0]["number"], 2);
        assert_eq!(value["shows"][0]["seasons"][0]["episodes"][0]["number"], 3);
        assert!(
            value
                .get("episodes")
                .is_none()
        );
    }

    #[test]
    fn documented_sync_and_playback_shapes_deserialize() {
        let items: AllItemsResponse = serde_json::from_value(serde_json::json!({
            "shows": [{
                "last_watched_at": "2026-08-18T12:00:00Z",
                "user_rating": null,
                "status": "watching",
                "show": {
                    "title": "Example",
                    "runtime": 43,
                    "ids": { "simkl": 42, "tvdb": "123" }
                },
                "seasons": [{
                    "number": 1,
                    "episodes": [{
                        "number": 2,
                        "watched_at": "2026-08-18T12:00:00Z",
                        "tvdb": { "season": 3, "episode": 14 }
                    }]
                }]
            }],
            "anime": [{
                "status": "watching",
                "anime": {
                    "title": "Example Anime",
                    "ids": { "simkl": 99, "tmdb": "1429" }
                }
            }]
        }))
        .unwrap();
        assert_eq!(
            items.shows[0]
                .show
                .as_ref()
                .unwrap()
                .runtime,
            Some(43)
        );
        assert_eq!(items.shows[0].seasons[0].episodes[0].number, Some(2));
        assert_eq!(
            items.shows[0].seasons[0].episodes[0]
                .tvdb
                .as_ref()
                .map(|coordinate| (coordinate.season, coordinate.episode)),
            Some((3, 14))
        );
        assert_eq!(
            items.anime[0]
                .anime
                .as_ref()
                .and_then(|anime| anime
                    .ids
                    .simkl),
            Some(99)
        );

        let playback: Vec<PlaybackSession> =
            serde_json::from_value(serde_json::json!([{
                "id": 7,
                "progress": 45.5,
                "paused_at": "2026-08-18T12:00:00Z",
                "type": "episode",
                "episode": {
                    "season": 1,
                    "episode": 5,
                    "tvdb_season": 2,
                    "tvdb_number": 9
                },
                "show": { "title": "Example", "ids": { "simkl": 42 } }
            }]))
            .unwrap();
        assert_eq!(
            playback[0]
                .episode
                .as_ref()
                .unwrap()
                .number,
            Some(5)
        );
        assert_eq!(
            playback[0]
                .episode
                .as_ref()
                .and_then(|episode| episode
                    .tvdb_season
                    .zip(episode.tvdb_number)),
            Some((2, 9))
        );
        assert_eq!(
            playback[0]
                .watched_at
                .as_deref(),
            Some("2026-08-18T12:00:00Z")
        );
    }

    #[test]
    fn optional_sync_path_segments_match_the_reference() {
        assert_eq!(AllItemsEndpoint::default().path(), "sync/all-items");
        assert_eq!(
            AllItemsEndpoint {
                media_type: Some("shows".into()),
                status: Some("completed".into()),
                ..Default::default()
            }
            .path(),
            "sync/all-items/shows/completed"
        );
        assert_eq!(PlaybackEndpoint::default().path(), "sync/playback");
    }
}
