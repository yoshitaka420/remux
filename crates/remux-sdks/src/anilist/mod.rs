use reqwest::{StatusCode, header::RETRY_AFTER};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::Duration;

pub const DEFAULT_GRAPHQL_URL: &str = "https://graphql.anilist.co";
pub const DEFAULT_OAUTH_BASE_URL: &str = "https://anilist.co/api/v2/oauth";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("AniList rejected the access token")]
    Unauthorized,
    #[error("AniList rate limit reached; retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
    #[error("AniList HTTP {status}: {message}")]
    Http { status: u16, message: String },
    #[error("AniList GraphQL error: {message}")]
    GraphQl {
        message: String,
        status: Option<u16>,
    },
    #[error("invalid AniList response: {0}")]
    InvalidResponse(String),
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error(transparent)]
    Url(#[from] url::ParseError),
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    graphql_url: String,
    oauth_base_url: String,
    client_id: i64,
    client_secret: String,
}

impl Client {
    pub fn new(
        client_id: i64,
        client_secret: impl Into<String>,
        graphql_url: impl Into<String>,
        oauth_base_url: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, Error> {
        let graphql_url = graphql_url.into();
        let oauth_base_url = oauth_base_url.into();
        url::Url::parse(&graphql_url)?;
        url::Url::parse(&oauth_base_url)?;
        let http = reqwest::Client::builder()
            .user_agent(concat!("remux/", env!("CARGO_PKG_VERSION")));
        #[cfg(not(target_arch = "wasm32"))]
        let http = http
            .connect_timeout(connect_timeout)
            .timeout(request_timeout);
        #[cfg(target_arch = "wasm32")]
        let _ = (connect_timeout, request_timeout);
        let http = http.build()?;
        Ok(Self {
            http,
            graphql_url,
            oauth_base_url: oauth_base_url
                .trim_end_matches('/')
                .to_string(),
            client_id,
            client_secret: client_secret.into(),
        })
    }

    pub fn authorization_url(
        &self,
        redirect_uri: &str,
        state: &str,
    ) -> Result<String, Error> {
        let mut url = url::Url::parse(&format!("{}/authorize", self.oauth_base_url))?;
        url.query_pairs_mut()
            .append_pair(
                "client_id",
                &self
                    .client_id
                    .to_string(),
            )
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("state", state);
        Ok(url.to_string())
    }

    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
    ) -> Result<TokenResponse, Error> {
        let response = self
            .http
            .post(format!("{}/token", self.oauth_base_url))
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&serde_json::json!({
                "grant_type": "authorization_code",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            }))
            .send()
            .await?;
        decode_http(response).await
    }

    pub async fn viewer(&self, access_token: &str) -> Result<Viewer, Error> {
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "Viewer")]
            viewer: Viewer,
        }
        let data: Data = self
            .graphql(
                "query { Viewer { id name } }",
                serde_json::json!({}),
                access_token,
            )
            .await?;
        Ok(data.viewer)
    }

    pub async fn media_by_id(
        &self,
        anilist_id: i64,
        access_token: &str,
    ) -> Result<Option<Media>, Error> {
        self.media(
            "query ($id: Int!) { Media(id: $id, type: ANIME) { id idMal format episodes duration title { userPreferred } } }",
            serde_json::json!({ "id": anilist_id }),
            access_token,
        )
        .await
    }

    pub async fn media_by_mal_id(
        &self,
        mal_id: i64,
        access_token: &str,
    ) -> Result<Option<Media>, Error> {
        self.media(
            "query ($idMal: Int!) { Media(idMal: $idMal, type: ANIME) { id idMal format episodes duration title { userPreferred } } }",
            serde_json::json!({ "idMal": mal_id }),
            access_token,
        )
        .await
    }

    async fn media(
        &self,
        query: &str,
        variables: serde_json::Value,
        access_token: &str,
    ) -> Result<Option<Media>, Error> {
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "Media")]
            media: Option<Media>,
        }
        let data: Data = self
            .graphql(query, variables, access_token)
            .await?;
        Ok(data.media)
    }

    pub async fn media_list_page(
        &self,
        user_id: i64,
        page: i64,
        access_token: &str,
    ) -> Result<MediaListPage, Error> {
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "Page")]
            page: MediaListPage,
        }
        let data: Data = self
            .graphql(
                r#"
                query ($userId: Int!, $page: Int!) {
                  Page(page: $page, perPage: 50) {
                    pageInfo { currentPage hasNextPage }
                    mediaList(userId: $userId, type: ANIME, sort: UPDATED_TIME_DESC) {
                      id status score(format: POINT_10_DECIMAL) progress updatedAt completedAt { year month day }
                      media { id idMal format episodes duration title { userPreferred } }
                    }
                  }
                }
                "#,
                serde_json::json!({ "userId": user_id, "page": page }),
                access_token,
            )
            .await?;
        Ok(data.page)
    }

    pub async fn save_media_list_entry(
        &self,
        input: &SaveMediaListEntry,
        access_token: &str,
    ) -> Result<MediaListEntry, Error> {
        #[derive(Deserialize)]
        struct Data {
            #[serde(rename = "SaveMediaListEntry")]
            entry: MediaListEntry,
        }
        let data: Data = self
            .graphql(
                r#"
                mutation ($mediaId: Int!, $status: MediaListStatus, $progress: Int, $scoreRaw: Int) {
                  SaveMediaListEntry(mediaId: $mediaId, status: $status, progress: $progress, scoreRaw: $scoreRaw) {
                    id status score(format: POINT_10_DECIMAL) progress updatedAt
                    media { id idMal format episodes duration title { userPreferred } }
                  }
                }
                "#,
                serde_json::to_value(input).map_err(|error| {
                    Error::InvalidResponse(format!("encoding mutation variables: {error}"))
                })?,
                access_token,
            )
            .await?;
        Ok(data.entry)
    }

    async fn graphql<T: DeserializeOwned>(
        &self,
        query: &str,
        variables: serde_json::Value,
        access_token: &str,
    ) -> Result<T, Error> {
        let response = self
            .http
            .post(&self.graphql_url)
            .bearer_auth(access_token)
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&serde_json::json!({ "query": query, "variables": variables }))
            .send()
            .await?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| {
                value
                    .to_str()
                    .ok()
            })
            .and_then(|value| {
                value
                    .parse::<u64>()
                    .ok()
            });
        let bytes = response
            .bytes()
            .await?;
        if status == StatusCode::UNAUTHORIZED {
            return Err(Error::Unauthorized);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited {
                retry_after_secs: retry_after
                    .unwrap_or(60)
                    .max(1),
            });
        }
        if !status.is_success() {
            return Err(Error::Http {
                status: status.as_u16(),
                message: response_message(&bytes),
            });
        }
        let envelope: GraphQlEnvelope<T> =
            serde_json::from_slice(&bytes).map_err(|error| {
                Error::InvalidResponse(format!("decoding GraphQL response: {error}"))
            })?;
        if let Some(error) = envelope
            .errors
            .and_then(|errors| {
                errors
                    .into_iter()
                    .next()
            })
        {
            return match error.status {
                Some(401) => Err(Error::Unauthorized),
                Some(429) => Err(Error::RateLimited {
                    retry_after_secs: retry_after
                        .unwrap_or(60)
                        .max(1),
                }),
                status => Err(Error::GraphQl {
                    message: error.message,
                    status,
                }),
            };
        }
        envelope
            .data
            .ok_or_else(|| {
                Error::InvalidResponse(
                    "GraphQL response omitted both data and errors".to_string(),
                )
            })
    }
}

async fn decode_http<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, Error> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| {
            value
                .to_str()
                .ok()
        })
        .and_then(|value| {
            value
                .parse::<u64>()
                .ok()
        });
    let bytes = response
        .bytes()
        .await?;
    if status == StatusCode::UNAUTHORIZED {
        return Err(Error::Unauthorized);
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(Error::RateLimited {
            retry_after_secs: retry_after
                .unwrap_or(60)
                .max(1),
        });
    }
    if !status.is_success() {
        return Err(Error::Http {
            status: status.as_u16(),
            message: response_message(&bytes),
        });
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        Error::InvalidResponse(format!("decoding HTTP response: {error}"))
    })
}

fn response_message(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("message")
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    value
                        .get("error")
                        .and_then(serde_json::Value::as_str)
                })
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| {
            String::from_utf8_lossy(bytes)
                .chars()
                .take(500)
                .collect()
        })
}

#[derive(Debug, Deserialize)]
struct GraphQlEnvelope<T> {
    data: Option<T>,
    errors: Option<Vec<GraphQlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphQlError {
    message: String,
    status: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: Option<String>,
    pub expires_in: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Viewer {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaListPage {
    pub page_info: PageInfo,
    #[serde(default)]
    pub media_list: Vec<MediaListEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    pub current_page: i64,
    pub has_next_page: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaListEntry {
    pub id: i64,
    pub status: Option<MediaListStatus>,
    pub score: Option<f32>,
    pub progress: Option<i64>,
    pub updated_at: Option<i64>,
    pub completed_at: Option<FuzzyDate>,
    pub media: Media,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Media {
    pub id: i64,
    pub id_mal: Option<i64>,
    pub format: Option<MediaFormat>,
    pub episodes: Option<i64>,
    pub duration: Option<i64>,
    pub title: MediaTitle,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaTitle {
    pub user_preferred: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MediaFormat {
    Tv,
    TvShort,
    Movie,
    Special,
    Ova,
    Ona,
    Music,
}

impl MediaFormat {
    pub fn is_movie(self) -> bool {
        matches!(self, Self::Movie)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MediaListStatus {
    Current,
    Planning,
    Completed,
    Dropped,
    Paused,
    Repeating,
}

impl MediaListStatus {
    pub fn is_completed(self) -> bool {
        matches!(self, Self::Completed | Self::Repeating)
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct FuzzyDate {
    pub year: Option<i32>,
    pub month: Option<u32>,
    pub day: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveMediaListEntry {
    pub media_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<MediaListStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score_raw: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_url_encodes_redirect_and_state() {
        let client = Client::new(
            42,
            "secret",
            DEFAULT_GRAPHQL_URL,
            DEFAULT_OAUTH_BASE_URL,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .unwrap();
        let url = url::Url::parse(
            &client
                .authorization_url("https://remux.test/callback?a=1", "csrf state")
                .unwrap(),
        )
        .unwrap();
        let params = url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            params
                .get("client_id")
                .map(|value| value.as_ref()),
            Some("42")
        );
        assert_eq!(
            params
                .get("redirect_uri")
                .map(|value| value.as_ref()),
            Some("https://remux.test/callback?a=1")
        );
        assert_eq!(
            params
                .get("state")
                .map(|value| value.as_ref()),
            Some("csrf state")
        );
    }

    #[test]
    fn mutation_omits_unrelated_fields() {
        let encoded = serde_json::to_value(SaveMediaListEntry {
            media_id: 1,
            status: Some(MediaListStatus::Completed),
            progress: None,
            score_raw: None,
        })
        .unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({ "mediaId": 1, "status": "COMPLETED" })
        );
    }

    #[test]
    fn repeating_entries_remain_watched_during_a_rewatch() {
        assert!(MediaListStatus::Completed.is_completed());
        assert!(MediaListStatus::Repeating.is_completed());
        assert!(!MediaListStatus::Current.is_completed());
    }
}
