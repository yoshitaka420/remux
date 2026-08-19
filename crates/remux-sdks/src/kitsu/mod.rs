use crate::{Endpoint, NoAuth, RestClient};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::Display)]
pub enum AnimeMappingSite {
    #[strum(to_string = "myanimelist/anime")]
    MyAnimeList,
    #[strum(to_string = "anilist/anime")]
    AniList,
}

#[derive(Debug, Clone)]
pub struct ReverseMappingsEndpoint {
    pub site: AnimeMappingSite,
    pub external_id: i64,
}

#[derive(Serialize)]
struct ReverseMappingsQuery {
    #[serde(rename = "filter[externalSite]")]
    external_site: String,
    #[serde(rename = "filter[externalId]")]
    external_id: i64,
    include: &'static str,
}

impl Endpoint for ReverseMappingsEndpoint {
    type Output = MappingsResponse;

    fn path(&self) -> String {
        "mappings".to_string()
    }

    fn query_params(&self) -> impl Serialize + '_ {
        ReverseMappingsQuery {
            external_site: self
                .site
                .to_string(),
            external_id: self.external_id,
            include: "item",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingsEndpoint {
    #[serde(skip)]
    pub kitsu_id: i64,
}

impl Endpoint for MappingsEndpoint {
    type Output = MappingsResponse;

    fn path(&self) -> String {
        format!("anime/{}/mappings", self.kitsu_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingsResponse {
    pub data: Vec<MappingEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingEntry {
    pub attributes: MappingAttributes,
    #[serde(default)]
    pub relationships: Option<MappingRelationships>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingRelationships {
    pub item: MappingItemRelationship,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingItemRelationship {
    #[serde(default)]
    pub data: Option<MappingItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingItem {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MappingAttributes {
    pub external_site: String,
    pub external_id: String,
}

impl MappingsResponse {
    pub fn kitsu_anime_id(&self) -> Option<i64> {
        self.data
            .iter()
            .find_map(|entry| {
                entry
                    .relationships
                    .as_ref()?
                    .item
                    .data
                    .as_ref()
                    .filter(|item| item.kind == "anime")
            })
            .and_then(|item| {
                item.id
                    .parse()
                    .ok()
            })
    }

    pub fn mal_id(&self) -> Option<i64> {
        self.data
            .iter()
            .find(|entry| {
                matches!(
                    entry
                        .attributes
                        .external_site
                        .as_str(),
                    "myanimelist/anime" | "myanimelist"
                )
            })
            .and_then(|entry| {
                entry
                    .attributes
                    .external_id
                    .parse()
                    .ok()
            })
    }

    pub fn tvdb_id(&self) -> Option<i64> {
        self.data
            .iter()
            .find(|e| {
                e.attributes
                    .external_site
                    == "thetvdb"
                    || e.attributes
                        .external_site
                        == "thetvdb/series"
            })
            .and_then(|e| {
                e.attributes
                    .external_id
                    .parse()
                    .ok()
            })
    }
}

pub fn client() -> RestClient<NoAuth> {
    RestClient::new("https://kitsu.io/api/edge/").expect("Kitsu base URL is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_mapping_query_uses_kitsu_filter_shape() {
        let endpoint = ReverseMappingsEndpoint {
            site: AnimeMappingSite::MyAnimeList,
            external_id: 5114,
        };

        assert_eq!(endpoint.path(), "mappings");
        assert_eq!(
            endpoint.query(),
            vec![
                (
                    "filter%5BexternalSite%5D".to_string(),
                    "myanimelist%2Fanime".to_string(),
                ),
                ("filter%5BexternalId%5D".to_string(), "5114".to_string()),
                ("include".to_string(), "item".to_string()),
            ]
        );
    }

    #[test]
    fn extracts_kitsu_anime_id_from_reverse_mapping() {
        let response: MappingsResponse = serde_json::from_value(serde_json::json!({
            "data": [{
                "attributes": {
                    "externalSite": "myanimelist/anime",
                    "externalId": "5114"
                },
                "relationships": {
                    "item": {
                        "data": { "type": "anime", "id": "3936" }
                    }
                }
            }]
        }))
        .unwrap();

        assert_eq!(response.kitsu_anime_id(), Some(3936));
    }
}
