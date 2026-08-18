use crate::{Body, Endpoint};
use chrono::NaiveDateTime;
use http::Method;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingConnectionDto {
    pub addon_id: Uuid,
    pub addon_name: String,
    pub provider: String,
    pub connected: bool,
    pub status: Option<String>,
    pub event_filters: Vec<String>,
    pub supported_events: Vec<String>,
    pub default_event_filter: Vec<String>,
    pub auth_flow: String,
    pub history_import: bool,
    pub progress_import: bool,
    pub watch_state_sync: String,
    pub ratings_sync: String,
    pub last_success_at: Option<NaiveDateTime>,
    pub last_verified_at: Option<NaiveDateTime>,
    pub last_error_at: Option<NaiveDateTime>,
    pub last_error: Option<String>,
    pub pending_events: usize,
    pub failed_events: usize,
    pub latest_failed_event: Option<TrackingFailedEventDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingFailedEventDto {
    pub event_kind: String,
    pub error: String,
    pub failed_at: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingPinStartDto {
    pub verification_url: String,
    pub user_code: String,
    pub poll_token: String,
    pub interval_seconds: u64,
    pub expires_in_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingPinPollRequest {
    pub poll_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackingPinStatus {
    Pending,
    Approved,
    Denied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingPinPollDto {
    pub status: TrackingPinStatus,
    pub connection: Option<TrackingConnectionDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingFiltersRequest {
    pub event_filters: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingSyncResultDto {
    pub received: usize,
    pub matched: usize,
    pub applied: usize,
    pub payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrackingSyncJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
}

impl TrackingSyncJobStatus {
    pub fn active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackingSyncJobDto {
    pub id: Uuid,
    pub status: TrackingSyncJobStatus,
    pub queued_at: NaiveDateTime,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub received: usize,
    pub processed: usize,
    pub matched: usize,
    pub applied: usize,
    pub payload_bytes: usize,
    pub latest_error: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct GetTrackingAddons;

impl Endpoint for GetTrackingAddons {
    type Output = Vec<TrackingConnectionDto>;
    fn path(&self) -> String {
        "/remux/tracking/addons".to_string()
    }
}

#[derive(Debug, Clone)]
pub struct BeginTrackingPin {
    pub addon_id: Uuid,
}

impl Endpoint for BeginTrackingPin {
    type Output = TrackingPinStartDto;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/pin", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::POST
    }
}

#[derive(Debug, Clone)]
pub struct PollTrackingPin {
    pub addon_id: Uuid,
    pub payload: TrackingPinPollRequest,
}

impl Endpoint for PollTrackingPin {
    type Output = TrackingPinPollDto;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/pin/poll", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::POST
    }
    fn body(&self) -> Body {
        Body::Json(serde_json::to_value(&self.payload).unwrap_or_default())
    }
}

#[derive(Debug, Clone)]
pub struct VerifyTrackingAddon {
    pub addon_id: Uuid,
}

impl Endpoint for VerifyTrackingAddon {
    type Output = TrackingConnectionDto;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/verify", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::POST
    }
    fn body(&self) -> Body {
        Body::Json(serde_json::json!({}))
    }
}

#[derive(Debug, Clone)]
pub struct DisconnectTrackingAddon {
    pub addon_id: Uuid,
}

impl Endpoint for DisconnectTrackingAddon {
    type Output = ();
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::DELETE
    }
}

#[derive(Debug, Clone)]
pub struct SetTrackingFilters {
    pub addon_id: Uuid,
    pub payload: TrackingFiltersRequest,
}

impl Endpoint for SetTrackingFilters {
    type Output = TrackingConnectionDto;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/filters", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::POST
    }
    fn body(&self) -> Body {
        Body::Json(serde_json::to_value(&self.payload).unwrap_or_default())
    }
}

#[derive(Debug, Clone)]
pub struct SyncTrackingAddon {
    pub addon_id: Uuid,
}

impl Endpoint for SyncTrackingAddon {
    type Output = TrackingSyncJobDto;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/sync", self.addon_id)
    }
    fn method(&self) -> Method {
        Method::POST
    }
}

#[derive(Debug, Clone)]
pub struct GetTrackingSyncStatus {
    pub addon_id: Uuid,
}

impl Endpoint for GetTrackingSyncStatus {
    type Output = Option<TrackingSyncJobDto>;
    fn path(&self) -> String {
        format!("/remux/tracking/addons/{}/sync/status", self.addon_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_tracking_routes_stay_in_the_remux_namespace() {
        let addon_id = Uuid::nil();
        assert_eq!(GetTrackingAddons.path(), "/remux/tracking/addons");
        assert_eq!(
            BeginTrackingPin { addon_id }.path(),
            format!("/remux/tracking/addons/{addon_id}/pin")
        );
        assert_eq!(
            SyncTrackingAddon { addon_id }.path(),
            format!("/remux/tracking/addons/{addon_id}/sync")
        );
        assert_eq!(
            GetTrackingSyncStatus { addon_id }.path(),
            format!("/remux/tracking/addons/{addon_id}/sync/status")
        );
    }
}
