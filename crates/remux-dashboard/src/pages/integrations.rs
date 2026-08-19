use crate::{
    components::{Card, EmptyState, ErrorAlert, LoadingText, SuccessAlert},
    state::{fmt_datetime, AppState, IS_ADMIN},
};
use dioxus::prelude::*;
use futures::{
    future::{select, Either},
    pin_mut, FutureExt,
};
use gloo_timers::future::TimeoutFuture;
use remux_sdks::tracking::{
    BeginTrackingOauth, BeginTrackingPin, DisconnectTrackingAddon, GetTrackingAddons,
    GetTrackingSyncStatus, PollTrackingPin, SetTrackingFilters, SetTrackingRole,
    SyncTrackingAddon, TrackingConnectionDto, TrackingFiltersRequest,
    TrackingOauthStartRequest, TrackingPinPollRequest, TrackingPinStartDto,
    TrackingPinStatus, TrackingRoleRequest, TrackingSyncJobDto, TrackingSyncJobStatus,
    VerifyTrackingAddon,
};
use remux_sdks::Endpoint;
use std::collections::HashMap;
use uuid::Uuid;
use wasm_bindgen::{closure::Closure, JsCast};

const REQUEST_DEADLINE_MS: u32 = 20_000;
const LIST_DEADLINE_MS: u32 = 15_000;
const SYNC_POLL_INTERVAL_MS: u32 = 2_000;

/// Dropping reqwest's WASM request future aborts its Fetch AbortController, so
/// the deadline both updates the UI and cancels work the page no longer needs.
async fn execute_with_deadline<EP: Endpoint + Clone>(
    client: AppState,
    endpoint: EP,
    timeout_ms: u32,
    action: &str,
) -> Result<EP::Output, String> {
    let request = client
        .execute(endpoint)
        .fuse();
    let deadline = TimeoutFuture::new(timeout_ms).fuse();
    pin_mut!(request, deadline);
    match select(request, deadline).await {
        Either::Left((result, _)) => result.map_err(|error| error.to_string()),
        Either::Right(((), _)) => Err(format!(
            "{action} timed out after {} seconds",
            timeout_ms / 1_000
        )),
    }
}

async fn reconcile_connection(
    client: AppState,
    addon_id: Uuid,
) -> Option<TrackingConnectionDto> {
    execute_with_deadline(
        client,
        GetTrackingAddons,
        LIST_DEADLINE_MS,
        "Connection check",
    )
    .await
    .ok()?
    .into_iter()
    .find(|connection| {
        connection.addon_id == addon_id
            && connection.connected
            && connection
                .status
                .as_deref()
                == Some("connected")
    })
}

fn sync_job_label(job: &TrackingSyncJobDto) -> String {
    match job.status {
        TrackingSyncJobStatus::Queued => "Sync queued…".to_string(),
        TrackingSyncJobStatus::Running if job.received == 0 => {
            "Downloading provider history…".to_string()
        }
        TrackingSyncJobStatus::Running => format!(
            "Syncing: {} of {} checked, {} matched, {} updated",
            job.processed, job.received, job.matched, job.applied
        ),
        TrackingSyncJobStatus::Completed => format!(
            "Sync complete: {} received, {} matched, {} updated",
            job.received, job.matched, job.applied
        ),
        TrackingSyncJobStatus::Failed => format!(
            "Sync failed: {}",
            job.latest_error
                .as_deref()
                .unwrap_or("Unknown provider error")
        ),
    }
}

fn event_label(event: &str) -> &'static str {
    match event {
        "playback_start" => "Playback starts",
        "playback_progress" => "Pause and resume",
        "playback_stop" => "Playback stops",
        "mark_played" => "Mark watched",
        "mark_unplayed" => "Mark unwatched",
        "favorite" => "Favorites",
        "rating" => "Ratings",
        _ => "Other activity",
    }
}

fn sync_label(direction: &str) -> &'static str {
    match direction {
        "both" => "Two-way",
        "push" => "Remux → service",
        "pull" => "Service → Remux",
        _ => "Not supported",
    }
}

#[component]
pub fn IntegrationsPage(app_state: AppState) -> Element {
    let mut connections: Signal<Option<Vec<TrackingConnectionDto>>> =
        use_signal(|| None);
    let mut refresh = use_signal(|| 0_u32);
    let mut error = use_signal(|| Option::<String>::None);
    let mut success = use_signal(|| Option::<String>::None);
    let mut busy: Signal<Option<Uuid>> = use_signal(|| None);
    let mut active_pin: Signal<Option<(Uuid, TrackingPinStartDto)>> =
        use_signal(|| None);
    let mut sync_jobs: Signal<HashMap<Uuid, TrackingSyncJobDto>> =
        use_signal(HashMap::new);

    use_effect(move || {
        let Some(window) = web_sys::window() else {
            return;
        };
        if window
            .location()
            .search()
            .is_ok_and(|query| query.contains("tracking=connected"))
        {
            success.set(Some(
                "AniList connected. Initial history sync is queued.".to_string(),
            ));
        }
    });

    // A PIN approval can finish while the tab is backgrounded. Re-run the
    // durable connection/status reconciliation as soon as the page regains
    // focus instead of waiting for another PIN response.
    use_effect(move || {
        let Some(window) = web_sys::window() else {
            return;
        };
        let callback = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            let next = *refresh.peek() + 1;
            refresh.set(next);
        });
        let _ = window.add_event_listener_with_callback(
            "focus",
            callback
                .as_ref()
                .unchecked_ref(),
        );
        callback.forget();
    });

    let load_client = app_state.clone();
    use_effect(move || {
        let _refresh = *refresh.read();
        let client = load_client.clone();
        spawn(async move {
            match execute_with_deadline(
                client.clone(),
                GetTrackingAddons,
                LIST_DEADLINE_MS,
                "Loading integrations",
            )
            .await
            {
                Ok(items) => {
                    let active_addon = active_pin
                        .peek()
                        .as_ref()
                        .map(|(addon_id, _)| *addon_id);
                    if let Some(active_addon) = active_addon {
                        if items
                            .iter()
                            .any(|connection| {
                                connection.addon_id == active_addon
                                    && connection.connected
                                    && connection
                                        .status
                                        .as_deref()
                                        == Some("connected")
                            })
                        {
                            active_pin.set(None);
                            busy.set(None);
                            success.set(Some(
                                "Simkl connected. Initial history sync is queued."
                                    .to_string(),
                            ));
                        }
                    }
                    let connected_addons = items
                        .iter()
                        .filter(|connection| connection.connected)
                        .map(|connection| connection.addon_id)
                        .collect::<Vec<_>>();
                    connections.set(Some(items));
                    error.set(None);

                    let mut latest = sync_jobs
                        .peek()
                        .clone();
                    latest.retain(|addon_id, _| connected_addons.contains(addon_id));
                    for addon_id in connected_addons {
                        if let Ok(status) = execute_with_deadline(
                            client.clone(),
                            GetTrackingSyncStatus { addon_id },
                            LIST_DEADLINE_MS,
                            "Loading sync status",
                        )
                        .await
                        {
                            match status {
                                Some(job) => {
                                    latest.insert(addon_id, job);
                                }
                                None => {
                                    latest.remove(&addon_id);
                                }
                            }
                        }
                    }
                    sync_jobs.set(latest);
                }
                Err(load_error) => {
                    connections.set(Some(Vec::new()));
                    error.set(Some(format!(
                        "Failed to load integrations: {load_error}"
                    )));
                }
            }
        });
    });

    let sync_poll_client = app_state.clone();
    use_effect(move || {
        let active_addons = sync_jobs
            .read()
            .iter()
            .filter_map(|(addon_id, job)| {
                job.status
                    .active()
                    .then_some(*addon_id)
            })
            .collect::<Vec<_>>();
        if active_addons.is_empty() {
            return;
        }
        let client = sync_poll_client.clone();
        spawn(async move {
            TimeoutFuture::new(SYNC_POLL_INTERVAL_MS).await;
            let mut latest = sync_jobs
                .peek()
                .clone();
            let mut terminal = false;
            for addon_id in active_addons {
                if let Ok(Some(job)) = execute_with_deadline(
                    client.clone(),
                    GetTrackingSyncStatus { addon_id },
                    LIST_DEADLINE_MS,
                    "Checking sync status",
                )
                .await
                {
                    let was_active = latest
                        .get(&addon_id)
                        .is_some_and(|previous| {
                            previous
                                .status
                                .active()
                        });
                    if was_active
                        && !job
                            .status
                            .active()
                    {
                        terminal = true;
                        match job.status {
                            TrackingSyncJobStatus::Completed => {
                                success.set(Some(sync_job_label(&job)));
                                error.set(None);
                            }
                            TrackingSyncJobStatus::Failed => {
                                error.set(Some(sync_job_label(&job)));
                            }
                            _ => {}
                        }
                    }
                    latest.insert(addon_id, job);
                }
            }
            sync_jobs.set(latest);
            if terminal {
                let next = *refresh.peek() + 1;
                refresh.set(next);
            }
        });
    });

    rsx! {
        Card {
            title: "Tracking services",
            p { class: "integration-intro",
                "Connect personal tracking accounts. One primary service can sync both ways; mirrors only receive Remux activity, preventing tracker databases from overwriting each other."
            }
            if let Some(message) = error.read().as_ref() {
                ErrorAlert { message: message.clone() }
            }
            if let Some(message) = success.read().as_ref() {
                SuccessAlert { message: message.clone() }
            }
            if connections.read().is_none() {
                LoadingText {}
            } else if connections.read().as_ref().is_some_and(Vec::is_empty) {
                EmptyState {
                    message: if *IS_ADMIN.read() {
                        "No tracking addon is configured. Add Simkl or AniList from the Addons page, enable Tracking, and return here."
                    } else {
                        "No tracking service is available yet. Ask an administrator to configure a tracking addon."
                    }
                }
            } else {
                div { class: "integration-list",
                    for connection in connections.read().clone().unwrap_or_default() {
                        {
                            let addon_id = connection.addon_id;
                            let is_simkl = connection.provider == "simkl";
                            let is_anilist = connection.provider == "anilist";
                            let is_primary = connection.sync_role == "primary";
                            let is_busy = *busy.read() == Some(addon_id);
                            let needs_reconnect = matches!(
                                connection.status.as_deref(),
                                Some("auth_expired") | Some("error")
                            );
                            let status_label = connection
                                .status
                                .clone()
                                .unwrap_or_else(|| {
                                    if connection.connected {
                                        "connected".to_string()
                                    } else {
                                        "not connected".to_string()
                                    }
                                });
                            let pin = active_pin
                                .read()
                                .as_ref()
                                .filter(|(id, _)| *id == addon_id)
                                .map(|(_, pin)| pin.clone());
                            let sync_job = sync_jobs
                                .read()
                                .get(&addon_id)
                                .cloned();
                            let sync_active = sync_job
                                .as_ref()
                                .is_some_and(|job| job.status.active());
                            rsx! {
                                div { class: "integration-card", key: "{addon_id}",
                                    div { class: "integration-header",
                                        div {
                                            div { class: "integration-name", "{connection.addon_name}" }
                                            if is_simkl {
                                                a {
                                                    class: "integration-provider",
                                                    href: "https://simkl.com",
                                                    target: "_blank",
                                                    rel: "noopener noreferrer",
                                                    img {
                                                        class: "integration-provider-icon",
                                                        src: "https://us.simkl.in/img_favicon/v2/favicon-192x192.png",
                                                        alt: "",
                                                    }
                                                    span { "Powered by Simkl" }
                                                }
                                            } else if is_anilist {
                                                a {
                                                    class: "integration-provider",
                                                    href: "https://anilist.co",
                                                    target: "_blank",
                                                    rel: "noopener noreferrer",
                                                    span { "Powered by AniList" }
                                                }
                                            } else {
                                                div { class: "integration-provider", "{connection.provider}" }
                                            }
                                        }
                                        span {
                                            class: if connection.status.as_deref() == Some("connected") {
                                                "integration-status integration-status-ok"
                                            } else if connection.connected {
                                                "integration-status integration-status-error"
                                            } else {
                                                "integration-status"
                                            },
                                            "{status_label}"
                                        }
                                    }

                                    div { class: "integration-summary",
                                        div { span { "Role" } strong { if is_primary { "Primary · two-way" } else { "Mirror · send-only" } } }
                                        div { span { "Watch state" } strong { "{sync_label(&connection.watch_state_sync)}" } }
                                        div { span { "Ratings" } strong { "{sync_label(&connection.ratings_sync)}" } }
                                        div { span { "Queue" } strong { "{connection.pending_events} pending" } }
                                    }

                                    if connection.connected {
                                        div { class: "integration-authority",
                                            div {
                                                strong { "Tracker authority" }
                                                span { "Only the primary imports remote changes. Mirrors receive local changes without writing back into Remux." }
                                            }
                                            div { class: "integration-role-actions",
                                                button {
                                                    class: if is_primary { "btn btn-primary" } else { "btn btn-ghost" },
                                                    disabled: is_busy || is_primary,
                                                    onclick: {
                                                        let role_client = app_state.clone();
                                                        move |_| {
                                                            busy.set(Some(addon_id));
                                                            error.set(None);
                                                            success.set(None);
                                                            let client = role_client.clone();
                                                            spawn(async move {
                                                                match execute_with_deadline(
                                                                    client,
                                                                    SetTrackingRole {
                                                                        addon_id,
                                                                        payload: TrackingRoleRequest { sync_role: "primary".to_string() },
                                                                    },
                                                                    REQUEST_DEADLINE_MS,
                                                                    "Changing tracker authority",
                                                                ).await {
                                                                    Ok(_) => {
                                                                        success.set(Some("Primary tracker changed. Its inbound sync is queued; other trackers are now send-only mirrors.".to_string()));
                                                                        let next = *refresh.peek() + 1;
                                                                        refresh.set(next);
                                                                    }
                                                                    Err(role_error) => error.set(Some(format!("Could not make this tracker primary: {role_error}"))),
                                                                }
                                                                busy.set(None);
                                                            });
                                                        }
                                                    },
                                                    "Primary"
                                                }
                                                button {
                                                    class: if is_primary { "btn btn-ghost" } else { "btn btn-primary" },
                                                    disabled: is_busy || !is_primary,
                                                    onclick: {
                                                        let role_client = app_state.clone();
                                                        move |_| {
                                                            busy.set(Some(addon_id));
                                                            error.set(None);
                                                            success.set(None);
                                                            let client = role_client.clone();
                                                            spawn(async move {
                                                                match execute_with_deadline(
                                                                    client,
                                                                    SetTrackingRole {
                                                                        addon_id,
                                                                        payload: TrackingRoleRequest { sync_role: "mirror".to_string() },
                                                                    },
                                                                    REQUEST_DEADLINE_MS,
                                                                    "Changing tracker authority",
                                                                ).await {
                                                                    Ok(_) => {
                                                                        success.set(Some("Tracker changed to a send-only mirror. No tracker will import until one is made primary.".to_string()));
                                                                        let next = *refresh.peek() + 1;
                                                                        refresh.set(next);
                                                                    }
                                                                    Err(role_error) => error.set(Some(format!("Could not make this tracker a mirror: {role_error}"))),
                                                                }
                                                                busy.set(None);
                                                            });
                                                        }
                                                    },
                                                    "Mirror"
                                                }
                                            }
                                        }
                                    }
                                    if is_anilist && is_primary {
                                        div { class: "integration-warning",
                                            "AniList is anime-only and stores episode counts rather than partial resume positions. It can still be primary, but non-anime and in-episode progress remain local to Remux."
                                        }
                                    }

                                    if let Some(last_error) = connection.last_error.as_ref() {
                                        div { class: "alert-error", "{last_error}" }
                                    }
                                    if connection.failed_events > 0 {
                                        div { class: "integration-warning",
                                            if let Some(failed) = connection.latest_failed_event.as_ref() {
                                                "{connection.failed_events} outbound event(s) could not be delivered. Latest {event_label(&failed.event_kind)} failure: {failed.error} ({fmt_datetime(failed.failed_at)})."
                                            } else {
                                                "{connection.failed_events} outbound event(s) could not be delivered."
                                            }
                                        }
                                    }

                                    if let Some(pin) = pin {
                                        div { class: "integration-pin",
                                            p { "Open Simkl, enter this PIN, and leave this page open:" }
                                            div { class: "integration-pin-row",
                                                code { "{pin.user_code}" }
                                                button {
                                                    class: "btn btn-ghost",
                                                    onclick: {
                                                        let code = pin.user_code.clone();
                                                        move |_| {
                                                            if let Some(window) = web_sys::window() {
                                                                let _ = window.navigator().clipboard().write_text(&code);
                                                            }
                                                        }
                                                    },
                                                    "Copy"
                                                }
                                                a {
                                                    class: "btn btn-primary",
                                                    href: "{pin.verification_url}",
                                                    target: "_blank",
                                                    rel: "noopener noreferrer",
                                                    "Open Simkl"
                                                }
                                            }
                                            span { "Waiting for approval…" }
                                            button {
                                                class: "btn btn-ghost",
                                                onclick: move |_| {
                                                    active_pin.set(None);
                                                    busy.set(None);
                                                },
                                                "Cancel"
                                            }
                                        }
                                    }

                                    if connection.connected {
                                        div { class: "integration-filters",
                                            div { class: "integration-section-title", "Send to {connection.addon_name}" }
                                            for event in connection.supported_events.clone() {
                                                {
                                                    let checked = connection.event_filters.contains(&event);
                                                    let current_filters = connection.event_filters.clone();
                                                    let event_for_change = event.clone();
                                                    let filter_client = app_state.clone();
                                                    rsx! {
                                                        label { class: "toggle-row", key: "{event}",
                                                            span { class: "toggle-label", "{event_label(&event)}" }
                                                            span { class: "toggle",
                                                                input {
                                                                    r#type: "checkbox",
                                                                    checked,
                                                                    disabled: is_busy,
                                                                    onchange: move |change| {
                                                                        let mut filters = current_filters.clone();
                                                                        if change.checked() {
                                                                            if !filters.contains(&event_for_change) {
                                                                                filters.push(event_for_change.clone());
                                                                            }
                                                                        } else {
                                                                            filters.retain(|value| value != &event_for_change);
                                                                        }
                                                                        busy.set(Some(addon_id));
                                                                        error.set(None);
                                                                        let client = filter_client.clone();
                                                                        spawn(async move {
                                                                            match client.execute(SetTrackingFilters {
                                                                                addon_id,
                                                                                payload: TrackingFiltersRequest { event_filters: filters },
                                                                            }).await {
                                                                                Ok(_) => {
                                                                                    let next = *refresh.peek() + 1;
                                                                                    refresh.set(next);
                                                                                }
                                                                                Err(update_error) => error.set(Some(format!(
                                                                                    "Failed to update tracking events: {update_error}"
                                                                                ))),
                                                                            }
                                                                            busy.set(None);
                                                                        });
                                                                    },
                                                                }
                                                                span { class: "toggle-track" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some(last_success) = connection.last_success_at {
                                        div { class: "integration-last-sync",
                                            "Last successful activity: {fmt_datetime(last_success)}"
                                        }
                                    }
                                    if let Some(last_verified) = connection.last_verified_at {
                                        div { class: "integration-last-sync",
                                            "Connection verified: {fmt_datetime(last_verified)}"
                                        }
                                    }
                                    if let Some(job) = sync_job.as_ref() {
                                        div {
                                            class: if job.status == TrackingSyncJobStatus::Failed {
                                                "integration-sync-status integration-sync-status-error"
                                            } else if job.status == TrackingSyncJobStatus::Completed {
                                                "integration-sync-status integration-sync-status-ok"
                                            } else {
                                                "integration-sync-status"
                                            },
                                            "{sync_job_label(job)}"
                                        }
                                    }

                                    div { class: "integration-actions",
                                        if !connection.connected || needs_reconnect {
                                            if connection.auth_flow == "oauth_redirect" {
                                                button {
                                                    class: "btn btn-primary",
                                                    disabled: is_busy,
                                                    onclick: {
                                                        let connect_client = app_state.clone();
                                                        move |_| {
                                                            let Some(window) = web_sys::window() else {
                                                                error.set(Some("A browser window is required for AniList OAuth.".to_string()));
                                                                return;
                                                            };
                                                            let origin = match window.location().origin() {
                                                                Ok(origin) => origin,
                                                                Err(_) => {
                                                                    error.set(Some("Could not determine the Remux public URL for AniList OAuth.".to_string()));
                                                                    return;
                                                                }
                                                            };
                                                            let redirect_uri = format!(
                                                                "{origin}/remux/tracking/oauth/callback"
                                                            );
                                                            busy.set(Some(addon_id));
                                                            active_pin.set(None);
                                                            error.set(None);
                                                            success.set(None);
                                                            let client = connect_client.clone();
                                                            spawn(async move {
                                                                match execute_with_deadline(
                                                                    client,
                                                                    BeginTrackingOauth {
                                                                        addon_id,
                                                                        payload: TrackingOauthStartRequest { redirect_uri },
                                                                    },
                                                                    REQUEST_DEADLINE_MS,
                                                                    "Starting the AniList connection",
                                                                ).await {
                                                                    Ok(started) => {
                                                                        if let Some(window) = web_sys::window() {
                                                                            if window.location().set_href(&started.authorization_url).is_err() {
                                                                                error.set(Some("Could not open AniList authorization.".to_string()));
                                                                                busy.set(None);
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(connect_error) => {
                                                                        error.set(Some(format!(
                                                                            "Could not start AniList connection: {connect_error}"
                                                                        )));
                                                                        busy.set(None);
                                                                    }
                                                                }
                                                            });
                                                        }
                                                    },
                                                    if is_busy { "Connecting…" } else if needs_reconnect { "Reconnect AniList" } else { "Connect AniList" }
                                                }
                                            } else {
                                                button {
                                                class: "btn btn-primary",
                                                disabled: is_busy,
                                                onclick: {
                                                    let connect_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        active_pin.set(None);
                                                        error.set(None);
                                                        success.set(None);
                                                        let client = connect_client.clone();
                                                        spawn(async move {
                                                            let outcome = match execute_with_deadline(
                                                                client.clone(),
                                                                BeginTrackingPin { addon_id },
                                                                REQUEST_DEADLINE_MS,
                                                                "Starting the Simkl connection",
                                                            ).await {
                                                                Err(connect_error) => Err(format!(
                                                                    "Could not start Simkl connection: {connect_error}"
                                                                )),
                                                                Ok(started) => {
                                                                    active_pin.set(Some((addon_id, started.clone())));
                                                                    let interval_ms = started
                                                                        .interval_seconds
                                                                        .max(1)
                                                                        .saturating_mul(1_000)
                                                                        .min(u32::MAX as u64)
                                                                        as u32;
                                                                    let expires_at = js_sys::Date::now()
                                                                        + started.expires_in_seconds as f64 * 1_000.0;
                                                                    loop {
                                                                        TimeoutFuture::new(interval_ms).await;
                                                                        let still_active = active_pin
                                                                            .peek()
                                                                            .as_ref()
                                                                            .is_some_and(|(id, pin)| {
                                                                                *id == addon_id
                                                                                    && pin.poll_token == started.poll_token
                                                                            });
                                                                        if !still_active {
                                                                            break Ok(false);
                                                                        }

                                                                        // The approval response may have been lost after the
                                                                        // backend saved the connection. Reconcile first on every
                                                                        // pass so we never reuse Simkl's single-use PIN.
                                                                        if reconcile_connection(client.clone(), addon_id).await.is_some() {
                                                                            break Ok(true);
                                                                        }
                                                                        if js_sys::Date::now() >= expires_at {
                                                                            break Err("The Simkl PIN expired. Start again.".to_string());
                                                                        }

                                                                        let poll = execute_with_deadline(
                                                                            client.clone(),
                                                                            PollTrackingPin {
                                                                                addon_id,
                                                                                payload: TrackingPinPollRequest {
                                                                                    poll_token: started.poll_token.clone(),
                                                                                },
                                                                            },
                                                                            REQUEST_DEADLINE_MS,
                                                                            "Waiting for Simkl approval",
                                                                        ).await;
                                                                        match poll {
                                                                            Ok(result) if result.status == TrackingPinStatus::Pending => {}
                                                                            Ok(result) if result.status == TrackingPinStatus::Approved => {
                                                                                break Ok(true);
                                                                            }
                                                                            Ok(_) => {
                                                                                if reconcile_connection(client.clone(), addon_id).await.is_some() {
                                                                                    break Ok(true);
                                                                                }
                                                                                break Err("Simkl denied or expired the PIN request.".to_string());
                                                                            }
                                                                            Err(poll_error) => {
                                                                                // Reconcile immediately after a timeout/network
                                                                                // error; the server may have committed approval.
                                                                                if reconcile_connection(client.clone(), addon_id).await.is_some() {
                                                                                    break Ok(true);
                                                                                }
                                                                                break Err(format!(
                                                                                    "Could not finish Simkl connection: {poll_error}"
                                                                                ));
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            };

                                                            // One cleanup path covers success, denial, timeout,
                                                            // expiry, cancellation, and begin failures.
                                                            active_pin.set(None);
                                                            busy.set(None);
                                                            match outcome {
                                                                Ok(true) => {
                                                                    success.set(Some(
                                                                        "Simkl connected. Initial history sync is queued."
                                                                            .to_string(),
                                                                    ));
                                                                    let next = *refresh.peek() + 1;
                                                                    refresh.set(next);
                                                                }
                                                                Ok(false) => {}
                                                                Err(connect_error) => {
                                                                    error.set(Some(connect_error));
                                                                }
                                                            }
                                                        });
                                                    }
                                                },
                                                if is_busy { "Connecting…" } else if needs_reconnect { "Reconnect" } else { "Connect" }
                                                }
                                            }
                                        }
                                        if connection.connected {
                                            if is_primary {
                                                button {
                                                class: "btn btn-ghost",
                                                disabled: is_busy || sync_active,
                                                onclick: {
                                                    let sync_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        error.set(None);
                                                        success.set(None);
                                                        let client = sync_client.clone();
                                                        spawn(async move {
                                                            match execute_with_deadline(
                                                                client,
                                                                SyncTrackingAddon { addon_id },
                                                                REQUEST_DEADLINE_MS,
                                                                "Queueing sync",
                                                            ).await {
                                                                Ok(job) => {
                                                                    sync_jobs.write().insert(addon_id, job);
                                                                    success.set(Some("Sync queued. Progress will continue if you leave or refresh this page.".to_string()));
                                                                }
                                                                Err(sync_error) => error.set(Some(format!("Could not queue sync: {sync_error}"))),
                                                            }
                                                            busy.set(None);
                                                        });
                                                    }
                                                },
                                                if sync_active { "Syncing…" } else { "Sync now" }
                                                }
                                            }
                                            button {
                                                class: "btn btn-ghost",
                                                disabled: is_busy,
                                                onclick: {
                                                    let verify_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        error.set(None);
                                                        success.set(None);
                                                        let client = verify_client.clone();
                                                        spawn(async move {
                                                            match execute_with_deadline(
                                                                client,
                                                                VerifyTrackingAddon { addon_id },
                                                                REQUEST_DEADLINE_MS,
                                                                "Verifying the connection",
                                                            ).await {
                                                                Ok(verified) => {
                                                                    let message = verified
                                                                        .last_verified_at
                                                                        .map(|at| format!(
                                                                            "Connection verified at {}.",
                                                                            fmt_datetime(at)
                                                                        ))
                                                                        .unwrap_or_else(|| "Connection verified.".to_string());
                                                                    success.set(Some(message));
                                                                    let next = *refresh.peek() + 1;
                                                                    refresh.set(next);
                                                                }
                                                                Err(verify_error) => error.set(Some(format!("Verification failed: {verify_error}"))),
                                                            }
                                                            busy.set(None);
                                                        });
                                                    }
                                                },
                                                "Verify"
                                            }
                                            button {
                                                class: "btn btn-ghost integration-disconnect",
                                                disabled: is_busy,
                                                onclick: {
                                                    let disconnect_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        error.set(None);
                                                        let client = disconnect_client.clone();
                                                        spawn(async move {
                                                            match client.execute(DisconnectTrackingAddon { addon_id }).await {
                                                                Ok(()) => {
                                                                    success.set(Some("Tracking account disconnected.".to_string()));
                                                                    let next = *refresh.peek() + 1;
                                                                    refresh.set(next);
                                                                }
                                                                Err(disconnect_error) => error.set(Some(format!(
                                                                    "Could not disconnect account: {disconnect_error}"
                                                                ))),
                                                            }
                                                            busy.set(None);
                                                        });
                                                    }
                                                },
                                                "Disconnect"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
