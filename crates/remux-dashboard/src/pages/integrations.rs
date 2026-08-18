use crate::{
    components::{Card, EmptyState, ErrorAlert, LoadingText, SuccessAlert},
    state::{fmt_datetime, AppState, IS_ADMIN},
};
use dioxus::prelude::*;
use gloo_timers::future::TimeoutFuture;
use remux_sdks::tracking::{
    BeginTrackingPin, DisconnectTrackingAddon, GetTrackingAddons, PollTrackingPin,
    SetTrackingFilters, SyncTrackingAddon, TrackingConnectionDto,
    TrackingFiltersRequest, TrackingPinPollRequest, TrackingPinStartDto,
    TrackingPinStatus, VerifyTrackingAddon,
};
use uuid::Uuid;

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

    let load_client = app_state.clone();
    use_effect(move || {
        let _refresh = *refresh.read();
        let client = load_client.clone();
        spawn(async move {
            match client
                .execute(GetTrackingAddons)
                .await
            {
                Ok(items) => {
                    connections.set(Some(items));
                    error.set(None);
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

    rsx! {
        Card {
            title: "Tracking services",
            p { class: "integration-intro",
                "Connect a personal tracking account. Playback updates are queued durably, and watched history, resume positions, and ratings can be imported into Remux."
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
                        "No tracking addon is configured. Add Simkl from the Addons page, enable Tracking, and return here."
                    } else {
                        "No tracking service is available yet. Ask an administrator to configure the Simkl addon."
                    }
                }
            } else {
                div { class: "integration-list",
                    for connection in connections.read().clone().unwrap_or_default() {
                        {
                            let addon_id = connection.addon_id;
                            let is_simkl = connection.provider == "simkl";
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
                                        div { span { "Watch state" } strong { "{sync_label(&connection.watch_state_sync)}" } }
                                        div { span { "Ratings" } strong { "{sync_label(&connection.ratings_sync)}" } }
                                        div { span { "Queue" } strong { "{connection.pending_events} pending" } }
                                    }

                                    if let Some(last_error) = connection.last_error.as_ref() {
                                        div { class: "alert-error", "{last_error}" }
                                    }
                                    if connection.failed_events > 0 {
                                        div { class: "integration-warning",
                                            "{connection.failed_events} event(s) could not be delivered. Reconnect if the token expired."
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

                                    div { class: "integration-actions",
                                        if !connection.connected || needs_reconnect {
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
                                                            match client.execute(BeginTrackingPin { addon_id }).await {
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
                                                                            break;
                                                                        }
                                                                        if js_sys::Date::now() >= expires_at {
                                                                            error.set(Some("The Simkl PIN expired. Start again.".to_string()));
                                                                            active_pin.set(None);
                                                                            busy.set(None);
                                                                            break;
                                                                        }
                                                                        match client.execute(PollTrackingPin {
                                                                            addon_id,
                                                                            payload: TrackingPinPollRequest {
                                                                                poll_token: started.poll_token.clone(),
                                                                            },
                                                                        }).await {
                                                                            Ok(result) if result.status == TrackingPinStatus::Pending => {}
                                                                            Ok(result) if result.status == TrackingPinStatus::Approved => {
                                                                                success.set(Some("Simkl connected. Initial history import is running.".to_string()));
                                                                                active_pin.set(None);
                                                                                busy.set(None);
                                                                                let next = *refresh.peek() + 1;
                                                                                refresh.set(next);
                                                                                break;
                                                                            }
                                                                            Ok(_) => {
                                                                                error.set(Some("Simkl denied or expired the PIN request.".to_string()));
                                                                                active_pin.set(None);
                                                                                busy.set(None);
                                                                                break;
                                                                            }
                                                                            Err(poll_error) => {
                                                                                error.set(Some(format!("Could not finish Simkl connection: {poll_error}")));
                                                                                active_pin.set(None);
                                                                                busy.set(None);
                                                                                break;
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                                Err(connect_error) => {
                                                                    error.set(Some(format!("Could not start Simkl connection: {connect_error}")));
                                                                    busy.set(None);
                                                                }
                                                            }
                                                        });
                                                    }
                                                },
                                                if is_busy { "Connecting…" } else if needs_reconnect { "Reconnect" } else { "Connect" }
                                            }
                                        }
                                        if connection.connected {
                                            button {
                                                class: "btn btn-ghost",
                                                disabled: is_busy,
                                                onclick: {
                                                    let sync_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        error.set(None);
                                                        success.set(None);
                                                        let client = sync_client.clone();
                                                        spawn(async move {
                                                            match client.execute(SyncTrackingAddon { addon_id }).await {
                                                                Ok(result) => success.set(Some(format!(
                                                                    "Sync complete: {} matched, {} updated.",
                                                                    result.matched, result.applied
                                                                ))),
                                                                Err(sync_error) => error.set(Some(format!("Sync failed: {sync_error}"))),
                                                            }
                                                            busy.set(None);
                                                            let next = *refresh.peek() + 1;
                                                            refresh.set(next);
                                                        });
                                                    }
                                                },
                                                "Sync now"
                                            }
                                            button {
                                                class: "btn btn-ghost",
                                                disabled: is_busy,
                                                onclick: {
                                                    let verify_client = app_state.clone();
                                                    move |_| {
                                                        busy.set(Some(addon_id));
                                                        error.set(None);
                                                        let client = verify_client.clone();
                                                        spawn(async move {
                                                            match client.execute(VerifyTrackingAddon { addon_id }).await {
                                                                Ok(_) => {
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
