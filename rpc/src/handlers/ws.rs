//! WebSocket subscription handler for real-time pubsub events.
//!
//! Clients connect to `/v1/ws` and send JSON subscription requests to receive
//! filtered event streams. The protocol is intentionally simple:
//!
//! **Subscribe**: `{"method": "slotSubscribe"}` or `{"method": "blockSubscribe"}`
//! **Unsubscribe**: `{"method": "slotUnsubscribe"}` or `{"method": "blockUnsubscribe"}`
//!
//! Events are delivered as JSON objects with a `"type"` discriminator field.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::{debug, instrument, warn};

use crate::server::{PubsubEvent, RpcState};

/// Tracks which event types a client has subscribed to.
#[derive(Debug, Default)]
struct Subscriptions {
    slot: bool,
    block: bool,
}

impl Subscriptions {
    /// Returns `true` if at least one subscription is active.
    fn has_any(&self) -> bool {
        self.slot || self.block
    }

    /// Returns `true` if this event matches an active subscription.
    fn matches(&self, event: &PubsubEvent) -> bool {
        match event {
            PubsubEvent::SlotUpdate { .. } => self.slot,
            PubsubEvent::BlockNotification { .. } => self.block,
        }
    }
}

/// Inbound message from the WebSocket client.
#[derive(Debug, Deserialize)]
struct ClientRequest {
    method: String,
}

/// Axum handler that upgrades an HTTP request to a WebSocket connection.
///
/// Once upgraded, the connection enters `handle_socket` which manages
/// subscriptions and event delivery for the lifetime of the connection.
#[instrument(skip_all)]
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<RpcState>>,
) -> impl IntoResponse {
    metrics::counter!("rpc_ws_upgrades").increment(1);
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Core WebSocket session loop.
///
/// Architecture:
/// - A `broadcast::Receiver` is created from `RpcState::pubsub_tx` at the
///   start of each connection. This means the receiver only sees events
///   published *after* the connection was established -- no backfill.
/// - We use `tokio::select!` to concurrently read client messages (subscribe /
///   unsubscribe) and receive broadcast events. The `biased` mode is not used
///   here because both branches are equally important and we want fair polling.
/// - If the broadcast receiver lags (buffer overflow), we log a warning and
///   continue -- the client simply misses those events.
/// - The loop exits when the client disconnects or sends a Close frame.
#[instrument(skip_all)]
async fn handle_socket(mut socket: WebSocket, state: Arc<RpcState>) {
    metrics::gauge!("rpc_ws_active_connections").increment(1.0);
    debug!("WebSocket client connected");

    let mut event_rx: broadcast::Receiver<PubsubEvent> = state.pubsub_tx.subscribe();
    let mut subs = Subscriptions::default();

    loop {
        tokio::select! {
            // Branch 1: Incoming message from the client
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_client_message(&text, &mut subs, &mut socket).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        debug!("WebSocket client disconnected");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        // Respond with Pong to keep the connection alive
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(_)) => {
                        // Ignore Binary / Pong frames
                    }
                    Some(Err(e)) => {
                        warn!(error = %e, "WebSocket receive error");
                        break;
                    }
                }
            }
            // Branch 2: Broadcast event from the validator
            event = event_rx.recv() => {
                match event {
                    Ok(ev) if subs.matches(&ev) => {
                        // Serialize and send only if the client is subscribed
                        match serde_json::to_string(&ev) {
                            Ok(json) => {
                                if socket.send(Message::Text(json.into())).await.is_err() {
                                    debug!("WebSocket send failed, closing");
                                    break;
                                }
                                metrics::counter!("rpc_ws_events_sent").increment(1);
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to serialize pubsub event");
                            }
                        }
                    }
                    Ok(_) => {
                        // Event does not match any active subscription -- skip
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(missed = n, "WebSocket subscriber lagged, events dropped");
                        metrics::counter!("rpc_ws_events_lagged").increment(n);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("pubsub channel closed, terminating WebSocket");
                        break;
                    }
                }
            }
        }
    }

    metrics::gauge!("rpc_ws_active_connections").decrement(1.0);
    debug!("WebSocket session ended");
}

/// Parse and apply a client subscription / unsubscription request.
///
/// Sends a JSON acknowledgement back to the client so it knows the request
/// was processed. Invalid methods receive an error response.
async fn handle_client_message(text: &str, subs: &mut Subscriptions, socket: &mut WebSocket) {
    let req: ClientRequest = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            let err_msg = serde_json::json!({
                "error": format!("invalid request: {e}")
            });
            let _ = socket.send(Message::Text(err_msg.to_string().into())).await;
            return;
        }
    };

    let (ack, recognized) = match req.method.as_str() {
        "slotSubscribe" => {
            subs.slot = true;
            (
                serde_json::json!({"result": "subscribed", "subscription": "slot"}),
                true,
            )
        }
        "slotUnsubscribe" => {
            subs.slot = false;
            (
                serde_json::json!({"result": "unsubscribed", "subscription": "slot"}),
                true,
            )
        }
        "blockSubscribe" => {
            subs.block = true;
            (
                serde_json::json!({"result": "subscribed", "subscription": "block"}),
                true,
            )
        }
        "blockUnsubscribe" => {
            subs.block = false;
            (
                serde_json::json!({"result": "unsubscribed", "subscription": "block"}),
                true,
            )
        }
        _ => (
            serde_json::json!({"error": format!("unknown method: {}", req.method)}),
            false,
        ),
    };

    if recognized {
        metrics::counter!("rpc_ws_subscriptions", "method" => req.method.clone()).increment(1);
        debug!(method = %req.method, active = subs.has_any(), "subscription updated");
    }

    let _ = socket.send(Message::Text(ack.to_string().into())).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscriptions_default_has_none() {
        let subs = Subscriptions::default();
        assert!(!subs.has_any());
        assert!(!subs.matches(&PubsubEvent::SlotUpdate {
            slot: 1,
            parent: 0,
            root: 0,
        }));
        assert!(!subs.matches(&PubsubEvent::BlockNotification {
            slot: 1,
            block_hash: "abc".to_string(),
            tx_count: 0,
        }));
    }

    #[test]
    fn subscriptions_slot_only() {
        let subs = Subscriptions {
            slot: true,
            block: false,
        };
        assert!(subs.has_any());
        assert!(subs.matches(&PubsubEvent::SlotUpdate {
            slot: 1,
            parent: 0,
            root: 0,
        }));
        assert!(!subs.matches(&PubsubEvent::BlockNotification {
            slot: 1,
            block_hash: "abc".to_string(),
            tx_count: 0,
        }));
    }

    #[test]
    fn subscriptions_block_only() {
        let subs = Subscriptions {
            slot: false,
            block: true,
        };
        assert!(subs.has_any());
        assert!(!subs.matches(&PubsubEvent::SlotUpdate {
            slot: 1,
            parent: 0,
            root: 0,
        }));
        assert!(subs.matches(&PubsubEvent::BlockNotification {
            slot: 1,
            block_hash: "abc".to_string(),
            tx_count: 0,
        }));
    }

    #[test]
    fn subscriptions_both() {
        let subs = Subscriptions {
            slot: true,
            block: true,
        };
        assert!(subs.has_any());
        assert!(subs.matches(&PubsubEvent::SlotUpdate {
            slot: 5,
            parent: 4,
            root: 3,
        }));
        assert!(subs.matches(&PubsubEvent::BlockNotification {
            slot: 5,
            block_hash: "xyz".to_string(),
            tx_count: 10,
        }));
    }

    #[test]
    fn pubsub_event_serializes_with_type_tag() {
        let event = PubsubEvent::SlotUpdate {
            slot: 42,
            parent: 41,
            root: 40,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["type"], "SlotUpdate");
        assert_eq!(json["slot"], 42);
        assert_eq!(json["parent"], 41);
        assert_eq!(json["root"], 40);
    }

    #[test]
    fn pubsub_event_block_serializes_with_type_tag() {
        let event = PubsubEvent::BlockNotification {
            slot: 100,
            block_hash: "deadbeef".to_string(),
            tx_count: 7,
        };
        let json = serde_json::to_value(&event).expect("serialize");
        assert_eq!(json["type"], "BlockNotification");
        assert_eq!(json["slot"], 100);
        assert_eq!(json["block_hash"], "deadbeef");
        assert_eq!(json["tx_count"], 7);
    }

    #[test]
    fn client_request_deserializes() {
        let raw = r#"{"method": "slotSubscribe"}"#;
        let req: ClientRequest = serde_json::from_str(raw).expect("parse");
        assert_eq!(req.method, "slotSubscribe");
    }

    #[test]
    fn new_pubsub_channel_creates_working_pair() {
        let tx = RpcState::new_pubsub_channel();
        let mut rx = tx.subscribe();

        let event = PubsubEvent::SlotUpdate {
            slot: 1,
            parent: 0,
            root: 0,
        };
        tx.send(event.clone()).expect("send");

        let received = rx.try_recv().expect("recv");
        match received {
            PubsubEvent::SlotUpdate { slot, parent, root } => {
                assert_eq!(slot, 1);
                assert_eq!(parent, 0);
                assert_eq!(root, 0);
            }
            _ => panic!("unexpected event variant"),
        }
    }
}
