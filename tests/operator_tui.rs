//! Integration tests for the TUI.

mod common;

use common::{Harness, settle};
use serde_json::json;

/// The session view reflects the connected clients: each appears with its id,
/// initialize status, and in-flight request count.
#[tokio::test]
async fn snapshot_reflects_connected_clients() {
    let harness = Harness::start().await;

    // client-1 completes initialize. client-2 connects but does not.
    let mut client1 = harness.client().await;
    client1.send("initialize", json!({"clientID": "c1"}));
    client1.wait_for_response("initialize").await;
    let _client2 = harness.client().await;
    settle().await;

    let snap = harness.mux.snapshot();
    assert_eq!(snap.clients.len(), 2);

    let client1_info = snap
        .clients
        .iter()
        .find(|c| c.id == "client-1")
        .expect("client-1 in snapshot");
    assert!(client1_info.initialized, "client-1 completed initialize");
    assert_eq!(
        client1_info.pending_requests, 0,
        "no in-flight requests after responses settle"
    );

    let client2_info = snap
        .clients
        .iter()
        .find(|c| c.id == "client-2")
        .expect("client-2 in snapshot");
    assert!(
        !client2_info.initialized,
        "client-2 has not completed initialize"
    );
}

/// A client's self-reported DAP identity (`clientName` or `clientID`) is
/// surfaced in the operator view once it has sent `initialize`.
#[tokio::test]
async fn snapshot_surfaces_client_name() {
    let harness = Harness::start().await;

    let mut named = harness.client().await;
    named.send(
        "initialize",
        json!({"clientID": "helix-id", "clientName": "Helix"}),
    );
    named.wait_for_response("initialize").await;

    let mut id_only = harness.client().await;
    id_only.send("initialize", json!({"clientID": "vscode"}));
    id_only.wait_for_response("initialize").await;

    settle().await;
    let snap = harness.mux.snapshot();

    let named = snap.clients.iter().find(|c| c.id == "client-1").unwrap();
    assert_eq!(named.name.as_deref(), Some("Helix"), "clientName preferred");

    let id_only = snap.clients.iter().find(|c| c.id == "client-2").unwrap();
    assert_eq!(
        id_only.name.as_deref(),
        Some("vscode"),
        "clientID used when clientName is absent"
    );
}

/// A client that has not yet sent `initialize` has no name. The view falls back
/// to its transport id.
#[tokio::test]
async fn snapshot_has_no_name_before_initialize() {
    let harness = Harness::start().await;
    let _client = harness.client().await;
    settle().await;

    let snap = harness.mux.snapshot();
    assert_eq!(snap.clients[0].name, None);
}
