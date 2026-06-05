mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{FakeAdapter, FakeClient, Harness, settle};
use dap_mux::{ClientListener, Multiplexer, UpstreamTransport};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::time::timeout;

/// Session end loops in interactive mode, clients are disconnected and state
/// reset at the boundary, and the listen port stays stable across iterations.
#[tokio::test]
async fn session_loops_on_adapter_loss_with_stable_port() {
    // The listener is bound once for the whole "process" lifetime.
    let listener = ClientListener::bind("127.0.0.1", 0).await.unwrap();
    let port = listener.port();

    // --- Session 1: an adapter that drops its connection mid-session. ---
    let adapter1 = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let a1_port = adapter1.local_addr().unwrap().port();
    let dropper = tokio::spawn(async move {
        let (stream, _) = adapter1.accept().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(stream); // the adapter connection is lost
    });

    let mux1 = Multiplexer::new();
    mux1.connect_upstream(&UpstreamTransport::tcp("127.0.0.1", a1_port))
        .await
        .unwrap();
    let accept1 = listener.accept_into(Arc::clone(&mux1));

    let client1 = FakeClient::connect("127.0.0.1", port).await;
    settle().await;
    assert_eq!(mux1.client_count(), 1);

    // Losing the adapter ends the session.
    timeout(Duration::from_secs(2), mux1.wait_for_shutdown())
        .await
        .expect("session should end when the adapter connection drops");

    // The operator (the loop) tears the session down: oneshot clients close.
    mux1.end_session();
    drop(accept1);
    client1.wait_closed().await;
    assert_eq!(
        mux1.client_count(),
        0,
        "clients disconnected at the boundary"
    );

    // --- Session 2: same listener/port, a fresh adapter and fresh mux. ---
    let adapter2 = FakeAdapter::start().await;
    let mux2 = Multiplexer::new();
    mux2.connect_upstream(&UpstreamTransport::tcp("127.0.0.1", adapter2.port))
        .await
        .unwrap();
    let _accept2 = listener.accept_into(Arc::clone(&mux2));

    assert_eq!(
        listener.port(),
        port,
        "listen port must be stable across loop iterations"
    );

    // A client reconnects on the same port and starts a clean session.
    let mut client2 = FakeClient::connect("127.0.0.1", port).await;
    settle().await;
    client2.send("initialize", json!({"clientID": "c"}));
    let resp = client2.wait_for_response("initialize").await;
    assert_eq!(resp["success"], json!(true));

    // Fresh state: a brand-new initialize reached the new adapter, and the new
    // session's first client is numbered from scratch.
    assert_eq!(adapter2.count_command("initialize"), 1);
    let snap = mux2.snapshot();
    assert_eq!(snap.clients.len(), 1);
    assert_eq!(snap.clients[0].id, "client-1");

    let _ = dropper.await;
}

/// A debuggee `terminated` event from a still-connected adapter must not end
/// the session: no shutdown signal, the upstream stays connected, and clients
/// can keep working.
#[tokio::test]
async fn debuggee_termination_is_not_a_session_end() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.send("initialize", json!({"clientID": "c"}));
    client.wait_for_response("initialize").await;

    // The adapter (still connected) reports the debuggee terminated.
    harness.adapter.send_event("terminated", Value::Null);
    client.wait_for_event("terminated").await;

    // The session must not end.
    let shutdown = timeout(Duration::from_millis(300), harness.mux.wait_for_shutdown()).await;
    assert!(
        shutdown.is_err(),
        "debuggee termination must not signal shutdown"
    );
    assert!(
        harness.mux.snapshot().upstream_connected,
        "upstream should remain connected"
    );

    // And the session still routes requests.
    client.send("threads", Value::Null);
    let resp = client.wait_for_response("threads").await;
    assert_eq!(resp["success"], json!(true));
}
