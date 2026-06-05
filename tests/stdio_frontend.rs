//! The mux's stdin/stdout are stood in for by an in-memory duplex pipe
//! mimicking the "editor", so these exercise the frontend serving and
//! lifeline behavior without a real subprocess. Upstream is an ordinary TCP fake adapter.

mod common;

use std::time::Duration;

use common::{Harness, settle};
use dap_mux::protocol::{DapMessage, read_message, write_message};
use serde_json::json;
use tokio::io::{AsyncRead, BufReader, duplex, split};
use tokio::time::{Instant, timeout};

/// Read framed DAP messages from `reader` until one matches `pred`, or panic.
async fn read_until<R, F>(reader: &mut R, pred: F, what: &str) -> DapMessage
where
    R: AsyncRead + Unpin,
    F: Fn(&DapMessage) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = timeout(remaining, read_message(reader))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
            .expect("stream stayed open");
        if pred(&message) {
            return message;
        }
    }
}

/// The stdio-frontend client is multiplexed alongside a TCP client: an adapter
/// event reaches both.
#[tokio::test]
async fn stdio_frontend_client_is_multiplexed_with_tcp_clients() {
    let harness = Harness::start().await;

    // The "editor" drives the mux over a duplex pipe standing in for stdin/stdout.
    let (editor, mux_side) = duplex(64 * 1024);
    let (mux_read, mux_write) = split(mux_side);
    let _stdio = harness
        .mux
        .serve_stdio_frontend(BufReader::new(mux_read), mux_write);
    let (mut editor_read, mut editor_write) = split(editor);
    settle().await;
    assert_eq!(
        harness.mux.client_count(),
        1,
        "the stdio client is registered"
    );

    // The editor brings the session up like any client.
    write_message(
        &mut editor_write,
        &json!({"seq": 1, "type": "request", "command": "initialize",
                "arguments": {"clientID": "editor", "adapterID": "x"}}),
    )
    .await
    .unwrap();
    let response = read_until(
        &mut editor_read,
        |m| m["command"] == json!("initialize"),
        "initialize response",
    )
    .await;
    assert_eq!(response["success"], json!(true));
    assert!(
        response.get("body").is_some(),
        "capabilities relayed to the editor"
    );

    // A second client attaches over the TCP listener.
    let client2 = harness.client().await;
    assert_eq!(harness.mux.client_count(), 2);

    // An adapter event broadcasts to both the stdio client and the TCP client.
    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint", "threadId": 1}));
    let stopped = read_until(
        &mut editor_read,
        |m| m["event"] == json!("stopped"),
        "stopped event on the stdio client",
    )
    .await;
    assert_eq!(stopped["body"]["reason"], json!("breakpoint"));
    client2.wait_for_event("stopped").await;
}

/// The launcher's stdin reaching EOF ends the stdio-frontend session: the
/// serving task resolves and the client is removed.
#[tokio::test]
async fn stdio_frontend_stdin_eof_ends_the_session() {
    let harness = Harness::start().await;

    let (editor, mux_side) = duplex(1024);
    let (mux_read, mux_write) = split(mux_side);
    let handle = harness
        .mux
        .serve_stdio_frontend(BufReader::new(mux_read), mux_write);
    settle().await;
    assert_eq!(harness.mux.client_count(), 1);

    // The editor goes away: dropping its end is EOF on the mux's read side.
    drop(editor);

    timeout(Duration::from_secs(5), handle)
        .await
        .expect("the serving task resolves on stdin EOF")
        .expect("the read task did not panic");

    settle().await;
    assert_eq!(
        harness.mux.client_count(),
        0,
        "the stdio client is removed on EOF"
    );
}
