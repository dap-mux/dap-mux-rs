//! Integration tests for the `mux-runtime` spec scenarios.
mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{FakeAdapter, FakeClient, Harness};
use dap_mux::TcpUpstream;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{sleep, timeout};

// ---------------------------------------------------------------------------
// Attach to a running TCP DAP adapter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn attach_connects_to_listening_adapter() {
    // A fake adapter is listening. The mux connects and routes immediately.
    let harness = Harness::start().await;
    let mut client = harness.client().await;
    client.send("threads", Value::Null);
    let response = client.wait_for_response("threads").await;
    assert_eq!(response["success"], json!(true));
}

#[tokio::test]
async fn attach_retries_until_adapter_is_ready() {
    // Reserve a port, then close it so nothing is listening yet.
    let probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let transport = TcpUpstream {
        host: "127.0.0.1".to_string(),
        port,
        retry_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(25),
    };

    // Start connecting while the port is still closed.
    let connect = tokio::spawn(async move { transport.connect().await });

    // Bring the adapter up after a delay.
    sleep(Duration::from_millis(200)).await;
    let _listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();

    let io = connect.await.unwrap();
    assert!(io.is_ok(), "connect should succeed once the adapter is up");
}

#[tokio::test]
async fn attach_times_out_when_adapter_never_appears() {
    let probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let transport = TcpUpstream {
        host: "127.0.0.1".to_string(),
        port,
        retry_timeout: Duration::from_millis(250),
        retry_interval: Duration::from_millis(25),
    };

    let err = transport.connect().await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
}

// ---------------------------------------------------------------------------
// Upstream adapter loss ends the mux. debuggee termination does not.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn upstream_disconnect_notifies_clients_and_shuts_down() {
    // An adapter that accepts one connection, stays up briefly so a client can
    // attach, then drops the socket — simulating the debugger process dying.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let adapter = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        sleep(Duration::from_millis(300)).await;
        drop(stream);
    });

    let mux = dap_mux::Multiplexer::new();
    mux.connect_upstream(&dap_mux::UpstreamTransport::tcp("127.0.0.1", port))
        .await
        .unwrap();
    let mux_port = mux.serve("127.0.0.1", 0).await.unwrap();

    let client = FakeClient::connect("127.0.0.1", mux_port).await;
    sleep(Duration::from_millis(50)).await; // ensure the client is registered

    // Losing the adapter must signal shutdown...
    timeout(Duration::from_secs(2), mux.wait_for_shutdown())
        .await
        .expect("mux should signal shutdown when the adapter connection drops");
    // ...and tell the client its session is over.
    client.wait_for_event("terminated").await;

    let _ = adapter.await;
}

// ---------------------------------------------------------------------------
// Downstream client listener with OS-assigned port.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn os_assigned_port_is_reported() {
    // Harness serves on port 0. The reported port must be a real bound port.
    let harness = Harness::start().await;
    assert_ne!(
        harness.mux_port, 0,
        "an OS-assigned port should be reported"
    );

    // And a client can connect on it.
    let _client = FakeClient::connect("127.0.0.1", harness.mux_port).await;
}

#[tokio::test]
async fn multiple_clients_connect_on_listening_port() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;
    let mut client3 = harness.client().await;

    client1.send("threads", Value::Null);
    client2.send("threads", Value::Null);
    client3.send("threads", Value::Null);

    for c in [&client1, &client2, &client3] {
        let response = c.wait_for_response("threads").await;
        assert_eq!(response["success"], json!(true));
    }
    assert_eq!(harness.mux.client_count(), 3);
}

/// Spawn the real `dap-mux` binary as a TCP host attached to a
/// fake adapter, then drive a client through it. Demonstrates that the
/// self-contained binary connects upstream, reports its bound port, and routes
/// a full request.
#[tokio::test]
async fn compiled_binary_runs() {
    let adapter = FakeAdapter::start().await;

    let mut child = Command::new(env!("CARGO_BIN_EXE_dap-mux"))
        .args(["--attach", &adapter.port.to_string(), "-p", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn dap-mux binary");

    // The binary prints its bound port on stdout. Parse it.
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let port = timeout(Duration::from_secs(5), async {
        loop {
            match lines.next_line().await.unwrap() {
                Some(line) => {
                    if let Some(p) = parse_listening_port(&line) {
                        return p;
                    }
                }
                None => panic!("binary exited before reporting its port"),
            }
        }
    })
    .await
    .expect("binary should report a port within 5s");

    let mut client = FakeClient::connect("127.0.0.1", port).await;
    client.send("initialize", json!({"clientID": "test", "adapterID": "x"}));
    let response = client.wait_for_response("initialize").await;
    assert_eq!(response["success"], json!(true));
    assert!(response.get("body").is_some());

    child.start_kill().unwrap();
}

/// Extract the port from a line like
/// `● dap-mux listening on 127.0.0.1:54321 — Ctrl-C to stop`.
fn parse_listening_port(line: &str) -> Option<u16> {
    let after = line.split("127.0.0.1:").nth(1)?;
    let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}
