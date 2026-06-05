//! These exercise the transport mechanics of spawning the adapter — stdio
//! upstream, raw stderr capture, and reaping — using ordinary subprocesses
//! like `sh` or `sleep` or `echo` rather than a DAP-speaking adapter, since the routing
//! core is transport-agnostic and already covered over TCP.

use std::time::Duration;

use dap_mux::UpstreamTransport;
use tokio::time::{Instant, sleep};

/// A spawned adapter's stderr is captured raw and verbatim to `--adapter-log`.
#[tokio::test]
async fn adapter_stderr_is_captured_raw_to_the_adapter_log() {
    let dir = std::env::temp_dir().join(format!(
        "dap-mux-adapter-log-{}-{}",
        std::process::id(),
        line!()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let log_path = dir.join("adapter.log");

    // A trivial "adapter" that writes one line to stderr and exits. We never
    // speak DAP to it; we only assert the stderr pump persisted its output.
    let transport = UpstreamTransport::stdio(
        "sh",
        vec!["-c".into(), "echo adapter-says-hi >&2".into()],
        Some(log_path.clone()),
    );
    let io = transport.connect().await.expect("spawn succeeds");

    // The pump runs on a background task; poll the file until the line lands.
    let deadline = Instant::now() + Duration::from_secs(5);
    let contents = loop {
        if let Ok(text) = std::fs::read_to_string(&log_path)
            && text.contains("adapter-says-hi")
        {
            break text;
        }
        if Instant::now() >= deadline {
            panic!("adapter stderr never reached the adapter log");
        }
        sleep(Duration::from_millis(20)).await;
    };

    // Verbatim: the exact line the adapter wrote, with no added prefix.
    assert_eq!(contents, "adapter-says-hi\n");

    drop(io);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A spawned adapter is reaped via `start_kill` — the ownership cleanup the mux
/// performs on exit. Here we drive the underlying child directly to prove the
/// kill takes effect. The mux's `reap_adapter` delegates to the same call.
#[tokio::test]
async fn spawned_adapter_is_reaped() {
    let transport = UpstreamTransport::stdio("sh", vec!["-c".into(), "sleep 100".into()], None);
    let mut io = transport.connect().await.expect("spawn succeeds");
    let mut child = io.child.take().expect("stdio transport carries the child");
    assert!(child.id().is_some(), "child is running");

    child.start_kill().expect("kill signal sent");
    // The child exits promptly once killed. `wait` resolves rather than hanging
    // on the `sleep 100`.
    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("killed child exits within 5s")
        .expect("wait succeeds");
    assert!(!status.success(), "a killed process is not a clean exit");
}

/// Attach mode carries no child to reap. The mux did not launch the adapter.
#[tokio::test]
async fn attach_carries_no_child() {
    // Bind a listener so connect succeeds without retry noise.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    let accept = tokio::spawn(async move {
        let _ = listener.accept().await;
        // Hold the connection open briefly so the client side stays connected.
        sleep(Duration::from_millis(200)).await;
    });

    let transport = UpstreamTransport::tcp("127.0.0.1", port);
    let io = transport.connect().await.expect("attach connects");
    assert!(io.child.is_none(), "attach mode owns no adapter process");

    drop(io);
    let _ = accept.await;
}
