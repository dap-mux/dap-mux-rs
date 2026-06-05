//! Shared test harness: a mock DAP adapter and a mock DAP client.

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dap_mux::protocol::{DapMessage, read_message, write_message};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{Instant, sleep};

const POLL: Duration = Duration::from_millis(10);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// A mock DAP debug adapter: accepts one connection, records every request,
/// replies with canned success responses, and can emit events on demand.
pub struct FakeAdapter {
    pub port: u16,
    received: Arc<Mutex<Vec<DapMessage>>>,
    outbound: UnboundedSender<DapMessage>,
}

impl FakeAdapter {
    /// Bind on an OS-assigned localhost port and start serving.
    pub async fn start() -> FakeAdapter {
        Self::start_inner(false, false).await
    }

    /// `start`, but the adapter advertises `supportsRestartRequest` in its initialize capabilities.
    pub async fn start_with_restart() -> FakeAdapter {
        Self::start_inner(true, false).await
    }

    /// `start`, but the adapter advertises `supportsTerminateRequest` in its initialize capabilities.
    pub async fn start_with_terminate() -> FakeAdapter {
        Self::start_inner(false, true).await
    }

    async fn start_inner(supports_restart: bool, supports_terminate: bool) -> FakeAdapter {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(Mutex::new(Vec::new()));
        let (outbound, mut outbound_rx) = unbounded_channel::<DapMessage>();

        let received_task = Arc::clone(&received);
        let outbound_for_responses = outbound.clone();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = stream.set_nodelay(true);
            let (mut reader, mut writer) = stream.into_split();

            // Write loop: drain outbound to the proxy.
            tokio::spawn(async move {
                while let Some(msg) = outbound_rx.recv().await {
                    if write_message(&mut writer, &msg).await.is_err() {
                        break;
                    }
                }
            });

            // Read loop: record requests and enqueue canned responses.
            let mut seq = 1i64;
            while let Ok(msg) = read_message(&mut reader).await {
                received_task.lock().unwrap().push(msg.clone());
                let mut response = canned_response(&msg, &mut seq);
                if msg.get("command").and_then(Value::as_str) == Some("initialize") {
                    if supports_restart {
                        response["body"]["supportsRestartRequest"] = json!(true);
                    }
                    if supports_terminate {
                        response["body"]["supportsTerminateRequest"] = json!(true);
                    }
                }
                if outbound_for_responses.send(response).is_err() {
                    break;
                }
            }
        });

        FakeAdapter {
            port,
            received,
            outbound,
        }
    }

    /// Snapshot of every message the adapter has received from the proxy.
    pub fn received(&self) -> Vec<DapMessage> {
        self.received.lock().unwrap().clone()
    }

    /// Count received messages with the given `command`.
    pub fn count_command(&self, command: &str) -> usize {
        self.received()
            .iter()
            .filter(|m| m.get("command").and_then(Value::as_str) == Some(command))
            .count()
    }

    /// Emit a DAP event to the connected proxy.
    pub fn send_event(&self, event: &str, body: Value) {
        let mut msg = json!({"seq": 0, "type": "event", "event": event});
        if !body.is_null() {
            msg["body"] = body;
        }
        self.outbound.send(msg).unwrap();
    }

    /// Issue a reverse request (adapter -> client) through the proxy.
    pub fn send_reverse_request(&self, command: &str, arguments: Value) {
        self.outbound
            .send(json!({
                "seq": 0,
                "type": "request",
                "command": command,
                "arguments": arguments,
            }))
            .unwrap();
    }
}

fn canned_response(request: &DapMessage, seq: &mut i64) -> DapMessage {
    let command = request.get("command").and_then(Value::as_str).unwrap_or("");
    let request_seq = request.get("seq").cloned().unwrap_or(json!(0));
    let mut response = json!({
        "seq": *seq,
        "type": "response",
        "request_seq": request_seq,
        "success": true,
        "command": command,
    });
    *seq += 1;

    match command {
        "initialize" => {
            response["body"] = json!({
                "supportsConfigurationDoneRequest": true,
                "supportsEvaluateForHovers": true,
                "supportsSetVariable": true,
            });
        }
        "threads" => {
            response["body"] = json!({"threads": [{"id": 1, "name": "MainThread"}]});
        }
        "stackTrace" => {
            response["body"] = json!({
                "stackFrames": [
                    {"id": 1, "name": "main", "source": {"path": "target.py"}, "line": 10, "column": 1}
                ],
                "totalFrames": 1,
            });
        }
        "evaluate" => {
            let expr = request
                .get("arguments")
                .and_then(|a| a.get("expression"))
                .and_then(Value::as_str)
                .unwrap_or("");
            response["body"] =
                json!({"result": format!("<eval: {expr}>"), "variablesReference": 0});
        }
        // `variables` deliberately fails with a not-found message so tests can
        // exercise the stale-variable-reference rewrite.
        "variables" => {
            response["success"] = json!(false);
            response["message"] = json!("Variable reference not found");
        }
        _ => {}
    }
    response
}

/// A mock DAP client: connects to the mux, sends requests, and collects every
/// inbound message for assertions.
pub struct FakeClient {
    received: Arc<Mutex<Vec<DapMessage>>>,
    outbound: UnboundedSender<DapMessage>,
    /// Set true when the read loop ends and the mux closed the connection.
    closed: Arc<AtomicBool>,
    seq: i64,
}

impl FakeClient {
    /// Connect to the multiplexer at `host:port`.
    pub async fn connect(host: &str, port: u16) -> FakeClient {
        let stream = TcpStream::connect((host, port)).await.unwrap();
        let _ = stream.set_nodelay(true);
        let (mut reader, mut writer) = stream.into_split();
        let received = Arc::new(Mutex::new(Vec::new()));
        let (outbound, mut outbound_rx) = unbounded_channel::<DapMessage>();
        let closed = Arc::new(AtomicBool::new(false));

        let received_task = Arc::clone(&received);
        let closed_task = Arc::clone(&closed);
        tokio::spawn(async move {
            while let Ok(msg) = read_message(&mut reader).await {
                received_task.lock().unwrap().push(msg);
            }
            // The read loop ends when the mux closes the socket (EOF/error).
            closed_task.store(true, Ordering::SeqCst);
        });
        tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if write_message(&mut writer, &msg).await.is_err() {
                    break;
                }
            }
        });

        FakeClient {
            received,
            outbound,
            closed,
            seq: 1,
        }
    }

    /// Whether the mux has closed this client's connection.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Wait until the mux closes this client's connection, or panic on timeout.
    pub async fn wait_closed(&self) {
        let deadline = Instant::now() + DEFAULT_TIMEOUT;
        while Instant::now() < deadline {
            if self.is_closed() {
                return;
            }
            sleep(POLL).await;
        }
        panic!("Timed out waiting for the connection to close");
    }

    /// Send a DAP request, returning the `seq` used. The response appears in
    /// the received log asynchronously.
    pub fn send(&mut self, command: &str, arguments: Value) -> i64 {
        let seq = self.seq;
        self.seq += 1;
        let mut msg = json!({"seq": seq, "type": "request", "command": command});
        if !arguments.is_null() {
            msg["arguments"] = arguments;
        }
        self.outbound.send(msg).unwrap();
        seq
    }

    /// Send an arbitrary raw message. This will exercise the non-request path.
    pub fn send_raw(&self, msg: Value) {
        self.outbound.send(msg).unwrap();
    }

    /// Disconnect from the multiplexer.
    pub fn close(self) {
        // Dropping `self` drops `outbound`. the write task ends and the socket
        // closes. Explicit for readability at call sites.
    }

    /// Snapshot of every message received.
    pub fn received(&self) -> Vec<DapMessage> {
        self.received.lock().unwrap().clone()
    }

    /// Wait for a response to `command`, or panic on timeout.
    pub async fn wait_for_response(&self, command: &str) -> DapMessage {
        self.wait_for(
            |m| {
                m.get("type").and_then(Value::as_str) == Some("response")
                    && m.get("command").and_then(Value::as_str) == Some(command)
            },
            &format!("response to {command:?}"),
        )
        .await
    }

    /// Wait for a reverse request (adapter -> client) for `command`.
    pub async fn wait_for_request(&self, command: &str) -> DapMessage {
        self.wait_for(
            |m| {
                m.get("type").and_then(Value::as_str) == Some("request")
                    && m.get("command").and_then(Value::as_str) == Some(command)
            },
            &format!("reverse request {command:?}"),
        )
        .await
    }

    /// Wait for an event of type `event`, or panic on timeout.
    pub async fn wait_for_event(&self, event: &str) -> DapMessage {
        self.wait_for(
            |m| {
                m.get("type").and_then(Value::as_str) == Some("event")
                    && m.get("event").and_then(Value::as_str) == Some(event)
            },
            &format!("event {event:?}"),
        )
        .await
    }

    async fn wait_for<F>(&self, pred: F, what: &str) -> DapMessage
    where
        F: Fn(&DapMessage) -> bool,
    {
        let deadline = Instant::now() + DEFAULT_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(m) = self.received().iter().find(|m| pred(m)) {
                return m.clone();
            }
            sleep(POLL).await;
        }
        panic!("Timed out waiting for {what}");
    }
}

/// Pause briefly to let the mux observe a connection/disconnection.
pub async fn settle() {
    sleep(Duration::from_millis(50)).await;
}

/// A running multiplexer connected to a fresh fake adapter.
pub struct Harness {
    pub mux: Arc<dap_mux::Multiplexer>,
    pub mux_port: u16,
    pub adapter: FakeAdapter,
}

impl Harness {
    /// Start a fake adapter, connect a mux to it, and serve clients on an
    /// OS-assigned port.
    pub async fn start() -> Harness {
        Self::start_with(FakeAdapter::start().await).await
    }

    /// `start`, but drives a caller-supplied adapter.
    pub async fn start_with(adapter: FakeAdapter) -> Harness {
        Self::start_with_mux(adapter, dap_mux::Multiplexer::new()).await
    }

    /// `start_with`, but the mux's debuggee is owned by a separate operator (the TUI),
    /// so a client's `terminate` never stops it.
    pub async fn start_operator_owned_with(adapter: FakeAdapter) -> Harness {
        Self::start_with_mux(adapter, dap_mux::Multiplexer::new_operator_owned()).await
    }

    async fn start_with_mux(adapter: FakeAdapter, mux: Arc<dap_mux::Multiplexer>) -> Harness {
        mux.connect_upstream(&dap_mux::UpstreamTransport::tcp("127.0.0.1", adapter.port))
            .await
            .unwrap();
        let mux_port = mux.serve("127.0.0.1", 0).await.unwrap();
        Harness {
            mux,
            mux_port,
            adapter,
        }
    }

    /// Connect a new client to this mux and wait for it to be accepted.
    pub async fn client(&self) -> FakeClient {
        let c = FakeClient::connect("127.0.0.1", self.mux_port).await;
        settle().await;
        c
    }
}
