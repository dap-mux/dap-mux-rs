//! The DAP multiplexer: one upstream adapter, many downstream clients.
//!
//! Requests from clients are forwarded upstream with rewritten sequence
//! numbers. Responses are routed back to the originating client. Events are
//! broadcast to all connected clients. The mux owns the shared session
//! lifetime and replays state to late joiners.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use crate::client::{read_loop, write_loop};
use crate::compat::{
    is_known_reverse_request, pick_reverse_request_target, rewrite_stale_variable_error,
    should_filter_event,
};
use crate::protocol::{DapMessage, is_event, is_request, is_response};
use crate::seq::SeqMap;
use crate::upstream::{UpstreamIo, UpstreamTransport};

/// Linear phases of a DAP session, in order of progression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SessionPhase {
    /// Nothing initialized yet.
    PreInit,
    /// First `initialize` forwarded, awaiting the adapter's response.
    Initializing,
    /// Adapter responded; capabilities cached.
    Initialized,
    /// First `configurationDone` forwarded; session running.
    Configured,
}

/// Outbound message queue for one connection.
type Outbound = UnboundedSender<DapMessage>;

/// The DAP `command` field as a string, or "?" when absent. Used for log lines,
/// where showing a missing command beats erroring on it.
fn command_field(message: &DapMessage) -> &str {
    message
        .get("command")
        .and_then(Value::as_str)
        .unwrap_or("?")
}

/// The client's display identity from `initialize` arguments: the human-readable
/// `clientName`, falling back to the machine `clientID`. Both are optional in the
/// DAP spec, so this returns `None` when neither is present.
fn client_display_name(arguments: &Value) -> Option<String> {
    arguments
        .get("clientName")
        .or_else(|| arguments.get("clientID"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// A connected downstream client.
struct ClientConnection {
    id: String,
    /// Send queue feeding the client's write loop.
    tx: Outbound,
    /// Abort handle for the client's read task, so the mux can proactively
    /// close the connection at a session boundary (the read loop otherwise
    /// blocks on the socket until the client itself closes). `None` only in the
    /// brief window before the read task is spawned.
    read_task: Option<tokio::task::AbortHandle>,
    /// Whether this client has completed `initialize` (received its response or
    /// the replayed cached capabilities). Surfaced in the operator view.
    initialized: bool,
    /// The client's self-reported identity from the `initialize` request
    /// (`clientName`, falling back to `clientID`), once it has sent one.
    /// `None` until then, when the operator view falls back to [`id`](Self::id).
    display_name: Option<String>,
}

/// A point-in-time view of one client for the operator interface.
#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: String,
    /// The client's self-reported name (DAP `clientName`/`clientID`), or `None`
    /// until it has sent `initialize`.
    pub name: Option<String>,
    pub initialized: bool,
    pub pending_requests: usize,
}

/// A point-in-time view of the session for the operator interface.
#[derive(Debug, Clone)]
pub struct MuxSnapshot {
    pub phase: SessionPhase,
    pub upstream_connected: bool,
    pub clients: Vec<ClientInfo>,
}

/// A reverse request forwarded to a client, awaiting that client's response.
struct PendingReverseRequest {
    /// The adapter's original `seq`, restored when the response is forwarded up.
    adapter_seq: i64,
    /// The client the request was routed to, so it can be failed back to the
    /// adapter if that client disconnects before responding.
    client_id: String,
    /// The DAP `command`, needed to synthesize a failure response upstream.
    command: String,
}

/// Shared, serialized mux state.
struct MuxState {
    seq_map: SeqMap,
    /// Connected clients, in connection order.
    clients: Vec<ClientConnection>,
    /// Cached `initialize` arguments per client, in initialize order.
    client_init_args: Vec<(String, Value)>,
    client_counter: u64,
    phase: SessionPhase,
    pending_initialize: Vec<(String, DapMessage)>,
    cached_capabilities: Option<Value>,
    initialized_event: Option<DapMessage>,
    last_stopped_event: Option<DapMessage>,
    upstream_tx: Option<Outbound>,
    /// Reverse requests forwarded to a client, awaiting the client's response:
    /// `proxy_seq -> adapter's original seq`. The client sees `proxy_seq`; its
    /// response is rewritten back to the adapter's seq before forwarding up.
    /// The owning client and command are kept so the request can be failed back
    /// to the adapter if that client disconnects before answering.
    reverse_requests: HashMap<i64, PendingReverseRequest>,
    next_reverse_seq: i64,
}

impl MuxState {
    fn new() -> Self {
        Self {
            seq_map: SeqMap::new(),
            clients: Vec::new(),
            client_init_args: Vec::new(),
            client_counter: 0,
            phase: SessionPhase::PreInit,
            pending_initialize: Vec::new(),
            cached_capabilities: None,
            initialized_event: None,
            last_stopped_event: None,
            upstream_tx: None,
            reverse_requests: HashMap::new(),
            next_reverse_seq: 1,
        }
    }

    fn client_tx(&self, client_id: &str) -> Option<&Outbound> {
        self.clients
            .iter()
            .find(|c| c.id == client_id)
            .map(|c| &c.tx)
    }

    fn send_to(&self, client_id: &str, message: DapMessage) -> bool {
        match self.client_tx(client_id) {
            Some(tx) => tx.send(message).is_ok(),
            None => false,
        }
    }

    /// Mark a client as having completed `initialize`.
    fn mark_initialized(&mut self, client_id: &str) {
        if let Some(c) = self.clients.iter_mut().find(|c| c.id == client_id) {
            c.initialized = true;
        }
    }

    fn set_init_args(&mut self, client_id: &str, args: Value) {
        if let Some(entry) = self
            .client_init_args
            .iter_mut()
            .find(|(id, _)| id == client_id)
        {
            entry.1 = args;
        } else {
            self.client_init_args.push((client_id.to_string(), args));
        }
    }

    /// Remove a client that asked to leave via `disconnect`: close its
    /// connection and clean up its session state, while the session continues
    /// for any other clients.
    ///
    /// Removing the [`ClientConnection`] drops its send half, but any already
    /// queued message — notably the synthetic `disconnect` response sent just
    /// before this call — is flushed first: the write loop drains the channel
    /// before it observes the close. Aborting the read task closes the socket's
    /// read half, which the connection would otherwise hold open until the
    /// client itself closed.
    fn disconnect_client(&mut self, client_id: &str) {
        if let Some(position) = self.clients.iter().position(|c| c.id == client_id) {
            let connection = self.clients.remove(position);
            if let Some(read_task) = &connection.read_task {
                read_task.abort();
            }
        }
        let removed = self.cleanup_client_state(client_id);
        tracing::info!(client_id, removed, "Client disconnected at its request");
    }

    /// Tear down a departed client's per-session state. Shared by both exit
    /// paths — a client's own `disconnect`, and an observed connection drop —
    /// so the set of state cleaned for a client lives in one place. Returns the
    /// number of in-flight forward requests dropped.
    ///
    /// Any reverse request the client still owed the adapter a response for is
    /// failed back to the adapter, so a `runInTerminal`/`startDebugging` does
    /// not leave the adapter blocked on a client that is gone.
    fn cleanup_client_state(&mut self, client_id: &str) -> usize {
        let removed = self.seq_map.cleanup(client_id);
        self.client_init_args.retain(|(id, _)| id != client_id);
        self.pending_initialize.retain(|(id, _)| id != client_id);
        self.fail_reverse_requests_for_client(client_id);
        removed
    }

    // ------------------------------------------------------------------
    // Synthetic / cached responses to clients
    // ------------------------------------------------------------------

    fn respond_with_cached_initialize(&mut self, client_id: &str, message: &DapMessage) {
        if self.client_tx(client_id).is_none() {
            return;
        }
        let request_seq = message.get("seq").cloned().unwrap_or_else(|| json!(0));
        let mut response = json!({
            "seq": 0,
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": "initialize",
        });
        if let Some(caps) = &self.cached_capabilities {
            response["body"] = caps.clone();
        }
        self.send_to(client_id, response);
        self.mark_initialized(client_id);
        tracing::debug!(client_id, "MUX -> client: initialize (cached capabilities)");

        if let Some(event) = &self.initialized_event {
            let event = event.clone();
            self.send_to(client_id, event);
            tracing::debug!(client_id, "MUX -> client: initialized (replayed)");
        }
        if let Some(event) = &self.last_stopped_event {
            let event = event.clone();
            self.send_to(client_id, event);
            tracing::debug!(client_id, "MUX -> client: stopped (replayed)");
        }
    }

    /// Tell a client its `initialize` failed because the adapter rejected the
    /// session's initialize. Sent to clients queued behind the in-flight
    /// initialize so they do not wait on a response that will never come.
    fn respond_initialize_failure(&self, client_id: &str, message: &DapMessage, reason: &str) {
        if self.client_tx(client_id).is_none() {
            return;
        }
        let request_seq = message.get("seq").cloned().unwrap_or_else(|| json!(0));
        let response = json!({
            "seq": 0,
            "type": "response",
            "request_seq": request_seq,
            "success": false,
            "command": "initialize",
            "message": reason,
        });
        self.send_to(client_id, response);
        tracing::info!(client_id, "MUX -> client: initialize rejected (adapter failed initialize)");
    }

    fn respond_synthetic_success(&self, client_id: &str, message: &DapMessage) {
        if self.client_tx(client_id).is_none() {
            return;
        }
        let command = message.get("command").cloned().unwrap_or_else(|| json!(""));
        let request_seq = message.get("seq").cloned().unwrap_or_else(|| json!(0));
        let response = json!({
            "seq": 0,
            "type": "response",
            "request_seq": request_seq,
            "success": true,
            "command": command,
        });
        self.send_to(client_id, response);
        tracing::debug!(client_id, ?command, "MUX -> client: synthetic success");
    }

    /// Whether the adapter advertised `supportsRestartRequest` in its cached
    /// initialize capabilities.
    fn adapter_supports_restart(&self) -> bool {
        self.cached_capabilities
            .as_ref()
            .and_then(|caps| caps.get("supportsRestartRequest"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// Whether the adapter advertised `supportsTerminateRequest` in its cached
    /// initialize capabilities.
    fn adapter_supports_terminate(&self) -> bool {
        self.cached_capabilities
            .as_ref()
            .and_then(|caps| caps.get("supportsTerminateRequest"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// Reject a `restart` the adapter cannot honor, honestly, so the client
    /// knows the restart did not happen (rather than a misleading success).
    fn respond_restart_unsupported(&self, client_id: &str, message: &DapMessage) {
        if self.client_tx(client_id).is_none() {
            return;
        }
        let request_seq = message.get("seq").cloned().unwrap_or_else(|| json!(0));
        let response = json!({
            "seq": 0,
            "type": "response",
            "request_seq": request_seq,
            "success": false,
            "command": "restart",
            "message": "restart unsupported: the debug adapter does not advertise \
                        supportsRestartRequest; emulated restart cannot work through the mux",
        });
        self.send_to(client_id, response);
        tracing::info!(
            client_id,
            "restart rejected (adapter lacks supportsRestartRequest)"
        );
    }

    // ------------------------------------------------------------------
    // Routing — adapter -> client(s)
    // ------------------------------------------------------------------

    fn route_response(&mut self, message: DapMessage) {
        let Some(request_seq) = message.get("request_seq").and_then(Value::as_i64) else {
            tracing::warn!(?message, "Response missing request_seq");
            return;
        };

        // The first initialize response settles the session's init state. On
        // success, cache capabilities and serve the clients queued behind it
        // from that cache. On failure, the adapter rejected initialize: drop
        // back to the pre-init state so a later client may try again, and fail
        // the queued clients rather than leave them waiting on a response that
        // will never come.
        let is_init_response = message.get("command").and_then(Value::as_str)
            == Some("initialize")
            && self.phase == SessionPhase::Initializing;
        if is_init_response {
            let succeeded = message
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let buffered = std::mem::take(&mut self.pending_initialize);
            if succeeded {
                self.phase = SessionPhase::Initialized;
                self.cached_capabilities = message.get("body").cloned();
                tracing::debug!("Cached adapter capabilities from initialize response");
                for (buffered_id, buffered_message) in &buffered {
                    self.respond_with_cached_initialize(buffered_id, buffered_message);
                }
            } else {
                self.phase = SessionPhase::PreInit;
                let reason = message
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("debug adapter rejected initialize")
                    .to_string();
                tracing::warn!(reason, "Adapter rejected initialize");
                for (buffered_id, buffered_message) in &buffered {
                    self.respond_initialize_failure(buffered_id, buffered_message, &reason);
                }
            }
        }

        let Some(pending) = self.seq_map.resolve(request_seq) else {
            tracing::warn!(request_seq, "No pending request for response");
            return;
        };

        if self.client_tx(&pending.client_id).is_none() {
            tracing::debug!(client_id = %pending.client_id, request_seq, "Client gone, dropping response");
            return;
        }

        let mut restored = message;
        restored["request_seq"] = json!(pending.client_seq);
        let command = command_field(&restored);
        // The client that drove the first `initialize` learns its result here;
        // only a successful initialize actually initializes it.
        if command == "initialize"
            && restored
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            self.mark_initialized(&pending.client_id);
        }
        tracing::debug!(client_id = %pending.client_id, command, request_seq = pending.client_seq, "DA -> client");
        self.send_to(&pending.client_id, restored);
    }

    fn broadcast_event(&mut self, message: DapMessage) {
        let event_name = message.get("event").and_then(Value::as_str).unwrap_or("?");
        tracing::debug!(event = event_name, "DA -> * event");
        match event_name {
            "initialized" => self.initialized_event = Some(message.clone()),
            "stopped" => self.last_stopped_event = Some(message.clone()),
            "continued" | "terminated" => self.last_stopped_event = None,
            _ => {}
        }
        for c in &self.clients {
            let _ = c.tx.send(message.clone());
        }
    }

    fn route_reverse_request(&mut self, mut message: DapMessage) {
        // The adapter is blocked until it gets a response, so any path that
        // cannot deliver the request to a client must answer the adapter itself.
        let adapter_seq = message.get("seq").and_then(Value::as_i64).unwrap_or(0);
        let command = command_field(&message).to_string();

        let Some(target_id) = pick_reverse_request_target(&self.client_init_args, &message) else {
            self.fail_reverse_request_to_adapter(
                adapter_seq,
                &command,
                "dap-mux: no client available to handle reverse request",
            );
            return;
        };
        let target_id = target_id.to_string();
        if self.client_tx(&target_id).is_none() {
            tracing::warn!(client_id = %target_id, "Target client for reverse request is gone");
            self.fail_reverse_request_to_adapter(
                adapter_seq,
                &command,
                "dap-mux: target client for reverse request is gone",
            );
            return;
        }

        // Rewrite the adapter's seq to a mux-allocated one so the client's
        // response can be correlated back (adapters may reuse or zero the seq),
        // remembering the mapping to restore it when forwarding the response up.
        let proxy_seq = self.next_reverse_seq;
        self.next_reverse_seq += 1;
        self.reverse_requests.insert(
            proxy_seq,
            PendingReverseRequest {
                adapter_seq,
                client_id: target_id.clone(),
                command: command.clone(),
            },
        );
        message["seq"] = json!(proxy_seq);

        tracing::debug!(client_id = %target_id, command = %command, proxy_seq, "DA -> client: reverse request");
        self.send_to(&target_id, message);
    }

    // ------------------------------------------------------------------
    // Routing — client -> adapter (reverse-request responses)
    // ------------------------------------------------------------------

    /// Forward a client's response to a previously routed reverse request back
    /// up to the adapter, restoring the adapter's original seq. Returns `true`
    /// if `message` was such a response and was handled.
    fn forward_reverse_response(&mut self, message: DapMessage) -> bool {
        let Some(proxy_seq) = message.get("request_seq").and_then(Value::as_i64) else {
            return false;
        };
        let Some(pending) = self.reverse_requests.remove(&proxy_seq) else {
            return false;
        };
        let mut forwarded = message;
        forwarded["request_seq"] = json!(pending.adapter_seq);
        match &self.upstream_tx {
            Some(tx) => {
                let _ = tx.send(forwarded);
                tracing::debug!(
                    adapter_seq = pending.adapter_seq,
                    "client -> DA: reverse-request response"
                );
            }
            None => tracing::warn!("no upstream connection. Dropping reverse-request response"),
        }
        true
    }

    /// Answer a reverse request the mux could not route to a client by sending
    /// a failure response up to the adapter, so the adapter is not left blocked.
    fn fail_reverse_request_to_adapter(&self, adapter_seq: i64, command: &str, reason: &str) {
        let response = json!({
            "seq": 0,
            "type": "response",
            "request_seq": adapter_seq,
            "success": false,
            "command": command,
            "message": reason,
        });
        match &self.upstream_tx {
            Some(tx) => {
                let _ = tx.send(response);
                tracing::info!(adapter_seq, command, "MUX -> DA: reverse request failed");
            }
            None => tracing::warn!("no upstream connection; cannot fail reverse request"),
        }
    }

    /// Fail every reverse request still routed to `client_id` back to the
    /// adapter and forget them, so a departing client does not strand the
    /// adapter on a response that can no longer arrive.
    fn fail_reverse_requests_for_client(&mut self, client_id: &str) {
        let mut orphaned = Vec::new();
        self.reverse_requests.retain(|_, pending| {
            let owned_by_client = pending.client_id == client_id;
            if owned_by_client {
                orphaned.push((pending.adapter_seq, pending.command.clone()));
            }
            !owned_by_client
        });
        for (adapter_seq, command) in orphaned {
            self.fail_reverse_request_to_adapter(
                adapter_seq,
                &command,
                "dap-mux: client handling reverse request disconnected before responding",
            );
        }
    }
}

/// DAP multiplexer handle.
pub struct Multiplexer {
    state: Mutex<MuxState>,
    /// Fired when the session can no longer continue (the upstream adapter
    /// connection was lost). [`Multiplexer::wait_for_shutdown`] awaits this.
    shutdown: tokio::sync::Notify,
    /// The adapter process when the mux spawned it, held for the session's
    /// lifetime so the mux can reap it on exit. `None` in attach mode (the mux
    /// did not launch the adapter and must not terminate it). The handle has
    /// `kill_on_drop`, so dropping the mux is the backstop reap.
    spawned_child: Mutex<Option<tokio::process::Child>>,
    /// Whether a separate operator (the TUI) owns the debuggee's lifetime
    /// rather than a client. When true, a client's `terminate` only detaches
    /// that client — the shared debuggee is the operator's to end.
    operator_owned: bool,
}

impl Multiplexer {
    /// Create a new, unconnected multiplexer whose debuggee is owned by its
    /// clients (the stdio frontend, or a host launched to serve them).
    pub fn new() -> Arc<Self> {
        Self::build(false)
    }

    /// Create a multiplexer whose debuggee is owned by a separate operator (the
    /// TUI). A client's `terminate` then only detaches that client; ending the
    /// shared debuggee is the operator's call.
    pub fn new_operator_owned() -> Arc<Self> {
        Self::build(true)
    }

    fn build(operator_owned: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(MuxState::new()),
            shutdown: tokio::sync::Notify::new(),
            spawned_child: Mutex::new(None),
            operator_owned,
        })
    }

    /// Resolve once the session has ended because the upstream adapter
    /// connection was lost. The caller (the binary's `run`) should then exit.
    ///
    /// A debuggee *terminating* (a `terminated`/`exited` event) does not resolve
    /// this — the adapter stays alive across runs so clients can `restart`. Only
    /// the adapter connection itself dropping ends the mux.
    pub async fn wait_for_shutdown(&self) {
        self.shutdown.notified().await;
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Connect to the debug adapter through `transport` and start its loops.
    ///
    /// When `transport` spawned the adapter, the child handle is retained for
    /// reaping (see [`reap_adapter`](Self::reap_adapter)).
    pub async fn connect_upstream(
        self: &Arc<Self>,
        transport: &UpstreamTransport,
    ) -> Result<(), std::io::Error> {
        let UpstreamIo {
            reader,
            writer,
            child,
        } = transport.connect().await?;
        let (tx, rx) = unbounded_channel::<DapMessage>();
        {
            let mut state = self.state.lock().unwrap();
            state.upstream_tx = Some(tx);
        }
        *self.spawned_child.lock().unwrap() = child;

        let mux_for_message = Arc::clone(self);
        let mux_for_disconnect = Arc::clone(self);
        tokio::spawn(read_loop(
            "upstream".to_string(),
            reader,
            move |_, message| mux_for_message.handle_upstream_message(message),
            move |_| mux_for_disconnect.handle_upstream_disconnect(),
        ));
        tokio::spawn(write_loop("upstream".to_string(), writer, rx));
        Ok(())
    }

    /// Reap the adapter the mux spawned, if any.
    ///
    /// Spawning makes the mux the adapter's owner, so the mux terminates the
    /// child it launched when it exits — a leaked adapter holding a port or a
    /// stopped inferior is a worse, more confusing state than a clean failure.
    /// This is ownership cleanup, not session recovery: it never respawns. In
    /// attach mode there is no spawned child and this is a no-op (the mux must
    /// not terminate an adapter it merely connected to).
    pub fn reap_adapter(&self) {
        if let Some(mut child) = self.spawned_child.lock().unwrap().take() {
            let _ = child.start_kill();
            tracing::info!("Reaped spawned debug adapter");
        }
    }

    /// The upstream adapter connection was lost: the session cannot continue.
    ///
    /// Tells every client the session is over (a synthetic `terminated` event,
    /// preceded by an `output` notice for visibility), drops the dead upstream
    /// queue, and signals [`wait_for_shutdown`](Self::wait_for_shutdown).
    fn handle_upstream_disconnect(&self) {
        {
            let mut state = self.state.lock().unwrap();
            tracing::error!(
                clients = state.clients.len(),
                "Debug adapter connection closed; notifying clients and shutting down"
            );
            let notice = json!({
                "seq": 0,
                "type": "event",
                "event": "output",
                "body": {
                    "category": "console",
                    "output": "dap-mux: debug adapter connection lost; shutting down\n",
                },
            });
            let terminated = json!({"seq": 0, "type": "event", "event": "terminated"});
            for c in &state.clients {
                let _ = c.tx.send(notice.clone());
                let _ = c.tx.send(terminated.clone());
            }
            // Any further client request has nowhere to go.
            state.upstream_tx = None;
        }
        self.shutdown.notify_one();
    }

    /// Start accepting client connections on `host:port`.
    ///
    /// Returns the actual bound port, which is useful when `port` is 0 for an
    /// OS-assigned port, and spawns the accept loop as a background task.
    pub async fn serve(self: &Arc<Self>, host: &str, port: u16) -> Result<u16, std::io::Error> {
        let listener = TcpListener::bind((host, port)).await?;
        let actual_port = listener.local_addr()?.port();
        tracing::info!(
            host,
            port = actual_port,
            "Multiplexer listening for clients"
        );

        let mux = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => mux.accept_client(stream),
                    Err(err) => {
                        tracing::warn!(%err, "accept failed");
                    }
                }
            }
        });
        Ok(actual_port)
    }

    /// Number of currently connected clients.
    pub fn client_count(&self) -> usize {
        self.state.lock().unwrap().clients.len()
    }

    /// A point-in-time view of the session for the operator interface:
    /// session phase, whether the upstream is connected, and one entry per
    /// connected client (id, initialize status, in-flight request count).
    pub fn snapshot(&self) -> MuxSnapshot {
        let state = self.state.lock().unwrap();
        let clients = state
            .clients
            .iter()
            .map(|c| ClientInfo {
                id: c.id.clone(),
                name: c.display_name.clone(),
                initialized: c.initialized,
                pending_requests: state.seq_map.pending_for(&c.id),
            })
            .collect();
        MuxSnapshot {
            phase: state.phase,
            upstream_connected: state.upstream_tx.is_some(),
            clients,
        }
    }

    /// End the current session: close every downstream client connection.
    ///
    /// Aborts each client's read task and drops its send queue (ending the
    /// write task and closing the socket). This realizes the oneshot-client
    /// model — a client connection belongs to exactly one session — so callers
    /// recreate the [`Multiplexer`] for the next session rather than reusing
    /// this one (its session state is intentionally not reset in place).
    pub fn end_session(&self) {
        let mut state = self.state.lock().unwrap();
        let count = state.clients.len();
        for c in state.clients.drain(..) {
            if let Some(handle) = c.read_task {
                handle.abort();
            }
            // Dropping `c` here drops its send half — the only handle to that
            // channel — which is how the client's write task drains and stops
            // and its socket closes.
        }
        state.upstream_tx = None;
        if count > 0 {
            tracing::info!(clients = count, "session ended; clients disconnected");
        }
    }

    // ------------------------------------------------------------------
    // Client acceptance
    // ------------------------------------------------------------------

    fn accept_client(self: &Arc<Self>, stream: tokio::net::TcpStream) {
        let _ = stream.set_nodelay(true);
        let (reader, writer) = stream.into_split();
        // Buffer the read half: `read_message` reads the framing header a byte
        // at a time, so an unbuffered split half would be one syscall per byte.
        let reader = tokio::io::BufReader::new(reader);
        // The read task is detached here: a TCP client's session boundary closes
        // it via the stored abort handle, not by awaiting this handle. (The stdio
        // frontend, by contrast, awaits the handle to learn when stdin ends.)
        drop(self.register_client(reader, writer));
    }

    /// Serve a single downstream client over the given byte streams — the mux's
    /// own stdin/stdout in the stdio frontend, so an editor that launched the
    /// mux can drive it as a stdio adapter. The client is multiplexed exactly
    /// like a TCP client; the returned handle resolves when the stream ends
    /// (the launcher's stdin reaching EOF), which the caller treats as the end
    /// of the session.
    pub fn serve_stdio_frontend<R, W>(self: &Arc<Self>, reader: R, writer: W) -> JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        self.register_client(reader, writer)
    }

    /// Register one downstream client over `reader`/`writer`, spawn its read and
    /// write loops, and return the read task's handle. The transport is opaque:
    /// a TCP socket's halves, or the process's stdin/stdout in the stdio
    /// frontend.
    fn register_client<R, W>(self: &Arc<Self>, reader: R, writer: W) -> JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = unbounded_channel::<DapMessage>();

        let client_id = {
            let mut state = self.state.lock().unwrap();
            state.client_counter += 1;
            let id = format!("client-{}", state.client_counter);
            state.clients.push(ClientConnection {
                id: id.clone(),
                tx,
                read_task: None,
                initialized: false,
                display_name: None,
            });
            id
        };
        tracing::info!(client_id = %client_id, "Client connected");

        tokio::spawn(write_loop(client_id.clone(), writer, rx));

        let mux_for_message = Arc::clone(self);
        let mux_for_disconnect = Arc::clone(self);
        let read_handle = tokio::spawn(read_loop(
            client_id.clone(),
            reader,
            move |id, message| mux_for_message.handle_client_message(id, message),
            move |id| mux_for_disconnect.handle_client_disconnect(id),
        ));

        // Record the read task's abort handle so the session boundary can
        // proactively close this client (the read loop otherwise blocks on the
        // transport until the client closes).
        let mut state = self.state.lock().unwrap();
        if let Some(c) = state.clients.iter_mut().find(|c| c.id == client_id) {
            c.read_task = Some(read_handle.abort_handle());
        }
        read_handle
    }

    // ------------------------------------------------------------------
    // Message routing — client -> adapter
    // ------------------------------------------------------------------

    /// Route a message from a downstream client to the adapter.
    pub fn handle_client_message(&self, client_id: &str, message: DapMessage) {
        let mut state = self.state.lock().unwrap();

        if !is_request(&message) {
            // A client's response to a reverse request (e.g. runInTerminal) is
            // forwarded back to the adapter rather than dropped as stray noise.
            if is_response(&message) && state.forward_reverse_response(message) {
                return;
            }
            tracing::warn!(client_id, "Ignoring non-request from client");
            return;
        }

        let command = command_field(&message).to_string();

        // Cache initialize arguments for reverse-request routing, and record the
        // client's self-reported name for the operator view.
        if command == "initialize" {
            let args = message
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if let Some(name) = client_display_name(&args)
                && let Some(c) = state.clients.iter_mut().find(|c| c.id == client_id)
            {
                c.display_name = Some(name);
            }
            state.set_init_args(client_id, args);
        }

        // Late-join: respond to initialize locally from cached capabilities.
        // DAP says initialize can only be sent once to the adapter.
        if command == "initialize" && state.phase >= SessionPhase::Initialized {
            state.respond_with_cached_initialize(client_id, &message);
            return;
        }

        // In-flight guard: a first initialize is forwarded but unanswered.
        // Real adapters silently ignore a second initialize, which would
        // leave the second client waiting forever — so buffer it.
        if command == "initialize" && state.phase == SessionPhase::Initializing {
            state
                .pending_initialize
                .push((client_id.to_string(), message));
            tracing::debug!(client_id, "initialize buffered (first still in-flight)");
            return;
        }

        if command == "initialize" {
            state.phase = SessionPhase::Initializing;
        }

        // Once configured, late-joining clients must not re-send
        // attach/launch/configurationDone to the adapter.
        if (command == "attach" || command == "launch") && state.phase == SessionPhase::Configured {
            state.respond_synthetic_success(client_id, &message);
            return;
        }

        if command == "configurationDone" {
            if state.phase == SessionPhase::Configured {
                state.respond_synthetic_success(client_id, &message);
                return;
            }
            // Mark configured before forwarding so a concurrent
            // configurationDone from another client is intercepted.
            state.phase = SessionPhase::Configured;
        }

        // `terminate` asks to stop the shared debuggee. Honor it for real only
        // when this is the sole client, no separate operator owns the session
        // (the TUI), and the adapter advertises the capability — then forward
        // it so the debuggee stops gracefully and the adapter's own
        // `terminated` event flows back. Otherwise — other clients attached, an
        // operator owns the debuggee, or the adapter cannot terminate — absorb
        // it: detach just this client with a synthetic `terminated` (the event
        // a DAP client waits on to finish tearing down, e.g. Helix's
        // "Terminating debug session…"), and leave the debuggee running for the
        // rest.
        if command == "terminate" {
            let sole_client_owns_debuggee = state.clients.len() == 1 && !self.operator_owned;
            if !(sole_client_owns_debuggee && state.adapter_supports_terminate()) {
                state.respond_synthetic_success(client_id, &message);
                let terminated = json!({"seq": 0, "type": "event", "event": "terminated"});
                state.send_to(client_id, terminated);
                return;
            }
            // Sole client, no operator, adapter can terminate: fall through to
            // forward the request upstream.
        }

        // `disconnect` means this client is leaving. Acknowledge it, then close
        // and drop the connection so it frees the socket and leaves the operator
        // view; the session continues for any other clients.
        if command == "disconnect" {
            state.respond_synthetic_success(client_id, &message);
            state.disconnect_client(client_id);
            return;
        }

        // `restart` re-runs the shared debuggee rather than ending the session,
        // so it is a legitimate request from any client. Forward the native
        // `restart` when the adapter supports it (the debuggee restarts in place
        // and all clients re-sync from the adapter's events); otherwise reject
        // honestly — emulated disconnect+relaunch cannot work through a shared
        // mux, and a synthetic success would lie to the client.
        if command == "restart" {
            if !state.adapter_supports_restart() {
                state.respond_restart_unsupported(client_id, &message);
                return;
            }
            // The debuggee is about to re-run: drop the stale stop so a client
            // joining mid-restart isn't replayed a stop from the previous run.
            state.last_stopped_event = None;
            // Fall through to forward the request upstream.
        }

        let Some(original_seq) = message.get("seq").and_then(Value::as_i64) else {
            tracing::warn!(client_id, "request missing seq, dropping");
            return;
        };
        let proxy_seq = state.seq_map.allocate(client_id, original_seq);

        let mut forwarded = message;
        forwarded["seq"] = json!(proxy_seq);
        match &state.upstream_tx {
            Some(tx) => {
                let _ = tx.send(forwarded);
            }
            None => tracing::warn!("no upstream connection. Dropping forwarded request"),
        }
        tracing::debug!(client_id, command, original_seq, proxy_seq, "client -> DA");
    }

    // ------------------------------------------------------------------
    // Message routing — adapter -> client(s)
    // ------------------------------------------------------------------

    /// Route a message from the adapter to the appropriate client(s).
    pub fn handle_upstream_message(&self, message: DapMessage) {
        let mut state = self.state.lock().unwrap();
        if is_response(&message) {
            let message = rewrite_stale_variable_error(message);
            state.route_response(message);
        } else if is_event(&message) {
            if should_filter_event(&message) {
                return;
            }
            state.broadcast_event(message);
        } else if is_known_reverse_request(&message) {
            state.route_reverse_request(message);
        } else {
            tracing::warn!(kind = ?message.get("type"), "Unexpected message type from adapter");
        }
    }

    // ------------------------------------------------------------------
    // Disconnect
    // ------------------------------------------------------------------

    /// Clean up when a client disconnects. The session continues for others.
    pub fn handle_client_disconnect(&self, client_id: &str) {
        let mut state = self.state.lock().unwrap();
        state.clients.retain(|c| c.id != client_id);
        let removed = state.cleanup_client_state(client_id);
        tracing::info!(
            client_id,
            removed,
            "Client removed (pending requests cleaned up)"
        );
    }
}

/// A downstream client listener bound once for the process lifetime.
///
/// In interactive mode the process outlives an individual session, but clients
/// must keep reconnecting to the same address. Binding the listener here —
/// outside the per-session [`Multiplexer`] — keeps the port stable across loop
/// iterations: each session gets a fresh mux, but the same `ClientListener`
/// feeds it.
pub struct ClientListener {
    listener: Arc<TcpListener>,
    port: u16,
}

impl ClientListener {
    /// Bind on `host:port` (port 0 = OS-assigned). The bound port is stable for
    /// the listener's lifetime and reported by [`port`](Self::port).
    pub async fn bind(host: &str, port: u16) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind((host, port)).await?;
        let port = listener.local_addr()?.port();
        tracing::info!(host, port, "Client listener bound");
        Ok(Self {
            listener: Arc::new(listener),
            port,
        })
    }

    /// The bound port — unchanged across sessions.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Start accepting clients into `mux`. Accepting continues until the
    /// returned [`AcceptGuard`] is dropped (the session boundary), at which
    /// point this listener can feed a new mux for the next session.
    pub fn accept_into(&self, mux: Arc<Multiplexer>) -> AcceptGuard {
        let listener = Arc::clone(&self.listener);
        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => mux.accept_client(stream),
                    Err(err) => tracing::warn!(%err, "accept failed"),
                }
            }
        });
        AcceptGuard { handle }
    }
}

/// Stops the accept loop started by [`ClientListener::accept_into`] when
/// dropped, freeing the listener to feed the next session's mux.
pub struct AcceptGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for AcceptGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_ordering() {
        assert!(SessionPhase::PreInit < SessionPhase::Initializing);
        assert!(SessionPhase::Initializing < SessionPhase::Initialized);
        assert!(SessionPhase::Initialized < SessionPhase::Configured);
    }
}
