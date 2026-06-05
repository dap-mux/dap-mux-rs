//! Integration tests for the `dap-mux`.

mod common;

use common::{Harness, settle};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Request forwarding with sequence rewriting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn client_request_forwarded_with_rewritten_seq() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send(
        "initialize",
        json!({"clientID": "test", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;
    client.send("configurationDone", json!({}));
    client.wait_for_response("configurationDone").await;

    // The adapter sees proxy-allocated monotonic seqs 1, 2 — not the client's.
    let seqs: Vec<i64> = harness
        .adapter
        .received()
        .iter()
        .map(|m| m.get("seq").and_then(Value::as_i64).unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2]);
}

#[tokio::test]
async fn response_restores_client_seq() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    let seq = client.send("threads", Value::Null);
    let resp = client.wait_for_response("threads").await;
    assert_eq!(resp["request_seq"].as_i64(), Some(seq));
    assert_eq!(resp["success"], json!(true));
}

#[tokio::test]
async fn non_request_from_client_is_dropped() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    // Send a bare event — must not reach the adapter.
    client.send_raw(json!({"seq": 99, "type": "event", "event": "noise"}));
    settle().await;
    // A following real request still works, proving the mux survived.
    client.send("threads", Value::Null);
    client.wait_for_response("threads").await;

    assert!(
        harness
            .adapter
            .received()
            .iter()
            .all(|m| m.get("type").and_then(Value::as_str) == Some("request")),
        "no non-request should be forwarded upstream"
    );
}

// ---------------------------------------------------------------------------
// Response routing to originating client
// ---------------------------------------------------------------------------

#[tokio::test]
async fn responses_routed_to_correct_client() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;

    let seq1 = client1.send("threads", Value::Null);
    let seq2 = client2.send("stackTrace", json!({"threadId": 1}));

    let response1 = client1.wait_for_response("threads").await;
    let response2 = client2.wait_for_response("stackTrace").await;
    assert_eq!(response1["request_seq"].as_i64(), Some(seq1));
    assert_eq!(response2["request_seq"].as_i64(), Some(seq2));

    // No cross-contamination.
    let client1_cmds: Vec<String> = response_commands(&client1.received());
    let client2_cmds: Vec<String> = response_commands(&client2.received());
    assert!(!client1_cmds.contains(&"stackTrace".to_string()));
    assert!(!client2_cmds.contains(&"threads".to_string()));
}

#[tokio::test]
async fn response_for_departed_client_is_dropped() {
    let h = Harness::start().await;
    let mut client1 = h.client().await;
    let mut client2 = h.client().await;

    // client2 issues a request, then disconnects before the response is routed.
    client2.send("threads", Value::Null);
    client2.close();
    settle().await;

    // The session keeps working for client1 — no panic, no error.
    client1.send("threads", Value::Null);
    let r = client1.wait_for_response("threads").await;
    assert_eq!(r["success"], json!(true));
}

// ---------------------------------------------------------------------------
// Event broadcast
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_delivered_to_all_clients() {
    let harness = Harness::start().await;
    let client1 = harness.client().await;
    let client2 = harness.client().await;

    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint", "threadId": 1}));

    let event1 = client1.wait_for_event("stopped").await;
    let event2 = client2.wait_for_event("stopped").await;
    assert_eq!(event1["body"]["reason"], json!("breakpoint"));
    assert_eq!(event2["body"]["reason"], json!("breakpoint"));
}

// ---------------------------------------------------------------------------
// Single initialize with cached capabilities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn late_initialize_answered_from_cache() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    client1.send(
        "initialize",
        json!({"clientID": "helix", "adapterID": "debugpy"}),
    );
    let response1 = client1.wait_for_response("initialize").await;
    let upstream_count = harness.adapter.received().len();

    let mut client2 = harness.client().await;
    client2.send(
        "initialize",
        json!({"clientID": "repl", "adapterID": "debugpy"}),
    );
    let response2 = client2.wait_for_response("initialize").await;

    assert_eq!(response2["success"], json!(true));
    assert_eq!(response2["body"], response1["body"]);
    // The adapter did NOT receive a second initialize.
    assert_eq!(harness.adapter.received().len(), upstream_count);
    assert_eq!(harness.adapter.count_command("initialize"), 1);
}

#[tokio::test]
async fn concurrent_initialize_buffered_and_both_succeed() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;

    // Both race to send initialize before the adapter responds.
    client1.send(
        "initialize",
        json!({"clientID": "helix", "adapterID": "debugpy"}),
    );
    client2.send(
        "initialize",
        json!({"clientID": "ipython", "adapterID": "debugpy"}),
    );

    let response1 = client1.wait_for_response("initialize").await;
    let response2 = client2.wait_for_response("initialize").await;
    assert_eq!(response1["success"], json!(true));
    assert_eq!(response2["success"], json!(true));
    assert_eq!(response1["body"], response2["body"]);

    // Exactly one initialize reached the adapter.
    assert_eq!(harness.adapter.count_command("initialize"), 1);
}

// ---------------------------------------------------------------------------
// Late-join state replay
// ---------------------------------------------------------------------------

#[tokio::test]
async fn late_joiner_receives_initialized_and_current_stop() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    client1.send(
        "initialize",
        json!({"clientID": "helix", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    harness.adapter.send_event("initialized", Value::Null);
    client1.wait_for_event("initialized").await;
    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint", "threadId": 1}));
    client1.wait_for_event("stopped").await;

    // Late joiner: cached initialize, then replayed initialized + stopped.
    let mut client2 = harness.client().await;
    client2.send(
        "initialize",
        json!({"clientID": "repl", "adapterID": "debugpy"}),
    );
    client2.wait_for_response("initialize").await;
    client2.wait_for_event("initialized").await;
    let stopped = client2.wait_for_event("stopped").await;
    assert_eq!(stopped["body"]["reason"], json!("breakpoint"));
}

#[tokio::test]
async fn stopped_state_cleared_on_continued() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    client1.send(
        "initialize",
        json!({"clientID": "helix", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint", "threadId": 1}));
    client1.wait_for_event("stopped").await;
    harness
        .adapter
        .send_event("continued", json!({"threadId": 1}));
    client1.wait_for_event("continued").await;

    let mut client2 = harness.client().await;
    client2.send(
        "initialize",
        json!({"clientID": "repl", "adapterID": "debugpy"}),
    );
    client2.wait_for_response("initialize").await;

    settle().await;
    assert!(
        !client2
            .received()
            .iter()
            .any(|m| m.get("event").and_then(Value::as_str) == Some("stopped")),
        "no stopped replay after continued"
    );
}

#[tokio::test]
async fn most_recent_stop_is_replayed() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    client1.send(
        "initialize",
        json!({"clientID": "helix", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint", "threadId": 1}));
    client1.wait_for_event("stopped").await;
    harness
        .adapter
        .send_event("continued", json!({"threadId": 1}));
    client1.wait_for_event("continued").await;
    harness
        .adapter
        .send_event("stopped", json!({"reason": "step", "threadId": 2}));
    client1.wait_for_event("stopped").await;

    let mut client2 = harness.client().await;
    client2.send(
        "initialize",
        json!({"clientID": "repl", "adapterID": "debugpy"}),
    );
    client2.wait_for_response("initialize").await;

    let stopped = client2.wait_for_event("stopped").await;
    assert_eq!(stopped["body"]["reason"], json!("step"));
    assert_eq!(stopped["body"]["threadId"], json!(2));
}

// ---------------------------------------------------------------------------
// Session-lifecycle command interception
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disconnect_and_terminate_are_intercepted() {
    for command in ["disconnect", "terminate"] {
        let harness = Harness::start().await;
        let mut client = harness.client().await;

        client.send(command, json!({}));
        let response = client.wait_for_response(command).await;
        assert_eq!(
            response["success"],
            json!(true),
            "{command} should succeed synthetically"
        );
        assert_eq!(
            harness.adapter.count_command(command),
            0,
            "{command} must not reach the adapter"
        );
    }
}

#[tokio::test]
async fn terminate_emits_terminated_event_to_requester() {
    // A DAP client finishes tearing down only when it sees `terminated`;
    // without it the client hangs (e.g. Helix on "Terminating…"). The mux
    // still does not forward terminate — the shared debuggee keeps running.
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send("terminate", json!({}));
    let response = client.wait_for_response("terminate").await;
    assert_eq!(response["success"], json!(true));

    let event = client.wait_for_event("terminated").await;
    assert_eq!(event["event"], json!("terminated"));
    assert_eq!(harness.adapter.count_command("terminate"), 0);
}

#[tokio::test]
async fn terminate_forwarded_when_sole_client_and_capable() {
    // The only client, no operator, adapter supports terminate: the mux stops
    // the shared debuggee for real by forwarding the request upstream.
    let harness = Harness::start_with(common::FakeAdapter::start_with_terminate().await).await;
    let mut client = harness.client().await;
    client.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;

    client.send("terminate", json!({}));
    let response = client.wait_for_response("terminate").await;
    assert_eq!(response["success"], json!(true));
    assert_eq!(
        harness.adapter.count_command("terminate"),
        1,
        "a sole capable client's terminate reaches the adapter"
    );
}

#[tokio::test]
async fn terminate_absorbed_with_other_clients_present() {
    // A second client is attached, so terminate must not stop the shared
    // debuggee even though the adapter advertises the capability.
    let harness = Harness::start_with(common::FakeAdapter::start_with_terminate().await).await;
    let mut client1 = harness.client().await;
    client1.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    let _client2 = harness.client().await;

    client1.send("terminate", json!({}));
    let response = client1.wait_for_response("terminate").await;
    assert_eq!(response["success"], json!(true));
    let event = client1.wait_for_event("terminated").await;
    assert_eq!(event["event"], json!("terminated"));
    assert_eq!(
        harness.adapter.count_command("terminate"),
        0,
        "terminate is absorbed while another client is attached"
    );
}

#[tokio::test]
async fn terminate_absorbed_under_operator_even_when_sole_client() {
    // The operator (TUI) owns the debuggee, so a lone client's terminate
    // detaches the client without stopping the shared session.
    let harness =
        Harness::start_operator_owned_with(common::FakeAdapter::start_with_terminate().await).await;
    let mut client = harness.client().await;
    client.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;

    client.send("terminate", json!({}));
    let response = client.wait_for_response("terminate").await;
    assert_eq!(response["success"], json!(true));
    let event = client.wait_for_event("terminated").await;
    assert_eq!(event["event"], json!("terminated"));
    assert_eq!(harness.adapter.count_command("terminate"), 0);
}

#[tokio::test]
async fn restart_forwarded_when_adapter_supports_it() {
    let harness = Harness::start_with(common::FakeAdapter::start_with_restart().await).await;
    let mut client = harness.client().await;

    client.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;

    client.send("restart", json!({}));
    let response = client.wait_for_response("restart").await;
    assert_eq!(response["success"], json!(true));
    // A restart-capable adapter actually receives the restart request.
    assert_eq!(harness.adapter.count_command("restart"), 1);
}

#[tokio::test]
async fn restart_rejected_when_adapter_lacks_support() {
    // The default fake adapter does not advertise supportsRestartRequest.
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;

    client.send("restart", json!({}));
    let response = client.wait_for_response("restart").await;
    assert_eq!(response["success"], json!(false));
    assert!(
        response
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("supportsRestartRequest"),
        "rejection should explain the missing capability"
    );
    // And it must not reach the adapter.
    assert_eq!(harness.adapter.count_command("restart"), 0);
}

#[tokio::test]
async fn session_survives_disconnect_request() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    client1.send("disconnect", json!({}));
    client1.wait_for_response("disconnect").await;
    settle().await;

    let mut client2 = harness.client().await;
    client2.send("threads", Value::Null);
    let response = client2.wait_for_response("threads").await;
    assert_eq!(response["success"], json!(true));
}

#[tokio::test]
async fn disconnect_request_removes_the_client() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send("disconnect", json!({}));
    client.wait_for_response("disconnect").await;
    settle().await;

    assert_eq!(
        harness.mux.client_count(),
        0,
        "the client is dropped from the session after it disconnects"
    );
}

#[tokio::test]
async fn late_attach_and_configuration_done_absorbed() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;

    // Drive client1 through full configuration.
    client1.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    client1.send("attach", json!({}));
    client1.wait_for_response("attach").await;
    client1.send("configurationDone", json!({}));
    client1.wait_for_response("configurationDone").await;

    let attach_count = harness.adapter.count_command("attach");
    let cd_count = harness.adapter.count_command("configurationDone");

    // Late joiner's attach/configurationDone are intercepted.
    let mut client2 = harness.client().await;
    client2.send(
        "initialize",
        json!({"clientID": "c2", "adapterID": "debugpy"}),
    );
    client2.wait_for_response("initialize").await;
    client2.send("attach", json!({}));
    let attach_resp = client2.wait_for_response("attach").await;
    client2.send("configurationDone", json!({}));
    let cd_resp = client2.wait_for_response("configurationDone").await;

    assert_eq!(attach_resp["success"], json!(true));
    assert_eq!(cd_resp["success"], json!(true));
    assert_eq!(harness.adapter.count_command("attach"), attach_count);
    assert_eq!(harness.adapter.count_command("configurationDone"), cd_count);

    // And the late joiner can still issue normal requests.
    client2.send("threads", Value::Null);
    let response = client2.wait_for_response("threads").await;
    assert_eq!(response["success"], json!(true));
}

#[tokio::test]
async fn first_attach_is_forwarded() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client.wait_for_response("initialize").await;
    client.send("attach", json!({}));
    let response = client.wait_for_response("attach").await;
    assert_eq!(response["success"], json!(true));
    assert_eq!(harness.adapter.count_command("attach"), 1);
}

// ---------------------------------------------------------------------------
// Reverse-request routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_in_terminal_routed_to_opted_in_client() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;

    // client1 does not opt in. client2 declares support.
    client1.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    client2.send(
        "initialize",
        json!({"clientID": "c2", "supportsRunInTerminalRequest": true}),
    );
    client2.wait_for_response("initialize").await;

    harness
        .adapter
        .send_reverse_request("runInTerminal", json!({"args": ["echo", "hi"]}));

    let request = client2.wait_for_request("runInTerminal").await;
    assert_eq!(request["command"], json!("runInTerminal"));
    settle().await;
    assert!(
        !client1
            .received()
            .iter()
            .any(|m| m.get("command").and_then(Value::as_str) == Some("runInTerminal")),
        "opted-out client must not receive the reverse request"
    );
}

#[tokio::test]
async fn reverse_request_response_forwarded_to_adapter() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    client.send(
        "initialize",
        json!({"clientID": "c1", "supportsRunInTerminalRequest": true}),
    );
    client.wait_for_response("initialize").await;

    // The fake adapter assigns seq 0 to its reverse request.
    harness
        .adapter
        .send_reverse_request("runInTerminal", json!({"args": ["echo", "hi"]}));
    let request = client.wait_for_request("runInTerminal").await;

    // The client replies; the mux must forward that response back upstream,
    // restoring the adapter's original seq (0) into request_seq.
    let proxy_seq = request["seq"].as_i64().expect("reverse request has a seq");
    client.send_raw(json!({
        "seq": 500,
        "type": "response",
        "request_seq": proxy_seq,
        "success": true,
        "command": "runInTerminal",
        "body": {"processId": 4242},
    }));

    let mut got = false;
    for _ in 0..40 {
        if harness.adapter.received().iter().any(|m| {
            m.get("type").and_then(Value::as_str) == Some("response")
                && m.get("command").and_then(Value::as_str) == Some("runInTerminal")
                && m.get("request_seq").and_then(Value::as_i64) == Some(0)
        }) {
            got = true;
            break;
        }
        settle().await;
    }
    assert!(
        got,
        "adapter never received the forwarded reverse-request response"
    );
}

#[tokio::test]
async fn reverse_request_falls_back_to_first_client() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;

    client1.send(
        "initialize",
        json!({"clientID": "c1", "adapterID": "debugpy"}),
    );
    client1.wait_for_response("initialize").await;
    client2.send(
        "initialize",
        json!({"clientID": "c2", "adapterID": "debugpy"}),
    );
    client2.wait_for_response("initialize").await;

    harness
        .adapter
        .send_reverse_request("runInTerminal", json!({"args": ["echo", "hi"]}));

    let request = client1.wait_for_request("runInTerminal").await;
    assert_eq!(request["command"], json!("runInTerminal"));
}

// ---------------------------------------------------------------------------
// Adapter-quirk event filtering & stale-variable rewrite
// ---------------------------------------------------------------------------

#[tokio::test]
async fn debugpy_custom_events_are_not_forwarded() {
    let harness = Harness::start().await;
    let client = harness.client().await;

    harness
        .adapter
        .send_event("debugpySockets", json!({"sockets": []}));
    harness.adapter.send_event("debugpyAttach", json!({}));
    // A normal event after them proves the stream is intact and ordered.
    harness
        .adapter
        .send_event("stopped", json!({"reason": "breakpoint"}));

    client.wait_for_event("stopped").await;
    let events: Vec<String> = client
        .received()
        .iter()
        .filter_map(|m| m.get("event").and_then(Value::as_str).map(String::from))
        .collect();
    assert!(!events.contains(&"debugpySockets".to_string()));
    assert!(!events.contains(&"debugpyAttach".to_string()));
}

#[tokio::test]
async fn stale_variable_error_is_clarified() {
    let harness = Harness::start().await;
    let mut client = harness.client().await;

    // The fake adapter returns a failed `variables` response whose message
    // names a not-found reference; the mux appends the clarifying note.
    client.send("variables", json!({"variablesReference": 999}));

    let response = client.wait_for_response("variables").await;
    assert_eq!(response["success"], json!(false));
    let message = response
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("");
    assert!(
        message.contains("invalidated when execution resumes"),
        "got message: {message:?}"
    );
}

// ---------------------------------------------------------------------------
// Client disconnect cleanup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disconnect_cleans_up_and_others_unaffected() {
    let harness = Harness::start().await;
    let mut client1 = harness.client().await;
    let mut client2 = harness.client().await;

    // client2 has an outstanding request, then disconnects.
    client2.send("evaluate", json!({"expression": "x", "context": "repl"}));
    client2.close();
    settle().await;

    // client1 keeps working. Events still flow.
    client1.send("threads", Value::Null);
    let response = client1.wait_for_response("threads").await;
    assert_eq!(response["success"], json!(true));

    harness
        .adapter
        .send_event("continued", json!({"threadId": 1}));
    let event = client1.wait_for_event("continued").await;
    assert_eq!(event["body"]["threadId"], json!(1));

    assert_eq!(harness.mux.client_count(), 1);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn response_commands(msgs: &[Value]) -> Vec<String> {
    msgs.iter()
        .filter(|m| m.get("type").and_then(Value::as_str) == Some("response"))
        .filter_map(|m| m.get("command").and_then(Value::as_str).map(String::from))
        .collect()
}
