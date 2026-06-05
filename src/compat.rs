//! Debug-adapter compatibility layer.
//!
//! Filters, rewrites, and routes messages that need special handling due to
//! adapter quirks or DAP edge cases. The multiplexer calls into this module
//! before forwarding messages.

use serde_json::{Value, json};

/// debugpy custom events that strict DAP clients may choke on.
const DEBUGPY_FILTERED_EVENTS: &[&str] = &["debugpySockets", "debugpyAttach"];

/// Events that signal subprocess debugging. Deferred — log only.
const SUBPROCESS_EVENTS: &[&str] = &["startDebugging"];

/// Reverse requests that the adapter may send.
const REVERSE_REQUESTS: &[&str] = &["runInTerminal", "startDebugging"];

/// Return whether an event should be dropped rather than forwarded.
///
/// Filters debugpy-specific custom events that non-debugpy clients don't
/// understand, and the (unsupported) subprocess-debugging event.
pub fn should_filter_event(message: &Value) -> bool {
    let event = message.get("event").and_then(Value::as_str).unwrap_or("");
    if DEBUGPY_FILTERED_EVENTS.contains(&event) {
        tracing::debug!(event, "Filtering debugpy custom event");
        return true;
    }
    if SUBPROCESS_EVENTS.contains(&event) {
        tracing::info!(event, "Subprocess debug event received (not yet supported)");
        return true;
    }
    false
}

/// Return whether this is a recognized reverse request from the adapter.
///
/// DAP allows the adapter to send requests to the client (e.g.
/// `runInTerminal`). These need special routing — they cannot be broadcast
/// like events.
pub fn is_known_reverse_request(message: &Value) -> bool {
    message.get("type").and_then(Value::as_str) == Some("request")
        && message
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|c| REVERSE_REQUESTS.contains(&c))
}

/// Choose which client should handle a reverse request.
///
/// For `runInTerminal`, prefer a client that declared
/// `supportsRunInTerminalRequest` in its initialize arguments. Falls back to
/// the first connected client. `clients` is an ordered list of
/// `(client_id, initialize_arguments)`.
pub fn pick_reverse_request_target<'a>(
    clients: &'a [(String, Value)],
    message: &Value,
) -> Option<&'a str> {
    let command = message.get("command").and_then(Value::as_str).unwrap_or("");

    if command == "runInTerminal" {
        for (client_id, init_args) in clients {
            if init_args
                .get("supportsRunInTerminalRequest")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                tracing::debug!(client_id, "Routing runInTerminal (opted in)");
                return Some(client_id);
            }
        }
    }

    if let Some((first, _)) = clients.first() {
        tracing::debug!(command, client_id = %first, "Routing reverse request (fallback)");
        return Some(first);
    }

    tracing::warn!(command, "No clients available for reverse request");
    None
}

/// Improve error messages for stale variable-reference requests.
///
/// When execution resumes, variable references become invalid. Adapters may
/// return cryptic errors; this appends a clarifying note. Returns the message
/// unchanged when it doesn't match.
pub fn rewrite_stale_variable_error(mut message: Value) -> Value {
    let is_failed_var_request = message.get("type").and_then(Value::as_str) == Some("response")
        && !message
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        && matches!(
            message.get("command").and_then(Value::as_str),
            Some("variables") | Some("scopes") | Some("evaluate")
        );

    if is_failed_var_request {
        let original = message.get("message").and_then(Value::as_str).unwrap_or("");
        let lowered = original.to_lowercase();
        if lowered.contains("invalid") || lowered.contains("not found") {
            // Build the replacement before mutating: `original` borrows `message`,
            // and the borrow must end before we can assign back into it.
            let annotated =
                format!("{original} (variable references are invalidated when execution resumes)");
            message["message"] = json!(annotated);
        }
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn filters_debugpy_events() {
        assert!(should_filter_event(
            &json!({"type": "event", "event": "debugpySockets"})
        ));
        assert!(should_filter_event(
            &json!({"type": "event", "event": "debugpyAttach"})
        ));
        assert!(should_filter_event(
            &json!({"type": "event", "event": "startDebugging"})
        ));
        assert!(!should_filter_event(
            &json!({"type": "event", "event": "stopped"})
        ));
    }

    #[test]
    fn recognizes_reverse_requests() {
        assert!(is_known_reverse_request(
            &json!({"type": "request", "command": "runInTerminal"})
        ));
        assert!(!is_known_reverse_request(
            &json!({"type": "request", "command": "initialize"})
        ));
        assert!(!is_known_reverse_request(
            &json!({"type": "response", "command": "runInTerminal"})
        ));
    }

    #[test]
    fn prefers_opted_in_client() {
        let clients = vec![
            ("c1".to_string(), json!({})),
            (
                "c2".to_string(),
                json!({"supportsRunInTerminalRequest": true}),
            ),
        ];
        let message = json!({"type": "request", "command": "runInTerminal"});
        assert_eq!(pick_reverse_request_target(&clients, &message), Some("c2"));
    }

    #[test]
    fn falls_back_to_first_client() {
        let clients = vec![("c1".to_string(), json!({})), ("c2".to_string(), json!({}))];
        let message = json!({"type": "request", "command": "runInTerminal"});
        assert_eq!(pick_reverse_request_target(&clients, &message), Some("c1"));
    }

    #[test]
    fn no_target_when_no_clients() {
        let clients: Vec<(String, Value)> = vec![];
        let message = json!({"type": "request", "command": "runInTerminal"});
        assert_eq!(pick_reverse_request_target(&clients, &message), None);
    }

    #[test]
    fn rewrites_stale_variable_error() {
        let message = json!({
            "type": "response", "success": false, "command": "variables",
            "message": "Variable not found"
        });
        let out = rewrite_stale_variable_error(message);
        let m = out.get("message").and_then(Value::as_str).unwrap();
        assert!(m.contains("invalidated when execution resumes"));
    }

    #[test]
    fn leaves_unrelated_error_alone() {
        let message = json!({
            "type": "response", "success": false, "command": "variables",
            "message": "Internal error"
        });
        let out = rewrite_stale_variable_error(message.clone());
        assert_eq!(out, message);
    }

    #[test]
    fn leaves_successful_response_alone() {
        let message = json!({"type": "response", "success": true, "command": "variables"});
        let out = rewrite_stale_variable_error(message.clone());
        assert_eq!(out, message);
    }
}
