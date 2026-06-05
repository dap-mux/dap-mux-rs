//! A `tracing` layer that records formatted log lines into a bounded ring
//! buffer for the TUI's log pane.
//!
//! Under `--ui` the stderr layer is suppressed (it would corrupt the rendered
//! interface); this layer captures the same events the mux already emits and
//! the interface renders them. The buffer is bounded so a long-running session
//! cannot grow it without limit — older lines are dropped (fail-visibly,
//! oldest-first; we do not try to persist the full history here).

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// One captured event: its level (so the operator view can filter by verbosity),
/// its `tracing` target (so adapter stderr can be shown distinctly from the
/// mux's own logs), and the rendered text (message plus appended `key=value`
/// fields).
#[derive(Clone)]
pub struct LogLine {
    pub level: Level,
    pub target: String,
    pub text: String,
}

/// A bounded, cloneable buffer of captured log lines shared between the tracing
/// layer (writer) and the TUI (reader).
#[derive(Clone)]
pub struct LogBuffer {
    inner: Arc<Mutex<VecDeque<LogLine>>>,
    capacity: usize,
}

impl LogBuffer {
    /// Create a buffer retaining at most `capacity` most-recent lines.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    fn push(&self, line: LogLine) {
        let mut buffer = self.inner.lock().unwrap();
        if buffer.len() == self.capacity {
            buffer.pop_front();
        }
        buffer.push_back(line);
    }

    /// Snapshot of the retained lines, oldest first.
    pub fn lines(&self) -> Vec<LogLine> {
        self.inner.lock().unwrap().iter().cloned().collect()
    }

    /// Number of retained lines.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    /// Whether the buffer holds no lines.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

/// A `tracing_subscriber::Layer` that formats each event into [`LogBuffer`].
pub struct LogBufferLayer {
    buffer: LogBuffer,
}

impl LogBufferLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self { buffer }
    }
}

impl<S: Subscriber> Layer<S> for LogBufferLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = LineVisitor::default();
        event.record(&mut visitor);
        let mut text = visitor.message;
        if !visitor.fields.is_empty() {
            text.push_str(&visitor.fields);
        }
        self.buffer.push(LogLine {
            level: *metadata.level(),
            target: metadata.target().to_string(),
            text,
        });
    }
}

/// Renders an event into a `message` plus appended `key=value` fields.
#[derive(Default)]
struct LineVisitor {
    message: String,
    fields: String,
}

impl Visit for LineVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            let _ = write!(self.fields, " {}={}", field.name(), value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            let _ = write!(self.fields, " {}={:?}", field.name(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(buffer: &LogBuffer) -> Vec<String> {
        buffer.lines().into_iter().map(|line| line.text).collect()
    }

    #[test]
    fn retains_most_recent_within_capacity() {
        let buffer = LogBuffer::new(3);
        for i in 0..5 {
            buffer.push(LogLine {
                level: Level::INFO,
                target: "test".into(),
                text: format!("line {i}"),
            });
        }
        assert_eq!(texts(&buffer), vec!["line 2", "line 3", "line 4"]);
    }

    #[test]
    fn empty_buffer_reports_empty() {
        let buffer = LogBuffer::new(10);
        assert!(buffer.is_empty());
        buffer.push(LogLine {
            level: Level::INFO,
            target: "test".into(),
            text: "x".into(),
        });
        assert!(!buffer.is_empty());
    }

    #[test]
    fn layer_captures_events_into_buffer() {
        use tracing_subscriber::prelude::*;

        let buffer = LogBuffer::new(10);
        let subscriber = tracing_subscriber::registry().with(LogBufferLayer::new(buffer.clone()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(client_id = "client-1", "pane line");
        });

        let lines = buffer.lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].level, Level::INFO);
        assert!(lines[0].text.contains("pane line"), "message captured");
        assert!(
            lines[0].text.contains("client_id=client-1"),
            "fields captured"
        );
    }
}
