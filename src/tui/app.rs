//! The interactive operator application: the connect → session → connect loop,
//! the live session/client view, and the async event loop.

use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers};
use futures::StreamExt;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Wrap};
use tokio::sync::oneshot;
use tracing::Level;
use tracing_subscriber::filter::LevelFilter;

use crate::cli::parse_attach;
use crate::mux::{ClientListener, Multiplexer, SessionPhase};
use crate::tui::log_buffer::{LogBuffer, LogLine};
use crate::tui::terminal::Tui;
use crate::upstream::UpstreamTransport;

/// How often the interface re-renders and polls session/connect state.
const TICK: Duration = Duration::from_millis(100);

/// Log levels the operator can cycle the pane through, least to most verbose.
/// The pane captures at `DEBUG`, so that is the most verbose option here.
const DISPLAY_LEVELS: [Level; 4] = [Level::ERROR, Level::WARN, Level::INFO, Level::DEBUG];

/// What the operator is currently doing.
enum AppState {
    /// Editing an address, not connected.
    Idle,
    /// A connect attempt is in flight (retrying against a not-yet-ready
    /// adapter is handled inside `connect_upstream`).
    Connecting {
        mux: Arc<Multiplexer>,
        rx: oneshot::Receiver<anyhow::Result<()>>,
        target: String,
    },
    /// A live session: clients are accepted into `mux` until it ends.
    Connected {
        mux: Arc<Multiplexer>,
        target: String,
        // Dropping this stops accepting clients (the session boundary).
        _accept: crate::mux::AcceptGuard,
    },
}

/// Where the operator interface draws its upstream from.
enum Upstream {
    /// A target named on the command line — an attach address or a spawn
    /// command. The interface connects to it on entry and reconnects to the
    /// same target after a session ends.
    Configured {
        transport: UpstreamTransport,
        label: String,
    },
    /// No upstream was named: the operator types an attach address on the
    /// connect screen.
    Interactive,
}

/// The operator interface state.
pub struct App {
    listener: ClientListener,
    log_buffer: LogBuffer,
    /// The pane shows lines at this level or more severe; adjustable at runtime.
    display_level: Level,
    state: AppState,
    /// The upstream this interface drives.
    upstream: Upstream,
    /// Address-entry buffer on the connect screen (interactive upstream only).
    input: String,
    /// One-line feedback (connecting, connect failure, session ended, …).
    status: String,
    should_quit: bool,
}

impl App {
    /// Build the app for the given `upstream`. A configured upstream auto-
    /// connects when [`run`](Self::run) starts; an interactive one waits on the
    /// connect screen for the operator to enter an address.
    fn new(
        listener: ClientListener,
        log_buffer: LogBuffer,
        upstream: Upstream,
        display_level: Level,
    ) -> Self {
        let (input, status) = match &upstream {
            Upstream::Configured { label, .. } => (label.clone(), format!("Connecting to {label}…")),
            Upstream::Interactive => (
                String::new(),
                "Enter an upstream address (PORT or HOST:PORT) and press Enter.".to_string(),
            ),
        };
        Self {
            listener,
            log_buffer,
            display_level,
            state: AppState::Idle,
            upstream,
            input,
            status,
            should_quit: false,
        }
    }

    /// Run the event loop until the operator quits. Renders on a tick reading a
    /// `MuxSnapshot`, takes input from crossterm's async `EventStream`.
    pub async fn run(mut self, terminal: &mut Tui) -> anyhow::Result<()> {
        let mut events = EventStream::new();
        let mut tick = tokio::time::interval(TICK);

        // A command-line upstream connects straight away; an interactive one
        // waits for the operator to submit an address from the connect screen.
        if let Upstream::Configured { transport, label } = &self.upstream {
            let transport = transport.clone();
            let label = label.clone();
            self.start_connect(transport, label);
        }

        loop {
            terminal.draw(|f| self.render(f))?;

            tokio::select! {
                _ = tick.tick() => self.on_tick(),
                maybe_event = events.next() => match maybe_event {
                    Some(Ok(Event::Key(key))) => self.on_key(key),
                    Some(Err(err)) => {
                        tracing::warn!(%err, "input stream error");
                        self.should_quit = true;
                    }
                    None => self.should_quit = true,
                    _ => {}
                },
            }

            if self.should_quit {
                // Oneshot teardown: the present operator is leaving.
                if let Some(mux) = self.current_mux() {
                    mux.end_session();
                }
                return Ok(());
            }
        }
    }

    fn current_mux(&self) -> Option<&Arc<Multiplexer>> {
        match &self.state {
            AppState::Connecting { mux, .. } | AppState::Connected { mux, .. } => Some(mux),
            AppState::Idle => None,
        }
    }

    // ------------------------------------------------------------------
    // Tick: advance connect attempts and detect session end.
    // ------------------------------------------------------------------

    fn on_tick(&mut self) {
        match &mut self.state {
            AppState::Connecting { rx, target, mux } => {
                match rx.try_recv() {
                    Ok(Ok(())) => {
                        // Connected: start accepting clients into this mux.
                        let mux = Arc::clone(mux);
                        let target = target.clone();
                        let accept = self.listener.accept_into(Arc::clone(&mux));
                        self.status = format!(
                            "Connected to {target}. Clients may connect on 127.0.0.1:{}.",
                            self.listener.port()
                        );
                        self.state = AppState::Connected {
                            mux,
                            target,
                            _accept: accept,
                        };
                    }
                    Ok(Err(err)) => {
                        self.status = format!("Connect to {target} failed: {err}");
                        self.state = AppState::Idle;
                    }
                    Err(oneshot::error::TryRecvError::Empty) => {} // still trying
                    Err(oneshot::error::TryRecvError::Closed) => {
                        self.status = format!("Connect to {target} aborted.");
                        self.state = AppState::Idle;
                    }
                }
            }
            AppState::Connected { mux, target, .. } => {
                // A session ends when the upstream adapter connection is lost. The mux
                // clears its upstream queue on disconnect; observe that and
                // loop back to the connect screen (the process lives on).
                if !mux.snapshot().upstream_connected {
                    let target = target.clone();
                    mux.end_session();
                    self.status = format!(
                        "Session with {target} ended (adapter connection lost). \
                         Press Enter to connect again, Esc to quit."
                    );
                    self.state = AppState::Idle;
                }
            }
            AppState::Idle => {}
        }
    }

    // ------------------------------------------------------------------
    // Input: quit and address entry.
    // ------------------------------------------------------------------

    fn on_key(&mut self, key: KeyEvent) {
        // Ctrl-C always quits, in any state.
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }

        match &self.state {
            AppState::Idle => match key.code {
                KeyCode::Esc => self.should_quit = true,
                KeyCode::Enter => self.submit_connect(),
                // Address entry is for the interactive upstream only; a
                // configured target has no editable field.
                KeyCode::Backspace if matches!(self.upstream, Upstream::Interactive) => {
                    self.input.pop();
                }
                KeyCode::Char(c) if matches!(self.upstream, Upstream::Interactive) => {
                    self.input.push(c)
                }
                _ => {}
            },
            // While connecting or connected the address field is inert, so these
            // keys are free for control: quit, and adjust the log pane verbosity
            // (`+`/`-` would collide with hostname entry on the connect screen).
            AppState::Connecting { .. } | AppState::Connected { .. } => match key.code {
                KeyCode::Esc | KeyCode::Char('q') => self.should_quit = true,
                KeyCode::Char('+') => self.adjust_verbosity(1),
                KeyCode::Char('-') => self.adjust_verbosity(-1),
                _ => {}
            },
        }
    }

    /// Step the pane's display level toward more (`+1`) or less (`-1`) verbose,
    /// saturating at the ends of [`DISPLAY_LEVELS`].
    fn adjust_verbosity(&mut self, step: i32) {
        let Some(current) = DISPLAY_LEVELS.iter().position(|&l| l == self.display_level) else {
            return;
        };
        let next = (current as i32 + step).clamp(0, DISPLAY_LEVELS.len() as i32 - 1);
        self.display_level = DISPLAY_LEVELS[next as usize];
    }

    /// Begin connecting: reconnect to the configured target, or parse and
    /// connect to the address the operator typed on the connect screen.
    fn submit_connect(&mut self) {
        match &self.upstream {
            Upstream::Configured { transport, label } => {
                let transport = transport.clone();
                let label = label.clone();
                self.start_connect(transport, label);
            }
            Upstream::Interactive => {
                let entry = self.input.trim().to_string();
                if entry.is_empty() {
                    self.status = "Enter an address first (PORT or HOST:PORT).".to_string();
                    return;
                }
                let (host, port) = match parse_attach(&entry) {
                    Ok(target) => target,
                    Err(err) => {
                        self.status = err.to_string();
                        return;
                    }
                };
                self.start_connect(UpstreamTransport::tcp(host.clone(), port), format!("{host}:{port}"));
            }
        }
    }

    /// Spawn a connect attempt against `transport` and enter the connecting
    /// state. The connect retries a not-yet-ready adapter internally; this
    /// returns immediately and the tick loop observes the outcome.
    fn start_connect(&mut self, transport: UpstreamTransport, target: String) {
        // The operator owns the debuggee here, so a client's terminate detaches
        // only that client rather than stopping the shared session.
        let mux = Multiplexer::new_operator_owned();
        let (tx, rx) = oneshot::channel();
        let connect_mux = Arc::clone(&mux);
        tokio::spawn(async move {
            let res = connect_mux
                .connect_upstream(&transport)
                .await
                .map_err(anyhow::Error::from);
            let _ = tx.send(res);
        });

        self.status = format!("Connecting to {target}… (retrying until the adapter is ready)");
        self.state = AppState::Connecting { mux, rx, target };
    }

    // ------------------------------------------------------------------
    // Rendering: connect screen, or the session/client view plus log pane.
    // ------------------------------------------------------------------

    fn render(&self, f: &mut Frame) {
        let chunks = Layout::vertical([
            Constraint::Length(3), // header
            Constraint::Min(6),    // body
            Constraint::Length(9), // log pane
            Constraint::Length(1), // help
        ])
        .split(f.area());

        self.render_header(f, chunks[0]);
        match &self.state {
            AppState::Connected { mux, .. } => self.render_session(f, chunks[1], mux),
            AppState::Idle | AppState::Connecting { .. } => self.render_connect(f, chunks[1]),
        }
        self.render_logs(f, chunks[2]);
        self.render_help(f, chunks[3]);
    }

    fn render_header(&self, f: &mut Frame, area: Rect) {
        let (label, color) = match &self.state {
            AppState::Idle => ("idle — not connected".to_string(), Color::Gray),
            AppState::Connecting { target, .. } => {
                (format!("connecting to {target}…"), Color::Yellow)
            }
            AppState::Connected { target, mux, .. } => {
                let phase = phase_label(mux.snapshot().phase);
                (format!("connected to {target} — {phase}"), Color::Green)
            }
        };
        let text = Line::from(vec![
            Span::styled(
                "dap-mux operator",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(label, Style::default().fg(color)),
        ]);
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" listening on 127.0.0.1:{} ", self.listener.port()));
        f.render_widget(Paragraph::new(text).block(block), area);
    }

    fn render_connect(&self, f: &mut Frame, area: Rect) {
        let interactive = matches!(self.upstream, Upstream::Interactive);
        let editable = interactive && matches!(self.state, AppState::Idle);
        let input_line = if editable {
            // A simple cursor marker; raw mode hides the real cursor.
            format!("{}\u{2588}", self.input)
        } else {
            self.input.clone()
        };
        // A configured upstream shows its fixed target; an interactive one an
        // editable address field.
        let field_label = if interactive {
            "  address: "
        } else {
            "  upstream: "
        };
        let lines = vec![
            Line::from(""),
            Line::from(vec![
                Span::styled(field_label, Style::default().fg(Color::Cyan)),
                Span::raw(input_line),
            ]),
            Line::from(""),
            Line::from(Span::styled(
                format!("  {}", self.status),
                Style::default().fg(Color::Yellow),
            )),
        ];
        let block = Block::default().borders(Borders::ALL).title(" connect ");
        f.render_widget(
            Paragraph::new(lines).block(block).wrap(Wrap { trim: true }),
            area,
        );
    }

    fn render_session(&self, f: &mut Frame, area: Rect, mux: &Arc<Multiplexer>) {
        let snapshot = mux.snapshot();
        let rows = Layout::vertical([Constraint::Length(2), Constraint::Min(3)]).split(area);

        let upstream = if snapshot.upstream_connected {
            Span::styled("connected", Style::default().fg(Color::Green))
        } else {
            Span::styled("lost", Style::default().fg(Color::Red))
        };
        let summary = Line::from(vec![
            Span::raw(" upstream: "),
            upstream,
            Span::raw("   phase: "),
            Span::styled(
                phase_label(snapshot.phase),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(format!("   clients: {}", snapshot.clients.len())),
        ]);
        f.render_widget(Paragraph::new(summary), rows[0]);

        let items: Vec<ListItem> = if snapshot.clients.is_empty() {
            vec![ListItem::new(Span::styled(
                " (no clients connected)",
                Style::default().fg(Color::DarkGray),
            ))]
        } else {
            snapshot
                .clients
                .iter()
                .map(|c| {
                    let init = if c.initialized {
                        Span::styled("initialized", Style::default().fg(Color::Green))
                    } else {
                        Span::styled("connecting", Style::default().fg(Color::Yellow))
                    };
                    let label = c.name.as_deref().unwrap_or(&c.id);
                    ListItem::new(Line::from(vec![
                        Span::raw(format!(" {label:<14} ")),
                        init,
                        Span::raw(format!("   pending: {}", c.pending_requests)),
                    ]))
                })
                .collect()
        };
        let block = Block::default().borders(Borders::ALL).title(" clients ");
        f.render_widget(List::new(items).block(block), rows[1]);
    }

    fn render_logs(&self, f: &mut Frame, area: Rect) {
        let lines = self.log_buffer.lines();
        let shown: Vec<&LogLine> = lines
            .iter()
            .filter(|l| l.level <= self.display_level)
            .collect();
        let visible = area.height.saturating_sub(2) as usize; // height minus the top and bottom borders
        let start = shown.len().saturating_sub(visible);
        let text: Vec<Line> = shown[start..]
            .iter()
            .map(|l| {
                // Adapter stderr is tagged so it reads as the adapter's output,
                // not the mux's own logs.
                let is_adapter = l.target == "adapter";
                let mut spans = vec![Span::styled(
                    format!("{:<5} ", l.level),
                    Style::default().fg(level_color(l.level)),
                )];
                if is_adapter {
                    spans.push(Span::styled(
                        "adapter ",
                        Style::default()
                            .fg(Color::Magenta)
                            .add_modifier(Modifier::BOLD),
                    ));
                }
                let text_style = if is_adapter {
                    Style::default().fg(Color::Magenta)
                } else {
                    Style::default()
                };
                spans.push(Span::styled(l.text.clone(), text_style));
                Line::from(spans)
            })
            .collect();
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" logs [{}]  +/- verbosity ", self.display_level));
        f.render_widget(Paragraph::new(text).block(block), area);
    }

    fn render_help(&self, f: &mut Frame, area: Rect) {
        let help = match &self.state {
            AppState::Idle if matches!(self.upstream, Upstream::Interactive) => {
                "type address · Enter connect · Esc/Ctrl-C quit"
            }
            AppState::Idle => "Enter connect · Esc/Ctrl-C quit",
            AppState::Connecting { .. } => "connecting… · Esc/q/Ctrl-C quit",
            AppState::Connected { .. } => "session live · +/- log level · Esc/q/Ctrl-C quit",
        };
        f.render_widget(
            Paragraph::new(Span::styled(
                format!(" {help}"),
                Style::default().fg(Color::DarkGray),
            )),
            area,
        );
    }
}

fn phase_label(phase: SessionPhase) -> &'static str {
    match phase {
        SessionPhase::PreInit => "pre-init",
        SessionPhase::Initializing => "initializing",
        SessionPhase::Initialized => "initialized",
        SessionPhase::Configured => "configured",
    }
}

fn level_color(level: Level) -> Color {
    match level {
        Level::ERROR => Color::Red,
        Level::WARN => Color::Yellow,
        Level::INFO => Color::White,
        Level::DEBUG | Level::TRACE => Color::DarkGray,
    }
}

/// The pane's starting display level, from the configured log level. The pane
/// captures at `DEBUG`, so a more verbose configured level (`TRACE`) starts
/// clamped to `DEBUG`; `OFF` starts showing only errors.
fn initial_display_level(configured: LevelFilter) -> Level {
    match configured.into_level() {
        Some(level) if level <= Level::DEBUG => level,
        Some(_) => Level::DEBUG,
        None => Level::ERROR,
    }
}

/// Entry point for `--ui`: set up logging into the pane, bind the stable
/// listener, enter the terminal, and run the operator loop.
pub async fn run_ui(config: crate::cli::ResolvedConfig) -> anyhow::Result<()> {
    use tracing_subscriber::{Layer, Registry};

    let log_buffer = LogBuffer::new(1000);
    // The pane captures at DEBUG regardless of the configured level (which still
    // governs the file sink), so the operator can raise verbosity at runtime
    // without a restart.
    let pane: Box<dyn Layer<Registry> + Send + Sync> =
        crate::tui::log_buffer::LogBufferLayer::new(log_buffer.clone())
            .with_filter(LevelFilter::DEBUG)
            .boxed();
    // stderr off (would corrupt the TUI); pane on; file independent. Adapter
    // stderr folds into the file only when no dedicated --adapter-log is set;
    // it always reaches the pane via the pane's own (target-agnostic) filter.
    let _guard = crate::cli::init_logging(
        config.level,
        config.log_file.as_deref(),
        false,
        config.adapter_log.is_none(),
        false,
        Some(pane),
    )?;

    // `--ui` always resolves to a TCP host, so the listener port is present.
    let listen_port = config.downstream.tcp.unwrap_or(0);
    let listener = ClientListener::bind("127.0.0.1", listen_port).await?;

    // A command-line upstream — attach or spawn — is connected automatically;
    // only when none was named does the interface prompt for an attach address.
    let upstream = match &config.upstream {
        crate::cli::UpstreamChoice::Attach(host, port) => Upstream::Configured {
            transport: UpstreamTransport::tcp(host.clone(), *port),
            label: if host == "127.0.0.1" {
                port.to_string()
            } else {
                format!("{host}:{port}")
            },
        },
        crate::cli::UpstreamChoice::Spawn { command, args } => Upstream::Configured {
            transport: UpstreamTransport::stdio(
                command.clone(),
                args.clone(),
                config.adapter_log.clone(),
            ),
            label: command.clone(),
        },
        crate::cli::UpstreamChoice::Interactive => Upstream::Interactive,
    };

    let mut guard = crate::tui::terminal::TerminalGuard::enter()?;
    let app = App::new(listener, log_buffer, upstream, initial_display_level(config.level));
    let result = app.run(guard.terminal()).await;
    drop(guard); // restore the terminal before returning
    result
}
