//! Command-line interface for `dap-mux`.
//!
use std::path::{Path, PathBuf};

use clap::Parser;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::{LevelFilter, filter_fn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{Layer, Registry};

use crate::mux::Multiplexer;
use crate::upstream::UpstreamTransport;

/// The `tracing` target carrying a spawned adapter's stderr. Routed apart from
/// the mux's own logs so it can land in its own file and be filtered per-sink.
const ADAPTER_TARGET: &str = "adapter";

/// DAP multiplexer — one debug adapter, many clients.
///
/// There are two ways the mux talks to the adapter.
/// 1. stdio. In this case, the mux needs to spawn the adapter.
/// 2. TCP. This uses --attach and connects to a running process.
///
/// There are then two ways for the IDE to drive the mux.
/// 1. stdio. The IDE spawns the mux.
/// 2. TCP. The IDE connects to a running mux session.
///
/// You have the option of running TCP mode "headless" or with a TUI.
#[derive(Parser, Debug)]
#[command(name = "dap-mux", version, about, long_about = None)]
pub struct Cli {
    /// Attach to an already-running debug adapter ([host:]port).
    ///
    /// Mutually exclusive with `--adapter`. Exactly one upstream is supported.
    #[arg(long, short = 'a', value_name = "[HOST:]PORT")]
    pub attach: Option<String>,

    /// Serve one client over the mux's own stdin/stdout (the channel an editor
    /// that spawned the mux drives), in addition to any TCP listener.
    ///
    /// By default the mux is a standalone TCP host; `--stdio` makes it act as a
    /// plain stdio adapter for its launcher. Pair with `--mux-port` to serve
    /// that launcher over stdio while also accepting TCP clients. Cannot combine
    /// with `--ui`, whose TUI needs the terminal that stdout would carry DAP on.
    #[arg(long)]
    pub stdio: bool,

    /// Run the operator TUI — the display option of the TCP host.
    ///
    /// Renders to the terminal, so it cannot combine with `--stdio`. An upstream
    /// on the command line is optional: without `--attach` or a trailing adapter
    /// command, the TUI prompts for an attach address.
    #[arg(long)]
    pub ui: bool,

    /// Port for the TCP client listener (0 = OS-assigned).
    ///
    /// The listener always binds for a TCP host (the default and `--ui`),
    /// defaulting to an OS-assigned port. Under `--stdio` it binds only when
    /// this is given, so a stdio adapter opens a port for extra clients only on
    /// request.
    #[arg(long, short = 'p', value_name = "PORT")]
    pub mux_port: Option<u16>,

    /// Log level (TRACE, DEBUG, INFO, WARNING, ERROR).
    #[arg(long, short = 'l', default_value = "INFO")]
    pub log_level: String,

    /// Also write the mux's logs to this file.
    #[arg(long, value_name = "PATH")]
    pub log_file: Option<PathBuf>,

    /// Write the spawned adapter's stderr to this file, raw and verbatim.
    ///
    /// When set, adapter stderr is kept out of `--log-file` so the main log
    /// stays free of adapter noise. Without it, adapter stderr folds into
    /// `--log-file` as tagged events instead.
    #[arg(long, value_name = "PATH")]
    pub adapter_log: Option<PathBuf>,

    /// For a TCP host (no `--stdio`), also echo the spawned adapter's stderr to
    /// the terminal.
    ///
    /// Off by default so adapter chatter does not interleave with mux logs.
    #[arg(long)]
    pub echo_adapter_stderr: bool,

    /// The debug adapter to spawn, as a single command line, e.g.
    /// `--adapter "lldb-dap --port 0"`.
    ///
    /// Split into a command and arguments with shell-style word rules, so a
    /// launcher that can only pass one argument (an editor's adapter config)
    /// can still hand over an adapter with its own flags. Mutually exclusive
    /// with `--attach` and the trailing `-- <command>` form.
    #[arg(long, value_name = "COMMAND")]
    pub adapter: Option<String>,

    /// The debug adapter to spawn, given after `--` as a normal argv:
    /// `dap-mux … -- lldb-dap --port 0`.
    ///
    /// A shell-friendly alternative to `--adapter`: the shell tokenizes the
    /// words, so there is no inner quoting to get right. Mutually exclusive with
    /// `--adapter` and `--attach`.
    #[arg(last = true, value_name = "ADAPTER_COMMAND")]
    pub adapter_argv: Vec<String>,
}

/// A usage error in the command-line invocation.
///
/// These are reported to the user and are distinct from runtime failures.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(
        "an upstream is required: one of --attach <[host:]port>, --adapter <command>, or a trailing `-- <command>`. See --help."
    )]
    MissingUpstream,
    #[error(
        "more than one upstream was given: use exactly one of --attach <[host:]port>, --adapter <command>, or a trailing `-- <command>`. See --help."
    )]
    UpstreamConflict,
    #[error("--adapter value {0:?} is not a valid command line")]
    BadAdapterCommand(String),
    #[error(
        "--stdio cannot combine with --ui: the operator TUI renders to the terminal, which the stdio DAP channel would corrupt. See --help."
    )]
    StdioWithUi,
    #[error("invalid port in attach address: {0:?}")]
    InvalidPort(String),
    #[error(
        "{0:?} is not a valid log level. Choose from: \
         TRACE, DEBUG, INFO, SUCCESS, WARNING, ERROR, CRITICAL"
    )]
    InvalidLogLevel(String),
}

/// How to reach the upstream debug adapter, resolved from the CLI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamChoice {
    /// Attach to an already-running adapter over TCP.
    Attach(String, u16),
    /// Spawn the adapter and speak DAP over its stdin/stdout.
    Spawn { command: String, args: Vec<String> },
    /// No upstream named on the command line: entered interactively in the TUI.
    /// Only reachable under `--ui`; every other mode requires a named one.
    Interactive,
}

/// How the mux serves downstream clients, resolved from the CLI. The two
/// channels are independent and at least one is always enabled — a stdio client
/// over the mux's own pipes, a TCP listener, or both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Downstream {
    /// Serve one client over the mux's own stdin/stdout (`--stdio`).
    pub stdio: bool,
    /// Listen for clients over TCP on this port (0 = OS-assigned), or `None`
    /// for no listener. Always `Some` unless [`stdio`](Self::stdio) is the only
    /// channel.
    pub tcp: Option<u16>,
}

/// Parse an attach address like `5678` or `host:5678` into `(host, port)`,
/// defaulting the host to `127.0.0.1` when only a port is given.
pub fn parse_attach(value: &str) -> Result<(String, u16), ConfigError> {
    // Splitting on the last colon assumes an IPv4 host or hostname; a bare IPv6
    // literal like `::1` would mis-split. Debug adapters listen on a local
    // loopback port, so IPv6 attach targets are out of scope.
    let (host, port_str) = match value.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port),
        None => ("127.0.0.1".to_string(), value),
    };
    let port = port_str
        .parse::<u16>()
        .map_err(|_| ConfigError::InvalidPort(value.to_string()))?;
    Ok((host, port))
}

/// Validate the log level case-insensitively against the accepted set and map
/// it to a tracing level.
pub fn parse_log_level(level: &str) -> Result<LevelFilter, ConfigError> {
    match level.to_uppercase().as_str() {
        "TRACE" => Ok(LevelFilter::TRACE),
        "DEBUG" => Ok(LevelFilter::DEBUG),
        "INFO" | "SUCCESS" => Ok(LevelFilter::INFO),
        "WARNING" | "WARN" => Ok(LevelFilter::WARN),
        "ERROR" | "CRITICAL" => Ok(LevelFilter::ERROR),
        _ => Err(ConfigError::InvalidLogLevel(level.to_string())),
    }
}

/// Configure `tracing` sinks by mode. Up to three independent layers:
///
/// - **stderr**: on in the headless/stdio frontends, off under `--ui` (it would
///   corrupt the rendered interface).
/// - **file**: present whenever `--log-file` is given, in any mode.
/// - **pane**: an optional caller-supplied layer (the TUI's ring buffer);
///   `None` outside `--ui`.
///
/// `level` filters the file and stderr sinks. The pane carries its own filter
/// (the caller sets it), so the operator view can capture more verbosely than
/// the persisted sinks and adjust what it shows at runtime.
///
/// The spawned adapter's stderr arrives on the [`ADAPTER_TARGET`] target.
/// `adapter_in_file`/`adapter_in_stderr` gate whether each sink carries it: the
/// adapter target folds into the file only when no dedicated `--adapter-log` is
/// set, and reaches the terminal only when explicitly echoed.
///
/// Returns the file appender's worker guard, which must be kept alive for the
/// program's lifetime, or `None` when no log file is configured.
pub fn init_logging(
    level: LevelFilter,
    log_file: Option<&Path>,
    stderr: bool,
    adapter_in_file: bool,
    adapter_in_stderr: bool,
    pane_layer: Option<Box<dyn Layer<Registry> + Send + Sync>>,
) -> Result<Option<WorkerGuard>, std::io::Error> {
    let mut guard = None;
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();

    if let Some(path) = log_file {
        let file = std::fs::File::create(path)?;
        let (writer, g) = tracing_appender::non_blocking(file);
        guard = Some(g);
        layers.push(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(filter_fn(move |meta| {
                    if meta.target() == ADAPTER_TARGET && !adapter_in_file {
                        return false;
                    }
                    LevelFilter::from_level(*meta.level()) <= level
                }))
                .boxed(),
        );
    }
    if stderr {
        layers.push(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(filter_fn(move |meta| {
                    if meta.target() == ADAPTER_TARGET && !adapter_in_stderr {
                        return false;
                    }
                    LevelFilter::from_level(*meta.level()) <= level
                }))
                .boxed(),
        );
    }
    if let Some(pane) = pane_layer {
        layers.push(pane);
    }

    tracing_subscriber::registry().with(layers).init();
    Ok(guard)
}

/// A validated invocation, ready to run.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// How to reach the upstream adapter.
    pub upstream: UpstreamChoice,
    /// Which downstream client channels to serve.
    pub downstream: Downstream,
    /// Whether to run the operator TUI. Only set for a TCP host without a stdio
    /// client (the TUI owns the terminal).
    pub ui: bool,
    pub level: LevelFilter,
    pub log_file: Option<PathBuf>,
    /// Raw, verbatim destination for the spawned adapter's stderr.
    pub adapter_log: Option<PathBuf>,
    /// Whether to echo the adapter's stderr to the terminal (TCP host only).
    pub echo_adapter_stderr: bool,
}

impl Cli {
    /// Validate the invocation, returning a runnable config.
    ///
    /// Errors here are usage errors. The upstream axis (`--attach` vs a trailing
    /// `-- command`) and the downstream channels (stdio client and/or a TCP
    /// listener) are independent. The mux is a TCP host by default; `--stdio`
    /// adds the stdio client, and only `--ui` (which renders to the terminal)
    /// rules the stdio client out.
    pub fn resolve(&self) -> Result<ResolvedConfig, ConfigError> {
        if self.stdio && self.ui {
            return Err(ConfigError::StdioWithUi);
        }
        // The TCP listener binds for any host (the default and `--ui`) and
        // whenever a port is requested; under `--stdio` alone there is no
        // listener unless `--mux-port` asks for one.
        let tcp = if self.mux_port.is_some() || self.ui || !self.stdio {
            Some(self.mux_port.unwrap_or(0))
        } else {
            None
        };
        let downstream = Downstream {
            stdio: self.stdio,
            tcp,
        };

        // A spawned adapter can be named two ways: `--adapter` as one string (a
        // launcher that passes a single argument splits it shell-style here), or
        // a trailing `-- <cmd...>` the shell already tokenized. At most one.
        let spawn_command = match (self.adapter.as_deref(), self.adapter_argv.as_slice()) {
            (Some(_), [_, ..]) => return Err(ConfigError::UpstreamConflict),
            (Some(line), []) => match shlex::split(line) {
                Some(words) if !words.is_empty() => Some(words),
                _ => return Err(ConfigError::BadAdapterCommand(line.to_string())),
            },
            (None, [_, ..]) => Some(self.adapter_argv.clone()),
            (None, []) => None,
        };
        if spawn_command.is_some() && self.attach.is_some() {
            return Err(ConfigError::UpstreamConflict);
        }
        let upstream = if let Some(mut words) = spawn_command {
            let command = words.remove(0);
            UpstreamChoice::Spawn {
                command,
                args: words,
            }
        } else if let Some(addr) = self.attach.as_deref() {
            let (host, port) = parse_attach(addr)?;
            UpstreamChoice::Attach(host, port)
        } else if self.ui {
            // Only the TUI can prompt for an upstream at runtime; every other
            // mode needs one named on the command line.
            UpstreamChoice::Interactive
        } else {
            return Err(ConfigError::MissingUpstream);
        };

        let level = parse_log_level(&self.log_level)?;
        Ok(ResolvedConfig {
            upstream,
            downstream,
            ui: self.ui,
            level,
            log_file: self.log_file.clone(),
            adapter_log: self.adapter_log.clone(),
            echo_adapter_stderr: self.echo_adapter_stderr,
        })
    }
}

/// Build the upstream transport for a resolved upstream choice.
fn build_transport(config: &ResolvedConfig) -> UpstreamTransport {
    match &config.upstream {
        UpstreamChoice::Attach(host, port) => UpstreamTransport::tcp(host.clone(), *port),
        UpstreamChoice::Spawn { command, args } => {
            UpstreamTransport::stdio(command.clone(), args.clone(), config.adapter_log.clone())
        }
        UpstreamChoice::Interactive => {
            unreachable!("Interactive upstream is only valid under --ui, handled by run_ui")
        }
    }
}

/// Run the mux from a validated config: the operator TUI, or the plain server.
pub async fn run(config: ResolvedConfig) -> anyhow::Result<()> {
    if config.ui {
        crate::tui::run_ui(config).await
    } else {
        run_serving(config).await
    }
}

/// Serve the session over the chosen downstream channels — a stdio client, a
/// TCP listener, or both — until the session ends. It ends when the upstream
/// adapter connection is lost, when a stdio client's stdin reaches EOF, or (for
/// a TCP host with no stdio client) when the operator interrupts.
///
/// stdout is the DAP wire whenever a stdio client is served, so the startup
/// banner prints only for a pure TCP host, and no logging sink ever targets
/// stdout (logs go to stderr and/or the log file).
async fn run_serving(config: ResolvedConfig) -> anyhow::Result<()> {
    let serving_stdio = config.downstream.stdio;
    let _guard = init_logging(
        config.level,
        config.log_file.as_deref(),
        true,
        config.adapter_log.is_none(),
        // Echoing the adapter's stderr to the terminal is a TCP-host courtesy; a
        // stdio client shares this process, so its launcher would see the noise.
        !serving_stdio && config.echo_adapter_stderr,
        None,
    )?;

    let mux = Multiplexer::new();
    let transport = build_transport(&config);
    mux.connect_upstream(&transport).await?;

    if let Some(port) = config.downstream.tcp {
        let actual_port = mux.serve("127.0.0.1", port).await?;
        if serving_stdio {
            tracing::info!(port = actual_port, "also listening for TCP clients");
        } else {
            // No stdio client, so stdout is free to announce the bound port.
            println!(
                "● {} listening on 127.0.0.1:{actual_port} — Ctrl-C to stop",
                env!("CARGO_PKG_NAME")
            );
        }
    }

    let stdio_client = serving_stdio.then(|| {
        let stdin = tokio::io::BufReader::new(tokio::io::stdin());
        let stdout = tokio::io::stdout();
        mux.serve_stdio_frontend(stdin, stdout)
    });

    // A stdio client's stdin EOF ends the session; a pure TCP host runs until
    // interrupted. Either way, a lost upstream ends it (non-zero exit).
    let result = match stdio_client {
        Some(stdio_client) => tokio::select! {
            () = mux.wait_for_shutdown() => {
                Err(anyhow::anyhow!("debug adapter connection lost; shutting down"))
            }
            _ = stdio_client => {
                tracing::info!("client stdin closed; ending session");
                Ok(())
            }
        },
        None => tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                tracing::info!("interrupted; shutting down");
                Ok(())
            }
            () = mux.wait_for_shutdown() => {
                Err(anyhow::anyhow!("debug adapter connection lost; shutting down"))
            }
        },
    };
    mux.reap_adapter();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_attach_port_only() {
        assert_eq!(
            parse_attach("5678").unwrap(),
            ("127.0.0.1".to_string(), 5678)
        );
    }

    #[test]
    fn parse_attach_host_and_port() {
        assert_eq!(
            parse_attach("192.168.1.1:5678").unwrap(),
            ("192.168.1.1".to_string(), 5678)
        );
    }

    #[test]
    fn parse_attach_localhost() {
        assert_eq!(
            parse_attach("localhost:9999").unwrap(),
            ("localhost".to_string(), 9999)
        );
    }

    #[test]
    fn parse_attach_bad_port() {
        assert!(parse_attach("notaport").is_err());
    }

    #[test]
    fn log_level_valid_and_case_insensitive() {
        assert_eq!(parse_log_level("debug").unwrap(), LevelFilter::DEBUG);
        assert_eq!(parse_log_level("WARNING").unwrap(), LevelFilter::WARN);
        assert_eq!(parse_log_level("CRITICAL").unwrap(), LevelFilter::ERROR);
    }

    #[test]
    fn log_level_invalid_mentions_value() {
        let err = parse_log_level("VERBOS").unwrap_err();
        assert!(err.to_string().contains("VERBOS"));
    }

    #[test]
    fn no_upstream_is_a_usage_error() {
        // The default TCP host still needs an upstream named on the line.
        let cli = Cli::try_parse_from(["dap-mux"]).unwrap();
        let err = cli.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::MissingUpstream));
    }

    #[test]
    fn default_is_a_tcp_host() {
        // No `--stdio`: a standalone TCP host on an OS-assigned port.
        let cli = Cli::try_parse_from(["dap-mux", "--attach", "5678"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.downstream,
            Downstream {
                stdio: false,
                tcp: Some(0)
            }
        );
        assert!(!config.ui);
        assert_eq!(
            config.upstream,
            UpstreamChoice::Attach("127.0.0.1".to_string(), 5678)
        );
    }

    #[test]
    fn stdio_flag_serves_the_launcher() {
        // `--stdio` alone is the plain stdio adapter: no TCP listener.
        let cli = Cli::try_parse_from(["dap-mux", "--stdio", "--adapter", "lldb-dap"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.downstream,
            Downstream {
                stdio: true,
                tcp: None
            }
        );
    }

    #[test]
    fn stdio_with_mux_port_serves_both_channels() {
        let cli = Cli::try_parse_from([
            "dap-mux",
            "--stdio",
            "--mux-port",
            "7000",
            "--adapter",
            "lldb-dap",
        ])
        .unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.downstream,
            Downstream {
                stdio: true,
                tcp: Some(7000)
            }
        );
    }

    #[test]
    fn mux_port_sets_the_host_listener() {
        let cli = Cli::try_parse_from(["dap-mux", "--mux-port", "7000", "--attach", "5678"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.downstream,
            Downstream {
                stdio: false,
                tcp: Some(7000)
            }
        );
    }

    #[test]
    fn adapter_command_is_split_shell_style() {
        // One `--adapter` string carries the command and its own flags.
        let cli =
            Cli::try_parse_from(["dap-mux", "--adapter", "lldb-dap --option-a --option-b"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.upstream,
            UpstreamChoice::Spawn {
                command: "lldb-dap".to_string(),
                args: vec!["--option-a".to_string(), "--option-b".to_string()],
            }
        );
    }

    #[test]
    fn adapter_command_respects_quoting() {
        // A quoted argument with spaces survives the split as one word.
        let cli =
            Cli::try_parse_from(["dap-mux", "--adapter", "py -m debugpy --log 'a b'"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.upstream,
            UpstreamChoice::Spawn {
                command: "py".to_string(),
                args: vec![
                    "-m".to_string(),
                    "debugpy".to_string(),
                    "--log".to_string(),
                    "a b".to_string(),
                ],
            }
        );
    }

    #[test]
    fn unparseable_adapter_command_is_rejected() {
        // An unbalanced quote has no shell-word reading.
        let cli = Cli::try_parse_from(["dap-mux", "--adapter", "lldb-dap 'unterminated"]).unwrap();
        let err = cli.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::BadAdapterCommand(_)));
    }

    #[test]
    fn trailing_command_is_the_shell_side_spawn_signal() {
        // `-- cmd args` is the shell-tokenized alternative to --adapter.
        let cli =
            Cli::try_parse_from(["dap-mux", "--", "lldb-dap", "--option-a", "--option-b"]).unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(
            config.upstream,
            UpstreamChoice::Spawn {
                command: "lldb-dap".to_string(),
                args: vec!["--option-a".to_string(), "--option-b".to_string()],
            }
        );
    }

    #[test]
    fn adapter_flag_and_trailing_form_conflict() {
        // The two spawn spellings name the same upstream; only one is allowed.
        let cli =
            Cli::try_parse_from(["dap-mux", "--adapter", "lldb-dap", "--", "lldb-dap"]).unwrap();
        let err = cli.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::UpstreamConflict));
    }

    #[test]
    fn attach_and_spawn_conflict() {
        let cli =
            Cli::try_parse_from(["dap-mux", "--attach", "5678", "--adapter", "lldb-dap"]).unwrap();
        let err = cli.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::UpstreamConflict));
    }

    #[test]
    fn adapter_log_recorded() {
        let cli = Cli::try_parse_from([
            "dap-mux",
            "--adapter-log",
            "/tmp/a.log",
            "--adapter",
            "lldb-dap",
        ])
        .unwrap();
        let config = cli.resolve().unwrap();
        assert_eq!(config.adapter_log, Some(PathBuf::from("/tmp/a.log")));
    }

    #[test]
    fn ui_is_a_tcp_host_without_a_stdio_client() {
        let cli = Cli::try_parse_from(["dap-mux", "--ui", "--attach", "5678"]).unwrap();
        let config = cli.resolve().unwrap();
        assert!(config.ui);
        assert_eq!(
            config.downstream,
            Downstream {
                stdio: false,
                tcp: Some(0)
            }
        );
        assert_eq!(
            config.upstream,
            UpstreamChoice::Attach("127.0.0.1".to_string(), 5678)
        );
    }

    #[test]
    fn ui_without_an_upstream_is_interactive() {
        let cli = Cli::try_parse_from(["dap-mux", "--ui"]).unwrap();
        let config = cli.resolve().expect("--ui without an upstream is allowed");
        assert!(config.ui);
        assert_eq!(config.upstream, UpstreamChoice::Interactive);
    }

    #[test]
    fn spawn_under_ui_is_allowed() {
        // Spawn a stdio-only adapter upstream while serving clients over TCP and
        // watching in the TUI — the axes are independent.
        let cli = Cli::try_parse_from(["dap-mux", "--ui", "--adapter", "lldb-dap"]).unwrap();
        let config = cli.resolve().unwrap();
        assert!(config.ui);
        assert!(!config.downstream.stdio);
        assert_eq!(
            config.upstream,
            UpstreamChoice::Spawn {
                command: "lldb-dap".to_string(),
                args: vec![],
            }
        );
    }

    #[test]
    fn stdio_and_ui_conflict() {
        // The TUI and a stdio DAP client both claim stdout.
        let cli = Cli::try_parse_from(["dap-mux", "--stdio", "--ui", "--attach", "5678"]).unwrap();
        let err = cli.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::StdioWithUi));
    }
}
