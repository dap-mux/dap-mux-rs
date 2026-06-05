//! Upstream transport abstraction.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStderr, Command};
use tokio::time::{Instant, sleep};

/// A connected upstream: framed-message reader and writer halves, plus the
/// spawned adapter process when the mux launched it.
///
/// The reader/writer are boxed trait objects so the routing core is agnostic to
/// the concrete transport: a TCP socket's split halves for [`TcpUpstream`], or a
/// child process's stdout/stdin for [`StdioUpstream`]. `child` is `Some` only
/// when the mux spawned the adapter, and carries ownership for reaping.
pub struct UpstreamIo {
    pub reader: Box<dyn AsyncRead + Unpin + Send>,
    pub writer: Box<dyn AsyncWrite + Unpin + Send>,
    pub child: Option<Child>,
}

impl std::fmt::Debug for UpstreamIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UpstreamIo { .. }")
    }
}

/// How to reach the upstream debug adapter.
#[derive(Clone, Debug)]
pub enum UpstreamTransport {
    /// Connect to an already-running adapter over TCP.
    Tcp(TcpUpstream),
    /// Spawn the adapter as a child process and speak DAP over its stdin/stdout.
    Stdio(StdioUpstream),
}

impl UpstreamTransport {
    /// Construct a TCP transport with default retry settings.
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self::Tcp(TcpUpstream::new(host, port))
    }

    /// Construct a stdio transport that spawns `command` with `args`. When
    /// `adapter_log` is set, the adapter's stderr is also written there verbatim.
    pub fn stdio(
        command: impl Into<String>,
        args: Vec<String>,
        adapter_log: Option<PathBuf>,
    ) -> Self {
        Self::Stdio(StdioUpstream::new(command, args, adapter_log))
    }

    /// Establish the connection, retrying per the transport's policy.
    pub async fn connect(&self) -> Result<UpstreamIo, std::io::Error> {
        match self {
            Self::Tcp(t) => t.connect().await,
            Self::Stdio(s) => s.connect().await,
        }
    }
}

/// TCP transport to a listening DAP adapter.
#[derive(Clone, Debug)]
pub struct TcpUpstream {
    pub host: String,
    pub port: u16,
    pub retry_timeout: Duration,
    pub retry_interval: Duration,
}

impl TcpUpstream {
    /// Default policy: retry connection-refused for up to 10s, polling 100ms.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            retry_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(100),
        }
    }

    /// Open a TCP connection, retrying on connection-refused until the timeout.
    ///
    /// Retrying (rather than probing first) avoids making adapters like
    /// debugpy observe a spurious connect/disconnect that can make them exit
    /// early.
    pub async fn connect(&self) -> Result<UpstreamIo, std::io::Error> {
        let deadline = Instant::now() + self.retry_timeout;
        loop {
            match TcpStream::connect((self.host.as_str(), self.port)).await {
                Ok(stream) => {
                    if let Err(err) = stream.set_nodelay(true) {
                        tracing::debug!(%err, "set_nodelay failed; proceeding without it");
                    }
                    let (reader, writer) = stream.into_split();
                    tracing::info!(host = %self.host, port = self.port, "Connected to debug adapter");
                    return Ok(UpstreamIo {
                        // Buffer the read half: `read_message` reads the framing
                        // header a byte at a time, so an unbuffered split half
                        // would be one syscall per header byte.
                        reader: Box::new(BufReader::new(reader)),
                        writer: Box::new(writer),
                        child: None,
                    });
                }
                Err(err) => {
                    if Instant::now() >= deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "Timed out connecting to debug adapter at {}:{} after {:.0}s ({err})",
                                self.host,
                                self.port,
                                self.retry_timeout.as_secs_f64(),
                            ),
                        ));
                    }
                    sleep(self.retry_interval).await;
                }
            }
        }
    }
}

/// Stdio transport: spawn the adapter and speak DAP over its stdin/stdout.
///
/// This is the native channel for adapters that have no TCP listener — the mux
/// can only pipe into a process it launched, so spawning and stdio are bound
/// together (a spawned adapter is reached over its pipes, never a socket).
#[derive(Clone, Debug)]
pub struct StdioUpstream {
    pub command: String,
    pub args: Vec<String>,
    /// Destination for the adapter's raw, verbatim stderr, if any.
    pub adapter_log: Option<PathBuf>,
}

impl StdioUpstream {
    pub fn new(
        command: impl Into<String>,
        args: Vec<String>,
        adapter_log: Option<PathBuf>,
    ) -> Self {
        Self {
            command: command.into(),
            args,
            adapter_log,
        }
    }

    /// Spawn the adapter and return its stdout as the reader and stdin as the
    /// writer. The child's stderr is pumped to the `adapter` log target (and,
    /// when configured, a raw file). `kill_on_drop` is the backstop that reaps
    /// the child if the owning [`UpstreamIo`] is dropped without an explicit
    /// reap.
    pub async fn connect(&self) -> Result<UpstreamIo, std::io::Error> {
        let mut child = Command::new(&self.command)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!("failed to spawn debug adapter {:?}: {err}", self.command),
                )
            })?;

        // The pipes are present because they were configured above; a missing
        // one is a tokio invariant violation, not a runtime condition.
        let stdout = child.stdout.take().expect("child stdout was piped");
        let stdin = child.stdin.take().expect("child stdin was piped");
        let stderr = child.stderr.take().expect("child stderr was piped");

        pump_adapter_stderr(stderr, self.adapter_log.clone());
        tracing::info!(command = %self.command, "Spawned debug adapter");

        Ok(UpstreamIo {
            // Buffer for the same byte-at-a-time header read reason as TCP.
            reader: Box::new(BufReader::new(stdout)),
            writer: Box::new(stdin),
            child: Some(child),
        })
    }
}

/// Drain a spawned adapter's stderr, tee'ing it two ways from a single read:
/// raw verbatim bytes to `adapter_log` (the faithful capture), and one
/// `tracing` event per line under the `adapter` target (for the live/structured
/// views). The target is emitted at a neutral level because adapters log
/// informational output to stderr, not only errors.
fn pump_adapter_stderr(stderr: ChildStderr, adapter_log: Option<PathBuf>) {
    tokio::spawn(async move {
        let mut file = match &adapter_log {
            Some(path) => match tokio::fs::File::create(path).await {
                Ok(file) => Some(file),
                Err(err) => {
                    tracing::warn!(%err, ?path, "could not open adapter log; raw stderr will not be persisted");
                    None
                }
            },
            None => None,
        };

        let mut reader = BufReader::new(stderr);
        let mut line = Vec::new();
        loop {
            line.clear();
            match reader.read_until(b'\n', &mut line).await {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(file) = &mut file {
                        let _ = file.write_all(&line).await;
                    }
                    let text = String::from_utf8_lossy(&line);
                    let text = text.trim_end_matches(['\r', '\n']);
                    if !text.is_empty() {
                        tracing::info!(target: "adapter", "{text}");
                    }
                }
                Err(err) => {
                    tracing::warn!(%err, "error reading adapter stderr");
                    break;
                }
            }
        }
        if let Some(mut file) = file {
            let _ = file.flush().await;
        }
    });
}
