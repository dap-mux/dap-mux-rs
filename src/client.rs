//! Per-connection read/write loops.
//!
//! Each downstream client (and the upstream adapter) is modelled as a tokio
//! read task plus a write task draining an `mpsc` send queue. Decoupling the two
//! means routing never blocks on a slow writer: messages queue instead.
//!
//! Routing decisions are synchronous (lock the mux state, enqueue), so the
//! read loop takes plain `Fn` callbacks rather than async ones.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::protocol::{DapMessage, read_message, write_message};

/// Read framed DAP messages from `reader`, dispatching each to `on_message`.
///
/// When the stream ends or errors, `on_disconnect` is invoked once and the
/// loop returns.
pub async fn read_loop<R, M, D>(id: String, mut reader: R, on_message: M, on_disconnect: D)
where
    R: AsyncRead + Unpin,
    M: Fn(&str, DapMessage),
    D: FnOnce(&str),
{
    loop {
        match read_message(&mut reader).await {
            Ok(message) => on_message(&id, message),
            Err(err) => {
                tracing::info!(id = %id, %err, "connection closed");
                on_disconnect(&id);
                return;
            }
        }
    }
}

/// Drain `rx`, writing each queued message to `writer` until the channel
/// closes or a write fails.
pub async fn write_loop<W>(id: String, mut writer: W, mut rx: UnboundedReceiver<DapMessage>)
where
    W: AsyncWrite + Unpin,
{
    while let Some(message) = rx.recv().await {
        if let Err(err) = write_message(&mut writer, &message).await {
            tracing::warn!(id = %id, %err, "connection lost during write");
            return;
        }
    }
}
