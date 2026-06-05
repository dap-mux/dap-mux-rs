//! DAP message framing and classification.
//!
//! The Debug Adapter Protocol uses `Content-Length`-framed JSON over a byte
//! stream (identical to LSP). This module provides the framing layer and a
//! handful of classification helpers. Messages flow through the proxy as
//! [`serde_json::Value`] objects; the mux inspects only the fields it needs.

use std::io;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const HEADER_SEPARATOR: &[u8] = b"\r\n\r\n";
const CONTENT_LENGTH: &str = "Content-Length:";

/// A DAP message as it flows through the proxy: always a JSON object.
pub type DapMessage = Value;

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

/// Return whether `message` is a DAP request.
pub fn is_request(message: &DapMessage) -> bool {
    message.get("type").and_then(Value::as_str) == Some("request")
}

/// Return whether `message` is a DAP response.
pub fn is_response(message: &DapMessage) -> bool {
    message.get("type").and_then(Value::as_str) == Some("response")
}

/// Return whether `message` is a DAP event.
pub fn is_event(message: &DapMessage) -> bool {
    message.get("type").and_then(Value::as_str) == Some("event")
}

// ---------------------------------------------------------------------------
// Framing — pure functions (no I/O)
// ---------------------------------------------------------------------------

/// Serialize a DAP message to `Content-Length`-framed bytes.
pub fn encode_message(message: &DapMessage) -> Vec<u8> {
    let body = serde_json::to_vec(message).expect("DAP message must serialize to JSON");
    let header = format!("Content-Length: {}\r\n\r\n", body.len());
    let mut out = Vec::with_capacity(header.len() + body.len());
    out.extend_from_slice(header.as_bytes());
    out.extend_from_slice(&body);
    out
}

/// Parse a `Content-Length` header and return the declared body length.
pub fn decode_header(data: &[u8]) -> io::Result<usize> {
    let text = std::str::from_utf8(data).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, format!("non-ASCII header: {e}"))
    })?;
    for line in text.split("\r\n") {
        if let Some(rest) = line.strip_prefix(CONTENT_LENGTH) {
            return rest.trim().parse::<usize>().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad Content-Length: {e}"),
                )
            });
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Missing Content-Length in header: {text:?}"),
    ))
}

/// Deserialize a JSON body into a DAP message.
pub fn decode_body(data: &[u8]) -> io::Result<DapMessage> {
    serde_json::from_slice(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("invalid DAP body: {e}")))
}

// ---------------------------------------------------------------------------
// Framing — async I/O
// ---------------------------------------------------------------------------

/// Read one `Content-Length`-framed DAP message from `reader`.
///
/// Returns an `UnexpectedEof` error when the stream ends before a complete
/// message is received.
pub async fn read_message<R>(reader: &mut R) -> io::Result<DapMessage>
where
    R: AsyncRead + Unpin,
{
    let header = read_header(reader).await?;
    let content_length = decode_header(&header)?;
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).await?;
    let message = decode_body(&body)?;
    tracing::trace!(?message, "DAP recv");
    Ok(message)
}

/// Write one `Content-Length`-framed DAP message to `writer`.
pub async fn write_message<W>(writer: &mut W, message: &DapMessage) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    tracing::trace!(?message, "DAP send");
    writer.write_all(&encode_message(message)).await?;
    writer.flush().await
}

/// Read bytes until the header/body separator is found.
async fn read_header<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut buffer = Vec::with_capacity(64);
    while !buffer.ends_with(HEADER_SEPARATOR) {
        let byte = reader.read_u8().await.map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Connection closed while reading DAP header",
                )
            } else {
                e
            }
        })?;
        buffer.push(byte);
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classification() {
        assert!(is_request(&json!({"type": "request", "command": "next"})));
        assert!(!is_request(&json!({"type": "event", "event": "stopped"})));
        assert!(is_response(&json!({"type": "response", "command": "next"})));
        assert!(is_event(&json!({"type": "event", "event": "stopped"})));
    }

    #[test]
    fn encode_round_trips() {
        let message = json!({"seq": 1, "type": "request", "command": "next"});
        let bytes = encode_message(&message);
        let sep = bytes
            .windows(4)
            .position(|w| w == HEADER_SEPARATOR)
            .unwrap();
        let len = decode_header(&bytes[..sep + 4]).unwrap();
        let body = &bytes[sep + 4..];
        assert_eq!(len, body.len());
        assert_eq!(decode_body(body).unwrap(), message);
    }

    #[test]
    fn decode_header_missing() {
        assert!(decode_header(b"Cromulent: 5\r\n\r\n").is_err());
    }

    #[tokio::test]
    async fn read_write_round_trip() {
        let message =
            json!({"seq": 7, "type": "event", "event": "stopped", "body": {"reason": "step"}});
        let bytes = encode_message(&message);
        let mut cursor = std::io::Cursor::new(bytes);
        let got = read_message(&mut cursor).await.unwrap();
        assert_eq!(got, message);
    }

    #[tokio::test]
    async fn read_eof_is_error() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let err = read_message(&mut cursor).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
