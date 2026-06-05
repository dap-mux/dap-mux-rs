//! dap-mux: a language-agnostic DAP multiplexer.
//!
//! One upstream debug adapter, many downstream DAP clients. The mux rewrites
//! sequence numbers, routes responses, broadcasts events, and replays session
//! state to late joiners — all transport- and language-independent.

pub mod cli;
pub mod client;
pub mod compat;
pub mod mux;
pub mod protocol;
pub mod seq;
pub mod tui;
pub mod upstream;

pub use mux::{AcceptGuard, ClientInfo, ClientListener, Multiplexer, MuxSnapshot, SessionPhase};
pub use upstream::{TcpUpstream, UpstreamTransport};
