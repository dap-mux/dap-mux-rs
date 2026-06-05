//! The operator TUI (`--ui`): the display option of the TCP frontend.
//!
//! Headless TCP and the stdio frontend render none of this. The TUI is a live
//! view over the same TCP-served session: it shows the upstream and connected
//! clients and routes logs to an in-app pane instead of stderr. The upstream is
//! the one chosen on the command line (attach or spawn); when none was given it
//! prompts for an attach address.

mod app;
pub mod log_buffer;
pub mod terminal;

pub use app::{App, run_ui};
