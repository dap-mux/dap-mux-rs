//! Terminal setup/teardown with restoration on exit and on panic.
//!
//! Raw mode and the alternate screen must be undone whether the program exits
//! normally or panics, or the operator's shell is left wrecked. An RAII guard
//! restores on drop (normal exit); a panic hook restores before the default
//! hook prints (panic).

use std::io::{self, Stdout};

use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

/// The concrete terminal type the interface draws to.
pub type Tui = Terminal<CrosstermBackend<Stdout>>;

/// Owns the terminal's raw/alternate-screen state and restores it on drop.
pub struct TerminalGuard {
    terminal: Tui,
}

impl TerminalGuard {
    /// Enter raw mode + the alternate screen, install the restore panic hook,
    /// and build the ratatui terminal.
    pub fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        install_panic_hook();
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    /// Mutable access to the underlying terminal for drawing.
    pub fn terminal(&mut self) -> &mut Tui {
        &mut self.terminal
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = restore_terminal();
    }
}

/// Undo raw mode and leave the alternate screen. Idempotent enough to call from
/// both the drop guard and the panic hook.
pub fn restore_terminal() -> io::Result<()> {
    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    Ok(())
}

/// Install a panic hook that runs `restore` before delegating to the previous
/// hook, so the terminal is restored before the panic message prints.
pub fn install_restore_panic_hook<F>(restore: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        original(info);
    }));
}

fn install_panic_hook() {
    install_restore_panic_hook(|| {
        let _ = restore_terminal();
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// The panic hook installed by [`install_restore_panic_hook`] runs the
    /// restore callback during unwinding — the logic behind "terminal restored
    /// after a panic" without needing a real TTY.
    #[test]
    fn panic_hook_runs_restore_callback() {
        let restored = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&restored);
        install_restore_panic_hook(move || flag.store(true, Ordering::SeqCst));

        let result = std::panic::catch_unwind(|| panic!("simulated panic"));
        assert!(result.is_err());
        assert!(
            restored.load(Ordering::SeqCst),
            "restore callback should run during panic unwinding"
        );
    }
}
