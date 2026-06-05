//! `dap-mux` binary entrypoint.

use clap::Parser;

use dap_mux::cli::{Cli, run};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let config = match cli.resolve() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("Error: {err}");
            std::process::exit(2);
        }
    };

    if let Err(err) = run(config).await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}
