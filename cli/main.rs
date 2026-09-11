#![allow(clippy::arc_with_non_send_sync)]
mod app;
mod commands;
mod config;
mod dot_command;
mod helper;
mod input;
mod manual;
mod mcp_server;
mod opcodes_dictionary;
mod read_state_machine;
mod sync_server;

#[cfg(feature = "mvcc_repl")]
mod mvcc_repl;

use config::CONFIG_DIR;
use mcp_server::TursoMcpServer;
use rustyline::{error::ReadlineError, Config, Editor};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, LazyLock},
};

use crate::sync_server::{OpenConfig, TursoSyncServer};

#[cfg(all(feature = "mimalloc", not(target_family = "wasm"), not(miri)))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

pub static HOME_DIR: LazyLock<PathBuf> =
    LazyLock::new(|| dirs::home_dir().expect("Could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".limbo_history"));

fn run_mcp_server(app: app::Limbo) -> anyhow::Result<()> {
    let conn = app.get_connection();
    let interrupt_count = app.get_interrupt_count();
    let mcp_server = TursoMcpServer::new(conn, interrupt_count);

    mcp_server.run()
}

fn run_sync_server(app: app::Limbo) -> anyhow::Result<()> {
    let address = app.opts.sync_server_address.clone().unwrap();
    let interrupt_count = app.get_interrupt_count();

    let sync_server = match app.opts.sync_dir.clone() {
        Some(dir) => TursoSyncServer::new_dir(
            address,
            dir,
            interrupt_count,
            OpenConfig {
                vfs: app.opts.vfs.clone(),
                flags: app.opts.flags,
                db_opts: app.db_opts,
                max_open: app.opts.sync_max_databases,
            },
        )?,
        None => TursoSyncServer::new(
            address,
            app.opts.db_file.clone(),
            app.get_connection(),
            interrupt_count,
        )?,
    };

    sync_server.run()
}

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "mvcc_repl")]
    {
        use clap::Parser as _;
        let opts = app::Opts::parse();
        if opts.mvcc {
            let path = opts
                .database
                .as_ref()
                .and_then(|p| p.to_str())
                .unwrap_or(":memory:")
                .to_owned();
            return mvcc_repl::run(&path, opts.experimental_mvcc_passive_checkpoint);
        }
    }

    let (mut app, _guard) = app::Limbo::new()?;

    if app.is_mcp_mode() {
        return run_mcp_server(app);
    }
    if app.is_sync_server_mode() {
        return run_sync_server(app);
    }

    let interactive_stdin = std::io::IsTerminal::is_terminal(&std::io::stdin());
    if interactive_stdin {
        let mut rl = Editor::with_config(rustyline_config())?;
        if HISTORY_FILE.exists() {
            rl.load_history(HISTORY_FILE.as_path())?;
        }
        let config_file = CONFIG_DIR.join("limbo.toml");

        let config = config::Config::from_config_file(config_file);
        tracing::info!("Configuration: {:?}", config);
        app = app.with_config(config);

        app = app.with_readline(rl);
    } else {
        tracing::debug!("not in tty");
    }

    loop {
        match app.readline() {
            Ok(_) => app.consume(false),
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if app.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    let _ = app.close_conn();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                // consume remaining input before exit
                app.consume(true);
                let _ = app.close_conn();
                break;
            }
            Err(err) => {
                let _ = app.close_conn();
                anyhow::bail!(err)
            }
        }
    }
    if !interactive_stdin && app.has_query_error() {
        std::process::exit(1);
    }
    Ok(())
}
