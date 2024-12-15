mod app;
mod opcodes_dictionary;

use clap::Parser;
use rustyline::{error::ReadlineError, DefaultEditor};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[allow(clippy::arc_with_non_send_sync)]
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts = app::Opts::parse();
    let mut app = app::Limbo::new(&opts)?;
    let interrupt_count = Arc::new(AtomicUsize::new(0));
    {
        let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
        ctrlc::set_handler(move || {
            // Increment the interrupt count on Ctrl-C
            interrupt_count.fetch_add(1, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
    }

    if let Some(sql) = opts.sql {
        if sql.trim().starts_with('.') {
            app.handle_dot_command(&sql);
        } else if let Err(e) = app.query(&sql, &interrupt_count) {
            eprintln!("{}", e);
        }
        return Ok(());
    }
    println!("Limbo v{}", env!("CARGO_PKG_VERSION"));
    println!("Enter \".help\" for usage hints.");
    let _ = app.display_in_memory();
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().expect("Could not determine home directory");
    let history_file = home.join(".limbo_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    loop {
        let readline = rl.readline(&app.prompt);
        match readline {
            Ok(line) => match app.handle_input_line(line.trim(), &interrupt_count, &mut rl) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{}", e);
                }
            },
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    let _ = app.close_conn();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                let _ = app.close_conn();
                break;
            }
            Err(err) => {
                let _ = app.close_conn();
                anyhow::bail!(err)
            }
        }
    }
    rl.save_history(history_file.as_path())?;
    Ok(())
}
