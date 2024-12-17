mod app;
mod opcodes_dictionary;

use rustyline::{error::ReadlineError, DefaultEditor};
use std::sync::atomic::Ordering;

#[allow(clippy::arc_with_non_send_sync)]
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut app = app::Limbo::new()?;
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().expect("Could not determine home directory");
    let history_file = home.join(".limbo_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    loop {
        let readline = rl.readline(&app.prompt);
        match readline {
            Ok(line) => match app.handle_input_line(line.trim(), &mut rl) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{}", e);
                }
            },
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
