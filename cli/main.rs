mod app;
mod opcodes_dictionary;

use clap::Parser;
use rustyline::{error::ReadlineError, DefaultEditor};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[allow(clippy::arc_with_non_send_sync)]
fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts = app::Opts::parse();
    let mut app = app::Limbo::new(&opts)?;

    {
        let interrupt_count: Arc<AtomicUsize> = app.interrupt_count.clone();
        ctrlc::set_handler(move || {
            // Increment the interrupt count on Ctrl-C
            interrupt_count.fetch_add(1, Ordering::SeqCst);
        })
        .expect("Error setting Ctrl-C handler");
    }

    if let Some(sql) = opts.sql {
        if sql.trim().starts_with('.') {
            app.handle_dot_command(&sql);
        } else {
            app.query(&sql)?;
        }
        return Ok(());
    }
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().expect("Could not determine home directory");
    let history_file = home.join(".limbo_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    const PROMPT: &str = "limbo> ";
    let mut input_buff = String::new();
    let mut prompt = PROMPT.to_string();
    loop {
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let line = line.trim();
                if input_buff.is_empty() {
                    if line.is_empty() {
                        continue;
                    }
                    if line.starts_with('.') {
                        app.handle_dot_command(line);
                        rl.add_history_entry(line.to_owned())?;
                        app.reset_interrupt_count();
                        continue;
                    }
                }
                if line.ends_with(';') {
                    input_buff.push_str(line);
                    input_buff.split(';').for_each(|stmt| {
                        if let Err(e) = app.query(stmt) {
                            eprintln!("{}", e);
                        }
                    });
                    input_buff.clear();
                    prompt = PROMPT.to_string();
                } else {
                    input_buff.push_str(line);
                    input_buff.push(' ');
                    prompt = match calc_parens_offset(&input_buff) {
                        n if n < 0 => String::from(")x!...>"),
                        0 => String::from("   ...> "),
                        n if n < 10 => format!("(x{}...> ", n),
                        _ => String::from("(.....> "),
                    };
                }
                rl.add_history_entry(line.to_owned())?;
                app.reset_interrupt_count();
            }
            Err(ReadlineError::Interrupted) => {
                // At prompt, increment interrupt count
                if app.incr_inturrupt_count() >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    app.close();
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                input_buff.clear();
                continue;
            }
            Err(ReadlineError::Eof) => {
                app.close();
                break;
            }
            Err(err) => {
                app.close();
                anyhow::bail!(err)
            }
        }
    }
    rl.save_history(history_file.as_path())?;
    Ok(())
}

fn calc_parens_offset(input: &str) -> i32 {
    input.chars().fold(0, |acc, c| match c {
        '(' => acc + 1,
        ')' => acc - 1,
        _ => acc,
    })
}
