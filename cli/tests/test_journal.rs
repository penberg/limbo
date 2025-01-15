/// rexpect does not work on Windows.
/// https://github.com/rust-cli/rexpect/issues/11
#[cfg(not(target_os = "windows"))]
mod tests {
    use assert_cmd::cargo::cargo_bin;
    use rexpect::error::*;
    use rexpect::session::spawn_command;
    use std::process;

    #[test]
    fn test_pragma_journal_mode_wal() -> Result<(), Error> {
        let mut child = spawn_command(run_cli(), Some(1000))?;
        child.exp_regex("limbo>")?; // skip everything until limbo cursor appear
        child.exp_regex(".?")?;
        child.send_line("pragma journal_mode;")?;
        child.exp_string("wal")?;
        child.send_line(".quit")?;
        child.exp_eof()?;
        Ok(())
    }

    #[ignore = "wal checkpoint not yet implemented"]
    #[test]
    fn test_pragma_wal_checkpoint() -> Result<(), Error> {
        let mut child = spawn_command(run_cli(), Some(1000))?;
        child.exp_regex("limbo>")?; // skip everything until limbo cursor appear
        child.exp_regex(".?")?;
        child.send_line("pragma wal_checkpoint;")?;
        child.exp_string("0|0|0")?;
        child.send_line(".quit")?;
        child.exp_eof()?;
        Ok(())
    }

    fn run_cli() -> process::Command {
        let bin_path = cargo_bin("limbo");
        let mut cmd = process::Command::new(bin_path);
        cmd
    }
}
