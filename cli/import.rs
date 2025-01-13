use anyhow::Error;
use clap::Parser;
use limbo_core::Connection;
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    rc::Rc,
    sync::{Arc, LazyLock},
};

pub static IMPORT_HELP: LazyLock<String> = LazyLock::new(|| {
    let empty: [&'static str; 2] = [".import", "--help"];
    let opts = ImportArgs::try_parse_from(empty);
    opts.map_err(|e| e.to_string()).unwrap_err()
});

#[derive(Debug, Parser)]
#[command(name = ".import")]
pub struct ImportArgs {
    /// Use , and \n as column and row separators
    #[arg(long, default_value = "true")]
    csv: bool,
    /// "Verbose" - increase auxiliary output
    #[arg(short, default_value = "false")]
    verbose: bool,
    /// Skip the first N rows of input
    #[arg(long, default_value = "0")]
    skip: u64,
    file: PathBuf,
    table: String,
}

pub struct ImportFile<'a> {
    conn: Rc<Connection>,
    io: Arc<dyn limbo_core::IO>,
    writer: &'a mut dyn Write,
}

impl<'a> ImportFile<'a> {
    pub fn new(
        conn: Rc<Connection>,
        io: Arc<dyn limbo_core::IO>,
        writer: &'a mut dyn Write,
    ) -> Self {
        Self { conn, io, writer }
    }

    pub fn import(&mut self, args: &[&str]) -> Result<(), Error> {
        let import_args = ImportArgs::try_parse_from(args.iter());
        match import_args {
            Ok(args) => {
                self.import_csv(args);
                Ok(())
            }
            Err(err) => Err(anyhow::anyhow!(err.to_string())),
        }
    }

    pub fn import_csv(&mut self, args: ImportArgs) {
        let file = match File::open(args.file) {
            Ok(file) => file,
            Err(e) => {
                let _ = self.writer.write_all(format!("{:?}\n", e).as_bytes());
                return;
            }
        };

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(file);

        let mut success_rows = 0u64;
        let mut failed_rows = 0u64;

        for result in rdr.records().skip(args.skip as usize) {
            let record = result.unwrap();

            if !record.is_empty() {
                let mut values_string = String::new();

                for r in record.iter() {
                    values_string.push('\'');
                    // The string can have a single quote which needs to be escaped
                    values_string.push_str(&r.replace("'", "''"));
                    values_string.push_str("',");
                }

                // remove the last comma after last element
                values_string.pop();

                let insert_string =
                    format!("INSERT INTO {} VALUES ({});", args.table, values_string);

                match self.conn.query(insert_string) {
                    Ok(rows) => {
                        if let Some(mut rows) = rows {
                            while let Ok(x) = rows.next_row() {
                                match x {
                                    limbo_core::StepResult::IO => {
                                        self.io.run_once().unwrap();
                                    }
                                    limbo_core::StepResult::Done => break,
                                    limbo_core::StepResult::Interrupt => break,
                                    limbo_core::StepResult::Busy => {
                                        let _ =
                                            self.writer.write_all("database is busy\n".as_bytes());
                                        break;
                                    }
                                    limbo_core::StepResult::Row(_) => todo!(),
                                }
                            }
                        }
                        success_rows += 1;
                    }
                    Err(_err) => {
                        failed_rows += 1;
                    }
                }
            }
        }

        if args.verbose {
            let _ = self.writer.write_all(
                format!(
                    "Added {} rows with {} errors using {} lines of input\n",
                    success_rows,
                    failed_rows,
                    success_rows + failed_rows
                )
                .as_bytes(),
            );
        }
    }
}
