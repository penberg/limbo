use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let built_file = out_dir.join("built.rs");

    built::write_built_file().expect("Failed to acquire build-time information");

    // So that we don't have to transform at runtime
    let sqlite_date = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    fs::write(
        &built_file,
        format!(
            "{}\npub const BUILT_TIME_SQLITE: &str = \"{}\";\n",
            fs::read_to_string(&built_file).unwrap(),
            sqlite_date
        ),
    )
    .expect("Failed to append to built file");
}
