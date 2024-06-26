use std::path::Path;

fn main() {
    let header_file = Path::new("include").join("sqlite3.h");
    cbindgen::generate(".")
        .expect("Failed to generate C bindings")
        .write_to_file(header_file);
}
