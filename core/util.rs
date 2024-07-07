pub fn normalize_ident(ident: &str) -> String {
    (if ident.starts_with('"') && ident.ends_with('"') {
        &ident[1..ident.len() - 1]
    } else {
        ident
    }).to_lowercase()
}
