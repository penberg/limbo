pub fn normalize_ident(mut ident: &str) -> String {
    if ident.starts_with('"') && ident.ends_with('"') {
        ident = &ident[1..ident.len() - 1];
    }
    ident.to_lowercase()
}
