pub fn normalize_ident(ident: &str) -> String {
    if ident.starts_with('"') && ident.ends_with('"') {
        ident[1..ident.len() - 1].to_string().to_lowercase()
    } else {
        ident.to_lowercase()
    }
}
