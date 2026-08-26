//! SQL Plan to Rust Integration Test Generator
//!
//! Usage: plan_to_test <path-to-plan.sql> > test_name.rs
//!
//! Parses SQL plan files from the simulator and generates a Rust integration test file.
//! Lines ending with "-- <number>" indicate which connection should execute that statement.
//! Lines beginning with "--" are comments and are ignored (except FAULT commands).

use anyhow::{Result, bail};
use regex::Regex;
use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!(
            "Usage: {} <path-to-plan.sql> > output_test_name.rs",
            args[0]
        );
        std::process::exit(1);
    }

    let sql_path = &args[1];
    let content = fs::read_to_string(sql_path).unwrap_or_else(|e| {
        eprintln!("Failed to read SQL file: {e}");
        std::process::exit(1);
    });

    let test_code = generate_test(&content, sql_path)?;
    println!("{test_code}");
    Ok(())
}

#[derive(Debug)]
struct SqlStatement {
    sql: String,
    connection: u32,
}

#[derive(Debug)]
enum PlanAction {
    Sql(SqlStatement),
    ReopenDatabase,
    Disconnect { connection: u32 },
}

fn parse_plan(content: &str) -> Result<(Vec<PlanAction>, BTreeSet<u32>)> {
    let mut actions = Vec::new();
    let mut connections = BTreeSet::new();

    // Regex to match "-- <number>" at end of line
    let conn_suffix_re = Regex::new(r";\s*--\s*(\d+)\s*$").unwrap();
    // Regex to match FAULT lines
    let fault_re = Regex::new(r"^--\s*FAULT\s+'([^']+)';\s*--\s*(\d+)").unwrap();

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip empty lines
        if trimmed.is_empty() {
            continue;
        }

        // Check for FAULT commands first
        if let Some(caps) = fault_re.captures(trimmed) {
            let fault_type = caps.get(1).unwrap().as_str();
            let conn_num: u32 = caps.get(2).unwrap().as_str().parse().unwrap();
            connections.insert(conn_num);

            match fault_type {
                "REOPEN_DATABASE" => {
                    actions.push(PlanAction::ReopenDatabase);
                }
                "DISCONNECT" => {
                    actions.push(PlanAction::Disconnect {
                        connection: conn_num,
                    });
                }
                "POWER_LOSS" => bail!(
                    "POWER_LOSS cannot be converted to a TempDatabase test; reproduce it with MemorySimIO or UnreliableIo"
                ),
                _ => bail!("unsupported simulator fault: {fault_type}"),
            }
            continue;
        }

        if trimmed.starts_with("-- FSYNC QUERY") {
            bail!(
                "FSYNC QUERY cannot be converted to a TempDatabase test; reproduce it with MemorySimIO or UnreliableIo"
            );
        }

        // Skip lines that begin with "--" (pure comments)
        if trimmed.starts_with("--") {
            continue;
        }

        // Try to find connection suffix
        if let Some(caps) = conn_suffix_re.captures(trimmed) {
            let conn_num: u32 = caps.get(1).unwrap().as_str().parse().unwrap();
            connections.insert(conn_num);

            // Extract SQL without the comment suffix
            let sql_end = caps.get(0).unwrap().start();
            let sql = trimmed[..sql_end + 1].trim().to_string(); // +1 to include the semicolon

            // Skip lines marked as FAULTY QUERY
            if trimmed.contains("FAULTY QUERY") {
                continue;
            }

            actions.push(PlanAction::Sql(SqlStatement {
                sql,
                connection: conn_num,
            }));
        }
    }

    Ok((actions, connections))
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn generate_test(content: &str, sql_path: &str) -> Result<String> {
    let (actions, connections) = parse_plan(content)?;

    let path = Path::new(sql_path);
    let test_name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("generated_test");

    // Derive a test name from the path if it contains bugbase ID
    let test_fn_name = if sql_path.contains(".bugbase/") {
        let parts: Vec<&str> = sql_path.split('/').collect();
        if let Some(pos) = parts.iter().position(|&p| p == ".bugbase") {
            if pos + 1 < parts.len() {
                format!("test_bugbase_{}", parts[pos + 1])
            } else {
                format!("test_{test_name}")
            }
        } else {
            format!("test_{test_name}")
        }
    } else {
        format!("test_{test_name}")
    };

    let num_connections = connections.iter().max().map(|&m| m + 1).unwrap_or(1);

    let mut code = String::new();

    // Imports
    code.push_str("use std::sync::Arc;\n\n");
    code.push_str("use turso_core::Connection;\n\n");
    code.push_str("use crate::common::TempDatabase;\n\n");

    // Test function
    code.push_str("#[turso_macros::test]\n");
    code.push_str(&format!("fn {test_fn_name}(tmp_db: TempDatabase) {{\n"));

    // Create connections
    code.push_str("    let mut connections: Vec<Arc<Connection>> = Vec::new();\n");
    code.push_str(&format!("    for _ in 0..{num_connections} {{\n"));
    code.push_str("        connections.push(tmp_db.connect_limbo());\n");
    code.push_str("    }\n\n");

    // Execute each statement
    for action in &actions {
        match action {
            PlanAction::Sql(stmt) => {
                let escaped_sql = escape_sql_string(&stmt.sql);
                let connection = stmt.connection;
                code.push_str(&format!("    // Connection {connection}\n"));
                code.push_str(&format!(
                    "    let _ = connections[{connection}].execute(\"{escaped_sql}\");\n\n"
                ));
            }
            PlanAction::ReopenDatabase => {
                code.push_str("    // REOPEN_DATABASE - reopen database for all connections\n");
                code.push_str("    drop(connections);\n");
                code.push_str("    let tmp_db = TempDatabase::new_with_existent(&tmp_db.path);\n");
                code.push_str("    let mut connections: Vec<Arc<Connection>> = Vec::new();\n");
                code.push_str(&format!("    for _ in 0..{num_connections} {{\n"));
                code.push_str("        connections.push(tmp_db.connect_limbo());\n");
                code.push_str("    }\n\n");
            }
            PlanAction::Disconnect { connection } => {
                code.push_str(&format!("    // DISCONNECT connection {connection}\n"));
                code.push_str(&format!(
                    "    drop(std::mem::replace(&mut connections[{connection}], tmp_db.connect_limbo()));\n\n"
                ));
            }
        }
    }

    code.push_str("}\n");

    Ok(code)
}
