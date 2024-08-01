#![feature(coroutines)]
#![feature(coroutine_trait)]
#![feature(stmt_expr_attributes)]

use clap::Parser;
use hdrhistogram::Histogram;
use limbo_core::{Database, PlatformIO, IO};
use std::ops::{Coroutine, CoroutineState};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser)]
struct Opts {
    count: usize,
}

fn main() {
    env_logger::init();
    let opts = Opts::parse();
    let mut hist = Histogram::<u64>::new(2).unwrap().into_sync();
    let io = Arc::new(PlatformIO::new().unwrap());
    let mut tenants = vec![];
    for i in 0..opts.count {
        let mut recorder = hist.recorder();
        let io = io.clone();
        let tenant = #[coroutine]
        move || {
            let i_str = i.to_string();
            let database = format!("database{}.db", i_str);
            let db = Database::open_file(io.clone(), &database).unwrap();
            let conn = db.connect();
            let mut stmt = conn.prepare("SELECT * FROM user LIMIT 100").unwrap();
            for _ in 0..1000 {
                let now = Instant::now();
                let mut rows = stmt.query().unwrap();
                let mut count = 0;
                loop {
                    let row = rows.next_row().unwrap();
                    match row {
                        limbo_core::RowResult::Row(_) => {
                            count += 1;
                        }
                        limbo_core::RowResult::IO => yield,
                        limbo_core::RowResult::Done => {
                            break;
                        }
                    }
                }
                assert!(count == 100);
                recorder.record(now.elapsed().as_nanos() as u64).unwrap();
            }
        };
        tenants.push(tenant);
    }
    let mut completed = 0usize;
    while completed < opts.count {
        let len = tenants.len();
        for i in 0..tenants.len() {
            let tenant_idx = len - i - 1;
            match tenants.get_mut(tenant_idx) {
                Some(tenant) => {
                    let tenant = Pin::new(tenant);
                    match tenant.resume(()) {
                        CoroutineState::Complete(_) => {
                            let _ = tenants.remove(tenant_idx);
                            completed += 1;
                        }
                        CoroutineState::Yielded(_) => {}
                    }
                }
                None => {
                    continue;
                }
            }
        }
        if completed < opts.count {
            io.run_once().unwrap();
        }
    }
    hist.refresh();
    println!("count,p50,p90,p95,p99,p999,p9999,p99999");
    println!(
        "{},{},{},{},{},{},{},{}",
        opts.count,
        hist.value_at_quantile(0.5),
        hist.value_at_quantile(0.90),
        hist.value_at_quantile(0.95),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.value_at_quantile(0.9999),
        hist.value_at_quantile(0.99999)
    );
}
