use clap::Parser;
use hdrhistogram::Histogram;
use rusqlite::{Connection, params};
use std::thread;
use std::time::Instant;

#[derive(Parser)]
struct Opts {
    count: usize,
}

fn main() {
    let opts = Opts::parse();
    let mut hist = Histogram::<u64>::new(2).unwrap().into_sync();
    let mut tenants = vec![];
    for i in 0..opts.count {
        let mut recorder = hist.recorder();
        let database = format!("database{}.db", i);
        let t = thread::spawn(move || {
            let conn = Connection::open(&database).unwrap();
            let mut stmt = conn.prepare("SELECT * FROM user LIMIT 100").unwrap();
            for _ in 0..1000 {
                let now = Instant::now();
                let mut rows = stmt.query(params![]).unwrap();
                let mut count = 0;
                loop {
                    let row = rows.next().unwrap();
                    if row.is_none() {
                        break;
                    }
                    count += 1;
                }
                assert!(count == 100);
                recorder.record(now.elapsed().as_nanos() as u64).unwrap();
            }
        });
        tenants.push(t);
    }
    for t in tenants {
        t.join().unwrap();
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
