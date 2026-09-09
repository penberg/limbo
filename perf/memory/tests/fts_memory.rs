#![cfg(feature = "fts")]

use serde_json::Value;
use std::process::Command;

#[test]
fn cli_profiles_only_queries_and_reports_each_completed_query() {
    let directory = tempfile::tempdir().unwrap();
    for (state, queries) in [("first", 1), ("warm", 3)] {
        let path = directory.path().join(format!("{state}.json"));
        let output = Command::new(env!("CARGO_BIN_EXE_fts-memory"))
            .args([
                "--query",
                "common",
                "--state",
                state,
                "--documents",
                "103",
                "--queries",
                &queries.to_string(),
                "--dhat-file",
            ])
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_phase_events(&report, &output.stderr);
        assert_eq!(report["queries"], queries);
        assert_eq!(report["rows_per_query"], 103);
        let index = &report["index"];
        assert_eq!(index["segments"], 1);
        assert_eq!(index["segment_bytes"], index["largest_segment_bytes"]);
        assert!(index["segment_bytes"].as_u64().unwrap() > 0);
        assert_eq!(index["page_size"], 4096);
        let total = report["total_allocated_bytes"].as_u64().unwrap();
        let peak = report["peak_live_query_bytes"].as_u64().unwrap();
        let retained = report["retained_query_bytes"].as_u64().unwrap();
        assert!(total >= peak && peak >= retained && peak > 0);
        assert_eq!(
            report["allocated_bytes_per_query"].as_f64().unwrap(),
            total as f64 / queries as f64
        );
        let samples = report["query_heap"].as_array().unwrap();
        assert_eq!(samples.len(), queries as usize);
        for (i, sample) in samples.iter().enumerate() {
            assert_eq!(sample["completed_queries"], i + 1);
        }
        assert_eq!(samples.last().unwrap()["live_query_bytes"], retained);
        let profile: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let frames = profile["ftbl"].as_array().unwrap();
        assert!(
            frames
                .iter()
                .any(|frame| frame.as_str().unwrap().contains("QuerySession::query"))
        );
        assert!(
            !frames
                .iter()
                .any(|frame| frame.as_str().unwrap().contains("FtsFixture::"))
        );
        assert!(
            !frames
                .iter()
                .any(|frame| frame.as_str().unwrap().contains("FtsWorkload::prepare"))
        );
    }
}

#[test]
fn cli_measures_begin_and_commit_across_overlapping_mvcc_transactions() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("transactions.json");
    let output = Command::new(env!("CARGO_BIN_EXE_fts-memory"))
        .args([
            "--mode",
            "mvcc",
            "--connections",
            "2",
            "--transactions-per-connection",
            "3",
            "--queries-per-transaction",
            "2",
            "--query",
            "and",
            "--documents",
            "103",
            "--dhat-file",
        ])
        .arg(&path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_phase_events(&report, &output.stderr);
    assert_eq!(report["mode"], "mvcc");
    assert_eq!(report["queries"], 12);
    assert_eq!(report["transactions"], 6);
    assert_eq!(report["rows_per_query"], 18);
    assert_eq!(report["max_active_transactions"], 2);
    let samples = report["query_heap"].as_array().unwrap();
    assert_eq!(samples.len(), 3);
    for (i, sample) in samples.iter().enumerate() {
        assert_eq!(sample["completed_queries"], (i + 1) * 4);
        assert_eq!(sample["completed_transactions"], (i + 1) * 2);
    }
    let profile: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let frames = profile["ftbl"].as_array().unwrap();
    for function in [
        "QuerySession::begin",
        "QuerySession::query",
        "QuerySession::commit",
    ] {
        assert!(
            frames
                .iter()
                .any(|frame| frame.as_str().unwrap().contains(function)),
            "missing measured {function}"
        );
    }
    assert!(!frames.iter().any(|frame| {
        let frame = frame.as_str().unwrap();
        frame.contains("FtsFixture::")
            || frame.contains("FtsWorkload::prepare")
            || frame.contains("FtsWorkload::finish")
    }));
}

#[test]
fn undersized_index_is_rejected_before_profiling() {
    let directory = tempfile::tempdir().unwrap();
    let profile = directory.path().join("heap.json");
    let output = Command::new(env!("CARGO_BIN_EXE_fts-memory"))
        .args([
            "--documents",
            "5",
            "--min-index-bytes",
            "268435456",
            "--dhat-file",
        ])
        .arg(&profile)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!profile.exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("below requested minimum"), "{stderr}");
    assert!(!stderr.contains("\"phase\":\"run\""));
}

#[test]
fn cli_rejects_empty_workloads_and_repeated_first_queries() {
    for args in [
        vec!["--cache-pages", "199"],
        vec!["--queries", "0"],
        vec!["--documents", "0"],
        vec!["--state", "first", "--queries", "2"],
        vec!["--connections", "0"],
        vec!["--transactions-per-connection", "0"],
        vec![
            "--transactions-per-connection",
            "2",
            "--queries-per-transaction",
            "0",
        ],
        vec!["--queries-per-transaction", "2"],
        vec!["--queries", "2", "--transactions-per-connection", "2"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_fts-memory"))
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
    }
}

fn assert_phase_events(report: &Value, stderr: &[u8]) {
    let events: Vec<Value> = String::from_utf8_lossy(stderr)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["event"] == "phase")
        .collect();
    assert_eq!(events, *report["phases"].as_array().unwrap());
    let phases: Vec<&str> = events
        .iter()
        .map(|event| event["phase"].as_str().unwrap())
        .collect();
    assert_eq!(
        phases,
        ["setup", "open", "warmup", "run", "cleanup", "done"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0]["elapsed_ms"].as_u64().unwrap()
                <= pair[1]["elapsed_ms"].as_u64().unwrap())
    );
}
