use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// Asking the OS for `:0` and closing the listener lets a concurrent test in this
// binary be handed the same port, connect to the wrong server, and see its
// requests reset when that server is dropped. Hand out a distinct port per call
// instead; `start` retries when another process holds one.
fn free_port() -> u16 {
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let base = 20_000 + (std::process::id() as u16 % 20_000);
    base + NEXT.fetch_add(1, Ordering::Relaxed) % 1_000
}

const START_ATTEMPTS: u32 = 5;
const READY_TIMEOUT: Duration = Duration::from_secs(10);

fn start(extra: &[&str], dir: &std::path::Path) -> Server {
    let mut last_failure = String::new();

    for attempt in 1..=START_ATTEMPTS {
        let port = free_port();
        let mut args = vec!["--sync-server".to_string(), format!("127.0.0.1:{port}")];
        args.extend(extra.iter().map(|s| s.to_string()));

        // IMPORTANT: Stdio::null(), never piped(). Nothing drains the pipes, so a full
        // buffer would block the child inside write() and stall HTTP handling.
        let mut child = Command::new(env!("CARGO_BIN_EXE_tursodb"))
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn tursodb");

        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Server {
                    child,
                    port,
                    dir: dir.to_path_buf(),
                };
            }
            if let Some(status) = child.try_wait().expect("poll child status") {
                last_failure = format!(
                    "attempt {attempt}/{START_ATTEMPTS}: tursodb exited before serving on port {port}: {status}"
                );
                break;
            }
            if Instant::now() >= deadline {
                last_failure = format!(
                    "attempt {attempt}/{START_ATTEMPTS}: tursodb did not start serving on port {port} within {READY_TIMEOUT:?}"
                );
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    panic!("tursodb failed to start after {START_ATTEMPTS} attempts: {last_failure}");
}

/// Returns (status_code, body) with the headers stripped, so a substring
/// assertion cannot be satisfied by a header value such as `Content-Length`.
fn post_bytes(port: u16, path: &str, content_type: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let status = String::from_utf8_lossy(&raw[..raw.len().min(64)])
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let header_end = find(&raw, b"\r\n\r\n")
        .unwrap_or_else(|| panic!("response has no header terminator: {raw:?}"));
    (status, raw[header_end + 4..].to_vec())
}

fn post(port: u16, path: &str, json: &str) -> (u16, String) {
    let (status, body) = post_bytes(port, path, "application/json", json.as_bytes());
    (status, String::from_utf8_lossy(&body).into_owned())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn sql(stmt: &str) -> String {
    format!(r#"{{"requests":[{{"type":"execute","stmt":{{"sql":"{stmt}"}}}}]}}"#)
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("turso-multi-sync-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn databases_are_isolated() {
    let dir = tmpdir("iso");
    let srv = start(&["--sync-dir", dir.to_str().unwrap()], &dir);

    assert_eq!(
        post(srv.port, "/db/db1/v2/pipeline", &sql("CREATE TABLE t1(x)")).0,
        200
    );
    assert_eq!(
        post(srv.port, "/db/db2/v2/pipeline", &sql("CREATE TABLE t2(x)")).0,
        200
    );

    // `handle_pipeline` answers 200 even when a statement fails, so read each
    // table back before checking that neither database sees the other's.
    let (_, own1) = post(srv.port, "/db/db1/v2/pipeline", &sql("SELECT * FROM t1"));
    assert!(
        !own1.contains("error"),
        "db1 must see its own table: {own1}"
    );
    let (_, own2) = post(srv.port, "/db/db2/v2/pipeline", &sql("SELECT * FROM t2"));
    assert!(
        !own2.contains("error"),
        "db2 must see its own table: {own2}"
    );

    let (_, a) = post(srv.port, "/db/db1/v2/pipeline", &sql("SELECT * FROM t2"));
    assert!(a.contains("error"), "db1 must not see db2's table: {a}");
    let (_, b) = post(srv.port, "/db/db2/v2/pipeline", &sql("SELECT * FROM t1"));
    assert!(b.contains("error"), "db2 must not see db1's table: {b}");

    assert_eq!(
        post(
            srv.port,
            "/db/db1/v2/pipeline",
            &sql("INSERT INTO t1(x) VALUES (111)")
        )
        .0,
        200
    );
    assert_eq!(
        post(
            srv.port,
            "/db/db2/v2/pipeline",
            &sql("INSERT INTO t2(x) VALUES (222)")
        )
        .0,
        200
    );
    let (_, read1) = post(srv.port, "/db/db1/v2/pipeline", &sql("SELECT x FROM t1"));
    assert!(
        read1.contains("111"),
        "db1 must read back its value: {read1}"
    );
    assert!(
        !read1.contains("222"),
        "db1 must not see db2's value: {read1}"
    );
    let (_, read2) = post(srv.port, "/db/db2/v2/pipeline", &sql("SELECT x FROM t2"));
    assert!(
        read2.contains("222"),
        "db2 must read back its value: {read2}"
    );
    assert!(
        !read2.contains("111"),
        "db2 must not see db1's value: {read2}"
    );

    assert!(dir.join("db1").join("data").exists());
    assert!(dir.join("db2").join("data").exists());
}

#[test]
fn rejects_unsafe_database_names() {
    let dir = tmpdir("bad");
    let srv = start(&["--sync-dir", dir.to_str().unwrap()], &dir);

    // The 404s never reach `validate_db_name`: their tails match no known route.
    for (path, expected_status) in [
        ("/db/../x/v2/pipeline", 404),
        ("/db/a/b/v2/pipeline", 404),
        ("/db//v2/pipeline", 400),
        ("/db/.hidden/v2/pipeline", 400),
        ("/db/../v2/pipeline", 400),
        ("/db/nul/v2/pipeline", 400),
        ("/db/COM1/v2/pipeline", 400),
    ] {
        let (status, _) = post(srv.port, path, &sql("SELECT 1"));
        assert_eq!(status, expected_status, "unexpected status for {path}");
    }
    let leaked: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(
        leaked.is_empty(),
        "rejected names must not create files: {leaked:?}"
    );
}

#[test]
fn single_database_mode_rejects_db_routes() {
    let dir = tmpdir("single");
    let file = dir.join("one.db");
    let srv = start(&[file.to_str().unwrap()], &dir);

    assert_eq!(
        post(srv.port, "/v2/pipeline", &sql("CREATE TABLE t(x)")).0,
        200
    );
    assert_eq!(
        post(srv.port, "/db/one/v2/pipeline", &sql("SELECT 1")).0,
        404
    );
}

#[test]
fn sync_max_databases_caps_open_handles() {
    let dir = tmpdir("cap");
    let srv = start(
        &[
            "--sync-dir",
            dir.to_str().unwrap(),
            "--sync-max-databases",
            "2",
        ],
        &dir,
    );

    assert_eq!(
        post(srv.port, "/db/a/v2/pipeline", &sql("CREATE TABLE t(x)")).0,
        200
    );
    assert_eq!(
        post(srv.port, "/db/b/v2/pipeline", &sql("CREATE TABLE t(x)")).0,
        200
    );
    assert_eq!(
        post(srv.port, "/db/c/v2/pipeline", &sql("CREATE TABLE t(x)")).0,
        503
    );
    assert_eq!(post(srv.port, "/db/a/v2/pipeline", &sql("SELECT 1")).0, 200);
    assert!(
        !dir.join("c").exists(),
        "a refused database must not reach the filesystem"
    );
}

#[test]
fn pull_updates_are_scoped_to_one_database() {
    let dir = tmpdir("pull");
    let srv = start(&["--sync-dir", dir.to_str().unwrap()], &dir);

    for (db, table, marker) in [("db1", "t1", "marker-one"), ("db2", "t2", "marker-two")] {
        let route = format!("/db/{db}/v2/pipeline");
        assert_eq!(
            post(srv.port, &route, &sql(&format!("CREATE TABLE {table}(x)"))).0,
            200
        );
        assert_eq!(
            post(
                srv.port,
                &route,
                &sql(&format!("INSERT INTO {table}(x) VALUES ('{marker}')"))
            )
            .0,
            200
        );
    }

    // A default-valued request pulls every page frame, so each response carries
    // the frames its own database wrote and nothing from the other.
    for (db, own, other) in [
        ("db1", "marker-one", "marker-two"),
        ("db2", "marker-two", "marker-one"),
    ] {
        let (status, frames) = post_bytes(
            srv.port,
            &format!("/db/{db}/pull-updates"),
            "application/protobuf",
            &[],
        );
        assert_eq!(status, 200, "pull-updates for {db}");
        assert!(
            find(&frames, own.as_bytes()).is_some(),
            "{db} must pull its own frames ({} bytes)",
            frames.len()
        );
        assert!(
            find(&frames, other.as_bytes()).is_none(),
            "{db} must not pull the other database's frames"
        );
    }
}

#[test]
fn sync_dir_requires_sync_server() {
    let dir = tmpdir("no-server");

    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .args(["--sync-dir", dir.to_str().unwrap()])
        .output()
        .expect("spawn tursodb");

    assert!(
        !output.status.success(),
        "expected nonzero exit, got {:?}",
        output.status
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--sync-server"),
        "unexpected stderr: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn sync_dir_rejects_positional_database() {
    let dir = tmpdir("conflict");
    let port = free_port();

    let output = Command::new(env!("CARGO_BIN_EXE_tursodb"))
        .args([
            "--sync-server",
            &format!("127.0.0.1:{port}"),
            "--sync-dir",
            dir.to_str().unwrap(),
            dir.join("explicit.db").to_str().unwrap(),
        ])
        .output()
        .expect("spawn tursodb");

    assert!(
        !output.status.success(),
        "expected nonzero exit, got {:?}",
        output.status
    );
    assert_eq!(output.status.code(), Some(2), "expected clap's usage exit");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("the argument '--sync-dir <SYNC_DIR>' cannot be used with '[DATABASE]'"),
        "unexpected stderr: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
