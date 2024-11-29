import test from "ava";

test.beforeEach(async (t) => {
    const [db, errorType, provider] = await connect();
    db.exec(`
        DROP TABLE IF EXISTS users;
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
    `);
    db.exec(
      "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.org')"
    );
    db.exec(
      "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')"
    );
    t.context = {
      db,
      errorType,
      provider
    };
  });

test.serial("Statement.raw().all()", async (t) => {
    const db = t.context.db;

    const stmt = db.prepare("SELECT * FROM users");
    const expected = [
        [1, "Alice", "alice@example.org"],
        [2, "Bob", "bob@example.com"],
    ];
    t.deepEqual(stmt.raw().all(), expected);
});

test.serial("Statement.raw().get()", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [
      1, "Alice", "alice@example.org"
  ];
  t.deepEqual(stmt.raw().get(), expected);

  const emptyStmt = db.prepare("SELECT * FROM users WHERE id = -1");
  t.is(emptyStmt.raw().get(), undefined);
});

test.serial("Statement.raw().iterate()", async (t) => {
  const db = t.context.db;

  const stmt = db.prepare("SELECT * FROM users");
  const expected = [
    { done: false, value: [1, "Alice", "alice@example.org"] },
    { done: false, value: [2, "Bob", "bob@example.com"] },
    { done: true, value: undefined },
  ];

  let iter = stmt.raw().iterate();
  t.is(typeof iter[Symbol.iterator], 'function');
  t.deepEqual(iter.next(), expected[0])
  t.deepEqual(iter.next(), expected[1])
  t.deepEqual(iter.next(), expected[2])

  const emptyStmt = db.prepare("SELECT * FROM users WHERE id = -1");
  t.is(typeof emptyStmt[Symbol.iterator], 'undefined');
  t.throws(() => emptyStmt.next(), { instanceOf: TypeError });
});

const connect = async (path_opt) => {
    const path = path_opt ?? "hello.db";
    const provider = process.env.PROVIDER;
    if (provider === "limbo-wasm") {
      const database = process.env.LIBSQL_DATABASE ?? path;
      const x = await import("limbo-wasm");
      const options = {};
      const db = new x.Database(database, options);
      return [db, x.SqliteError, provider];
    }
    if (provider == "better-sqlite3") {
      const x = await import("better-sqlite3");
      const options = {};
      const db = x.default(path, options);
      return [db, x.SqliteError, provider];
    }
    throw new Error("Unknown provider: " + provider);
  };