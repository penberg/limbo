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

test.serial("Statement.raw().iterate() is lazy evaluated", async (t) => {
    const db = t.context.db;

    db.exec("DROP TABLE IF EXISTS large_table");
    db.exec("CREATE TABLE large_table (id INTEGER PRIMARY KEY)");

    const stmt = db.prepare("INSERT INTO large_table (id) VALUES (?)");
    for (let i = 1; i <= 1000; i++) {
        stmt.run(i);
    }

    db.exec("ALTER TABLE large_table ADD COLUMN accessed INTEGER DEFAULT 0");

    // Create a trigger that sets accessed = 1 when a row is read
    db.exec(`
        CREATE TRIGGER track_access 
        BEFORE SELECT ON large_table
        BEGIN
            UPDATE large_table 
            SET accessed = 1 
            WHERE id = (SELECT id FROM large_table WHERE id = NEW.id);
        END
    `);

    const selectStmt = db.prepare("SELECT * FROM large_table ORDER BY id");
    const iter = selectStmt.raw().iterate();

    // Check that no rows have been accessed yet
    const initialAccess = db.prepare("SELECT COUNT(*) FROM large_table WHERE accessed = 1").get()[0];
    t.is(initialAccess, 0, "No rows should be accessed before iteration starts");

    // Get first 5 rows
    for (let i = 0; i < 5; i++) {
        iter.next();
    }

    // Verify only 5 rows were accessed
    const partialAccess = db.prepare("SELECT COUNT(*) FROM large_table WHERE accessed = 1").get()[0];
    t.is(partialAccess, 5, "Only requested rows should be accessed");

    // Verify specific rows were accessed
    const accessedRows = db.prepare("SELECT id FROM large_table WHERE accessed = 1 ORDER BY id").all();
    t.deepEqual(
        accessedRows.map(row => row[0]),
        [1, 2, 3, 4, 5],
        "First 5 rows should be accessed in order"
    );

    // Get next 3 rows
    for (let i = 0; i < 3; i++) {
        iter.next();
    }

    // Verify only 8 total rows were accessed
    const finalAccess = db.prepare("SELECT COUNT(*) FROM large_table WHERE accessed = 1").get()[0];
    t.is(finalAccess, 8, "Only requested rows should be accessed after additional iteration");
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