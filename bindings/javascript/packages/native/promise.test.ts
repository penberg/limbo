import { unlinkSync } from "node:fs";
import { expect, test } from 'vitest'
import { Database, connect, Transaction } from './promise.js'
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/better-sqlite3';

test('drizzle-orm', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const conn = await connect(path);
        const db = drizzle(conn);
        await db.run('CREATE TABLE t(x, y)');
        let tasks = [];
        for (let i = 0; i < 1234; i++) {
            tasks.push(db.run(sql`INSERT INTO t VALUES (${i}, randomblob(${i} * 5))`))
        }
        await Promise.all(tasks);
        expect(await db.all("SELECT COUNT(*) as cnt FROM t")).toEqual([{ cnt: 1234 }])
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('in-memory-db-async', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    const stmt = await db.prepare("SELECT * FROM t WHERE x % 2 = ?");
    const rows = await stmt.all([1]);
    expect(rows).toEqual([{ x: 1 }, { x: 3 }]);
})

test('busy timeout gives up on time when the write lock is never released', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const conn1 = await connect(path);
        await conn1.exec("CREATE TABLE t(x)");
        const conn2 = await connect(path, { timeout: 200 });

        await conn1.exec("BEGIN");
        await conn1.exec("INSERT INTO t VALUES (1)");

        // conn1 never commits, so conn2 must retry for the whole 200ms budget
        // and then surface the busy error — not fail fast, not hang. The
        // backoff sleeps add up to exactly 200ms (measured ~205ms with timer
        // slop); the upper bound only leaves room for slow CI timers.
        const start = performance.now();
        await expect(conn2.exec("INSERT INTO t VALUES (2)")).rejects.toThrow(/locked/);
        const elapsed = performance.now() - start;
        expect(elapsed).toBeGreaterThanOrEqual(190);
        expect(elapsed).toBeLessThan(500);

        await conn1.exec("ROLLBACK");
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('busy timeout sleeps instead of blocking the event loop', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const conn1 = await connect(path);
        await conn1.exec("CREATE TABLE t(x)");
        const conn2 = await connect(path, { timeout: 5000 });

        await conn1.exec("BEGIN");
        await conn1.exec("INSERT INTO t VALUES (1)");

        // conn2 hits conn1's write lock. Its step loop must park on a timer
        // (STEP_SLEEP) and yield the event loop, or the timer below never
        // fires, the lock is never released, and this test times out.
        const blocked = conn2.exec("INSERT INTO t VALUES (2)");
        await new Promise((resolve) => setTimeout(resolve, 100));
        await conn1.exec("COMMIT");
        await blocked;

        const stmt = await conn1.prepare("SELECT COUNT(*) as cnt FROM t");
        expect(await stmt.all()).toEqual([{ cnt: 2 }]);
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('exec multiple statements', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t(x); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)");
    const stmt = await db.prepare("SELECT * FROM t");
    const rows = await stmt.all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }]);
})

test('expanded rows collapse duplicate column names like better-sqlite3', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE role(path TEXT); CREATE TABLE org_unit(path TEXT)");
    await db.exec("INSERT INTO role VALUES ('/Employee'); INSERT INTO org_unit VALUES ('/')");

    const [row] = await db.all("SELECT role.path, org_unit.path FROM role JOIN org_unit");

    expect(Object.keys(row)).toEqual(["path"]);
    expect(row.path).toBe("/");
    expect(row[0]).toBe(undefined);
    expect(row[1]).toBe(undefined);
    expect(row).toEqual({ path: "/" });

    const stmt = await db.prepare("SELECT role.path, org_unit.path FROM role JOIN org_unit");
    expect(await stmt.raw(true).get()).toEqual(["/Employee", "/"]);
})

test('readonly-db', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const rw = await connect(path);
            await rw.exec("CREATE TABLE t(x)");
            await rw.exec("INSERT INTO t VALUES (1)");
            rw.close();
        }
        {
            const ro = await connect(path, { readonly: true });
            await expect(async () => await ro.exec("INSERT INTO t VALUES (2)")).rejects.toThrowError(/attempt to write a readonly database/g);
            expect(await (await ro.prepare("SELECT * FROM t")).all()).toEqual([{ x: 1 }])
            ro.close();
        }
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('file-must-exist', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    await expect(async () => await connect(path, { fileMustExist: true })).rejects.toThrowError(/failed to open database/);
})

test('implicit connect', async () => {
    const db = new Database(':memory:');
    const defer = await db.prepare("SELECT * FROM t");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: t/);
    await expect(async () => await db.prepare("SELECT * FROM t")).rejects.toThrowError(/no such table: t/);
    expect(await (await db.prepare("SELECT 1 as x")).all()).toEqual([{ x: 1 }]);
})

test('zero-limit-bug', async () => {
    const db = await connect(':memory:');
    const create = await db.prepare(`CREATE TABLE users (name TEXT NOT NULL);`);
    await create.run();

    const insert = await db.prepare(
        `insert into "users" values (?), (?), (?);`,
    );
    await insert.run('John', 'Jane', 'Jack');

    const stmt1 = await db.prepare(`select * from "users" limit ?;`);
    expect(await stmt1.all(0)).toEqual([]);
    let rows = [{ name: 'John' }, { name: 'Jane' }, { name: 'Jack' }, { name: 'John' }, { name: 'Jane' }, { name: 'Jack' }];
    for (const limit of [0, 1, 2, 3, 4, 5, 6, 7]) {
        const stmt2 = await db.prepare(`select * from "users" union all select * from "users" limit ?;`);
        expect(await stmt2.all(limit)).toEqual(rows.slice(0, Math.min(limit, 6)));
    }
})

test('avg-bug', async () => {
    const db = await connect(':memory:');
    const create = await db.prepare(`create table "aggregate_table" (
        "id" integer primary key autoincrement not null,
        "name" text not null,
        "a" integer,
        "b" integer,
        "c" integer,
        "null_only" integer
    );`);

    await create.run();
    const insert = await db.prepare(
        `insert into "aggregate_table" ("id", "name", "a", "b", "c", "null_only") values (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null), (null, ?, ?, ?, ?, null);`,
    );

    await insert.run(
        'value 1', 5, 10, 20,
        'value 1', 5, 20, 30,
        'value 2', 10, 50, 60,
        'value 3', 20, 20, null,
        'value 4', null, 90, 120,
        'value 5', 80, 10, null,
        'value 6', null, null, 150,
    );

    expect(await (await db.prepare(`select avg("a") from "aggregate_table";`)).get()).toEqual({ 'avg("a")': 24 });
    expect(await (await db.prepare(`select avg("null_only") from "aggregate_table";`)).get()).toEqual({ 'avg("null_only")': null });
    expect(await (await db.prepare(`select avg(distinct "b") from "aggregate_table";`)).get()).toEqual({ 'avg(distinct "b")': 42.5 });
})

test('insert returning test', async () => {
    const db = await connect(':memory:');
    await (await db.prepare(`create table t (x);`)).run();
    const x1 = await (await db.prepare(`insert into t values (1), (2) returning x`)).get();
    const x2 = await (await db.prepare(`insert into t values (3), (4) returning x`)).get();
    expect(x1).toEqual({ x: 1 });
    expect(x2).toEqual({ x: 3 });
    const all = await (await db.prepare(`select * from t`)).all();
    expect(all).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }])
})

test('offset-bug', async () => {
    const db = await connect(":memory:");
    await db.exec(`CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        verified integer not null default 0
    );`);
    const insert = await db.prepare(`INSERT INTO users (name) VALUES (?),(?);`);
    await insert.run('John', 'John1');

    const stmt = await db.prepare(`SELECT * FROM users LIMIT ? OFFSET ?;`);
    expect(await stmt.all(1, 1)).toEqual([{ id: 2, name: 'John1', verified: 0 }])
})

test('conflict-bug', async () => {
    const db = await connect(':memory:');

    const create = await db.prepare(`create table "conflict_chain_example" (
        id integer not null unique,
        name text not null,
        email text not null,
        primary key (id, name)
    )`);
    await create.run();

    await (await db.prepare(`insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?)`)).run(
        1,
        'John',
        'john@example.com',
        2,
        'John Second',
        '2john@example.com',
    );

    const insert = await db.prepare(
        `insert into "conflict_chain_example" ("id", "name", "email") values (?, ?, ?), (?, ?, ?) on conflict ("conflict_chain_example"."id", "conflict_chain_example"."name") do update set "email" = ? on conflict ("conflict_chain_example"."id") do nothing`,
    );
    await insert.run(1, 'John', 'john@example.com', 2, 'Anthony', 'idthief@example.com', 'john1@example.com');

    expect(await (await db.prepare("SELECT * FROM conflict_chain_example")).all()).toEqual([
        { id: 1, name: 'John', email: 'john1@example.com' },
        { id: 2, name: 'John Second', email: '2john@example.com' }
    ]);
})

test('on-disk db', async () => {
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect(path);
        await db1.exec("CREATE TABLE t(x)");
        await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const stmt1 = await db1.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt1.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows1 = await stmt1.all([1]);
        expect(rows1).toEqual([{ x: 1 }, { x: 3 }]);
        db1.close();

        const db2 = await connect(path);
        const stmt2 = await db2.prepare("SELECT * FROM t WHERE x % 2 = ?");
        expect(stmt2.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows2 = await stmt2.all([1]);
        expect(rows2).toEqual([{ x: 1 }, { x: 3 }]);
        db2.close();
    } finally {
        unlinkSync(path);
        unlinkSync(`${path}-wal`);
    }
})

test('attach', async () => {
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect(path1, { experimental: ["attach"] });
        await db1.exec("CREATE TABLE t(x)");
        await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
        const db2 = await connect(path2, { experimental: ["attach"] });
        await db2.exec("CREATE TABLE q(x)");
        await db2.exec("INSERT INTO q VALUES (4), (5), (6)");

        await db1.exec(`ATTACH '${path2}' as secondary`);

        const stmt = await db1.prepare("SELECT * FROM t UNION ALL SELECT * FROM secondary.q");
        expect(stmt.columns()).toEqual([{ name: "x", column: null, database: null, table: null, type: null }]);
        const rows = await stmt.all([1]);
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }]);
    } finally {
        unlinkSync(path1);
        unlinkSync(`${path1}-wal`);
        unlinkSync(path2);
        unlinkSync(`${path2}-wal`);
    }
})

test('fts', async () => {
    const db = await connect(":memory:", { experimental: ["index_method"] });
    await db.exec(`
        CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, body TEXT);
        INSERT INTO documents VALUES (1, 'Introduction to Rust', 'Rust is a systems programming language focused on safety and performance');
        INSERT INTO documents VALUES (2, 'JavaScript Guide', 'JavaScript is a dynamic programming language used for web development');
        INSERT INTO documents VALUES (3, 'Database Internals', 'Understanding how databases store and retrieve data efficiently');
        CREATE INDEX documents_fts ON documents USING fts (title, body);
    `);

    // fts_match search
    const matchResults = await (await db.prepare(
        "SELECT id, title, fts_score(title, body, 'programming language') as score FROM documents WHERE fts_match(title, body, 'programming language')"
    )).all();
    expect(matchResults.length).toBe(2);
    expect(matchResults.map(r => r.id).sort()).toEqual([1, 2]);
    for (const row of matchResults) {
        expect(row.score).toBeGreaterThan(0);
    }

    // fts_highlight
    const highlightResults = await (await db.prepare(
        "SELECT id, fts_highlight(title, '<b>', '</b>', 'Rust') as highlighted FROM documents WHERE fts_match(title, body, 'Rust')"
    )).all();
    expect(highlightResults.length).toBe(1);
    expect(highlightResults[0].id).toBe(1);
    expect(highlightResults[0].highlighted).toContain('<b>');
    expect(highlightResults[0].highlighted).toContain('Rust');

    // no match
    const noResults = await (await db.prepare(
        "SELECT * FROM documents WHERE fts_match(title, body, 'nonexistentterm')"
    )).all();
    expect(noResults.length).toBe(0);
})

test('blobs', async () => {
    const db = await connect(":memory:");
    const rows = await (await db.prepare("SELECT x'1020' as x")).all();
    expect(rows).toEqual([{ x: Buffer.from([16, 32]) }])
})

test('encryption', async () => {
    const path = `test-encryption-${(Math.random() * 10000) | 0}.db`;
    const hexkey = 'b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    const wrongKey = 'aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327';
    try {
        const db = await connect(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        await db.exec("CREATE TABLE t(x)");
        await db.exec("INSERT INTO t SELECT 'secret' FROM generate_series(1, 1024)");
        await db.exec("PRAGMA wal_checkpoint(truncate)");
        db.close();

        // Re-open with the same key - should work
        const db2 = await connect(path, {
            encryption: { cipher: 'aegis256', hexkey }
        });
        const rows = await (await db2.prepare("SELECT COUNT(*) as cnt FROM t")).all();
        expect(rows).toEqual([{ cnt: 1024 }]);
        db2.close();

        // Opening with wrong key MUST fail
        await expect(async () => {
            const db3 = await connect(path, {
                encryption: { cipher: 'aegis256', hexkey: wrongKey }
            });
            await (await db3.prepare("SELECT * FROM t")).all();
        }).rejects.toThrow();

        // Opening without encryption MUST fail
        await expect(async () => {
            const db4 = await connect(path);
            await (await db4.prepare("SELECT * FROM t")).all();
        }).rejects.toThrow();
    } finally {
        unlinkSync(path);
    }
})


test('example-1', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

    const insert = await db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
    await insert.run('Alice', 'alice@example.com');
    await insert.run('Bob', 'bob@example.com');

    const users = await (await db.prepare('SELECT * FROM users')).all();
    expect(users).toEqual([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' }
    ]);
})

test('example-2', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE users (name, email)');
    // Using transactions for atomic operations
    const transaction = db.transaction(async (users) => {
        const insert = await db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
        for (const user of users) {
            await insert.run(user.name, user.email);
        }
    });

    // Execute transaction
    await transaction([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);

    const rows = await (await db.prepare('SELECT * FROM users')).all();
    expect(rows).toEqual([
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' }
    ]);
})

// The transaction() wrapper must own the connection for the whole
// BEGIN..COMMIT window. Statements are only serialized individually, so a
// concurrent caller can currently interleave its statements into an open
// transaction: its BEGIN fails as nested, and its ROLLBACK then erases the
// first transaction's in-flight writes while later writes land in autocommit.
test('concurrent transaction() calls must not interleave', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(tag TEXT, i INTEGER)');

    const insertMany = db.transactionAsync(async (txn, tag: string, fail: boolean) => {
        for (let i = 0; i < 10; i++) {
            await txn.run('INSERT INTO t VALUES (?, ?)', [tag, i]);
        }
        if (fail) {
            throw new Error('abort transaction');
        }
    });

    const [committed, aborted] = await Promise.allSettled([
        insertMany('committed', false),
        insertMany('rolled-back', true),
    ]);

    // the first transaction commits; the second fails only with its own error
    expect(committed).toEqual({ status: 'fulfilled', value: undefined });
    expect(aborted.status).toBe('rejected');
    expect((aborted as PromiseRejectedResult).reason.message).toBe('abort transaction');

    // atomicity: every row of the committed transaction survives and no row
    // of the rolled-back transaction does
    const rows = await db.all('SELECT tag, COUNT(*) AS cnt FROM t GROUP BY tag ORDER BY tag');
    expect(rows).toEqual([{ tag: 'committed', cnt: 10 }]);
})

// Same root cause, worst manifestation: an independent autocommit statement
// racing with a transaction() call slips into the open BEGIN..ROLLBACK window,
// reports success, and is then silently erased by the transaction's rollback.
test('statement racing a transaction() must not be lost to its rollback', async () => {
    const db = await connect(':memory:');
    await db.exec('CREATE TABLE t(tag TEXT)');

    const failing = db.transactionAsync(async (txn) => {
        await txn.run('INSERT INTO t VALUES (?)', ['txn']);
        await new Promise((resolve) => setImmediate(resolve));
        throw new Error('abort transaction');
    });

    const [txnResult, plainResult] = await Promise.allSettled([
        failing(),
        db.run('INSERT INTO t VALUES (?)', ['plain']),
    ]);
    expect(txnResult.status).toBe('rejected');
    expect(plainResult.status).toBe('fulfilled');

    // the transaction rolled back, but the successful independent insert must survive
    expect(await db.all('SELECT tag FROM t')).toEqual([{ tag: 'plain' }]);
})

test('transaction.concurrent uses BEGIN CONCURRENT', async () => {
    const db = await connect(':memory:');
    const originalExec = db.exec;
    const calls: string[] = [];
    db.exec = async (sql) => {
        calls.push(sql);
    };

    try {
        const txn = db.transaction(async () => {
            calls.push('body');
        }).concurrent;
        await txn();
        expect(calls).toEqual(['BEGIN CONCURRENT', 'body', 'COMMIT']);
    } finally {
        db.exec = originalExec;
        await db.close();
    }
})

test('transactionAsync.concurrent uses BEGIN CONCURRENT', async () => {
    const db = await connect(':memory:');
    const originalExec = Transaction.prototype.exec;
    const calls: string[] = [];
    Transaction.prototype.exec = async (sql) => {
        calls.push(sql);
    };

    try {
        const txn = db.transactionAsync(async (_txn) => {
            calls.push('body');
        }).concurrent;
        await txn();
        expect(calls).toEqual(['BEGIN CONCURRENT', 'body', 'COMMIT']);
    } finally {
        Transaction.prototype.exec = originalExec;
        await db.close();
    }
})

// A callback that declares no parameters cannot be using the Transaction
// handle - it is the pre-0.8 shape whose statements would deadlock against
// the transaction's own lock, so it is rejected upfront.
test('transactionAsync() rejects callbacks that do not declare the handle', async () => {
    const db = await connect(':memory:');
    expect(() => db.transactionAsync(async () => { })).toThrow(/Transaction handle/);
    expect(() => db.transactionAsync((async (...args: any[]) => { }) as any)).toThrow(/Transaction handle/);
})

test('batch returns per-statement details and stops at the first failure', async () => {
    const db = await connect(":memory:");
    const results = await db.batch([
        "CREATE TABLE t_batch (id INTEGER PRIMARY KEY, name TEXT)",
        { sql: "INSERT INTO t_batch (name) VALUES (?)", args: ["Alice"] },
        { sql: "INSERT INTO t_batch (name) VALUES (?)", args: ["Bob"] },
        "SELECT name FROM t_batch ORDER BY id",
    ]);
    expect(results.length).toBe(4);
    expect(results[1].rowsAffected).toBe(1);
    expect(results[1].lastInsertRowid).toBe(1);
    expect(results[2].lastInsertRowid).toBe(2);
    expect(results[3].rows).toEqual([{ name: "Alice" }, { name: "Bob" }]);
    // The embedded engine does not report per-statement execution
    // statistics; the serverless driver fills these from the server.
    expect(results[1].rowsRead).toBeUndefined();
    expect(results[1].rowsWritten).toBeUndefined();
    expect(results[1].queryDurationMs).toBeUndefined();

    let error: any = null;
    try {
        await db.batch([
            { sql: "INSERT INTO t_batch (name) VALUES (?)", args: ["Carol"] },
            "INSERT INTO no_such_table VALUES (1)",
            { sql: "INSERT INTO t_batch (name) VALUES (?)", args: ["Dave"] },
        ]);
    } catch (e) {
        error = e;
    }
    expect(error).not.toBeNull();
    // The error identifies the failing statement and carries one entry
    // per statement: the completed first statement's ResultSet, null for
    // the failing statement and the skipped one after it.
    expect(error.batchIndex).toBe(1);
    expect(error.batchResults.length).toBe(3);
    expect(error.batchResults[0].rowsAffected).toBe(1);
    expect(error.batchResults[1]).toBeNull();
    expect(error.batchResults[2]).toBeNull();
    // Execution stopped at the failure: Carol committed, Dave never ran.
    const stmt = await db.prepare("SELECT COUNT(*) AS n FROM t_batch");
    expect(await stmt.all([])).toEqual([{ n: 3 }]);
})

test('atomic batch failure rolls back and reports the failing statement', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t_atomic (x)");
    let error: any = null;
    try {
        await db.batch([
            { sql: "INSERT INTO t_atomic VALUES (?)", args: [1] },
            "INSERT INTO no_such_table VALUES (1)",
        ], "immediate");
    } catch (e) {
        error = e;
    }
    expect(error).not.toBeNull();
    expect(error.batchIndex).toBe(1);
    expect(error.batchResults.length).toBe(2);
    const stmt = await db.prepare("SELECT COUNT(*) AS n FROM t_atomic");
    expect(await stmt.all([])).toEqual([{ n: 0 }]);
})

test('batch validates every bind value before executing the first statement', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t_batch_bind_validation (x)");

    let error: any;
    try {
        await db.batch([
            { sql: "INSERT INTO t_batch_bind_validation VALUES (?)", args: [1] },
            { sql: "INSERT INTO t_batch_bind_validation VALUES (?)", args: [Number.POSITIVE_INFINITY] },
        ]);
    } catch (caught) {
        error = caught;
    }

    expect(error.batchIndex).toBe(1);
    expect(error.batchResults).toEqual([]);
    const stmt = await db.prepare("SELECT COUNT(*) AS n FROM t_batch_bind_validation");
    expect(await stmt.all([])).toEqual([{ n: 0 }]);
})

test('batch validates every bigint before executing the first statement', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t_batch_bigint_validation (x)");

    let error: any;
    try {
        await db.batch([
            { sql: "INSERT INTO t_batch_bigint_validation VALUES (?)", args: [1] },
            { sql: "INSERT INTO t_batch_bigint_validation VALUES (?)", args: [1n << 63n] },
        ]);
    } catch (caught) {
        error = caught;
    }

    expect(error.batchIndex).toBe(1);
    expect(error.batchResults).toEqual([]);
    const stmt = await db.prepare("SELECT COUNT(*) AS n FROM t_batch_bigint_validation");
    expect(await stmt.all([])).toEqual([{ n: 0 }]);
})

test('batch coerces bind values once during validation', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t_batch_coercion (x TEXT)");
    let coercions = 0;
    const value = {
        toString() {
            coercions++;
            if (coercions > 1) throw new Error("value was coerced twice");
            return "coerced";
        },
    };

    await db.batch([
        { sql: "INSERT INTO t_batch_coercion VALUES (?)", args: [value] },
    ]);

    expect(coercions).toBe(1);
    const stmt = await db.prepare("SELECT x FROM t_batch_coercion");
    expect(await stmt.all([])).toEqual([{ x: "coerced" }]);
})

test('atomic batch rejects transaction control before BEGIN', async () => {
    const db = await connect(":memory:");
    await db.exec("CREATE TABLE t_batch_control (x)");
    const controls = [
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT nested",
        "RELEASE nested",
        "\ufeffCOMMIT",
    ];

    for (const control of controls) {
        let error: any;
        try {
            await db.batch([
                "INSERT INTO t_batch_control VALUES (1)",
                ` ; \n-- leading line comment\n; /* leading block comment */ ${control}`,
            ], "immediate");
        } catch (caught) {
            error = caught;
        }
        expect(error.batchIndex).toBe(1);
        expect(error.batchResults).toEqual([]);
    }

    const stmt = await db.prepare("SELECT COUNT(*) AS n FROM t_batch_control");
    expect(await stmt.all([])).toEqual([{ n: 0 }]);
})

test('atomic batch attaches a rollback failure to the primary error', async () => {
    const db = await connect(":memory:");
    const native = (db as any).db;
    const originalExecutor = native.executor.bind(native);
    native.executor = (sql: string, ...args: any[]) => {
        if (sql === "ROLLBACK") throw new Error("rollback failed");
        return originalExecutor(sql, ...args);
    };

    let error: any;
    try {
        await db.batch(["INSERT INTO no_such_batch_table VALUES (1)"], "immediate");
    } catch (caught) {
        error = caught;
    } finally {
        native.executor = originalExecutor;
    }

    expect(error).toBeInstanceOf(Error);
    expect(error.message).toMatch(/no_such_batch_table/);
    expect(error.rollbackError).toBeInstanceOf(Error);
    expect(error.rollbackError.message).toBe("rollback failed");
})
