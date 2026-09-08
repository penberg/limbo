import { unlinkSync } from "node:fs";
import { test as baseTest, expect } from 'vitest'
import { connect, Database, DatabaseRowMutation, DatabaseRowTransformResult, retryFetch } from './promise.js'
import { TursoServer } from './turso-server.js'

const localeCompare = (a: { x: string }, b: { x: string }) => a.x.localeCompare(b.x);
const intCompare = (a: { x: number }, b: { x: number }) => a.x - b.x;

const test = baseTest.extend<{ server: TursoServer }>({
    server: async ({ }, use) => {
        const server = await TursoServer.create();
        try { await use(server); } finally { server.close(); }
    },
});

function cleanup(path: string) {
    unlinkSync(path);
    unlinkSync(`${path}-wal`);
    unlinkSync(`${path}-info`);
    unlinkSync(`${path}-changes`);
    try { unlinkSync(`${path}-wal-revert`) } catch (e) { }
}

test.skipIf(process.env.LOCAL_SYNC_SERVER)('partial sync concurrency', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const dbs = [];
    for (let i = 0; i < 16; i++) {
        dbs.push(await connect({
            path: 'partial-1.db',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
            partialSyncExperimental: {
                bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
                segmentSize: 128 * 1024,
            },
        }));
    }
    const qs = [];
    for (const db of dbs) {
        qs.push((await db.prepare("SELECT COUNT(*) as cnt FROM partial")).all());
    }
    const values = await Promise.all(qs);
    expect(values).toEqual(new Array(16).fill([{ cnt: 2000 }]))
})

test.skipIf(process.env.LOCAL_SYNC_SERVER)('partial sync (prefix bootstrap strategy)', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 5000)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        longPollTimeoutMs: 100,
        tracing: 'info',
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 4096,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    // select of one record shouldn't increase amount of received data
    expect(await (await db.prepare("SELECT length(value) as length FROM partial LIMIT 1")).all()).toEqual([{ length: 1024 }]);
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    await (await db.prepare("INSERT INTO partial VALUES (-1)")).run();

    expect(await (await db.prepare("SELECT COUNT(*) as cnt FROM partial")).all()).toEqual([{ cnt: 2001 }]);
    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
}, { timeout: 300_000 })

test.skipIf(process.env.LOCAL_SYNC_SERVER)('partial sync (prefix bootstrap strategy; large segment size)', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 128 * 1024,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    const startLast = performance.now();
    // select of one record shouldn't increase amount of received data
    expect(await (await db.prepare("SELECT length(value) as length FROM partial LIMIT 1")).all()).toEqual([{ length: 1024 }]);
    console.info('select last', 'elapsed', performance.now() - startLast);

    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(2 * 128 * (1024 + 128));

    await (await db.prepare("INSERT INTO partial VALUES (-1)")).run();

    const startAll = performance.now();
    expect(await (await db.prepare("SELECT COUNT(*) as cnt FROM partial")).all()).toEqual([{ cnt: 2001 }]);
    console.info('select all', 'elapsed', performance.now() - startAll);

    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
})

test.skipIf(process.env.LOCAL_SYNC_SERVER)('partial sync (prefix bootstrap strategy; prefetch)', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial(value BLOB)");
        await db.exec("DELETE FROM partial");
        await db.exec("INSERT INTO partial SELECT randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'prefix', length: 128 * 1024 },
            segmentSize: 4 * 1024,
            prefetch: true,
        },
    });

    // 128kb plus some overhead
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(128 * (1024 + 128));

    const startLast = performance.now();
    // select of one record shouldn't increase amount of received data
    expect(await (await db.prepare("SELECT length(value) as length FROM partial LIMIT 1")).all()).toEqual([{ length: 1024 }]);
    console.info('select last', 'elapsed', performance.now() - startLast);

    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * 128 * (1024 + 128));

    await (await db.prepare("INSERT INTO partial VALUES (-1)")).run();

    const startAll = performance.now();
    expect(await (await db.prepare("SELECT COUNT(*) as cnt FROM partial")).all()).toEqual([{ cnt: 2001 }]);
    console.info('select all', 'elapsed', performance.now() - startAll);

    expect((await db.stats()).networkReceivedBytes).toBeGreaterThanOrEqual(2000 * 1024);
})

test.skipIf(process.env.LOCAL_SYNC_SERVER)('partial sync (query bootstrap strategy)', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS partial_keyed(key INTEGER PRIMARY KEY, value BLOB)");
        await db.exec("DELETE FROM partial_keyed");
        await db.exec("INSERT INTO partial_keyed SELECT value, randomblob(1024) FROM generate_series(1, 2000)");
        await db.push();
        await db.close();
    }

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        longPollTimeoutMs: 100,
        partialSyncExperimental: {
            bootstrapStrategy: { kind: 'query', query: 'SELECT * FROM partial_keyed WHERE key = 1000' },
            segmentSize: 4096,
        },
    });

    // we must sync only few pages
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * (4096 + 128));

    // select of one record shouldn't increase amount of received data by a lot
    expect(await (await db.prepare("SELECT length(value) as length FROM partial_keyed LIMIT 1")).all()).toEqual([{ length: 1024 }]);
    expect((await db.stats()).networkReceivedBytes).toBeLessThanOrEqual(10 * (4096 + 128));

    await (await db.prepare("INSERT INTO partial_keyed VALUES (-1, -1)")).run();
    const n1 = await db.stats();

    // same as bootstrap query - we shouldn't bring any more pages
    expect(await (await db.prepare("SELECT length(value) as length FROM partial_keyed WHERE key = 1000")).all()).toEqual([{ length: 1024 }]);
    const n2 = await db.stats();
    expect(n1.networkReceivedBytes).toEqual(n2.networkReceivedBytes);
})

test('concurrent-actions-consistency', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS rows(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM rows");
        await db.exec("INSERT INTO rows VALUES ('key', 0)");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    console.info('run_info', await (await db1.prepare("SELECT * FROM sqlite_master")).all());
    await db1.exec("PRAGMA busy_timeout=100");
    const pull = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            console.info('pull', i);
            try { await db1.pull(); }
            catch (e) { console.error('pull', e); }
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    const push = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await new Promise(resolve => setTimeout(resolve, (Math.random() + 1)));
            console.info('push', i);
            try {
                if ((await db1.stats()).cdcOperations > 0) {
                    const start = performance.now();
                    await db1.push();
                    console.info('push', performance.now() - start);
                }
            }
            catch (e) { console.error('push', e); }
        }
    }
    const run = async function (iterations: number) {
        let rows = 0;
        for (let i = 0; i < iterations; i++) {
            await (await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?")).run('key');
            rows += 1;
            const { cnt } = await (await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?")).get(['key']);
            expect(cnt).toBe(rows);
            await new Promise(resolve => setTimeout(resolve, 10 * (Math.random() + 1)));
        }
    }
    await Promise.all([pull(100), push(100), run(200)]);
}, 30_000)

test('simple-db', async () => {
    const db = new Database({ path: ':memory:' });
    expect(await (await db.prepare("SELECT 1 as x")).all()).toEqual([{ x: 1 }])
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    expect(await (await db.prepare("SELECT * FROM t")).all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    await expect(async () => await db.pull()).rejects.toThrowError(/sync is disabled as database was opened without sync support/);
})

test('experimental features are propagated to the local database', async () => {
    // Generated columns are gated behind the `generated_columns` experimental
    // feature. A synced database opened without the `experimental` option must
    // reject the DDL, proving the feature is off by default.
    const dbDefault = new Database({ path: ':memory:' });
    await dbDefault.connect();
    await expect(async () => await dbDefault.exec("CREATE TABLE t(x INTEGER, y INTEGER AS (x * 2))"))
        .rejects.toThrowError(/Generated columns require/);

    // The same statement succeeds once the feature is enabled, which only works
    // if `experimental` is threaded through the sync engine to the local engine.
    const dbExperimental = new Database({ path: ':memory:', experimental: ['generated_columns'] });
    await dbExperimental.connect();
    await dbExperimental.exec("CREATE TABLE t(x INTEGER, y INTEGER AS (x * 2))");
    await dbExperimental.exec("INSERT INTO t(x) VALUES (1), (2), (3)");
    expect(await (await dbExperimental.prepare("SELECT y FROM t ORDER BY x")).all()).toEqual([{ y: 2 }, { y: 4 }, { y: 6 }]);
})

test('experimental features are propagated to the synced database', async ({ server }) => {
    // Same as above, but for a remote-backed (sync engine) database: the
    // feature must be threaded through SyncEngine -> the local engine that the
    // sync engine opens internally.
    const dbDefault = await connect({ path: ':memory:', url: server.dbUrl() });
    await expect(async () => await dbDefault.exec("CREATE TABLE g(x INTEGER, y INTEGER AS (x * 2))"))
        .rejects.toThrowError(/Generated columns require/);
    await dbDefault.close();

    // With the feature enabled the gated DDL succeeds and the change pushes to
    // the remote...
    const writer = await connect({ path: ':memory:', url: server.dbUrl(), experimental: ['generated_columns'] });
    await writer.exec("CREATE TABLE g(x INTEGER, y INTEGER AS (x * 2))");
    await writer.exec("INSERT INTO g(x) VALUES (1), (2), (3)");
    expect(await (await writer.prepare("SELECT y FROM g ORDER BY x")).all()).toEqual([{ y: 2 }, { y: 4 }, { y: 6 }]);
    await writer.push();
    await writer.close();

    // ...and a fresh client bootstraps the generated-columns schema from the remote.
    const reader = await connect({ path: ':memory:', url: server.dbUrl(), experimental: ['generated_columns'] });
    expect(await (await reader.prepare("SELECT y FROM g ORDER BY x")).all()).toEqual([{ y: 2 }, { y: 4 }, { y: 6 }]);
    await reader.close();
})

test('implicit connect', async ({ server }) => {
    const db = new Database({ path: ':memory:', url: server.dbUrl() });
    const defer = await db.prepare("SELECT * FROM not_found");
    await expect(async () => await defer.all()).rejects.toThrowError(/no such table: not_found/);
    await expect(async () => await db.prepare("SELECT * FROM not_found")).rejects.toThrowError(/no such table: not_found/);
    expect(await (await db.prepare("SELECT 1 as x")).all()).toEqual([{ x: 1 }]);
})

test('pull with no remote changes updates lastPullUnixTime', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (1)");
        await db.push();
        await db.close();
    }

    // fresh client bootstraps from the remote - nothing new to pull afterwards
    const db = await connect({ path: ':memory:', url: server.dbUrl() });
    const before = (await db.stats()).lastPullUnixTime;

    // unix time has 1s resolution - wait long enough for the timestamp to advance
    await new Promise(resolve => setTimeout(resolve, 1500));

    // remote has no new changes - pull returns false but must still bump lastPullUnixTime
    expect(await db.pull()).toBe(false);

    const after = (await db.stats()).lastPullUnixTime;
    expect(after).toBeGreaterThan(before);
})

test('defered sync', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.exec("INSERT INTO t VALUES (100)");
        await db.push();
        await db.close();
    }

    let url: string | null = null;
    const db = new Database({ path: ':memory:', url: () => url });
    await (await db.prepare("CREATE TABLE t(x)")).run();
    await (await db.prepare("INSERT INTO t VALUES (1), (2), (3), (42)")).run();
    expect(await (await db.prepare("SELECT * FROM t")).all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
    await expect(async () => await db.pull()).rejects.toThrow(/url is empty - sync is paused/);
    url = server.dbUrl();
    await db.pull();
    expect(await (await db.prepare("SELECT * FROM t")).all()).toEqual([{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }, { x: 42 }]);
})

// TODO: re-enable encryption tests once local sync server supports the encrypted-tenant URL pattern.
// test('encryption sync', async ({ server }) => {
//     const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
//     const URL = server.dbUrl();
//     {
//         const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
//         await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
//         await db.exec("DELETE FROM t");
//         await db.push();
//         await db.close();
//     }
//     const db1 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
//     const db2 = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
//     await db1.exec("INSERT INTO t VALUES (1), (2), (3)");
//     await db2.exec("INSERT INTO t VALUES (4), (5), (6)");
//     expect(await db1.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
//     expect(await db2.prepare("SELECT * FROM t").all()).toEqual([{ x: 4 }, { x: 5 }, { x: 6 }]);
//     await Promise.all([db1.push(), db2.push()]);
//     await Promise.all([db1.pull(), db2.pull()]);
//     const expected = [{ x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }, { x: 5 }, { x: 6 }];
//     expect((await db1.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
//     expect((await db2.prepare("SELECT * FROM t").all()).sort(intCompare)).toEqual(expected.sort(intCompare));
// });

// test('defered encryption sync', async ({ server }) => {
//     const URL = server.dbUrl();
//     const KEY = 'l/FWopMfZisTLgBX4A42AergrCrYKjiO3BfkJUwv83I=';
//     let url = null;
//     {
//         const db = await connect({ path: ':memory:', url: URL, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
//         await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
//         await db.exec("DELETE FROM t");
//         await db.exec("INSERT INTO t VALUES (100)");
//         await db.push();
//         await db.close();
//     }
//     const db = await connect({ path: ':memory:', url: () => url, remoteEncryption: { key: KEY, cipher: 'aes256gcm' } });
//     await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
//     await db.exec("INSERT INTO t VALUES (1), (2), (3)");
//     expect(await db.prepare("SELECT * FROM t").all()).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
//
//     url = URL;
//     await db.pull();
//
//     const expected = [{ x: 100 }, { x: 1 }, { x: 2 }, { x: 3 }];
//     expect((await db.prepare("SELECT * FROM t").all())).toEqual(expected);
// });

test('select-after-push', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
        await db.push();
    }
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        const rows = await (await db.prepare('SELECT * FROM t')).all();
        expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }])
    }
})

test('select-without-push', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS t(x)");
        await db.exec("DELETE FROM t");
        await db.push();
        await db.close();
    }
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    }
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        const rows = await (await db.prepare('SELECT * FROM t')).all();
        expect(rows).toEqual([])
    }
})

test('merge-non-overlapping-keys', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2')");

    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db2.exec("INSERT INTO q VALUES ('k3', 'value3'), ('k4', 'value4'), ('k5', 'value5')");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await (await db1.prepare('SELECT * FROM q')).all();
    const rows2 = await (await db1.prepare('SELECT * FROM q')).all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value3' }, { x: 'k4', y: 'value4' }, { x: 'k5', y: 'value5' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");

    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await (await db1.prepare('SELECT * FROM q')).all();
    const rows2 = await (await db1.prepare('SELECT * FROM q')).all();
    const expected = [{ x: 'k1', y: 'value1' }, { x: 'k2', y: 'value2' }, { x: 'k3', y: 'value5' }, { x: 'k4', y: 'value4' }];
    expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
})

test('last-push-wins-with-delete', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db1.exec("INSERT INTO q VALUES ('k1', 'value1'), ('k2', 'value2'), ('k4', 'value4')");
    await db1.exec("DELETE FROM q")

    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db2.exec("INSERT INTO q VALUES ('k1', 'value3'), ('k2', 'value4'), ('k3', 'value5')");

    await db2.push();
    await db1.push();
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await (await db1.prepare('SELECT * FROM q')).all();
    const rows2 = await (await db1.prepare('SELECT * FROM q')).all();
    const expected = [{ x: 'k3', y: 'value5' }];
    expect(rows1).toEqual(expected)
    expect(rows2).toEqual(expected)
})

test('constraint-conflict', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS u(x TEXT PRIMARY KEY, y UNIQUE)");
        await db.exec("DELETE FROM u");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db1.exec("INSERT INTO u VALUES ('k1', 'value1')");

    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db2.exec("INSERT INTO u VALUES ('k2', 'value1')");

    await db1.push();
    await expect(async () => await db2.push()).rejects.toThrow(/UNIQUE constraint failed: u.y/);
})

test('checkpoint', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    for (let i = 0; i < 1000; i++) {
        await db1.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    expect((await db1.stats()).mainWalSize).toBeGreaterThan(4096 * 1000);
    await db1.checkpoint();
    expect((await db1.stats()).mainWalSize).toBe(0);
    let revertWal = (await db1.stats()).revertWalSize;
    expect(revertWal).toBeLessThan(4096 * 1000 / 50);

    for (let i = 0; i < 1000; i++) {
        await db1.exec(`UPDATE q SET y = 'u${i}' WHERE x = 'k${i}'`);
    }
    await db1.checkpoint();
    expect((await db1.stats()).revertWalSize).toBe(revertWal);
})


test('persistence-push', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const db1 = await connect({ path: path, url: server.dbUrl() });
            await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
            await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
            await db1.close();
        }

        {
            const db2 = await connect({ path: path, url: server.dbUrl() });
            await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
            await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);
            const stmt = await db2.prepare('SELECT * FROM q');
            const rows = await stmt.all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
            expect(rows).toEqual(expected)
            stmt.close();
            await db2.close();
        }

        {
            const db3 = await connect({ path: path, url: server.dbUrl() });
            await db3.push();
            await db3.close();
        }

        {
            const db4 = await connect({ path: path, url: server.dbUrl() });
            const rows = await (await db4.prepare('SELECT * FROM q')).all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
            expect(rows).toEqual(expected)
            await db4.close();
        }
    }
    finally {
        cleanup(path);
    }
})

test('persistence-offline', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        {
            const db = await connect({ path: path, url: server.dbUrl() });
            await db.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
            await db.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
            await db.push();
            await db.close();
        }
        {
            const db = await connect({ path: path, url: "https://not-valid-url.localhost" });
            const rows = await (await db.prepare("SELECT * FROM q")).all();
            const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }];
            expect(rows.sort(localeCompare)).toEqual(expected.sort(localeCompare))
            await db.close();
        }
    } finally {
        cleanup(path);
    }
})

test('persistence-pull-push', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const path1 = `test-${(Math.random() * 10000) | 0}.db`;
    const path2 = `test-${(Math.random() * 10000) | 0}.db`;
    try {
        const db1 = await connect({ path: path1, url: server.dbUrl() });
        await db1.exec(`INSERT INTO q VALUES ('k1', 'v1')`);
        await db1.exec(`INSERT INTO q VALUES ('k2', 'v2')`);
        const stats1 = await db1.stats();

        const db2 = await connect({ path: path2, url: server.dbUrl() });
        await db2.exec(`INSERT INTO q VALUES ('k3', 'v3')`);
        await db2.exec(`INSERT INTO q VALUES ('k4', 'v4')`);

        await Promise.all([db1.push(), db2.push()]);
        await Promise.all([db1.pull(), db2.pull()]);
        const stats2 = await db1.stats();
        console.info(stats1, stats2);
        expect(stats1.revision).not.toBe(stats2.revision);

        const rows1 = await (await db1.prepare('SELECT * FROM q')).all();
        const rows2 = await (await db2.prepare('SELECT * FROM q')).all();
        const expected = [{ x: 'k1', y: 'v1' }, { x: 'k2', y: 'v2' }, { x: 'k3', y: 'v3' }, { x: 'k4', y: 'v4' }];
        expect(rows1.sort(localeCompare)).toEqual(expected.sort(localeCompare))
        expect(rows2.sort(localeCompare)).toEqual(expected.sort(localeCompare))
    } finally {
        cleanup(path1);
        cleanup(path2);
    }
})

test('update', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl(), longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db = await connect({ path: ':memory:', url: server.dbUrl() });
    await db.exec("INSERT INTO q VALUES ('1', '2')")
    await db.push();
    await db.exec("INSERT INTO q VALUES ('1', '2') ON CONFLICT DO UPDATE SET y = '3'")
    await db.push();
})

test('concurrent-updates', { timeout: process.platform === 'win32' ? 120_000 : undefined }, async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl(), longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({
        path: ':memory:',
        url: server.dbUrl(),
    });
    await db1.exec("PRAGMA busy_timeout=100");
    async function pull(db: Database) {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull error', e);
        } finally {
            console.error('pull ok');
            setTimeout(async () => await pull(db), 0);
        }
    }
    async function push(db: Database) {
        try {
            await db.push();
        } catch (e) {
            console.error('push error', e);
        } finally {
            console.error('push ok');
            setTimeout(async () => await push(db), 0);
        }
    }

    setTimeout(async () => await pull(db1), 0)
    setTimeout(async () => await push(db1), 0)
    for (let i = 0; i < 1000; i++) {
        try {
            await Promise.all([
                db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`),
                db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`)
            ]);
            console.info('insert ok');
        } catch (e) {
            console.error('insert error', e);
        }
        await new Promise(resolve => setTimeout(resolve, 1));
    }
})

test('corruption-bug-1', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl(), longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    const db1 = await connect({
        path: ':memory:',
        url: server.dbUrl(),
    });
    for (let i = 0; i < 100; i++) {
        await db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
    }
    await db1.pull();
    await db1.push();
    for (let i = 0; i < 100; i++) {
        await db1.exec(`INSERT INTO q VALUES ('1', 0) ON CONFLICT DO UPDATE SET y = randomblob(1024)`);
    }
    await db1.pull();
    await db1.push();
})

test('pull-push-concurrent', async ({ server }) => {
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl(), longPollTimeoutMs: 5000 });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x TEXT PRIMARY KEY, y)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }
    let pullResolve: ((..._: any) => void) | null = null;
    const pullFinish = new Promise(resolve => pullResolve = resolve);
    let pushResolve: ((..._: any) => void) | null = null;
    const pushFinish = new Promise(resolve => pushResolve = resolve);
    let stopPull = false;
    let stopPush = false;
    const db = await connect({ path: ':memory:', url: server.dbUrl() });
    let pull = async () => {
        try {
            await db.pull();
        } catch (e) {
            console.error('pull', e);
        } finally {
            if (!stopPull) {
                setTimeout(pull, 0);
            } else {
                pullResolve!()
            }
        }
    }
    let push = async () => {
        try {
            if ((await db.stats()).cdcOperations > 0) {
                await db.push();
            }
        } catch (e) {
            console.error('push', e);
        } finally {
            if (!stopPush) {
                setTimeout(push, 0);
            } else {
                pushResolve!();
            }
        }
    }
    setTimeout(pull, 0);
    setTimeout(push, 0);
    for (let i = 0; i < 1000; i++) {
        await db.exec(`INSERT INTO q VALUES ('k${i}', 'v${i}')`);
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
    stopPush = true;
    await pushFinish;
    stopPull = true;
    await pullFinish;
    console.info(await db.stats());
})

test('checkpoint-and-actions', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
            longPollTimeoutMs: 100,
        });
        await db.exec("CREATE TABLE IF NOT EXISTS rows(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM rows");
        await db.exec("INSERT INTO rows VALUES ('key', 0)");
        await db.push();
        await db.close();
    }
    const db1 = await connect({ path: ':memory:', url: server.dbUrl() });
    await db1.exec("PRAGMA busy_timeout=100");
    const pull = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            try {
                await db1.pull();
            }
            catch (e) { console.error('pull', e); }
            await new Promise(resolve => setTimeout(resolve, 0));
        }
    }
    const push = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await new Promise(resolve => setTimeout(resolve, 5));
            try {
                if ((await db1.stats()).cdcOperations > 0) {
                    const start = performance.now();
                    await db1.push();
                    console.info('push', performance.now() - start);
                }
            }
            catch (e) { console.error('push', e); }
        }
    }
    let rows = 0;
    const run = async function (iterations: number) {
        for (let i = 0; i < iterations; i++) {
            await (await db1.prepare("UPDATE rows SET value = value + 1 WHERE key = ?")).run('key');
            rows += 1;
            const { cnt } = await (await db1.prepare("SELECT value as cnt FROM rows WHERE key = ?")).get(['key']);
            console.info('CHECK', cnt, rows);
            expect(cnt).toBe(rows);
            await new Promise(resolve => setTimeout(resolve, 10 * (1 + Math.random())));
        }
    }
    await Promise.all([pull(40), push(40), run(100)]);
})

test('transform', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after!.value - m.before!.value, m.after!.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: server.dbUrl(), transform: transform });
    const db2 = await connect({ path: ':memory:', url: server.dbUrl(), transform: transform });

    await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");

    await Promise.all([db1.push(), db2.push()]);
    await Promise.all([db1.pull(), db2.pull()]);

    const rows1 = await (await db1.prepare('SELECT * FROM counter')).all();
    const rows2 = await (await db2.prepare('SELECT * FROM counter')).all();
    expect(rows1).toEqual([{ key: '1', value: 2 }]);
    expect(rows2).toEqual([{ key: '1', value: 2 }]);
})

test('simple-transform', async ({ server }) => {
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after!.value - (m.before?.value ?? 0), m.after!.key]
        }
    } as DatabaseRowTransformResult);
    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        transform: transform,
    });
    await db.exec("CREATE TABLE IF NOT EXISTS counter (key TEXT PRIMARY KEY, value INTEGER)");
    await db.exec("INSERT INTO counter VALUES ('clicks', 0)");
    await db.push(); // <- aborts the process here
})

test('transform-many', async ({ server }) => {
    {
        const db = await connect({
            path: ':memory:',
            url: server.dbUrl(),
        });
        await db.exec("CREATE TABLE IF NOT EXISTS counter(key TEXT PRIMARY KEY, value INTEGER)");
        await db.exec("DELETE FROM counter");
        await db.exec("INSERT INTO counter VALUES ('1', 0)")
        await db.push();
        await db.close();
    }
    const transform = (m: DatabaseRowMutation) => ({
        operation: 'rewrite',
        stmt: {
            sql: `UPDATE counter SET value = value + ? WHERE key = ?`,
            values: [m.after!.value - m.before!.value, m.after!.key]
        }
    } as DatabaseRowTransformResult);
    const db1 = await connect({ path: ':memory:', url: server.dbUrl(), transform: transform });
    const db2 = await connect({ path: ':memory:', url: server.dbUrl(), transform: transform });

    for (let i = 0; i < 1002; i++) {
        await db1.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }
    for (let i = 0; i < 1001; i++) {
        await db2.exec("UPDATE counter SET value = value + 1 WHERE key = '1'");
    }

    let start = performance.now();
    await Promise.all([db1.push(), db2.push()]);
    console.info('push', performance.now() - start);

    start = performance.now();
    await Promise.all([db1.pull(), db2.pull()]);
    console.info('pull', performance.now() - start);

    const rows1 = await (await db1.prepare('SELECT * FROM counter')).all();
    const rows2 = await (await db2.prepare('SELECT * FROM counter')).all();
    expect(rows1).toEqual([{ key: '1', value: 1001 + 1002 }]);
    expect(rows2).toEqual([{ key: '1', value: 1001 + 1002 }]);
})

test('push operations threshold splits batches', async ({ server }) => {
    // Seed schema on the remote.
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE IF NOT EXISTS q(x INTEGER PRIMARY KEY, y TEXT)");
        await db.exec("DELETE FROM q");
        await db.push();
        await db.close();
    }

    // total intentionally chosen so total % threshold != 0 — exercises the
    // trailing-batch flush as well as the threshold-triggered seals.
    const threshold = 13;
    const total = 1003;

    const db1 = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        pushOperationsThreshold: threshold,
    });
    for (let i = 0; i < total; i++) {
        await db1.exec(`INSERT INTO q VALUES (${i}, 'v${i}')`);
    }
    // A multi-row transaction larger than the threshold; must not be split
    // across HTTP batches.
    await db1.exec("BEGIN");
    for (let i = total; i < total + threshold * 2 + 5; i++) {
        await db1.exec(`INSERT INTO q VALUES (${i}, 'v${i}')`);
    }
    await db1.exec("COMMIT");

    await db1.push();

    const expectedTotal = total + threshold * 2 + 5;

    // Fresh client bootstraps from the remote — every row pushed in batches
    // must be visible.
    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    const cnt = await (await db2.prepare('SELECT COUNT(*) as c FROM q')).all();
    expect(cnt).toEqual([{ c: expectedTotal }]);
    const last = await (await db2.prepare('SELECT MAX(x) as m FROM q')).all();
    expect(last).toEqual([{ m: expectedTotal - 1 }]);
})

test('custom fetch override is invoked', async ({ server }) => {
    let calls = 0;
    const trackingFetch: typeof fetch = (input, init) => {
        calls += 1;
        return fetch(input, init);
    };
    const db = await connect({ path: ':memory:', url: server.dbUrl(), fetch: trackingFetch });
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    await db.push();
    expect(calls).toBeGreaterThan(0);
    const rows = await (await db.prepare('SELECT x FROM t ORDER BY x')).all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
})

test('retryFetch retries transient network failures', async ({ server }) => {
    let attemptsObserved = 0;
    // Drop the first 2 push attempts (status 503), then forward.
    let dropsRemaining = 2;
    const flakyFetch: typeof fetch = async (input, init) => {
        attemptsObserved += 1;
        if (init?.method === 'POST' && dropsRemaining > 0) {
            dropsRemaining -= 1;
            return new Response('flaky', { status: 503 });
        }
        return fetch(input, init);
    };
    const wrapped = retryFetch({ fetch: flakyFetch, attempts: 5, delayMs: 10, backoff: 2 });
    const db = await connect({ path: ':memory:', url: server.dbUrl(), fetch: wrapped });
    await db.exec("CREATE TABLE t(x)");
    await db.exec("INSERT INTO t VALUES (1), (2), (3)");
    await db.push();
    // Both POST drops should have been retried — attemptsObserved counts every
    // call (including dropped ones), so we expect at least the retries plus the
    // eventual successful POSTs.
    expect(attemptsObserved).toBeGreaterThanOrEqual(2);
    expect(dropsRemaining).toBe(0);
    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    const rows = await (await db2.prepare('SELECT x FROM t ORDER BY x')).all();
    expect(rows).toEqual([{ x: 1 }, { x: 2 }, { x: 3 }]);
})

test('retryFetch propagates persistent errors', async () => {
    const wrapped = retryFetch({
        fetch: async () => { throw new TypeError('persistent failure'); },
        attempts: 3,
        delayMs: 5,
    });
    await expect(wrapped(new URL('http://localhost/'), {} as RequestInit)).rejects.toThrow(/persistent failure/);
})

test('pullBytesThreshold splits bootstrap into multiple chunks', async ({ server }) => {
    const PAGE_SIZE = 4096;
    const THRESHOLD = 8192;
    const PAGES_PER_CHUNK = Math.ceil(THRESHOLD / PAGE_SIZE); // 2

    // Seed a remote with enough data to span multiple 4KB pages.
    {
        const seed = await connect({ path: ':memory:', url: server.dbUrl() });
        await seed.exec("CREATE TABLE big(x INTEGER PRIMARY KEY, y BLOB)");
        await seed.exec("INSERT INTO big SELECT value, randomblob(1024) FROM generate_series(1, 50)");
        await seed.push();
        await seed.close();
    }
    // Ask the server itself for its db_size (= page count) — the chunk count
    // is a deterministic function of this number, so the test can assert it
    // exactly. Reading the local file's PRAGMA page_count after a probe
    // bootstrap is unreliable: connect() augments the local DB with sync
    // bookkeeping after the engine truncates it, inflating page_count past
    // what the server actually returned.
    const pageCountRows = await server.dbSql('PRAGMA page_count');
    const serverPages = Number(pageCountRows[0][0]);

    let pullUpdatesCalls = 0;
    const trackedFetch: typeof fetch = async (input, init) => {
        if (init?.method === 'POST' && input.toString().endsWith('/pull-updates')) {
            pullUpdatesCalls += 1;
        }
        return fetch(input, init);
    };

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        pullBytesThreshold: THRESHOLD,
        fetch: trackedFetch,
    });

    const expectedChunks = Math.ceil(serverPages / PAGES_PER_CHUNK);
    expect(serverPages).toBeGreaterThan(PAGES_PER_CHUNK); // sanity: would split
    expect(pullUpdatesCalls).toBe(expectedChunks);

    // Bootstrapped data must be intact regardless of the chunked fetch.
    const cnt = await (await db.prepare("SELECT COUNT(*) as c FROM big")).all();
    expect(cnt).toEqual([{ c: 50 }]);
    const sizes = await (await db.prepare("SELECT length(y) as len FROM big LIMIT 1")).all();
    expect(sizes).toEqual([{ len: 1024 }]);
})

test('unreliable fetch eventually drains all push batches', async ({ server }) => {
    // Seed empty schema.
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE q(x INTEGER PRIMARY KEY)");
        await db.push();
        await db.close();
    }

    const THRESHOLD = 5;
    const TXNS = 4;
    const ROWS_PER_TXN = THRESHOLD;
    const TOTAL_ROWS = TXNS * ROWS_PER_TXN;

    // Unreliable fetch: lets the first push batch of each push() through and
    // fails every subsequent batch in the same push() with 503. fetch_last_change_id
    // resets the per-push counter (it's the first /v2/pipeline POST of every
    // push and uses SELECT, not BEGIN IMMEDIATE).
    let batchesThisPush = 0;
    const unreliableFetch: typeof fetch = async (input, init) => {
        if (init?.method === 'POST' && input.toString().endsWith('/v2/pipeline')) {
            const body = init.body ? new TextDecoder().decode(init.body as Uint8Array) : '';
            if (body.includes('BEGIN IMMEDIATE')) {
                batchesThisPush += 1;
                if (batchesThisPush > 1) {
                    return new Response('flaky', { status: 503 });
                }
            } else {
                // fetch_last_change_id: start of a new push attempt.
                batchesThisPush = 0;
            }
        }
        return fetch(input, init);
    };

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        pushOperationsThreshold: THRESHOLD,
        fetch: unreliableFetch,
    });

    // Generate TXNS distinct transactions, each exactly threshold-sized.
    for (let txn = 0; txn < TXNS; txn++) {
        await db.exec("BEGIN");
        for (let i = 0; i < ROWS_PER_TXN; i++) {
            await db.exec(`INSERT INTO q VALUES (${txn * ROWS_PER_TXN + i})`);
        }
        await db.exec("COMMIT");
    }

    // Each push() lands exactly one batch and then errors on the next. With
    // TXNS batches outstanding, the first (TXNS-1) push() calls reject; the
    // last one (a single remaining batch) completes cleanly.
    let attempts = 0;
    while (attempts < TXNS) {
        attempts += 1;
        try {
            await db.push();
            break;
        } catch (e) {
            // expected for the first TXNS - 1 attempts
            if (attempts >= TXNS) {
                throw e;
            }
        }
    }
    expect(attempts).toBe(TXNS);

    // Every row pushed across the multiple attempts must be visible to a
    // fresh client, in the exact order they were inserted (no gaps, no
    // duplicates, no reordering across batch boundaries).
    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    const rows = await (await db2.prepare('SELECT x FROM q ORDER BY x')).all();
    const expected = Array.from({ length: TOTAL_ROWS }, (_, i) => ({ x: i }));
    expect(rows).toEqual(expected);
})

test('push failure leaves server state on transaction boundary', async ({ server }) => {
    // Seed: empty schema on the remote.
    {
        const db = await connect({ path: ':memory:', url: server.dbUrl() });
        await db.exec("CREATE TABLE q(x INTEGER PRIMARY KEY)");
        await db.push();
        await db.close();
    }

    // /v2/pipeline POST traffic from a single push() is:
    //   1) fetch_last_change_id  (SELECT)
    //   2) push batch #1         (BEGIN ... COMMIT for txn 1)
    //   3) push batch #2         (BEGIN ... COMMIT for txn 2)
    // Drop the 3rd /v2/pipeline POST so only batch #1 lands on the remote.
    let pipelinePosts = 0;
    const flaky: typeof fetch = async (input, init) => {
        if (init?.method === 'POST' && input.toString().endsWith('/v2/pipeline')) {
            pipelinePosts += 1;
            if (pipelinePosts === 3) {
                return new Response('drop', { status: 503 });
            }
        }
        return fetch(input, init);
    };

    const db = await connect({
        path: ':memory:',
        url: server.dbUrl(),
        pushOperationsThreshold: 5,
        fetch: flaky,
    });

    // Two distinct transactions, each exactly threshold-sized → one batch each.
    await db.exec("BEGIN");
    for (let i = 0; i < 5; i++) await db.exec(`INSERT INTO q VALUES (${i})`);
    await db.exec("COMMIT");
    await db.exec("BEGIN");
    for (let i = 5; i < 10; i++) await db.exec(`INSERT INTO q VALUES (${i})`);
    await db.exec("COMMIT");

    // Second batch fails — push must reject.
    await expect(db.push()).rejects.toThrow();

    // Read the remote via a clean client. The first transaction must be fully
    // present (rows 0..4); the second must not have leaked any rows
    // (transaction-boundary respected, not split mid-flight).
    const db2 = await connect({ path: ':memory:', url: server.dbUrl() });
    const rows = await (await db2.prepare('SELECT x FROM q ORDER BY x')).all();
    expect(rows).toEqual([{ x: 0 }, { x: 1 }, { x: 2 }, { x: 3 }, { x: 4 }]);
})

test('push hands the request body to fetch as bytes without inflating memory', async ({ server }) => {
    let bodyBytes = 0;
    let bodyIsBytes = false;
    const inspectingFetch: typeof fetch = (input, init) => {
        if (init?.method === 'POST' && init.body != null) {
            bodyIsBytes = init.body instanceof Uint8Array;
            bodyBytes = Math.max(bodyBytes, (init.body as Uint8Array).byteLength);
        }
        return fetch(input, init);
    };
    const db = await connect({ path: ':memory:', url: server.dbUrl(), fetch: inspectingFetch });
    await db.exec("CREATE TABLE IF NOT EXISTS big(x INTEGER PRIMARY KEY, y BLOB)");
    const blobBytes = 16 * 1024 * 1024;
    await db.exec(`INSERT INTO big VALUES (1, randomblob(${blobBytes}))`);

    const rssBefore = process.memoryUsage().rss;
    await db.push();
    const rssGrowth = process.memoryUsage().rss - rssBefore;

    expect(bodyIsBytes).toBe(true);
    expect(bodyBytes).toBeGreaterThan(blobBytes);
    // Before the body crossed the napi boundary as a Uint8Array it was marshalled as
    // a JS Array<number>, one element per byte, which alone costs 8 bytes per
    // body byte in V8 and put push at roughly 20x the change-set size.
    expect(rssGrowth).toBeLessThan(6 * bodyBytes);
    console.log(`push body=${(bodyBytes / 1024 / 1024).toFixed(1)}MiB rssGrowth=${(rssGrowth / 1024 / 1024).toFixed(0)}MiB (${(rssGrowth / bodyBytes).toFixed(1)}x)`);
}, 120000)
