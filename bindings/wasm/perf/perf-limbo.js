import { run, bench, group, baseline } from 'mitata';

import { Database } from 'limbo-wasm';

const db = new Database('limbo.db');

db.exec('CREATE TABLE t (x)');
db.exec('INSERT INTO t VALUES (1)');
db.exec('INSERT INTO t VALUES (2)');
db.exec('INSERT INTO t VALUES (3)');

group('SQL queries [all()]', () => {
  const stmt1 = db.prepare("SELECT 1");
  bench('SELECT 1', () => {
    stmt1.all();
  });
  const stmt2 = db.prepare("SELECT * FROM t");
  bench('SELECT * FROM t', () => {
    stmt2.all();
  });
});

group('SQL queries [iterate()]', () => {
  const stmt1 = db.prepare("SELECT 1");
  const it1 = stmt1.iterate();
  bench('SELECT 1', () => {
    it1.next();
  });
  const stmt2 = db.prepare("SELECT * FROM t");
  const it2 = stmt2.iterate();
  bench('SELECT * FROM t', () => {
    it2.next();
  });
});

await run({
  units: false,
  silent: false,
  avg: true,
  json: false,
  colors: true,
  min_max: true,
  percentiles: true,
});
