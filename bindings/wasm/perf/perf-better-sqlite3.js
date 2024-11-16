import { run, bench, group, baseline } from 'mitata';

import Database from 'better-sqlite3';

const db = new Database('limbo.db');

const stmt = db.prepare("SELECT 1");

group('Statement', () => {
  bench('SELECT 1', () => {
    stmt.all();
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
