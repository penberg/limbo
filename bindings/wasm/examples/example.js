import { Database } from 'limbo-wasm';

const db = new Database('hello.db');

const stmt = db.prepare('SELECT * FROM users');

const users = stmt.all();

console.log(users);
