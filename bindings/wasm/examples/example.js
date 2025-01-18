import { Database } from 'limbo-wasm';

const db = new Database('hello.db');

db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

db.exec('INSERT INTO users (name) VALUES (\'Alice\')');

const stmt = db.prepare('SELECT * FROM users');

const users = stmt.all();

console.log(users);
