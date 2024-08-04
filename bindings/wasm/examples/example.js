import { Database } from 'limbo-wasm';

const db = new Database('hello.db');

db.exec("SELECT 'hello, world' AS message");
