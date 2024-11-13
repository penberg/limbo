import { Database } from 'limbo-wasm';

const db = new Database('hello.db');

console.log(db.exec("SELECT 'hello, world' AS message"));
