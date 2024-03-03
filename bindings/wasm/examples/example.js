import { Database } from 'limbo-wasm';

const db = new Database(':memory:');

db.exec("SELECT 'hello, world' AS message");
