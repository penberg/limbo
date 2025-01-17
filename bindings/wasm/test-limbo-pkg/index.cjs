// import { Database } from "limbo-wasm/node";

const { Database } = require("limbo-wasm");
// Rest of your code...

const db = new Database("test.db");
db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
db.exec("INSERT INTO users (name) VALUES ('test')");

const stmt = db.prepare("SELECT * FROM users");
console.log(stmt.all());
