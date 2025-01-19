import { VFS } from "./opfs.js";
import init, { Database } from "../dist/index.js";

let db = null;
let currentStmt = null;

async function initVFS() {
  const vfs = new VFS();
  await vfs.ready;
  self.vfs = vfs;
  return vfs;
}

async function initAll() {
  await initVFS();
  await init();
}

initAll().then(() => {
  self.postMessage({ type: "ready" });

  self.onmessage = (e) => {
    try {
      switch (e.data.op) {
        case "createDb": {
          db = new Database(e.data.path);
          self.postMessage({ type: "success", op: "createDb" });
          break;
        }
        case "exec": {
          log(e.data.sql);
          db.exec(e.data.sql);
          self.postMessage({ type: "success", op: "exec" });
          break;
        }
        case "prepare": {
          currentStmt = db.prepare(e.data.sql);
          const results = currentStmt.raw().all();
          self.postMessage({ type: "result", result: results });
          break;
        }
        case "get": {
          const row = currentStmt?.raw().get();
          self.postMessage({ type: "result", result: row });
          break;
        }
      }
    } catch (err) {
      self.postMessage({ type: "error", error: err.toString() });
    }
  };
}).catch((error) => {
  self.postMessage({ type: "error", error: error.toString() });
});

// logLevel:
//
// 0 = no logging output
// 1 = only errors
// 2 = warnings and errors
// 3 = debug, warnings, and errors
const logLevel = 1;

const loggers = {
  0: console.error.bind(console),
  1: console.warn.bind(console),
  2: console.log.bind(console),
};
const logImpl = (level, ...args) => {
  if (logLevel > level) loggers[level]("OPFS asyncer:", ...args);
};
const log = (...args) => logImpl(2, ...args);
const warn = (...args) => logImpl(1, ...args);
const error = (...args) => logImpl(0, ...args);
