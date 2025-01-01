import { VFS } from "./opfs.js";
import init, { Database } from "./../pkg/limbo_wasm.js";

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
          console.log(e.data.sql);
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

