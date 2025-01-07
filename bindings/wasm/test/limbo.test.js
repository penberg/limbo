import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  expect,
  test,
} from "vitest";
import { chromium } from "playwright";
import { createServer } from "vite";

let browser;
let context;
let page;
let server;

beforeAll(async () => {
  server = await createServer({
    configFile: "./vite.config.js",
    root: ".",
    server: {
      port: 5174,
    },
  });
  await server.listen();
  browser = await chromium.launch();
});

beforeEach(async () => {
  context = await browser.newContext();
  page = await context.newPage();
  globalThis.__page__ = page;
});

afterEach(async () => {
  await context.close();
});

afterAll(async () => {
  await browser.close();
  await server.close();
});

test("basic database operations", async () => {
  const page = globalThis.__page__;
  await page.goto("http://localhost:5174/limbo-test.html");

  const result = await page.evaluate(async () => {
    const worker = new Worker("./src/limbo-worker.js", { type: "module" });

    const waitForMessage = (type, op) =>
      new Promise((resolve, reject) => {
        const handler = (e) => {
          if (e.data.type === type && (!op || e.data.op === op)) {
            worker.removeEventListener("message", handler);
            resolve(e.data);
          } else if (e.data.type === "error") {
            worker.removeEventListener("message", handler);
            reject(e.data.error);
          }
        };
        worker.addEventListener("message", handler);
      });

    try {
      await waitForMessage("ready");
      worker.postMessage({ op: "createDb", path: "test.db" });
      await waitForMessage("success", "createDb");

      worker.postMessage({
        op: "exec",
        sql:
          "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
      });
      await waitForMessage("success", "exec");

      worker.postMessage({
        op: "exec",
        sql: "INSERT INTO users VALUES (1, 'Alice', 'alice@example.org');",
      });
      await waitForMessage("success", "exec");

      worker.postMessage({
        op: "prepare",
        sql: "SELECT * FROM users;",
      });

      const results = await waitForMessage("result");
      return results;
    } catch (error) {
      return { error: error.message };
    }
  });

  if (result.error) throw new Error(`Test failed: ${result.error}`);
  expect(result.result).toHaveLength(1);
  expect(result.result[0]).toEqual([1, "Alice", "alice@example.org"]);
});
