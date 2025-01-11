import { afterAll, beforeAll, beforeEach, expect, test } from "vitest";
import { setupTestEnvironment, teardownTestEnvironment } from "./helpers.js";

let testEnv;

beforeAll(async () => {
  testEnv = await setupTestEnvironment(5174);
});

beforeEach(async () => {
  const { page } = testEnv;
  await page.goto("http://localhost:5174/limbo-test.html");
});

afterAll(async () => {
  await teardownTestEnvironment(testEnv);
});

test("basic database operations", async () => {
  const { page } = testEnv;
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

