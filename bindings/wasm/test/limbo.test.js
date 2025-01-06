import { expect, test } from "vitest";

test("basic database operations", async () => {
  await globalThis.beforeEachPromise;
  const page = globalThis.__page__;
  await page.goto("http://localhost:5173/limbo-test.html");

  page.on("console", (msg) => console.log(msg.text()));

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
  console.log("test results: ", result);
  console.log("test results: ", result.result[0]);
  expect(result.result).toHaveLength(1);
  expect(result.result[0]).toEqual([1, "Alice", "alice@example.org"]);
  // expect(1).toEqual(1);
});
