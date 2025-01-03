import { afterAll, afterEach, beforeAll, beforeEach } from "vitest";
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
      port: 5173,
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
