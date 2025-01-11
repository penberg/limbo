import { createServer } from "vite";
import { chromium } from "playwright";

export async function setupTestEnvironment(port) {
  const server = await createServer({
    configFile: "./vite.config.js",
    root: ".",
    server: { port },
  });
  await server.listen();
  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();
  globalThis.__page__ = page;

  return { server, browser, context, page };
}

export async function teardownTestEnvironment({ server, browser, context }) {
  await context.close();
  await browser.close();
  await server.close();
}
