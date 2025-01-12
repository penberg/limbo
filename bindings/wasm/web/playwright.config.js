// Using Playwright (recommended)
import { expect, test } from "@playwright/test";

// playwright.config.js
export default {
  use: {
    headless: true,
    // Required for SharedArrayBuffer
    launchOptions: {
      args: ["--cross-origin-isolated"],
    },
  },
};
