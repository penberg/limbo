// test/opfs.test.js
import { afterAll, beforeAll, beforeEach, expect, test } from "vitest";
import { setupTestEnvironment, teardownTestEnvironment } from "./helpers.js";

let testEnv;

beforeAll(async () => {
  testEnv = await setupTestEnvironment(5173);
});

beforeEach(async () => {
  const { page } = testEnv;
  await page.goto("http://localhost:5173/index.html");
  await page.waitForFunction(() => window.VFSInterface !== undefined);
});

afterAll(async () => {
  await teardownTestEnvironment(testEnv);
});

const opfsImport = "./src/opfs-worker.js";

test("basic read/write functionality", async () => {
  const { page } = testEnv;
  const result = await page.evaluate(async () => {
    const vfs = new window.VFSInterface("./src/opfs-worker.js");
    let fd;
    try {
      fd = await vfs.open("test.txt", {});
      const writeData = new Uint8Array([1, 2, 3, 4]);
      const bytesWritten = await vfs.pwrite(fd, writeData, 0);
      const readData = new Uint8Array(4);
      const bytesRead = await vfs.pread(fd, readData, 0);
      await vfs.close(fd);
      return { fd, bytesWritten, bytesRead, readData: Array.from(readData) };
    } catch (error) {
      if (fd !== undefined) await vfs.close(fd);
      return { error: error.message };
    }
  });

  if (result.error) throw new Error(`Test failed: ${result.error}`);
  expect(result.fd).toBe(1);
  expect(result.bytesWritten).toBe(4);
  expect(result.bytesRead).toBe(4);
  expect(result.readData).toEqual([1, 2, 3, 4]);
});

test("larger data read/write", async () => {
  const { page } = testEnv;
  const result = await page.evaluate(async () => {
    const vfs = new window.VFSInterface("./src/opfs-worker.js");
    let fd;
    try {
      fd = await vfs.open("large.txt", {});
      const writeData = new Uint8Array(1024).map((_, i) => i % 256);
      const bytesWritten = await vfs.pwrite(fd, writeData, 0);
      const readData = new Uint8Array(1024);
      const bytesRead = await vfs.pread(fd, readData, 0);
      await vfs.close(fd);
      return { bytesWritten, bytesRead, readData: Array.from(readData) };
    } catch (error) {
      if (fd !== undefined) await vfs.close(fd);
      return { error: error.message };
    }
  });

  if (result.error) throw new Error(`Test failed: ${result.error}`);
  expect(result.bytesWritten).toBe(1024);
  expect(result.bytesRead).toBe(1024);
  expect(result.readData).toEqual(
    Array.from({ length: 1024 }, (_, i) => i % 256),
  );
});

test("partial reads and writes", async () => {
  const { page } = testEnv;
  const result = await page.evaluate(async () => {
    const vfs = new window.VFSInterface("./src/opfs-worker.js");
    let fd;
    try {
      fd = await vfs.open("partial.txt", {});

      const writeData1 = new Uint8Array([1, 2, 3, 4]);
      const writeData2 = new Uint8Array([5, 6, 7, 8]);
      await vfs.pwrite(fd, writeData1, 0);
      await vfs.pwrite(fd, writeData2, 4);

      const readData1 = new Uint8Array(2);
      const readData2 = new Uint8Array(4);
      const readData3 = new Uint8Array(2);

      await vfs.pread(fd, readData1, 0);
      await vfs.pread(fd, readData2, 2);
      await vfs.pread(fd, readData3, 6);

      await vfs.close(fd);
      return {
        readData1: Array.from(readData1),
        readData2: Array.from(readData2),
        readData3: Array.from(readData3),
      };
    } catch (error) {
      if (fd !== undefined) await vfs.close(fd);
      return { error: error.message };
    }
  });

  if (result.error) throw new Error(`Test failed: ${result.error}`);
  expect(result.readData1).toEqual([1, 2]);
  expect(result.readData2).toEqual([3, 4, 5, 6]);
  expect(result.readData3).toEqual([7, 8]);
});

test("file size operations", async () => {
  const { page } = testEnv;
  const result = await page.evaluate(async () => {
    const vfs = new window.VFSInterface("./src/opfs-worker.js");
    let fd;
    try {
      fd = await vfs.open("size.txt", {});
      const writeData1 = new Uint8Array([1, 2, 3, 4]);
      await vfs.pwrite(fd, writeData1, 0);
      const size1 = await vfs.size(fd);

      const writeData2 = new Uint8Array([5, 6, 7, 8]);
      await vfs.pwrite(fd, writeData2, 4);
      const size2 = await vfs.size(fd);

      await vfs.close(fd);
      return { size1, size2 };
    } catch (error) {
      if (fd !== undefined) await vfs.close(fd);
      return { error: error.message };
    }
  });

  if (result.error) throw new Error(`Test failed: ${result.error}`);
  expect(Number(result.size1)).toBe(4);
  expect(Number(result.size2)).toBe(8);
});
