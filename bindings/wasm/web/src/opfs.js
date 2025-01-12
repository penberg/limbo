// First file: VFS class
class VFS {
  constructor() {
    this.transferBuffer = new SharedArrayBuffer(1024 * 1024); // 1mb
    this.statusBuffer = new SharedArrayBuffer(8); // Room for status + size

    this.statusArray = new Int32Array(this.statusBuffer);
    this.statusView = new DataView(this.statusBuffer);

    this.worker = new Worker(
      new URL("./opfs-sync-proxy.js", import.meta.url),
      { type: "module" },
    );

    this.isReady = false;
    this.ready = new Promise((resolve, reject) => {
      this.worker.addEventListener("message", async (e) => {
        if (e.data === "ready") {
          await this.initWorker();
          this.isReady = true;
          resolve();
        }
      }, { once: true });
      this.worker.addEventListener("error", reject, { once: true });
    });

    this.worker.onerror = (e) => {
      console.error("Sync proxy worker error:", e.message);
    };
  }

  initWorker() {
    return new Promise((resolve) => {
      this.worker.addEventListener("message", (e) => {
        log("eventListener: ", e.data);
        resolve();
      }, { once: true });

      this.worker.postMessage({
        cmd: "init",
        transferBuffer: this.transferBuffer,
        statusBuffer: this.statusBuffer,
      });
    });
  }

  open(path) {
    Atomics.store(this.statusArray, 0, 0);
    this.worker.postMessage({ cmd: "open", path });
    Atomics.wait(this.statusArray, 0, 0);

    const result = this.statusView.getInt32(4, true);
    log("opfs.js open result: ", result);
    log("opfs.js open result type: ", typeof result);

    return result;
  }

  close(fd) {
    Atomics.store(this.statusArray, 0, 0);
    this.worker.postMessage({ cmd: "close", fd });
    Atomics.wait(this.statusArray, 0, 0);
    return true;
  }

  pread(fd, buffer, offset) {
    let bytesRead = 0;

    while (bytesRead < buffer.byteLength) {
      const chunkSize = Math.min(
        this.transferBuffer.byteLength,
        buffer.byteLength - bytesRead,
      );

      Atomics.store(this.statusArray, 0, 0);
      this.worker.postMessage({
        cmd: "read",
        fd,
        offset: offset + bytesRead,
        size: chunkSize,
      });

      Atomics.wait(this.statusArray, 0, 0);
      const readSize = this.statusView.getInt32(4, true);
      buffer.set(
        new Uint8Array(this.transferBuffer, 0, readSize),
        bytesRead,
      );
      log("opfs pread buffer: ", [...buffer]);

      bytesRead += readSize;
      if (readSize < chunkSize) break;
    }

    return bytesRead;
  }

  pwrite(fd, buffer, offset) {
    log("write buffer size: ", buffer.byteLength);
    Atomics.store(this.statusArray, 0, 0);
    this.worker.postMessage({
      cmd: "write",
      fd,
      buffer: buffer,
      offset: offset,
    });

    Atomics.wait(this.statusArray, 0, 0);
    log(
      "opfs pwrite length statusview: ",
      this.statusView.getInt32(4, true),
    );
    return this.statusView.getInt32(4, true);
  }

  size(fd) {
    Atomics.store(this.statusArray, 0, 0);
    this.worker.postMessage({ cmd: "size", fd });
    Atomics.wait(this.statusArray, 0, 0);

    const result = this.statusView.getInt32(4, true);
    log("opfs.js size result: ", result);
    log("opfs.js size result type: ", typeof result);
    return BigInt(result);
  }

  sync(fd) {
    Atomics.store(this.statusArray, 0, 0);
    this.worker.postMessage({ cmd: "sync", fd });
    Atomics.wait(this.statusArray, 0, 0);
  }
}

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

export { VFS };
