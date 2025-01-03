// opfs-sync-proxy.js
let transferBuffer, statusBuffer, statusArray, statusView;
let transferArray;
let rootDir = null;
const handles = new Map();
let nextFd = 1;

self.postMessage("ready");

onmessage = async (e) => {
  log("handle message: ", e.data);
  if (e.data.cmd === "init") {
    log("init");
    transferBuffer = e.data.transferBuffer;
    statusBuffer = e.data.statusBuffer;

    transferArray = new Uint8Array(transferBuffer);
    statusArray = new Int32Array(statusBuffer);
    statusView = new DataView(statusBuffer);

    self.postMessage("done");
    return;
  }

  const result = await handleCommand(e.data);
  sendResult(result);
};

self.onerror = (error) => {
  console.error("opfssync error: ", error);
  // Don't close, keep running
  return true; // Prevents default error handling
};

function handleCommand(msg) {
  log(`handle message: ${msg.cmd}`);
  switch (msg.cmd) {
    case "open":
      return handleOpen(msg.path);
    case "close":
      return handleClose(msg.fd);
    case "read":
      return handleRead(msg.fd, msg.offset, msg.size);
    case "write":
      return handleWrite(msg.fd, msg.buffer, msg.offset);
    case "size":
      return handleSize(msg.fd);
    case "sync":
      return handleSync(msg.fd);
  }
}

async function handleOpen(path) {
  if (!rootDir) {
    rootDir = await navigator.storage.getDirectory();
  }
  const fd = nextFd++;

  const handle = await rootDir.getFileHandle(path, { create: true });
  const syncHandle = await handle.createSyncAccessHandle();

  handles.set(fd, syncHandle);
  return { fd };
}

function handleClose(fd) {
  const handle = handles.get(fd);
  handle.close();
  handles.delete(fd);
  return { success: true };
}

function handleRead(fd, offset, size) {
  const handle = handles.get(fd);
  const readBuffer = new ArrayBuffer(size);
  const readSize = handle.read(readBuffer, { at: offset });
  log("opfssync read: size: ", readBuffer.byteLength);

  const tmp = new Uint8Array(readBuffer);
  log("opfssync read buffer: ", [...tmp]);

  transferArray.set(tmp);

  return { success: true, length: readSize };
}

function handleWrite(fd, buffer, offset) {
  log("opfssync buffer size:", buffer.byteLength);
  log("opfssync write buffer: ", [...buffer]);
  const handle = handles.get(fd);
  const size = handle.write(buffer, { at: offset });
  return { success: true, length: size };
}

function handleSize(fd) {
  const handle = handles.get(fd);
  return { success: true, length: handle.getSize() };
}

function handleSync(fd) {
  const handle = handles.get(fd);
  handle.flush();
  return { success: true };
}

function sendResult(result) {
  if (result?.fd) {
    statusView.setInt32(4, result.fd, true);
  } else {
    log("opfs-sync-proxy: result.length: ", result.length);
    statusView.setInt32(4, result?.length || 0, true);
  }

  Atomics.store(statusArray, 0, 1);
  Atomics.notify(statusArray, 0);
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
