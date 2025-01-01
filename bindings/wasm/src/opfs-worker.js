import { VFS } from "./opfs.js";

const vfs = new VFS();

onmessage = async function (e) {
  if (!vfs.isReady) {
    console.log("opfs ready: ", vfs.isReady);
    await vfs.ready;
    console.log("opfs ready: ", vfs.isReady);
  }

  const { id, method, args } = e.data;

  console.log(`interface onmessage method: ${method}`);
  try {
    let result;
    switch (method) {
      case "open":
        result = vfs.open(args.path, args.flags);
        break;
      case "close":
        result = vfs.close(args.fd);
        break;
      case "pread": {
        const buffer = new Uint8Array(args.buffer);
        result = vfs.pread(args.fd, buffer, args.offset);
        self.postMessage(
          { id, size: result, error: null, buffer },
        );
        console.log("read size: ", result);
        console.log("read buffer: ", [...buffer]);
        return;
      }
      case "pwrite": {
        result = vfs.pwrite(args.fd, args.buffer, args.offset);
        console.log("write size: ", result);
        break;
      }
      case "size":
        result = vfs.size(args.fd);
        break;
      case "sync":
        result = vfs.sync(args.fd);
        break;
      default:
        throw new Error(`Unknown method: ${method}`);
    }

    self.postMessage(
      { id, result, error: null },
    );
  } catch (error) {
    self.postMessage({ id, result: null, error: error.message });
  }
};

console.log("opfs-worker.js");
// checkCompatibility();

// // In VFS class
// this.worker.onerror = (error) => {
//   console.error("Worker stack:", error.error?.stack || error.message);
// };

// checkCompatibility();
//
// async function checkCompatibility() {
//   console.log("begin check compatibility");
//   // OPFS API check
//   if (!("storage" in navigator && "getDirectory" in navigator.storage)) {
//     throw new Error("OPFS API not supported");
//   }
//
//   // SharedArrayBuffer support check
//   if (typeof SharedArrayBuffer === "undefined") {
//     throw new Error("SharedArrayBuffer not supported");
//   }
//
//   // Atomics API check
//   if (typeof Atomics === "undefined") {
//     throw new Error("Atomics API not supported");
//   }
//
//   // Permission check for OPFS
//   try {
//     const root = await navigator.storage.getDirectory();
//     await root.getFileHandle("test.txt", { create: true });
//   } catch (e) {
//     console.log(e);
//     console.log("throwing OPFS permission Denied");
//     throw new Error("OPFS permission denied");
//   }
//
//   // Cross-Origin-Isolation check for SharedArrayBuffer
//   if (!crossOriginIsolated) {
//     throw new Error("Cross-Origin-Isolation required for SharedArrayBuffer");
//   }
//
//   console.log("done check compatibility");
//   return true;
// }
