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
