export class VFSInterface {
  constructor(workerUrl) {
    this.worker = new Worker(workerUrl, { type: "module" });
    this.nextMessageId = 1;
    this.pendingRequests = new Map();

    this.worker.onmessage = (event) => {
      console.log("interface onmessage: ", event.data);
      let { id, result, error } = event.data;
      const resolver = this.pendingRequests.get(id);
      if (event.data?.buffer && event.data?.size) {
        result = { size: event.data.size, buffer: event.data.buffer };
      }

      if (resolver) {
        this.pendingRequests.delete(id);
        if (error) {
          resolver.reject(new Error(error));
        } else {
          resolver.resolve(result);
        }
      }
    };
  }

  _sendMessage(method, args) {
    const id = this.nextMessageId++;
    return new Promise((resolve, reject) => {
      this.pendingRequests.set(id, { resolve, reject });
      this.worker.postMessage({ id, method, args });
    });
  }

  async open(path, flags) {
    return await this._sendMessage("open", { path, flags });
  }

  async close(fd) {
    return await this._sendMessage("close", { fd });
  }

  async pwrite(fd, buffer, offset) {
    return await this._sendMessage("pwrite", { fd, buffer, offset }, [
      buffer.buffer,
    ]);
  }

  async pread(fd, buffer, offset) {
    console.log("interface in buffer: ", [...buffer]);
    const result = await this._sendMessage("pread", {
      fd,
      buffer: buffer,
      offset,
    }, []);
    console.log("interface out buffer: ", [...buffer]);
    buffer.set(new Uint8Array(result.buffer));
    return buffer.length;
  }

  async size(fd) {
    return await this._sendMessage("size", { fd });
  }

  async sync(fd) {
    return await this._sendMessage("sync", { fd });
  }
}
