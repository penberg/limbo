export class VFS {
  constructor() {
    return self.vfs;
  }

  open(path, flags) {
    const result = self.vfs.open(path);
    consol.log("webvfs open result: ", result);
    consol.log("webvfs open result type: ", typeof result);
    return result;
  }

  close(fd) {
    return self.vfs.close(fd);
  }

  pread(fd, buffer, offset) {
    return self.vfs.pread(fd, buffer, offset);
  }

  pwrite(fd, buffer, offset) {
    return self.vfs.pwrite(fd, buffer, offset);
  }

  size(fd) {
    return self.vfs.size(fd);
  }

  sync(fd) {
    return self.vfs.sync(fd);
  }
}
