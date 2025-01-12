const fs = require('node:fs');

class VFS {
  constructor() {
  }

  open(path, flags) {
    return fs.openSync(path, flags);
  }

  close(fd) {
    fs.closeSync(fd);
  }

  pread(fd, buffer, offset) {
    return fs.readSync(fd, buffer, 0, buffer.length, offset);
  }

  pwrite(fd, buffer, offset) {
    return fs.writeSync(fd, buffer, 0, buffer.length, offset);
  }

  size(fd) {
    let stats = fs.fstatSync(fd);
    return BigInt(stats.size);
  }

  sync(fd) {
    fs.fsyncSync(fd);
  }
}

module.exports = { VFS };
