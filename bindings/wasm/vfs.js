const fs = require('node:fs');

class VFS {
  constructor() {
  }

  open(path) {
    return fs.openSync(path, 'r');
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
}

module.exports = { VFS };