'use strict';
// Append-only event journal.
//
// A batch is written and fsync'd once, so acknowledging a batch is honest:
// every acknowledged event is durable.  A crash can leave at most one torn
// trailing line; replay stops there, and the client retries the unacknowledged
// tail (event ids and versions make that safe).

const fs = require('node:fs');
const path = require('node:path');

class Journal {
  constructor(file) {
    this.file = file;
    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.fd = fs.openSync(file, 'a');
    this.count = 0;
    this.bytesWritten = fs.fstatSync(this.fd).size;
  }

  bytes() {
    try {
      return fs.fstatSync(this.fd).size;
    } catch {
      return 0;
    }
  }

  appendMany(records) {
    if (records.length === 0) return;
    const payload = records.map((r) => JSON.stringify(r)).join('\n') + '\n';
    const buf = Buffer.from(payload, 'utf8');
    let off = 0;
    while (off < buf.length) off += fs.writeSync(this.fd, buf, off, buf.length - off);
    fs.fsyncSync(this.fd);
    this.count += records.length;
    this.bytesWritten += buf.length;
  }

  static readAll(file) {
    let raw;
    try {
      raw = fs.readFileSync(file, 'utf8');
    } catch (err) {
      if (err.code === 'ENOENT') return [];
      throw err;
    }
    const out = [];
    for (const line of raw.split('\n')) {
      if (!line.trim()) continue;
      try {
        out.push(JSON.parse(line));
      } catch {
        break; // torn tail after a crash; everything before it is durable
      }
    }
    return out;
  }

  close() {
    try {
      fs.fsyncSync(this.fd);
    } catch {
      /* ignore */
    }
    try {
      fs.closeSync(this.fd);
    } catch {
      /* ignore */
    }
  }
}

module.exports = { Journal };
