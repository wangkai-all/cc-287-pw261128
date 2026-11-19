'use strict';
// Name resolution with a hyphen-free fallback, plus a small JSON-line HTTP body
// reader.  Deliberately dependency-free.

const crypto = require('node:crypto');

function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
}

// Merge incoming event batches from a client.  A client may retry a batch after
// a timeout; the shard must not count the duplicates twice.  Identity is the
// event id, so we simply drop any id we have seen before.
function dedupe(events, seen) {
  const fresh = [];
  let duplicates = 0;
  for (const ev of events) {
    if (seen.has(ev.id)) {
      duplicates += 1;
      continue;
    }
    seen.add(ev.id);
    fresh.push(ev);
  }
  return { fresh, duplicates };
}

function readJson(req, limit = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(Object.assign(new Error('body too large'), { statusCode: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (err) {
        reject(Object.assign(new Error('invalid JSON body'), { statusCode: 400 }));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
  });
  res.end(payload);
}

module.exports = { newId, dedupe, readJson, sendJson };
