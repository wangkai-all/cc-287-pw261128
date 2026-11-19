'use strict';
// A 64-bit hash and a canonical encoding, shared by every component.
//
// Two hashes are used deliberately:
//   * `exactDigest` (SHA-256, truncated) is only for event-id / payload
//     identity in the exact and control planes;
//   * `hash64` (FNV-1a + splitmix64 finaliser) feeds the approximations, where
//     collision resistance beyond 64 bits buys nothing.
// Approximations never see raw keys, so the low-level merge code cannot
// accidentally depend on key ordering.

const crypto = require('node:crypto');

const MASK64 = (1n << 64n) - 1n;
const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;

function hash64(value) {
  const bytes = toBytes(value);
  let h = FNV_OFFSET;
  for (let i = 0; i < bytes.length; i += 1) {
    h ^= BigInt(bytes[i]);
    h = (h * FNV_PRIME) & MASK64;
  }
  h ^= h >> 30n;
  h = (h * 0xbf58476d1ce4e5b9n) & MASK64;
  h ^= h >> 27n;
  h = (h * 0x94d049bb133111ebn) & MASK64;
  h ^= h >> 31n;
  return h;
}

function exactDigest(value) {
  return crypto.createHash('sha256').update(toBytes(value)).digest('hex').slice(0, 24);
}

function toBytes(value) {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  return Buffer.from(String(value), 'utf8');
}

// Order-independent digest of a key set: sort, then hash.  Used to bind a
// summary to the exact live set it was built from.
function digestOfKeys(keys) {
  const sorted = [...keys].sort();
  const h = crypto.createHash('sha256');
  for (const k of sorted) {
    h.update(k);
    h.update('\u0000');
  }
  return h.digest('hex').slice(0, 24);
}

module.exports = { hash64, exactDigest, digestOfKeys, toBytes, MASK64 };
