'use strict';
// The small probabilistic structures used by a shard's approximate snapshot.
//
// All of them are *mergeable* (union-only) and none of them supports a decrement.
// That is the point of the exercise: deletions are handled above this layer by
// epoch-scoped rebuilding, not by subtracting.

const { toBytes } = require('./hash');

// ---------------------------------------------------------------------------
// Linear counting: one bit per hash bucket.  Union = bitwise OR.
// Estimate n = -m * ln(V / m), V = empty buckets.
// ---------------------------------------------------------------------------
class LinearCounter {
  constructor(bits) {
    this.m = bits;
    this.bytes = new Uint8Array((bits + 7) >> 3);
    this.set = 0;
  }

  static fromEmptyBits(bits) {
    return new LinearCounter(bits);
  }

  addHash(h) {
    const bit = Number(h % BigInt(this.m));
    const byte = bit >> 3;
    const mask = 1 << (bit & 7);
    if ((this.bytes[byte] & mask) === 0) {
      this.bytes[byte] |= mask;
      this.set += 1;
    }
  }

  add(value) {
    const { hash64 } = require('./hash');
    this.addHash(hash64(toBytes(value)));
  }

  merge(other) {
    const out = new LinearCounter(this.m);
    out.set = 0;
    for (let i = 0; i < this.bytes.length; i += 1) {
      const v = this.bytes[i] | other.bytes[i];
      out.bytes[i] = v;
      if (v) out.set += POPCOUNT[v];
    }
    return out;
  }

  estimate() {
    const m = this.m;
    // Only the first m bits are meaningful; padding bits are never set, so the
    // count of set bits within m is exactly `set` when m is a multiple of 8.
    const emptyInM = m - countSetWithin(this.bytes, m);
    if (emptyInM <= 0) return { estimate: Infinity, empty: 0, saturated: true, m };
    if (emptyInM === m) return { estimate: 0, empty: m, saturated: false, m };
    const est = -m * Math.log(emptyInM / m);
    return { estimate: est, empty: emptyInM, saturated: false, m };
  }

  serialize() {
    return { kind: 'linear', m: this.m, bits: Buffer.from(this.bytes).toString('base64') };
  }

  static deserialize(s) {
    const lc = new LinearCounter(s.m);
    const buf = Buffer.from(s.bits, 'base64');
    lc.bytes.set(buf);
    lc.set = 0;
    for (const b of lc.bytes) lc.set += POPCOUNT[b];
    return lc;
  }
}

const POPCOUNT = new Uint8Array(256);
for (let i = 0; i < 256; i += 1) {
  POPCOUNT[i] = (i & 1) + POPCOUNT[i >> 1];
}

function countSetWithin(bytes, m) {
  const fullBytes = m >> 3;
  let c = 0;
  for (let i = 0; i < fullBytes; i += 1) c += POPCOUNT[bytes[i]];
  const rem = m & 7;
  if (rem) {
    const mask = (1 << rem) - 1;
    c += POPCOUNT[bytes[fullBytes] & mask];
  }
  return c;
}

// ---------------------------------------------------------------------------
// KMV (k minimum values / bottom-k): exact arithmetic mean of the k smallest
// hash values turns into a cardinality estimate.  Union = keep k smallest.
// ---------------------------------------------------------------------------
class KMV {
  constructor(k) {
    this.k = k;
    this.heap = []; // max-heap on BigInt, size <= k
  }

  addBig(h) {
    if (this.heap.length < this.k) {
      this.heapPush(h);
      return;
    }
    if (h < this.heap[0]) {
      this.heap[0] = h;
      this.heapDown(0);
    }
  }

  add(value) {
    const { hash64 } = require('./hash');
    this.addBig(hash64(toBytes(value)));
  }

  heapPush(h) {
    this.heap.push(h);
    this.heapUp(this.heap.length - 1);
  }

  heapUp(i) {
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.heap[p] >= this.heap[i]) break;
      [this.heap[p], this.heap[i]] = [this.heap[i], this.heap[p]];
      i = p;
    }
  }

  heapDown(i) {
    const n = this.heap.length;
    for (;;) {
      const l = i * 2 + 1;
      const r = l + 1;
      let big = i;
      if (l < n && this.heap[l] > this.heap[big]) big = l;
      if (r < n && this.heap[r] > this.heap[big]) big = r;
      if (big === i) break;
      [this.heap[big], this.heap[i]] = [this.heap[i], this.heap[big]];
      i = big;
    }
  }

  merge(other) {
    const out = new KMV(this.k);
    const seen = new Set();
    const total = this.heap.length + other.heap.length;
    const values = new Array(total);
    let n = 0;
    for (const h of this.heap) values[n++] = h;
    for (const h of other.heap) values[n++] = h;
    values.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    for (const h of values) {
      if (seen.has(h)) continue; // same hash in both heaps = same entity
      seen.add(h);
      out.addBig(h);
      if (out.heap.length >= this.k && h > out.heap[0]) break;
    }
    return out;
  }

  estimate() {
    const n = this.heap.length;
    if (n === 0) return { estimate: 0, k: this.k, filled: 0 };
    const sorted = [...this.heap].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const kth = Number(sorted[sorted.length - 1]) / 2 ** 64; // largest of the k minima
    const kEff = n;
    if (kth >= 1) return { estimate: Infinity, k: this.k, filled: n, saturated: true };
    const est = (kEff - 1) / kth;
    return { estimate: est, k: this.k, filled: n, kth };
  }

  serialize() {
    return {
      kind: 'kmv',
      k: this.k,
      values: this.heap.map((h) => h.toString(16)),
    };
  }

  static deserialize(s) {
    const kv = new KMV(s.k);
    for (const hex of s.values) kv.addBig(BigInt('0x' + hex));
    return kv;
  }
}

// ---------------------------------------------------------------------------
// HyperLogLog: m registers, each holding a max rank (rho).  Union = max.
// Standard bias-corrected estimate with linear counting for small cardinalities.
// ---------------------------------------------------------------------------
class HyperLogLog {
  constructor(p) {
    this.p = p;
    this.m = 1 << p;
    this.registers = new Uint8Array(this.m);
  }

  addHash(h) {
    const idx = Number(h >> BigInt(64 - this.p));
    // remaining 64-p bits: count leading zeros + 1
    const rest = h & ((1n << BigInt(64 - this.p)) - 1n);
    let rho = 1;
    const bits = 64 - this.p;
    if (rest === 0n) {
      rho = bits + 1;
    } else {
      rho = bits - BigInt(rest).toString(2).length + 1;
    }
    if (rho > this.registers[idx]) this.registers[idx] = rho;
  }

  add(value) {
    const { hash64 } = require('./hash');
    this.addHash(hash64(toBytes(value)));
  }

  merge(other) {
    const out = new HyperLogLog(this.p);
    for (let i = 0; i < this.m; i += 1) {
      out.registers[i] = Math.max(this.registers[i], other.registers[i]);
    }
    return out;
  }

  estimate() {
    const m = this.m;
    let sum = 0;
    let zeros = 0;
    for (const r of this.registers) {
      sum += 2 ** -r;
      if (r === 0) zeros += 1;
    }
    const alpha = m === 16 ? 0.673 : m === 32 ? 0.697 : m === 64 ? 0.709 : 0.7213 / (1 + 1.079 / m);
    let est = (alpha * m * m) / sum;
    if (est <= 2.5 * m && zeros > 0) {
      est = m * Math.log(m / zeros);
    }
    return { estimate: est, m, zeros };
  }

  serialize() {
    return { kind: 'hll', p: this.p, registers: Buffer.from(this.registers).toString('base64') };
  }

  static deserialize(s) {
    const h = new HyperLogLog(s.p);
    h.registers.set(Buffer.from(s.registers, 'base64'));
    return h;
  }
}

module.exports = { LinearCounter, KMV, HyperLogLog };
