'use strict';
// The three union-only sketches as one "summary" object.
//
// None of these structures can remove an element.  That is a property of the
// data structures, not a shortcut, so the system never tries to subtract:
// deletion is handled by rebuilding the summary from the exact live set.
//
// A summary is sealed with a digest of the exact key set it was built from, so
// the coordinator can verify "this summary really corresponds to these keys"
// rather than trusting a timestamp.

const { digestOfKeys } = require('./hash');
const { canonical } = require('./time');
const { LinearCounter, KMV, HyperLogLog } = require('./sketches');
const { createHash } = require('node:crypto');

const DEFAULTS = Object.freeze({ linearBits: 1 << 16, kmvK: 512, hllP: 12 });

class Summary {
  constructor(opts = {}) {
    this.opts = { ...DEFAULTS, ...opts };
    this.linear = new LinearCounter(this.opts.linearBits);
    this.kmv = new KMV(this.opts.kmvK);
    this.hll = new HyperLogLog(this.opts.hllP);
    this.exactDigest = null;
    this.count = 0;
  }

  static fromKeys(keys, opts = {}) {
    const s = new Summary(opts);
    const { hash64 } = require('./hash');
    for (const key of keys) {
      const h = hash64(key);
      s.linear.addHash(h);
      s.kmv.addBig(h);
      s.hll.addHash(h);
      s.count += 1;
    }
    s.exactDigest = digestOfKeys(keys);
    return s;
  }

  estimates() {
    return {
      linear: round(this.linear.estimate().estimate),
      kmv: round(this.kmv.estimate().estimate),
      hll: round(this.hll.estimate().estimate),
    };
  }

  serialize() {
    return {
      exactDigest: this.exactDigest,
      count: this.count,
      linear: this.linear.serialize(),
      kmv: this.kmv.serialize(),
      hll: this.hll.serialize(),
    };
  }

  digest() {
    return createHash('sha256').update(canonical(this.serialize())).digest('hex').slice(0, 16);
  }

  static deserialize(payload) {
    const s = new Summary();
    s.linear = LinearCounter.deserialize(payload.linear);
    s.kmv = KMV.deserialize(payload.kmv);
    s.hll = HyperLogLog.deserialize(payload.hll);
    s.exactDigest = payload.exactDigest || null;
    s.count = payload.count || 0;
    s.opts = { linearBits: s.linear.m, kmvK: s.kmv.k, hllP: s.hll.p };
    return s;
  }
}

// Union of summary payloads.  `Summary.merge` is the sketch-level union; it is
// only a correct set summary if the inputs describe disjoint or identical key
// sets.  The coordinator checks that condition against exact version maps
// before publishing any merged estimate.
function mergeSummaries(payloads) {
  let linear = null;
  let kmv = null;
  let hll = null;
  for (const p of payloads) {
    const s = Summary.deserialize(p);
    linear = linear ? linear.merge(s.linear) : s.linear;
    kmv = kmv ? kmv.merge(s.kmv) : s.kmv;
    hll = hll ? hll.merge(s.hll) : s.hll;
  }
  return {
    estimates: {
      linear: round(linear.estimate().estimate),
      kmv: round(kmv.estimate().estimate),
      hll: round(hll.estimate().estimate),
    },
    bytes: payloads.reduce((n, p) => n + Buffer.byteLength(JSON.stringify(p)), 0),
  };
}

// Estimates are reported at 3 decimals so that "equal" in the verification
// output means "equal at published precision", not "bitwise equal float".
function round(v) {
  return Number.isFinite(v) ? Math.round(v * 1000) / 1000 : null;
}

function serializeBytes(summary) {
  return Buffer.byteLength(JSON.stringify(summary.serialize()));
}

module.exports = { Summary, mergeSummaries, serializeBytes, DEFAULTS };
