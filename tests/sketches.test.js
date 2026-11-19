'use strict';
// Unit tests for the approximate structures themselves: mergeability, absence of
// a decrement path, and accuracy inside the documented bands.

const test = require('node:test');
const assert = require('node:assert/strict');
const { LinearCounter, KMV, HyperLogLog } = require('../src/common/sketches');
const { Summary, mergeSummaries } = require('../src/common/summary');
const { hash64 } = require('../src/common/hash');
const { generate } = require('../scripts/workload');
const { mulberry32 } = require('../scripts/workload');

const keys = (n, prefix = 'k') => Array.from({ length: n }, (_, i) => `${prefix}${i}`);

test('linear counter: union is exact set union', () => {
  const a = new LinearCounter(1 << 14);
  const b = new LinearCounter(1 << 14);
  const left = keys(400, 'a');
  const right = keys(400, 'b');
  for (const k of left) a.addHash(hash64(k));
  for (const k of right) b.addHash(hash64(k));
  const union = a.merge(b);
  const direct = new LinearCounter(1 << 14);
  for (const k of [...new Set([...left, ...right])]) direct.addHash(hash64(k));
  assert.deepEqual([...union.bytes], [...direct.bytes]);
  assert.equal(union.estimate().empty, direct.estimate().empty);
});

test('linear counter: duplicate occurrences count once', () => {
  const lc = new LinearCounter(1 << 12);
  for (let i = 0; i < 50; i += 1) for (let r = 0; r < 7; r += 1) lc.addHash(hash64(`dup${i}`));
  const est = lc.estimate().estimate;
  assert.ok(est > 45 && est < 55, `expected ~50, got ${est}`);
});

test('linear counter: no decrement API exists', () => {
  const lc = new LinearCounter(1024);
  assert.equal(typeof lc.remove, 'undefined');
  assert.equal(typeof lc.subtract, 'undefined');
});

test('KMV: union keeps bottom-k across overlapping heaps', () => {
  const a = new KMV(64);
  const b = new KMV(64);
  const left = keys(500, 'x');
  const right = keys(300, 'y');
  for (const k of left) a.add(k);
  for (const k of right) b.add(k);
  const merged = a.merge(b);
  const direct = new KMV(64);
  for (const k of [...new Set([...left, ...right])]) direct.add(k);
  assert.deepEqual(
    merged.heap.map(String).sort(),
    direct.heap.map(String).sort(),
    'merged bottom-k must equal bottom-k of the union'
  );
});

test('KMV: duplicate hash values do not corrupt the bottom-k set', () => {
  const overlap = keys(200, 'same');
  const a = new KMV(16);
  const b = new KMV(16);
  for (const k of overlap) a.add(k);
  for (const k of overlap.slice(100)) b.add(k);
  const merged = a.merge(b);
  const direct = new KMV(16);
  for (const k of overlap) direct.add(k);
  assert.deepEqual(merged.heap.map(String).sort(), direct.heap.map(String).sort());
});

test('HLL: union is register-wise max and matches direct build', () => {
  const a = new HyperLogLog(10);
  const b = new HyperLogLog(10);
  const left = keys(800, 'p');
  const right = keys(600, 'q');
  for (const k of left) a.add(k);
  for (const k of right) b.add(k);
  const merged = a.merge(b);
  const direct = new HyperLogLog(10);
  for (const k of [...new Set([...left, ...right])]) direct.add(k);
  assert.deepEqual([...merged.registers], [...direct.registers]);
});

test('HLL: relative error inside the usual 1.04/sqrt(m) band on 2000 entities', () => {
  const hll = new HyperLogLog(12);
  const ks = keys(2000, 'hll');
  for (const k of ks) hll.add(k);
  const est = hll.estimate().estimate;
  const rel = Math.abs(est - 2000) / 2000;
  assert.ok(rel < 0.04, `relative error ${rel}`);
});

test('all three sketches recover the correct union size on split input', () => {
  const g = generate({ entities: 2500, seed: 91 });
  const perShard = g.perShard;
  const summaries = Object.entries(perShard).map(([, evs]) => {
    const live = new Map();
    for (const e of evs) {
      const cur = live.get(e.key);
      if (!cur || e.hlc.wall > cur.hlc.wall || (e.hlc.wall === cur.hlc.wall && e.op === 'delete')) live.set(e.key, e);
    }
    const set = new Set();
    for (const [k, e] of live) if (e.op !== 'delete') set.add(k);
    return { keys: [...set], payload: Summary.fromKeys([...set], { linearBits: 1 << 15, kmvK: 512, hllP: 12 }).serialize() };
  });
  const unionKeys = new Set();
  for (const s of summaries) for (const k of s.keys) unionKeys.add(k);
  const merged = mergeSummaries(summaries.map((s) => s.payload)).estimates;
  const direct = Summary.fromKeys([...unionKeys], { linearBits: 1 << 15, kmvK: 512, hllP: 12 }).estimates();
  assert.deepEqual(merged, direct);
  const rel = Math.abs(merged.linear - unionKeys.size) / unionKeys.size;
  assert.ok(rel < 0.03, `linear relative error ${rel}`);
});

test('summaries are sealed to the exact key set they were built from', () => {
  const a = Summary.fromKeys(keys(100, 's'));
  const b = Summary.fromKeys(keys(100, 's'));
  assert.equal(a.exactDigest, b.exactDigest);
  const c = Summary.fromKeys(keys(100, 'other'));
  assert.notEqual(a.exactDigest, c.exactDigest);
});

test('serialization round-trip preserves estimates', () => {
  const rnd = mulberry32(5);
  const ks = Array.from({ length: 700 }, (_, i) => `rt${i}-${Math.floor(rnd() * 1e9)}`);
  const s = Summary.fromKeys(ks, { linearBits: 1 << 13, kmvK: 128, hllP: 10 });
  const back = Summary.deserialize(JSON.parse(JSON.stringify(s.serialize())));
  assert.deepEqual(back.estimates(), s.estimates());
  assert.equal(back.exactDigest, s.exactDigest);
  assert.equal(back.digest(), s.digest());
});
