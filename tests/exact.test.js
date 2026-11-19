'use strict';
// Exact-state tests: versioning, tombstones, idempotent retries, and the
// guarantee that a stale add never resurrects a deleted entity.

const test = require('node:test');
const assert = require('node:assert/strict');
const { ShardState } = require('../src/shard/state');
const { Shard } = require('../src/shard/shard');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

let seq = 0;
const ev = (op, key, wall, node = 'n1', extra = {}) => ({
  id: extra.id || `e${++seq}`,
  op,
  key,
  hlc: { wall, seq: extra.seq || 0, node },
});

test('an entity with several add reports is one member of the live set', () => {
  const s = new ShardState('t');
  s.applyEvent(ev('add', 'u1', 10));
  s.applyEvent(ev('add', 'u1', 20));
  s.applyEvent(ev('add', 'u1', 30));
  assert.equal(s.liveKeys().length, 1);
  assert.equal(s.entities.get('u1').addReports, 3);
});

test('delete of a multi-occurrence entity is a tombstone, not a decrement', () => {
  const s = new ShardState('t');
  s.applyEvent(ev('add', 'u1', 10));
  s.applyEvent(ev('add', 'u1', 20));
  s.applyEvent(ev('add', 'u1', 30));
  const res = s.applyEvent(ev('delete', 'u1', 40));
  assert.equal(res.changed, true);
  assert.equal(s.liveKeys().length, 0);
  assert.equal(s.deletedKeys().length, 1);
});

test('retrying the same event id changes nothing', () => {
  const s = new ShardState('t');
  const e = ev('add', 'u1', 10, 'n1', { id: 'fixed-id' });
  const a = s.applyEvent(e);
  const b = s.applyEvent({ ...e });
  assert.equal(a.applied, true);
  assert.equal(b.applied, false);
  assert.equal(b.kind, 'duplicate-report');
  assert.equal(s.entities.get('u1').addReports, 1);
});

test('a stale add cannot resurrect a deleted entity', () => {
  const s = new ShardState('t');
  s.applyEvent(ev('add', 'u1', 10));
  s.applyEvent(ev('delete', 'u1', 50));
  const stale = s.applyEvent(ev('add', 'u1', 30));
  assert.equal(stale.applied, false);
  assert.equal(stale.kind, 'stale');
  assert.equal(s.liveKeys().length, 0);
  assert.equal(s.stats.correctionsIgnored, 1);
});

test('a newer add after a delete is a genuine re-creation, and only once', () => {
  const s = new ShardState('t');
  s.applyEvent(ev('add', 'u1', 10));
  s.applyEvent(ev('delete', 'u1', 50));
  const re = s.applyEvent(ev('add', 'u1', 60));
  assert.equal(re.kind, 'recreate');
  assert.equal(s.liveKeys().length, 1);
  // replay of that same newer add (same version) does nothing
  const dup = s.applyEvent({ ...ev('add', 'u1', 60), id: 'different-id-same-version' });
  assert.equal(dup.applied, false);
  assert.equal(s.liveKeys().length, 1);
});

test('a duplicate delete is idempotent', () => {
  const s = new ShardState('t');
  s.applyEvent(ev('add', 'u1', 10));
  s.applyEvent(ev('delete', 'u1', 50));
  const again = s.applyEvent(ev('delete', 'u1', 55));
  assert.equal(again.changed, false);
  assert.equal(again.kind, 'repeat-tombstone');
});

test('apply order does not change the final live set', () => {
  const events = [
    ev('add', 'a', 10),
    ev('add', 'b', 11),
    ev('delete', 'a', 30),
    ev('add', 'a', 20),
    ev('add', 'c', 12),
    ev('delete', 'c', 40),
    ev('add', 'c', 25),
  ];
  const forward = new ShardState('f');
  for (const e of events) forward.applyEvent(e);
  const backward = new ShardState('b');
  for (const e of [...events].reverse()) backward.applyEvent(e);
  assert.deepEqual(forward.liveKeys().sort(), backward.liveKeys().sort());
});

test('journal replay reproduces state and generation without double counting', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'as-state-'));
  try {
    const a = new Shard('s1', { dataDir: dir });
    a.ingest([
      { id: 'j1', op: 'add', key: 'k1', hlc: { wall: 10, seq: 0, node: 's1' } },
      { id: 'j2', op: 'add', key: 'k2', hlc: { wall: 11, seq: 0, node: 's1' } },
      { id: 'j3', op: 'add', key: 'k1', hlc: { wall: 12, seq: 0, node: 's1' } },
      { id: 'j4', op: 'delete', key: 'k1', hlc: { wall: 13, seq: 0, node: 's1' } },
    ]);
    const before = a.status();
    a.close();
    const b = new Shard('s1', { dataDir: dir });
    const loaded = b.load();
    const after = b.status();
    assert.equal(after.exact.live, before.exact.live);
    assert.equal(after.generation, before.generation);
    assert.equal(loaded.records, 4);
    // retrying the exact same batch after reboot must not change anything
    const retry = b.ingest([
      { id: 'j1', op: 'add', key: 'k1', hlc: { wall: 10, seq: 0, node: 's1' } },
      { id: 'j4', op: 'delete', key: 'k1', hlc: { wall: 13, seq: 0, node: 's1' } },
    ]);
    assert.equal(retry.every((r) => !r.applied), true);
    assert.equal(b.status().exact.live, before.exact.live);
    b.close();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('rebuilding a summary after a delete excludes the tombstoned key', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'as-summary-'));
  try {
    const s = new Shard('s1', { dataDir: dir, sketchOptions: { linearBits: 1 << 12, kmvK: 64, hllP: 8 } });
    s.ingest(
      Array.from({ length: 100 }, (_, i) => ({
        id: `a${i}`,
        op: 'add',
        key: `k${i}`,
        hlc: { wall: 10 + i, seq: 0, node: 's1' },
      }))
    );
    const before = s.bundle('test');
    assert.ok(Math.abs(before.payload.summary.count - 100) < 1e-9);
    s.ingest([{ id: 'del', op: 'delete', key: 'k0', hlc: { wall: 1000, seq: 0, node: 's1' } }]);
    const after = s.bundle('test');
    assert.equal(after.payload.summary.count, 99);
    assert.notEqual(after.payload.exactDigest, before.payload.exactDigest);
    s.close();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
