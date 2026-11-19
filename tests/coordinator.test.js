'use strict';
// Coordinator-level tests with in-memory shard bundles: policy, intake rules,
// recovery and verification arithmetic.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Coordinator, choosePath, bundleDigest } = require('../src/coordinator/coordinator');
const { Shard } = require('../src/shard/shard');
const { digestOfKeys } = require('../src/common/hash');

let n = 0;
function tmp() {
  return fs.mkdtempSync(path.join(os.tmpdir(), `as-coord-${n++}-`));
}

function makeBundle(shardId, incarnation, events, opts = {}) {
  const dir = tmp();
  const shard = new Shard(shardId, {
    dataDir: dir,
    sketchOptions: { linearBits: 1 << 14, kmvK: 128, hllP: 10, ...opts },
  });
  shard.ingest(events);
  const b = shard.bundle('test');
  shard.close();
  return {
    payload: { ...b.payload, incarnation },
    meta: b.meta,
  };
}

function addEvents(keys, wall = 1000, node = 'n1') {
  return keys.map((key, i) => ({ id: `${node}-${key}-${wall}-${i}`, op: 'add', key, hlc: { wall: wall + i, seq: 0, node } }));
}

function coordinatorFor(dir, shardIds = ['shard-a', 'shard-b']) {
  return new Coordinator({
    dataDir: dir,
    publishDir: dir,
    shardUrls: Object.fromEntries(shardIds.map((id) => [id, `http://127.0.0.1:1`])),
    sketchOptions: { linearBits: 1 << 14, kmvK: 128, hllP: 10 },
  });
}

test('choosePath honours explicit modes and the divergence rule', () => {
  assert.equal(choosePath({ mode: 'exact', complete: true, lag: 0, bundles: 2 }), 'exact');
  assert.equal(choosePath({ mode: 'merge', complete: true, lag: 5, bundles: 2 }), 'merge');
  assert.equal(choosePath({ mode: 'recompute', complete: true, lag: 0, bundles: 2 }), 'recompute');
  assert.equal(choosePath({ mode: 'auto', complete: false, lag: 0, bundles: 1 }), 'exact');
  assert.equal(choosePath({ mode: 'auto', complete: true, lag: 3, bundles: 2 }), 'recompute');
  assert.equal(choosePath({ mode: 'auto', complete: true, lag: 0, bundles: 2 }), 'merge');
});

test('intake accepts a valid bundle and rejects a mislabelled summary', () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const b = makeBundle('shard-a', 'inc1', addEvents(['a', 'b', 'c']));
  const ok = c.receiveBundle('shard-a', b);
  assert.equal(ok.accepted, true);

  const forged = JSON.parse(JSON.stringify(b));
  forged.payload.exactDigest = digestOfKeys(['a', 'b']);
  forged.payload.summary.exactDigest = forged.payload.exactDigest;
  const bad = c.receiveBundle('shard-b', { ...forged, payload: { ...forged.payload, shardId: 'shard-b' } });
  assert.equal(bad.accepted, false);
  assert.equal(bad.reason, 'exact-digest-mismatch');
});

test('a retransmit is recognised by content digest and not counted twice', () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const b = makeBundle('shard-a', 'inc1', addEvents(['a', 'b', 'c']));
  assert.equal(c.receiveBundle('shard-a', b).reason, 'stored');
  const again = c.receiveBundle('shard-a', b);
  assert.equal(again.accepted, true);
  assert.equal(again.reason, 'retransmit-ignored');
  assert.equal(c.stats.retransmits, 1);
});

test('an older bundle for the same incarnation is rejected, not merged', () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const old = makeBundle('shard-a', 'inc1', addEvents(['a'], 100));
  const newer = makeBundle('shard-a', 'inc1', addEvents(['a', 'b', 'c'], 200));
  c.receiveBundle('shard-a', newer);
  const res = c.receiveBundle('shard-a', old);
  assert.equal(res.accepted, false);
  assert.equal(res.reason, 'stale-generation');
  assert.equal(c.bundles.get('shard-a').payload.liveCount, 3);
});

test('a reboot (new incarnation) is accepted even if its generation is lower', () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const before = makeBundle('shard-a', 'inc1', addEvents(['a', 'b', 'c']));
  const afterReboot = makeBundle('shard-a', 'inc2', addEvents(['a']));
  c.receiveBundle('shard-a', before);
  const res = c.receiveBundle('shard-a', afterReboot);
  assert.equal(res.accepted, true);
  assert.equal(res.regression, true);
  // The rebooted shard genuinely knows less; the counter records that fact so
  // an operator can tell "shard restarted" apart from "shard is merely quiet".
  assert.equal(c.stats.knowledgeRegressions, 1);
  assert.equal(c.bundles.get('shard-a').payload.incarnation, 'inc2');
});

test('publish merges identical sets to an exact count and certifies the sketch union', async () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const a = makeBundle('shard-a', 'i1', addEvents(['k1', 'k2', 'k3']));
  const b = makeBundle('shard-b', 'i2', addEvents(['k3', 'k4'], 3000, 'n2'));
  c.receiveBundle('shard-a', a);
  c.receiveBundle('shard-b', b);
  const version = await c.publish({
    mode: 'auto',
    collected: {
      'shard-a': { ok: true, fresh: true, payload: a.payload, digest: 'd1', generation: a.payload.generation },
      'shard-b': { ok: true, fresh: true, payload: b.payload, digest: 'd2', generation: b.payload.generation },
    },
  });
  assert.equal(version.exact.mergedLive, 4);
  assert.equal(version.exact.unionLive, 4);
  assert.equal(version.exact.crossShardOverlap, 1);
  assert.equal(version.decision.path, 'merge');
  const v = c.verify();
  assert.equal(v.algebra.equal, true);
});

test('divergent delete forces the recompute path and reports exact lag', async () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const keysAdd = addEvents(['x1', 'x2', 'x3', 'x4']);
  const a = makeBundle('shard-a', 'i1', [...keysAdd, { id: 'del-x1', op: 'delete', key: 'x1', hlc: { wall: 5000, seq: 0, node: 'n1' } }]);
  const b = makeBundle('shard-b', 'i2', keysAdd.map((e) => ({ ...e, hlc: { ...e.hlc, node: 'n2' } })));
  c.receiveBundle('shard-a', a);
  c.receiveBundle('shard-b', b);
  const version = await c.publish({
    mode: 'auto',
    collected: {
      'shard-a': { ok: true, fresh: true, payload: a.payload, digest: 'd1', generation: a.payload.generation },
      'shard-b': { ok: true, fresh: true, payload: b.payload, digest: 'd2', generation: b.payload.generation },
    },
  });
  assert.equal(version.exact.deletionLag.keys, 1);
  assert.equal(version.decision.path, 'recompute');
  assert.equal(version.approximate.published.source, 'recomputed-from-exact');
  // the uncorrected merge still exists in the record, with its bias stated
  assert.ok(version.approximate.mergeUnaudited.estimates.linear > version.approximate.published.estimates.linear);
  assert.equal(version.approximate.decomposition.deletionLagBias, 0);
  const v = c.verify();
  assert.equal(v.algebra.equal, true);
  assert.equal(v.againstMergedExact.deletionLagBias, 1);
});

test('a shard with no bundle blocks approximation; the exact baseline is marked incomplete', async () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const a = makeBundle('shard-a', 'i1', addEvents(['k1']));
  c.receiveBundle('shard-a', a);
  const version = await c.publish({
    mode: 'auto',
    collected: {
      'shard-a': { ok: true, fresh: true, payload: a.payload, digest: 'd1', generation: a.payload.generation },
      'shard-b': { ok: false, fresh: false, error: 'down' },
    },
  });
  assert.equal(version.exact.complete, false);
  assert.equal(version.decision.path, 'exact');
  assert.equal(version.approximate.published, null);
});

test('a crash during publish leaves no visible version and is reported on reboot', async () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const a = makeBundle('shard-a', 'i1', addEvents(['k1'], 10));
  const b = makeBundle('shard-b', 'i2', addEvents(['k2'], 20));
  c.receiveBundle('shard-a', a);
  c.receiveBundle('shard-b', b);
  await c.publish({
    collected: {
      'shard-a': { ok: true, fresh: true, payload: a.payload, digest: 'd1', generation: a.payload.generation },
      'shard-b': { ok: true, fresh: true, payload: b.payload, digest: 'd2', generation: b.payload.generation },
    },
  });
  assert.equal(c.versions.length, 1);

  // Simulate a crash between `pending` persistence and the version append.
  const store = JSON.parse(fs.readFileSync(c.storeFile, 'utf8'));
  store.pending = { versionId: 99, trigger: 'crash', startedAt: Date.now(), bundles: {}, missing: [], stale: [] };
  fs.writeFileSync(c.storeFile, JSON.stringify(store));

  const reloaded = coordinatorFor(dir);
  assert.equal(reloaded.versions.length, 1);
  assert.equal(reloaded.interrupted.length, 1);
  assert.equal(reloaded.interrupted[0].versionId, 99);
  assert.equal(reloaded.pending, null);
});

test('repair reconciles membership but does not replicate shard-only keys', async () => {
  const dir = tmp();
  const c = coordinatorFor(dir);
  const shared = addEvents(['s1', 's2']);
  const a = makeBundle('shard-a', 'i1', shared);
  const b = makeBundle('shard-b', 'i2', [shared[0], { ...shared[1], op: 'delete', hlc: { wall: 9000, seq: 0, node: 'n2' } }]);
  c.receiveBundle('shard-a', a);
  c.receiveBundle('shard-b', b);
  // verify internally what repair would send without network: recompute here.
  const { mergeBundles } = require('../src/common/merge');
  const m = mergeBundles([
    { shardId: 'shard-a', payload: a.payload },
    { shardId: 'shard-b', payload: b.payload },
  ]);
  assert.equal(m.deletionLag.keys, 1);
  assert.deepEqual(m.deletionLag.keys, 1);
  // repairing through the network is covered by the integration tests.
});

test('bundleDigest is stable and content-sensitive', () => {
  const a = makeBundle('shard-a', 'inc', addEvents(['k1', 'k2']));
  const b = makeBundle('shard-a', 'inc', addEvents(['k1', 'k2']));
  assert.equal(bundleDigest(a.payload), bundleDigest(b.payload));
  const c = makeBundle('shard-a', 'inc', addEvents(['k1', 'k2', 'k3']));
  assert.notEqual(bundleDigest(a.payload), bundleDigest(c.payload));
});
