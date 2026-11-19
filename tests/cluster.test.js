'use strict';
// Integration tests: real shard processes, real HTTP, real journals.
// These are slower (process spawn + fsync) but they are the ones that prove the
// distributed behaviour, not just the algebra.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Cluster } = require('./harness');
const { generate } = require('../scripts/workload');

function dataDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'as-int-'));
}

async function withCluster(fn, opts = {}) {
  const dir = dataDir();
  const c = await new Cluster({ dataDir: dir, ...opts }).start();
  try {
    return await fn(c, dir);
  } finally {
    await c.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('two shard processes converge to the exact truth, and merge path certifies', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 900, seed: 12 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const v = await c.publish('auto');
    assert.equal(v.exact.mergedLive, g.liveCount);
    assert.equal(v.decision.path, 'merge');
    assert.ok(v.exact.crossShardOverlap > 0, 'workload should contain shared keys');
    const ver = await c.verify();
    assert.equal(ver.algebra.equal, true);
    assert.ok(Math.abs(v.approximate.errorAgainstMergedExact.error.linear) / g.liveCount < 0.03);
  });
});

test('duplicate event ids and retried batches are not double counted', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 300, seed: 4, duplicateFraction: 0 });
    const batch = g.perShard['shard-a'].slice(0, 200);
    const first = await c.post(`${c.shardUrl('shard-a')}/events`, { events: batch });
    const second = await c.post(`${c.shardUrl('shard-a')}/events`, { events: batch });
    assert.equal(first.body.applied > 0, true);
    assert.equal(second.body.applied, 0);
    assert.equal(second.body.changed, 0);
    const st = await c.shardStatus('shard-a');
    assert.equal(st.exact.stats.duplicateReports, batch.length);
  });
});

test('delete on one shard only: lag detected, recompute used, repair restores merge path', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 800, seed: 21 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const v1 = await c.publish('auto');
    assert.equal(v1.decision.path, 'merge');

    const scn = await c.post(c.coordUrl('/scenario/divergent-deletes'), { count: 20 });
    assert.equal(scn.body.ok, true);
    assert.ok(scn.body.changed > 0);

    const v2 = await c.publish('auto');
    assert.ok(v2.exact.deletionLag.keys > 0);
    assert.equal(v2.decision.path, 'recompute');
    assert.ok(v2.exact.mergedLive < v1.exact.mergedLive);
    assert.ok(v2.approximate.mergeUnaudited.estimates.linear > v2.approximate.published.estimates.linear);

    const rep = await c.repair();
    assert.equal(rep.ok, true);
    const v3 = await c.publish('auto');
    assert.equal(v3.exact.deletionLag.keys, 0);
    assert.equal(v3.decision.path, 'merge');
    assert.equal(v3.exact.mergedLive, v2.exact.mergedLive);
  });
});

test('a frozen shard is reused as stale, never as empty', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 500, seed: 33 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const v1 = await c.publish('auto');

    await c.post(`${c.shardUrl('shard-b')}/admin/freeze`, {});
    const out = await c.collect('auto');
    assert.equal(out.collected['shard-b'].fresh, false);
    assert.equal(out.version.collect['shard-b'].fresh, false);
    const b = out.version.exact.perShard.find((p) => p.shardId === 'shard-b');
    assert.ok(b.live > 0, 'frozen shard must still contribute its last known set');
    assert.equal(out.version.exact.mergedLive, v1.exact.mergedLive);

    await c.post(`${c.shardUrl('shard-b')}/admin/unfreeze`, {});
  });
});

test('an old summary retransmitted after the shard moved on is rejected', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 200, seed: 55 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    await c.publish('auto');
    await c.post(c.coordUrl('/scenario/stash'), {});
    await c.ingest('shard-a', [
      { id: 'after-stash', op: 'add', key: 'new_key_after_stash', hlc: { wall: Date.now(), seq: 0, node: 'test' } },
    ]);
    await c.publish('auto');
    const replay = await c.post(c.coordUrl('/scenario/replay-old'), { shardId: 'shard-a' });
    assert.equal(replay.body.result.accepted, false);
    assert.equal(replay.body.result.reason, 'stale-generation');
    // and the deleted/live state did not change because of the replay
    const v = await c.publish('auto');
    assert.ok(v.exact.mergedLive >= 1);
  });
});

test('shard killed and restarted replays its journal to the same live set', { timeout: 90000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 600, seed: 66 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const before = await c.shardStatus('shard-a');
    const v1 = await c.publish('auto');

    await c.restartShard('shard-a');
    const after = await c.shardStatus('shard-a');
    assert.equal(after.exact.live, before.exact.live);
    assert.equal(after.generation, before.generation);

    const v2 = await c.publish('auto');
    assert.equal(v2.exact.mergedLive, v1.exact.mergedLive);
  });
});

test('coordinator crash loses no committed version and exposes no partial one', { timeout: 90000 }, async () => {
  await withCluster(async (c, dir) => {
    const g = generate({ entities: 300, seed: 77 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const v1 = await c.publish('auto');

    await c.crashCoordinator();
    await c.restartCoordinator();
    const s = await c.state();
    assert.equal(s.latest.id, v1.id);
    assert.equal(s.latest.exact.mergedLive, v1.exact.mergedLive);

    const v2 = await c.publish('auto');
    assert.equal(v2.exact.mergedLive, v1.exact.mergedLive);
  });
});

test('events ingested after a bundle are counted on the next publish exactly once', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 300, seed: 88 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    const v1 = await c.publish('auto');
    const partsBefore = v1.exact.perShard.reduce((n, p) => n + p.live, 0);

    await c.ingest('shard-a', [
      { id: 'late-1', op: 'add', key: 'late_entity_1', hlc: { wall: Date.now(), seq: 0, node: 'test' } },
    ]);
    const v2 = await c.publish('auto');
    const partsAfter = v2.exact.perShard.reduce((n, p) => n + p.live, 0);
    assert.equal(partsAfter, partsBefore + 1);

    const v3 = await c.publish('auto');
    const partsThird = v3.exact.perShard.reduce((n, p) => n + p.live, 0);
    assert.equal(partsThird, partsAfter, 'repeated publish must not recount');
  });
});

test('the web page and reader state are served from the coordinator', { timeout: 60000 }, async () => {
  await withCluster(async (c) => {
    const g = generate({ entities: 200, seed: 99 });
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    await c.publish('auto');
    const page = await fetch(c.coordUrl('/'));
    assert.equal(page.status, 200);
    const html = await page.text();
    assert.match(html, /Active-set merge lab/);
    const s = await c.state();
    assert.ok(s.latest.exact && s.latest.approximate);
    assert.ok(Array.isArray(s.history));
    assert.ok(s.shards.length === 2);
    assert.ok(s.observations.length >= 1);
  });
});
