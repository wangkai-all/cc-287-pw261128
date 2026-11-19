'use strict';
// Exact merge algebra used by the coordinator.
//
// Every shard bundle carries a version map { key -> { v: HLC, d: 0|1 } }.
// Merging is last-writer-wins per key, by HLC.  This gives three things the
// approximate plane cannot give on its own:
//
//   1. the deduplicated exact active set (the baseline the UI compares against),
//   2. a proof that a stale/retransmitted bundle cannot resurrect a delete:
//      a tombstone with a higher version always wins the key,
//   3. an exact accounting of *deletion lag*: keys that one shard still sees
//      live after another shard has already tombstoned them.  Sketch union
//      cannot subtract those keys, so the coordinator reports them as a known
//      systematic bias instead of pretending the union is exact.
//
// Nothing here decrements a count per occurrence.  A key is in the merged live
// set iff the winning version for it is an add, no matter how many add reports
// any shard recorded.

const { compareHlc } = require('./time');
const { digestOfKeys } = require('./hash');

function winner(a, b) {
  if (!a) return b;
  if (!b) return a;
  const c = compareHlc(a.v, b.v);
  if (c !== 0) return c > 0 ? a : b;
  // Identical version: deletion wins the tie deterministically.
  if (a.d !== b.d) return a.d === 1 ? a : b;
  return a;
}

// bundles: [{ shardId, payload: { versions, liveCount, exactDigest, ... } }]
function mergeBundles(bundles) {
  const merged = new Map(); // key -> { v, d, from }
  const perShard = [];
  const unionLive = new Set();
  const shardLive = new Map();

  for (const b of bundles) {
    const p = b.payload;
    const live = new Set();
    for (const [key, entry] of Object.entries(p.versions || {})) {
      const n = { v: entry.v, d: entry.d, from: b.shardId };
      const cur = merged.get(key);
      const w = winner(cur, n);
      merged.set(key, w);
      if (entry.d === 0) live.add(key);
    }
    for (const k of live) unionLive.add(k);
    shardLive.set(b.shardId, live);
    perShard.push({
      shardId: b.shardId,
      generation: p.generation,
      live: live.size,
      versions: Object.keys(p.versions || {}).length,
      exactDigest: p.exactDigest,
      summaryDigest: p.summaryDigest,
    });
  }

  const mergedLive = new Set();
  const mergedDeleted = new Set();
  let versionConflicts = 0;
  let membershipConflicts = 0;
  const divergent = []; // live on some shard, deleted in merged resolution
  const divergentByShard = {};
  const conflictSample = [];

  // For every key, look at each shard's own entry to detect disagreement.
  const keyStates = new Map(); // key -> [ {shardId, d, v} ]
  for (const b of bundles) {
    for (const [key, entry] of Object.entries(b.payload.versions || {})) {
      if (!keyStates.has(key)) keyStates.set(key, []);
      keyStates.get(key).push({ shardId: b.shardId, d: entry.d, v: entry.v });
    }
  }

  for (const [key, w] of merged) {
    if (w.d === 0) mergedLive.add(key);
    else mergedDeleted.add(key);

    const states = keyStates.get(key) || [];
    let sawLive = false;
    let sawDeleted = false;
    let versionsDiffer = false;
    for (const s of states) {
      if (s.d === 0) sawLive = true;
      else sawDeleted = true;
      if (compareHlc(s.v, w.v) !== 0) versionsDiffer = true;
    }
    if (sawLive && sawDeleted) {
      membershipConflicts += 1;
      if (w.d === 1) {
        divergent.push(key);
        for (const s of states) {
          if (s.d === 0) divergentByShard[s.shardId] = (divergentByShard[s.shardId] || 0) + 1;
        }
        if (conflictSample.length < 12) {
          conflictSample.push({ key, reason: 'live-on-a-shard-but-tombstoned-in-merge', winningShard: w.from, states });
        }
      }
    }
    if (versionsDiffer) versionConflicts += 1;
  }

  const sumShardLive = perShard.reduce((n, s) => n + s.live, 0);
  return {
    mergedLive,
    mergedDeleted,
    mergedLiveKeys: [...mergedLive].sort(),
    mergedDeletedKeys: [...mergedDeleted].sort(),
    unionLive,
    perShard,
    stats: {
      keys: merged.size,
      mergedLive: mergedLive.size,
      mergedDeleted: mergedDeleted.size,
      sumShardLive,
      unionLive: unionLive.size,
      crossShardOverlap: sumShardLive - unionLive.size,
      dedupSavings: sumShardLive - mergedLive.size,
      membershipConflicts,
      versionConflicts,
      deletionLagKeys: divergent.length,
    },
    deletionLag: {
      keys: divergent.length,
      byShard: divergentByShard,
      sample: conflictSample,
    },
    // Expected number of extra hashes present in the union of per-shard
    // summaries, relative to the merged exact set.
    unionBias: unionLive.size - mergedLive.size,
  };
}

function digestMatches(payload) {
  const live = [];
  for (const [key, entry] of Object.entries(payload.versions || {})) if (entry.d === 0) live.push(key);
  return digestOfKeys(live) === payload.exactDigest;
}

module.exports = { mergeBundles, digestMatches, winner };
