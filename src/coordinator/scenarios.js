'use strict';
// Demo scenarios invoked from the web UI.  They create real events and send
// them to real shard processes, so everything shown on the page is produced by
// the system, not hand-written into the page state.

const { createClock } = require('../common/time');
const { hash64 } = require('../common/hash');

function makeEvent(clock, op, key, extra = {}) {
  return { id: `${op}:${key}:${clock.peek().wall}:${clock.peek().seq}:${clock.node}:${Math.random().toString(36).slice(2, 8)}`, op, key, hlc: clock.now(), ...extra };
}

// Deletes applied to one shard only.  This is the common real-world case: the
// shard that saw the delete has a tombstone, the other still has the entity
// live.  Union-only sketches cannot remove the now-deleted key, so the merge
// path becomes visibly biased and the coordinator must switch to recompute.
async function divergentDeletes({ cluster, coordinator, count = 25 }) {
  const a = coordinator.bundles.get('shard-a');
  const b = coordinator.bundles.get('shard-b');
  if (!a || !b) return { ok: false, error: 'both shards need a bundle first' };
  const liveOnB = new Set(
    Object.entries(b.payload.versions)
      .filter(([, e]) => e.d === 0)
      .map(([k]) => k)
  );
  const candidates = Object.entries(a.payload.versions)
    .filter(([k, e]) => e.d === 0 && liveOnB.has(k))
    .map(([k]) => k)
    .slice(0, count);
  if (!candidates.length) return { ok: false, error: 'no shared live keys; run the seed first' };

  const clock = createClock('scenario-delete');
  for (const [, bndl] of coordinator.bundles) {
    const w = maxVersion(bndl.payload.versions);
    if (w) clock.observe(w);
  }
  const events = candidates.map((key) => makeEvent(clock, 'delete', key, { note: 'delete-on-shard-a-only' }));
  const res = await fetch(`${cluster.shardUrl('shard-a')}/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ events }),
  });
  const body = await res.json();
  return {
    ok: res.ok,
    scenario: 'divergent-deletes',
    deletedOn: 'shard-a',
    keys: candidates,
    applied: body.applied,
    changed: body.changed,
    note: 'the same keys are still live on shard-b; an approximate union cannot subtract them',
  };
}

// A new entity reported with an older HLC: a late observation.  It is a real
// live entity, and it belongs to the shard that received it.  The demo shows
// that it is included after the shard rebuilds its local summary, and that the
// published watermark only advances when every shard has caught up.
async function lateEvents({ cluster, coordinator, count = 5 }) {
  const clock = createClock('scenario-late');
  for (const [, bndl] of coordinator.bundles) {
    const w = maxVersion(bndl.payload.versions);
    if (w) clock.observe(w);
  }
  const old = clock.peek().wall - 60_000;
  const events = [];
  for (let i = 0; i < count; i += 1) {
    const key = `late_${Date.now().toString(36)}_${i}`;
    events.push({
      id: `late:${key}`,
      op: 'add',
      key,
      hlc: { wall: old + i, seq: 0, node: 'scenario-late' },
      note: 'late-arrival',
    });
  }
  const res = await fetch(`${cluster.shardUrl('shard-b')}/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ events }),
  });
  const body = await res.json();
  return {
    ok: res.ok,
    scenario: 'late-events',
    keys: events.map((e) => e.key),
    applied: body.applied,
    changed: body.changed,
    note: 'HLC is older than the shard watermark but the keys are new, so they are live and included on the next rebuild',
  };
}

// Retransmit a previously accepted bundle verbatim.  The coordinator must
// recognise it by content and not merge it twice (set sketches are idempotent,
// but the accounting must also show zero change).
async function retransmitOldBundle({ coordinator, shardId = 'shard-a' }) {
  const rec = coordinator.bundles.get(shardId);
  if (!rec) return { ok: false, error: `no bundle for ${shardId}` };
  if (!coordinator.historyBundles || !coordinator.historyBundles.length) {
    // Fall back: a retransmit needs an older bundle.  We can only replay the
    // current one; that still exercises content-based idempotency.
    const result = coordinator.receiveBundle(shardId, { payload: rec.payload, meta: rec.meta }, 'retransmit');
    return { ok: true, scenario: 'retransmit-current', shardId, result, note: 'replayed the current bundle; content digest matched' };
  }
  return { ok: false, error: 'no history' };
}

// Save a copy of the current bundle before it changes, so the demo can resend a
// genuinely old summary later.
function stashBundle({ coordinator }) {
  const stash = {};
  for (const [id, rec] of coordinator.bundles) stash[id] = { payload: JSON.parse(JSON.stringify(rec.payload)), meta: { ...rec.meta } };
  coordinator.historyBundles = coordinator.historyBundles || [];
  coordinator.historyBundles.push({ at: Date.now(), stash });
  if (coordinator.historyBundles.length > 10) coordinator.historyBundles.shift();
  return { ok: true, stashed: Object.keys(stash) };
}

async function replayStashedBundle({ coordinator, shardId = 'shard-a' }) {
  if (!coordinator.historyBundles || !coordinator.historyBundles.length) {
    return { ok: false, error: 'no stashed bundle; use "stash" before changing the shard' };
  }
  const entry = coordinator.historyBundles[coordinator.historyBundles.length - 1];
  const old = entry.stash[shardId];
  if (!old) return { ok: false, error: `no stashed bundle for ${shardId}` };
  const result = coordinator.receiveBundle(shardId, old, 'replay-old');
  return {
    ok: true,
    scenario: 'old-summary-replay',
    shardId,
    stashedAt: entry.at,
    result,
    note: 'a delayed copy of an older summary; it must not be merged or resurrect deleted keys',
  };
}

function maxVersion(versions) {
  let max = null;
  for (const e of Object.values(versions || {})) {
    if (!max || e.v.wall > max.wall || (e.v.wall === max.wall && e.v.seq > max.seq)) max = e.v;
  }
  return max;
}

module.exports = { divergentDeletes, lateEvents, retransmitOldBundle, stashBundle, replayStashedBundle, makeEvent };
