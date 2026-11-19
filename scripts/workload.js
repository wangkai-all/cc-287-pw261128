'use strict';
// Deterministic demo workload generator.
//
// Routing model (this is what makes the cluster's numbers well-defined):
//   * an entity is *owned* by the shard chosen by hash(key); all of its
//     deletes are delivered to the owner;
//   * some observations of an entity are also reported by the other shard
//     (multi-device / multi-session), and deletes are fanned out too, so the
//     same entity is physically present in both shards' exact sets — the
//     union must dedupe it.
//
// Each entity has several *occurrences*, so events != entities.  Every event
// carries an explicit HLC and the generator builds the per-entity timeline
// first, which lets it inject retroactive corrections whose HLC is *older* than
// the delete they relate to.
//
// `perShard` is delivered in shuffled order (with optional duplicate retries)
// so the exact state is exercised for order-independence and idempotency.

const { compareHlc } = require('../src/common/time');

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function entityKey(i) {
  return `ent_${(((i + 1) * 2654435761) >>> 0).toString(36)}_${i}`;
}

function hashKey(key) {
  let h = 0;
  for (const c of key) h = (Math.imul(h, 31) + c.charCodeAt(0)) >>> 0;
  return h;
}

const SHARD_IDS = ['shard-a', 'shard-b'];

function generate({
  seed = 287,
  entities = 5000,
  maxOccurrences = 3,
  deleteFraction = 0.12,
  correctionFraction = 0.08,
  mirrorFraction = 0.35,
  duplicateFraction = 0.05,
  baseWall = 1_760_000_000_000,
} = {}) {
  const rnd = mulberry32(seed);
  const perShard = Object.fromEntries(SHARD_IDS.map((s) => [s, []]));
  const truth = new Map();
  let id = 0;
  let wall = baseWall;
  const nextHlc = (node) => ({ wall: (wall += 1), seq: 0, node });

  for (let i = 0; i < entities; i += 1) {
    const key = entityKey(i);
    const h = hashKey(key);
    const owner = SHARD_IDS[h % 2];
    const other = SHARD_IDS[(h + 1) % 2];
    const occCount = 1 + Math.floor(rnd() * maxOccurrences);
    const timeline = [];
    for (let o = 0; o < occCount; o += 1) {
      const shard = owner;
      timeline.push({ op: 'add', shard, hlc: nextHlc(owner), occurrence: o });
    }
    let deleted = false;
    let deleteWall = null;
    if (rnd() < deleteFraction) {
      const hlc = nextHlc(owner);
      deleteWall = hlc.wall;
      timeline.push({ op: 'delete', shard: owner, hlc, note: 'user-deleted' });
      deleted = true;
    }
    // Retroactive correction: an older observation of an entity that has since
    // been deleted.  It is inserted in the timeline *before* the delete, so it
    // arrives with an older HLC and must not resurrect the entity.
    if (deleted && rnd() < correctionFraction) {
      const lastAdd = [...timeline].reverse().find((e) => e.op === 'add');
      const midWall = Math.floor((lastAdd.hlc.wall + deleteWall) / 2);
      timeline.splice(timeline.length - 1, 0, {
        op: 'add',
        shard: other,
        hlc: { wall: midWall, seq: 0, node: other },
        note: 'retroactive-correction',
      });
    }
    for (const t of timeline) {
      perShard[t.shard].push({ id: `evt-${++id}`, op: t.op, key, hlc: t.hlc, shard: t.shard, note: t.note });
      // Mirror a fraction of observations onto the other shard, and mirror all
      // deletes, so both shards hold copies of the same entity.  The mirror is
      // another physical report of the *same logical observation*: its HLC is
      // the source HLC with the other shard as tiebreak, never a fresh, newer
      // timestamp.  Otherwise a mirrored retroactive correction would outrank
      // the delete and illegally resurrect the entity.
      const mirror = t.op === 'delete' || rnd() < mirrorFraction;
      if (mirror) {
        perShard[other].push({
          id: `evt-${++id}`,
          op: t.op,
          key,
          hlc: { wall: t.hlc.wall, seq: t.hlc.seq + 1, node: other },
          shard: other,
          note: t.note ? `${t.note}-mirror` : 'mirror',
          mirrorOf: t.id,
        });
      }
    }
    truth.set(key, { live: !deleted, occurrences: occCount, deleted });
  }

  for (const s of SHARD_IDS) {
    shuffle(perShard[s], mulberry32(seed + 17 + s.length));
    if (duplicateFraction > 0) {
      // Duplicate delivery: the same event id appears again in a later batch.
      for (const ev of perShard[s].slice()) {
        if (rnd() < duplicateFraction) perShard[s].push({ ...ev, retried: true });
      }
    }
  }

  const liveKeys = [...truth.entries()].filter(([, t]) => t.live).map(([k]) => k).sort();
  const allEvents = SHARD_IDS.flatMap((s) => perShard[s]);
  return {
    perShard,
    truth,
    liveCount: liveKeys.length,
    liveKeys,
    totalEntities: entities,
    totalEvents: allEvents.length,
    addReports: allEvents.filter((e) => e.op === 'add').length,
    deleteReports: allEvents.filter((e) => e.op === 'delete').length,
    corrections: allEvents.filter((e) => (e.note || '').startsWith('retroactive-correction')).length,
    retriedReports: allEvents.filter((e) => e.retried).length,
    multiShardKeys: [...truth.keys()].filter((k) => {
      const a = perShard[SHARD_IDS[0]].some((e) => e.key === k);
      const b = perShard[SHARD_IDS[1]].some((e) => e.key === k);
      return a && b;
    }).length,
  };
}

function shuffle(arr, rnd) {
  for (let i = arr.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rnd() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function sortByHlc(events) {
  return events.slice().sort((a, b) => compareHlc(a.hlc, b.hlc));
}

module.exports = { generate, entityKey, hashKey, sortByHlc, SHARD_IDS, mulberry32 };
