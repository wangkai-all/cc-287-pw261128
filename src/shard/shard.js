'use strict';
// Shard: exact key-versioned set + journal + locally built summary.
//
// The bundle is the unit of consistency between planes:
//
//   bundle = {
//     shardId,
//     versions:        { key -> { v: hlc, d: 0|1 } }  for every key the shard
//                      has ever seen, i.e. including tombstones,
//     liveCount,       number of keys with d=0
//     summary:         sketch of the live keys, sealed with the digest of
//                      those keys,
//     summaryDigest,
//     builtAt,
//   }
//
// The coordinator can therefore, for any pair of bundles:
//   * compute the exact union/dedup by LWW-merging `versions`,
//   * tell whether a sketch is stale relative to a version map,
//   * decide "local rebuild" (a shard's own exact set changed) vs "full
//     recompute" (the merged exact set needs re-deriving) without extra RPCs.
//
// A summary is only ever built *from* the exact set.  It is never adjusted.
// After any set change the shard marks its bundle dirty; the next export
// rebuilds it locally, and the cost of that rebuild is reported.

const path = require('node:path');
const { randomBytes } = require('node:crypto');
const { hash64, digestOfKeys } = require('../common/hash');
const { Journal } = require('../common/journal');
const { compareHlc, canonical, isHlc } = require('../common/time');
const { Summary, DEFAULTS } = require('../common/summary');
const { ShardState } = require('./state');

class Shard {
  constructor(id, { dataDir, sketchOptions = {} }) {
    this.id = id;
    this.dataDir = dataDir;
    this.sketchOptions = { ...DEFAULTS, ...sketchOptions };
    this.state = new ShardState(id);
    this.journal = new Journal(path.join(dataDir, `shard-${id}.jsonl`));
    // Incarnation identifies one process lifetime.  A restart gets a new one,
    // which is how the coordinator distinguishes "this shard rebooted" from
    // "this shard's old summary arrived late".
    this.incarnation = randomBytes(6).toString('hex');

    // Exact-set generations.  `version` increments whenever the live set
    // changes; `summary` is valid only while its generation matches.
    this.generation = 0;
    this.summary = null;
    this.summaryMeta = null;
    this.summaryHistory = [];
    this.localRebuilds = 0;
    this.ingestCount = 0;
    this.crashCount = 0;
    this.frozen = false;
  }

  // --- durability ---------------------------------------------------------
  load() {
    const records = Journal.readAll(this.journal.file);
    for (const rec of records) {
      if (rec.type === 'event') {
        const res = this.state.applyEvent(rec.event);
        if (res.changed) this.generation += 1;
      }
    }
    this.loaded = { records: records.length, live: this.state.liveKeys().length, generation: this.generation };
    return this.loaded;
  }

  // --- ingestion ----------------------------------------------------------
  // All-or-nothing journaling: validate first, then append+fsync the whole
  // batch, then apply in memory.  A client retry of an acknowledged batch is
  // recognised by event id and by version, so duplicates cannot inflate
  // anything.
  ingest(events) {
    const accepted = [];
    const results = [];
    for (const raw of events) {
      const { event, error } = validateEvent(raw);
      if (error) {
        results.push({ id: raw && raw.id, applied: false, kind: 'invalid', error });
        continue;
      }
      accepted.push(event);
    }
    if (accepted.length) this.journal.appendMany(accepted.map((event) => ({ type: 'event', event })));
    for (const ev of accepted) {
      const res = this.state.applyEvent(ev);
      if (res.changed) {
        this.generation += 1;
        this.summary = null; // stale by construction; next export rebuilds
      }
      results.push({ id: ev.id, ...res });
    }
    this.ingestCount += events.length;
    return results;
  }

  // --- summary lifecycle --------------------------------------------------
  // Local rebuild: O(live keys).  Tombstoned keys are simply absent from the
  // input, so deletion never has to be subtracted.
  rebuildSummary(reason = 'manual') {
    const t0 = process.hrtime.bigint();
    const live = this.state.liveKeys();
    const summary = Summary.fromKeys(live, this.sketchOptions);
    const durationMs = Number(process.hrtime.bigint() - t0) / 1e6;
    this.summary = summary;
    this.summaryMeta = {
      shardId: this.id,
      generation: this.generation,
      builtAt: Date.now(),
      reason,
      durationMs,
      exactLive: live.length,
      exactDigest: summary.exactDigest,
      summaryDigest: summary.digest(),
      scanUnits: live.length,
      approximateBytes: Buffer.byteLength(JSON.stringify(summary.serialize())),
    };
    this.localRebuilds += 1;
    this.summaryHistory.push({ ...this.summaryMeta });
    if (this.summaryHistory.length > 40) this.summaryHistory.shift();
    return this.summaryMeta;
  }

  bundle(reason = 'export') {
    if (!this.summary || this.summaryMeta.generation !== this.generation) {
      this.rebuildSummary(`export:${reason}`);
    }
    const versions = {};
    for (const [k, e] of this.state.entities) {
      versions[k] = { v: e.version, d: e.deleted ? 1 : 0, n: e.addReports };
    }
    const liveKeys = this.state.liveKeys();
    return {
      meta: { ...this.summaryMeta, exactDigest: digestOfKeys(liveKeys) },
      payload: {
        shardId: this.id,
        incarnation: this.incarnation,
        generation: this.generation,
        versions,
        liveCount: liveKeys.length,
        tombstoneCount: this.state.entities.size - liveKeys.length,
        summary: this.summary.serialize(),
        summaryDigest: this.summaryMeta.summaryDigest,
        exactDigest: this.summaryMeta.exactDigest,
      },
    };
  }

  status() {
    const summary = this.state.summary();
    return {
      shardId: this.id,
      generation: this.generation,
      frozen: this.frozen,
      exact: summary,
      summary: this.summaryMeta,
      summaryValid: !!this.summary && this.summaryMeta.generation === this.generation,
      localRebuilds: this.localRebuilds,
      journalBytes: this.journal.bytes(),
      ingestCount: this.ingestCount,
      crashCount: this.crashCount,
      loaded: this.loaded || null,
    };
  }

  close() {
    this.journal.close();
  }
}

function validateEvent(raw) {
  if (!raw || typeof raw !== 'object') return { error: 'event must be an object' };
  if (typeof raw.id !== 'string' || raw.id.length === 0) return { error: 'id required' };
  if (raw.op !== 'add' && raw.op !== 'delete') return { error: 'op must be add|delete' };
  if (typeof raw.key !== 'string' || raw.key.length === 0) return { error: 'key required' };
  if (!isHlc(raw.hlc)) return { error: 'hlc {wall,seq,node} required' };
  if (raw.hlc.wall > Date.now() + 24 * 3600 * 1000) return { error: 'hlc too far in the future' };
  return {
    event: {
      id: raw.id,
      op: raw.op,
      key: raw.key,
      hlc: { wall: raw.hlc.wall, seq: raw.hlc.seq, node: raw.hlc.node },
      note: typeof raw.note === 'string' ? raw.note : undefined,
    },
  };
}

module.exports = { Shard, validateEvent, DEFAULT_SKETCHES: DEFAULTS };
