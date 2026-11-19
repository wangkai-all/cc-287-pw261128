'use strict';
// Per-shard exact state.
//
// Identity
//   An entity is a `key` (a person/device id).  Shards can hold several
//   *reports* of the same key.  The exact set stores membership, plus the
//   number of add reports, and it is versioned per key.
//
// Versioning
//   Every event carries an HLC.  For each key the state keeps the highest
//   version observed and whether that version was an add or a delete.
//   Applying an event with an older or equal version changes nothing.  This is
//   what makes retries idempotent, makes reordered delivery converge, and makes
//   a stale add unable to undo a newer delete (no resurrection).
//
// Deletes
//   A delete is a *tombstone at a version*, not "count - 1".  It removes the
//   whole entity from the active set regardless of how many occurrences it
//   had; that is precisely why neither exact nor approximate counters may be
//   decremented per report.

const { compareHlc } = require('../common/time');

class ShardState {
  constructor(shardId) {
    this.shardId = shardId;
    this.entities = new Map(); // key -> Entity
    this.seenEvents = new Set();
    this.stats = {
      reports: 0, // physically distinct event ids accepted into the journal
      duplicateReports: 0, // same event id delivered again
      staleReports: 0, // older version than the key already has
      addReports: 0,
      deleteReports: 0,
      setChanges: 0, // live-set transitions: add of a new key, delete of a live key
      tombstoneWins: 0,
      correctionsIgnored: 0, // retroactive/stale observation of a deleted key
    };
  }

  // Returns { changed, kind, applied, version } where `changed` means the
  // *live membership* of the set changed (which is what invalidates sketches).
  applyEvent(ev) {
    if (this.seenEvents.has(ev.id)) {
      this.stats.duplicateReports += 1;
      return { applied: false, changed: false, kind: 'duplicate-report' };
    }
    this.seenEvents.add(ev.id);
    this.stats.reports += 1;
    if (ev.op === 'add') this.stats.addReports += 1;
    else this.stats.deleteReports += 1;

    const cur = this.entities.get(ev.key);
    if (!cur) {
      // First time this key is seen on this shard.  A first event that is a
      // delete still creates a tombstone version: it must beat stale adds.
      if (ev.op === 'add') {
        this.entities.set(ev.key, newEntity(ev));
        this.stats.setChanges += 1;
        return { applied: true, changed: true, kind: 'insert' };
      }
      this.entities.set(ev.key, newTombstone(ev));
      this.stats.tombstoneWins += 1;
      return { applied: true, changed: false, kind: 'tombstone-only' };
    }

    const cmp = compareHlc(ev.hlc, cur.version);
    if (cmp <= 0) {
      this.stats.staleReports += 1;
      if (ev.op === 'add' && cur.deleted) this.stats.correctionsIgnored += 1;
      return { applied: false, changed: false, kind: cmp === 0 ? 'same-version' : 'stale' };
    }

    const wasLive = !cur.deleted;
    cur.version = ev.hlc;
    if (ev.op === 'add') {
      cur.addReports += 1;
      cur.lastAdd = ev.hlc;
      if (wasLive) return { applied: true, changed: false, kind: 'extra-occurrence' };
      cur.deleted = false;
      cur.tombstone = null;
      this.stats.setChanges += 1;
      return { applied: true, changed: true, kind: 'recreate' };
    }

    // A delete at a strictly newer version.
    cur.deleted = true;
    cur.tombstone = ev.hlc;
    this.stats.tombstoneWins += 1;
    if (!wasLive) return { applied: true, changed: false, kind: 'repeat-tombstone' };
    this.stats.setChanges += 1;
    return { applied: true, changed: true, kind: 'delete' };
  }

  liveKeys() {
    const out = [];
    for (const [k, e] of this.entities) if (!e.deleted) out.push(k);
    return out;
  }

  deletedKeys() {
    const out = [];
    for (const [k, e] of this.entities) if (e.deleted) out.push(k);
    return out;
  }

  // Version map for one entity set, used in the coordinator's LWW merge and in
  // lag analysis.  Keys is optional to allow reporting a subset.
  versionMap(filter) {
    const out = {};
    for (const [k, e] of this.entities) {
      if (!filter || filter(e)) out[k] = e.version;
    }
    return out;
  }

  maxVersion() {
    let w = null;
    for (const e of this.entities.values()) if (!w || compareHlc(e.version, w) > 0) w = e.version;
    return w;
  }

  summary() {
    let liveOccurrences = 0;
    let liveKeyBytes = 0;
    let tombstoneKeyBytes = 0;
    let deleted = 0;
    for (const e of this.entities.values()) {
      if (e.deleted) {
        deleted += 1;
        tombstoneKeyBytes += Buffer.byteLength(e.key);
      } else {
        liveOccurrences += e.addReports;
        liveKeyBytes += Buffer.byteLength(e.key);
      }
    }
    return {
      keys: this.entities.size,
      live: this.liveKeys().length,
      deleted,
      liveOccurrences,
      liveKeyBytes,
      tombstoneKeyBytes,
      stats: { ...this.stats },
      maxVersion: this.maxVersion(),
    };
  }
}

function newEntity(ev) {
  return {
    key: ev.key,
    version: ev.hlc,
    deleted: false,
    addReports: 1,
    firstSeen: ev.hlc,
    lastAdd: ev.hlc,
    tombstone: null,
  };
}

function newTombstone(ev) {
  return {
    key: ev.key,
    version: ev.hlc,
    deleted: true,
    addReports: 0,
    firstSeen: ev.hlc,
    lastAdd: null,
    tombstone: ev.hlc,
  };
}

module.exports = { ShardState };
