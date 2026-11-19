'use strict';
// Coordinator: publication, exact baseline, merge/rebuild policy, recovery.
//
// Inputs are coherent per-shard bundles: { exact version map, live count,
// summary sealed to the exact key-set digest, generation, incarnation }.
// Because the summary arrives in the same bundle as the exact keys it was
// built from, one publish can never mix a summary from one instant with exact
// state from another.
//
// Two paths, chosen explicitly and reported on every published version:
//
//   merge      cheap path, O(summary bytes).  Union of shard summaries.  Valid
//              only while (a) every shard contributes a bundle, (b) each
//              summary is sealed to that shard's exact live set (checked at
//              intake), and (c) no key is live on one shard while the version
//              merge tombstones it.  When those hold, set-union of the
//              summaries is exactly the summary of the merged set.
//
//   recompute  corrected path, O(merged live keys).  Rebuild one summary from
//              the merged exact key set.  Required when deletion lag is
//              non-zero, available on demand, and used as the verification
//              oracle for the merge path.  It never subtracts from a sketch.
//
// Recovery: `pending` is persisted before a version is appended.  A crash
// before the append means the version was never visible to readers; on boot
// the partial record is reported and discarded, and the next publish recomputes
// from fresh bundles.  Nothing is counted twice because publish never consumes
// deltas.

const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { compareHlc, isHlc, hlcKey } = require('../common/time');
const { digestOfKeys } = require('../common/hash');
const { mergeBundles } = require('../common/merge');
const { Summary, mergeSummaries, serializeBytes } = require('../common/summary');

class Coordinator {
  constructor({ dataDir, publishDir, shardUrls, sketchOptions = {}, fetchTimeoutMs = 4000 }) {
    this.dataDir = dataDir;
    this.publishDir = publishDir;
    this.shardUrls = shardUrls;
    this.shardIds = Object.keys(shardUrls);
    this.fetchTimeoutMs = fetchTimeoutMs;
    this.sketchOptions = sketchOptions;
    this.storeFile = path.join(dataDir, 'coordinator.json');

    this.bundles = new Map(); // shardId -> { payload, digest, receivedAt, contentChangedAt, via }
    this.historyBundles = []; // stashed copies, used by the replay scenario
    this.observations = []; // last published outputs, for the accuracy chart
    this.versions = [];
    this.versionSeq = 0;
    this.pending = null;
    this.interrupted = [];
    this.repairs = [];
    this.lastVerification = null;
    this.lastError = null;
    this.stats = {
      bundleReceipts: 0,
      retransmits: 0,
      staleRejected: 0,
      digestConflicts: 0,
      bootAccepted: 0,
      knowledgeRegressions: 0,
      publishes: 0,
      mergePaths: 0,
      recomputePaths: 0,
      exactOnly: 0,
      failedCollects: 0,
      verifications: 0,
      repairs: 0,
    };
    this.load();
  }

  // --- persistence --------------------------------------------------------
  load() {
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(this.storeFile, 'utf8'));
    } catch (err) {
      if (err.code !== 'ENOENT') this.lastError = err.message;
      return;
    }
    this.bundles = new Map(Object.entries(raw.bundles || {}));
    this.versions = raw.versions || [];
    this.versionSeq = raw.versionSeq || this.versions.length;
    this.pending = raw.pending || null;
    this.interrupted = raw.interrupted || [];
    this.repairs = raw.repairs || [];
    this.historyBundles = raw.historyBundles || [];
    this.observations = raw.observations || [];
    this.lastVerification = raw.lastVerification || null;
    this.stats = { ...this.stats, ...(raw.stats || {}) };
    if (this.pending) {
      this.interrupted.push({
        ...this.pending,
        recoveredAt: Date.now(),
        outcome: 'discarded before publication; readers never saw it',
      });
      this.pending = null;
      this.save();
    }
  }

  save() {
    fs.mkdirSync(this.dataDir, { recursive: true });
    const tmp = `${this.storeFile}.tmp`;
    const body = {
      bundles: Object.fromEntries(this.bundles),
      versions: this.versions.slice(-200),
      versionSeq: this.versionSeq,
      pending: this.pending,
      interrupted: this.interrupted.slice(-20),
      repairs: this.repairs.slice(-20),
      historyBundles: this.historyBundles.slice(-10),
      observations: this.observations.slice(-200),
      lastVerification: this.lastVerification,
      stats: this.stats,
    };
    fs.writeFileSync(tmp, JSON.stringify(body, null, 2));
    fs.renameSync(tmp, this.storeFile);
  }

  // --- bundle intake ------------------------------------------------------
  // Rules that make late / retransmitted / restarted shards safe:
  //   same incarnation + same content digest      -> retransmit, ignored
  //   same incarnation + lower generation         -> stale relay, rejected
  //   same incarnation + same generation, diff sum -> corrupt/mislabelled, rejected
  //   different incarnation                       -> a (re)booted shard; accepted,
  //                                                  flagged if its generation is
  //                                                  lower than what we held
  receiveBundle(shardId, bundle, via = 'push') {
    this.stats.bundleReceipts += 1;
    const err = validateBundle(bundle, shardId);
    if (err) return { accepted: false, reason: 'invalid', error: err };
    const payload = bundle.payload;
    const digest = bundleDigest(payload);
    const prev = this.bundles.get(shardId);
    const now = Date.now();

    const exactLive = Object.entries(payload.versions)
      .filter(([, e]) => e.d === 0)
      .map(([k]) => k);
    // The summary must be the one built from the exact keys in this bundle.
    // This is what makes "union of summaries == summary of union" reliable.
    const want = digestOfKeys(exactLive);
    if (want !== payload.exactDigest || payload.summary.exactDigest !== want) {
      this.stats.digestConflicts += 1;
      return { accepted: false, reason: 'exact-digest-mismatch', expected: want };
    }

    if (prev) {
      const sameIncarnation = prev.payload.incarnation === payload.incarnation;
      if (sameIncarnation && prev.digest === digest) {
        this.stats.retransmits += 1;
        prev.receivedAt = now;
        this.save();
        return { accepted: true, reason: 'retransmit-ignored', digest, generation: payload.generation };
      }
      if (sameIncarnation && payload.generation < prev.payload.generation) {
        this.stats.staleRejected += 1;
        return {
          accepted: false,
          reason: 'stale-generation',
          offered: payload.generation,
          current: prev.payload.generation,
        };
      }
      if (
        sameIncarnation &&
        payload.generation === prev.payload.generation &&
        payload.summaryDigest !== prev.payload.summaryDigest
      ) {
        this.stats.digestConflicts += 1;
        return { accepted: false, reason: 'summary-digest-conflict', generation: payload.generation };
      }
      if (!sameIncarnation) this.stats.bootAccepted += 1;
      if (payload.generation < prev.payload.generation) {
        // The shard rebooted and lost a journal tail.  It is now behind what we
        // had cached.  Accept it (its state is genuinely smaller) but record
        // the regression: the client must retry unacknowledged events, and
        // /repair re-derives deleted keys from the merged version map.
        this.stats.knowledgeRegressions += 1;
      }
    }

    this.bundles.set(shardId, {
      payload,
      digest,
      receivedAt: now,
      contentChangedAt: now,
      via,
      meta: bundle.meta || {},
    });
    this.save();
    return {
      accepted: true,
      reason: 'stored',
      digest,
      generation: payload.generation,
      regression: !!prev && payload.generation < prev.payload.generation,
    };
  }

  // --- collection ---------------------------------------------------------
  // One coherent bundle per shard.  An unreachable shard contributes its last
  // received bundle, explicitly marked stale — never an empty set (which would
  // look like a mass deletion).
  async collect({ timeoutMs = this.fetchTimeoutMs } = {}) {
    const results = {};
    await Promise.all(
      this.shardIds.map(async (id) => {
        try {
          const res = await fetch(`${this.shardUrls[id]}/bundle`, { signal: AbortSignal.timeout(timeoutMs) });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const bundle = await res.json();
          const rec = this.receiveBundle(id, bundle, 'collect');
          if (!rec.accepted) throw new Error(`bundle rejected: ${rec.reason}`);
          const stored = this.bundles.get(id);
          results[id] = {
            ok: true,
            fresh: true,
            payload: stored.payload,
            digest: stored.digest,
            generation: stored.payload.generation,
            receivedAt: stored.receivedAt,
          };
        } catch (err) {
          this.stats.failedCollects += 1;
          const cached = this.bundles.get(id);
          if (cached) {
            results[id] = {
              ok: true,
              fresh: false,
              error: err.message,
              payload: cached.payload,
              digest: cached.digest,
              generation: cached.payload.generation,
              ageMs: Date.now() - cached.receivedAt,
            };
          } else {
            results[id] = { ok: false, fresh: false, error: err.message };
          }
        }
      })
    );
    return results;
  }

  // --- publish ------------------------------------------------------------
  async publish({ mode = 'auto', note = '', trigger = 'manual', collected = null } = {}) {
    const c = collected || (await this.collect());
    const usable = this.shardIds.filter((id) => c[id] && c[id].ok);
    const missing = this.shardIds.filter((id) => !c[id] || !c[id].ok);
    const stale = usable.filter((id) => !c[id].fresh);
    const complete = missing.length === 0;

    const versionId = this.versionSeq + 1;
    this.pending = {
      versionId,
      mode,
      trigger,
      note,
      startedAt: Date.now(),
      bundles: Object.fromEntries(usable.map((id) => [id, c[id].digest])),
      missing,
      stale,
    };
    this.save();

    const bundles = usable.map((id) => ({ shardId: id, payload: c[id].payload }));
    const merge = bundles.length ? mergeBundles(bundles) : emptyMerge();
    const exactLive = merge.stats.mergedLive;
    const lag = merge.deletionLag.keys;

    // The uncorrected union of shard summaries.  Kept on every version — even
    // when it is not what gets published — so the UI can show exactly what the
    // cheap path would have said and how far off it is.
    const mergedSummaries = bundles.length ? mergeSummaries(bundles.map((b) => b.payload.summary)) : null;
    const mergeEstimate = mergedSummaries ? mergedSummaries.estimates : null;
    const path = choosePath({ mode, complete, lag, bundles: bundles.length });
    let recompute = null;
    if (path === 'recompute' && complete) {
      const t0 = process.hrtime.bigint();
      const s = Summary.fromKeys(merge.mergedLiveKeys, this.sketchOptions);
      recompute = {
        estimates: roundEstimates(s.estimates()),
        exactDigest: s.exactDigest,
        durationMs: Number(process.hrtime.bigint() - t0) / 1e6,
        scanUnits: merge.mergedLiveKeys.length,
        approximateBytes: serializeBytes(s),
      };
    }

    const published =
      path === 'merge'
        ? {
            source: 'merged-summaries',
            estimates: mergeEstimate,
            error: errorAgainst(mergeEstimate, merge.stats.unionLive),
            errorBaseline: merge.stats.unionLive,
            errorBaselineLabel: 'union of shard live sets',
            relativeError: relativeAgainst(mergeEstimate, merge.stats.unionLive),
          }
        : path === 'recompute' && recompute
        ? {
            source: 'recomputed-from-exact',
            estimates: recompute.estimates,
            error: errorAgainst(recompute.estimates, exactLive),
            errorBaseline: exactLive,
            errorBaselineLabel: 'merged exact live set',
            relativeError: relativeAgainst(recompute.estimates, exactLive),
          }
        : null;

    // The number a reader actually cares about: the published estimate against
    // the merged exact live set.  On the merge path this includes the known,
    // exact deletion-lag term; `decomposition` below splits it out.
    const errorAgainstMergedExact = published
      ? {
          baseline: exactLive,
          error: errorAgainst(published.estimates, exactLive),
          relativeError: relativeAgainst(published.estimates, exactLive),
        }
      : null;

    const decision = explain({
      mode,
      path,
      complete,
      lag,
      merge,
      missing,
      stale,
      approximateAvailable: !!published,
    });

    const version = {
      id: versionId,
      createdAt: Date.now(),
      kind:
        path === 'merge'
          ? 'merged-approximate'
          : path === 'recompute'
          ? 'recomputed-approximate'
          : 'exact-only',
      trigger,
      note,
      decision,
      exact: {
        complete,
        mergedLive: exactLive,
        mergedDeleted: merge.stats.mergedDeleted,
        keys: merge.stats.keys,
        sumShardLive: merge.stats.sumShardLive,
        unionLive: merge.stats.unionLive,
        crossShardOverlap: merge.stats.crossShardOverlap,
        dedupSavings: merge.stats.dedupSavings,
        membershipConflicts: merge.stats.membershipConflicts,
        versionConflicts: merge.stats.versionConflicts,
        deletionLag: merge.deletionLag,
        perShard: merge.perShard,
        unionBias: merge.unionBias,
        watermark: watermarkOf(bundles),
      },
      approximate: {
        published,
        // Single consistent yardstick for all three estimators, whatever path
        // produced them.  The per-path `published.error` above is kept because
        // the merge path is verified against the union key set.
        errorAgainstMergedExact,
        mergeUnaudited: mergeEstimate
          ? {
              estimates: mergeEstimate,
              errorAgainstUnion: errorAgainst(mergeEstimate, merge.stats.unionLive),
              errorAgainstMerged: errorAgainst(mergeEstimate, exactLive),
              wouldBePublishedAs: 'merged-summaries',
            }
          : null,
        recompute,
        // Estimate decomposition, linear counter only (the clearest arithmetic).
        //   published estimate = exact live + sketch error (+ deletion-lag bias
        //   only on the merge path).
        // The deletion-lag term is exact, taken from version maps; it is a
        // count of keys the union counted that the merge tombstoned.  It is not
        // an estimate and no sketch decrement is involved.
        decomposition: published
          ? {
              publishedLinear: published.estimates.linear,
              exactLive,
              unionLive: merge.stats.unionLive,
              sketchError: round3(
                published.estimates.linear - (published.source === 'merged-summaries' ? merge.stats.unionLive : exactLive)
              ),
              deletionLagBias: published.source === 'merged-summaries' ? merge.stats.unionLive - exactLive : 0,
              residualVsMergedExact: round3(published.estimates.linear - exactLive),
              note:
                published.source === 'merged-summaries'
                  ? 'union sketch estimate = unionLive + sketchError; unionLive = exactLive + deletionLagBias'
                  : 'recomputed sketch estimate = exactLive + sketchError (no deletion-lag term)',
            }
          : null,
        storageBytes: published ? (recompute ? recompute.approximateBytes : mergedSummaries ? mergedSummaries.bytes : 0) : 0,
      },
      collect: Object.fromEntries(
        this.shardIds.map((id) => {
          const r = c[id] || { ok: false };
          return [
            id,
            {
              ok: !!r.ok,
              fresh: !!r.fresh,
              ageMs: r.ageMs || 0,
              generation: r.generation == null ? null : r.generation,
              exactLive: r.payload ? r.payload.liveCount : null,
              incarnation: r.payload ? r.payload.incarnation : null,
              summaryDigest: r.payload ? r.payload.summaryDigest : null,
              error: r.error || null,
            },
          ];
        })
      ),
      storage: this.storageReport(bundles),
      changeSinceBundle: await this.changeSinceBundle(c),
    };

    this.versions.push(version);
    this.versionSeq = versionId;
    this.pending = null;
    if (published) {
      this.observations.push({
        id: version.id,
        at: version.createdAt,
        path,
        exactLive,
        publishedLinear: published.estimates.linear,
        publishedKmv: published.estimates.kmv,
        publishedHll: published.estimates.hll,
        mergePathLinear: mergeEstimate ? mergeEstimate.linear : null,
        deletionLag: lag,
      });
      if (this.observations.length > 200) this.observations.shift();
    }
    this.stats.publishes += 1;
    if (path === 'merge') this.stats.mergePaths += 1;
    else if (path === 'recompute') this.stats.recomputePaths += 1;
    else this.stats.exactOnly += 1;
    this.save();
    this.writePublished();
    return version;
  }

  // Events that arrived at a shard after the bundle used for this publish.  They
  // are *not* counted in this version; the next publish re-collects bundles, so
  // they are counted exactly once.  Reported so the delay is visible.
  async changeSinceBundle(c) {
    const out = {};
    await Promise.all(
      this.shardIds.map(async (id) => {
        try {
          const res = await fetch(`${this.shardUrls[id]}/status`, { signal: AbortSignal.timeout(this.fetchTimeoutMs) });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const st = await res.json();
          const used = c[id] && c[id].payload ? c[id].payload.generation : null;
          out[id] = {
            generationAtBundle: used,
            generationNow: st.generation,
            changed: used !== null && st.generation !== used,
          };
        } catch (err) {
          out[id] = { unavailable: true, error: err.message };
        }
      })
    );
    return out;
  }

  // --- verification -------------------------------------------------------
  // Two independent checks, reported separately because they prove different
  // things:
  //
  //   mergeAlgebra: union of per-shard summaries must be bitwise identical to a
  //     summary built from the union of those shards' exact live keys.  This
  //     proves the cheap path implements true set union (dedup included) and is
  //     the reason the merge path can be trusted at all.
  //
  //   againstMergedExact: rebuild from the *merged* exact set (LWW across
  //     shards) and compare.  This is the honest measure of what users see; it
  //     includes deletion lag, so a non-zero gap is expected and is exactly the
  //     known bias, not an error in the merge algebra.
  verify() {
    const bundles = [...this.bundles.entries()]
      .filter(([id]) => this.shardIds.includes(id))
      .map(([shardId, b]) => ({ shardId, payload: b.payload }));
    if (bundles.length !== this.shardIds.length) {
      return { ok: false, reason: 'not all shards have a bundle', have: bundles.length, need: this.shardIds.length };
    }
    const merge = mergeBundles(bundles);
    const exactLive = merge.stats.mergedLive;

    const unionKeys = [...merge.unionLive];
    const rebuiltFromUnion = Summary.fromKeys(unionKeys, this.sketchOptions);
    const rebuiltFromUnionEst = roundEstimates(rebuiltFromUnion.estimates());
    const unionOfSummaries = mergeSummaries(bundles.map((b) => b.payload.summary)).estimates;

    const rebuiltFromMerged = Summary.fromKeys(merge.mergedLiveKeys, this.sketchOptions);
    const rebuiltFromMergedEst = roundEstimates(rebuiltFromMerged.estimates());

    const algebraDrift = {
      linear: round3(unionOfSummaries.linear - rebuiltFromUnionEst.linear),
      kmv: round3(unionOfSummaries.kmv - rebuiltFromUnionEst.kmv),
      hll: round3(unionOfSummaries.hll - rebuiltFromUnionEst.hll),
    };
    const last = this.versions[this.versions.length - 1] || null;
    const publishedEst = last && last.approximate.published ? last.approximate.published.estimates : null;
    const publishedSource = last && last.approximate.published ? last.approximate.published.source : null;
    const sameKeySetAsPublished =
      last && last.collect
        ? this.shardIds.every((id) => {
            const atPublish = last.collect[id] && last.collect[id].generation;
            const now = this.bundles.get(id);
            return now && atPublish === now.payload.generation;
          })
        : false;

    const unionEq = nearlyEqual(algebraDrift);
    const publishedMatches = publishedEst
      ? publishedSource === 'merged-summaries'
        ? nearlyEqual({
            linear: publishedEst.linear - unionOfSummaries.linear,
            kmv: publishedEst.kmv - unionOfSummaries.kmv,
            hll: publishedEst.hll - unionOfSummaries.hll,
          })
        : nearlyEqual({
            linear: publishedEst.linear - rebuiltFromMergedEst.linear,
            kmv: publishedEst.kmv - rebuiltFromMergedEst.kmv,
            hll: publishedEst.hll - rebuiltFromMergedEst.hll,
          })
      : null;

    const result = {
      verifiedAt: Date.now(),
      exactLive,
      unionLive: merge.stats.unionLive,
      deletionLagKeys: merge.deletionLag.keys,
      algebra: {
        unionOfSummaries,
        rebuiltFromUnionKeys: rebuiltFromUnionEst,
        equal: unionEq,
        drift: algebraDrift,
        note: 'set-union sketches: merging summaries must equal building one summary from the union keys',
      },
      againstMergedExact: {
        rebuilt: rebuiltFromMergedEst,
        unionEstimate: unionOfSummaries,
        // Two distinct components; only the first is a defect.
        sketchError: round3(unionOfSummaries.linear - merge.stats.unionLive),
        deletionLagBias: merge.stats.unionLive - exactLive,
        linearGap: round3(unionOfSummaries.linear - exactLive),
        note:
          'linearGap = sketchError + deletionLagBias. The lag term is exact from version maps; ' +
          'it disappears only on the recompute path, never by subtracting from a sketch.',
      },
      publishedAtLastVersion: publishedEst,
      publishedSource,
      publishedMatchesRecomputed: publishedMatches,
      bundlesUnchangedSincePublish: sameKeySetAsPublished,
    };
    this.lastVerification = result;
    this.stats.verifications += 1;
    this.save();
    this.writePublished();
    return result;
  }

  // --- repair (anti-entropy) ---------------------------------------------
  // Reconcile *membership* disagreements for keys that more than one shard
  // knows about.  Keys only one shard knows are not a disagreement — that is
  // ordinary sharding, and pushing them everywhere would turn the cluster into
  // full replicas and destroy the storage advantage being demonstrated.
  //
  // Repair events reuse the winning version, so they are idempotent and cannot
  // resurrect anything: a stale add re-delivered later still loses to the newer
  // tombstone.
  async repair() {
    const bundles = [...this.bundles.entries()]
      .filter(([id]) => this.shardIds.includes(id))
      .map(([shardId, b]) => ({ shardId, payload: b.payload }));
    if (!bundles.length) return { ok: false, reason: 'no bundles' };

    // key -> winning membership, considering every shard that has the key
    const winning = new Map(); // key -> { v, d, shards: n }
    for (const b of bundles) {
      for (const [key, e] of Object.entries(b.payload.versions)) {
        const cur = winning.get(key);
        if (!cur) {
          winning.set(key, { v: e.v, d: e.d, shards: 1 });
          continue;
        }
        cur.shards += 1;
        const cmp = compareHlc(e.v, cur.v);
        if (cmp > 0 || (cmp === 0 && e.d > cur.d)) {
          cur.v = e.v;
          cur.d = e.d;
        }
      }
    }

    const results = [];
    for (const b of bundles) {
      const send = [];
      for (const [key, w] of winning) {
        if (w.shards < 2) continue; // only one shard knows it: nothing to reconcile
        const own = b.payload.versions[key];
        if (own && own.d === w.d) continue;
        // Missing on this shard but known elsewhere, or membership disagrees.
        if (own || w.d === 1) send.push(repairEvent(key, w));
      }
      if (!send.length) {
        results.push({ shardId: b.shardId, repaired: 0 });
        continue;
      }
      try {
        const res = await fetch(`${this.shardUrls[b.shardId]}/events`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ events: send }),
          signal: AbortSignal.timeout(SHARD_WRITE_TIMEOUT),
        });
        const body = await res.json().catch(() => ({}));
        results.push({
          shardId: b.shardId,
          repaired: send.length,
          applied: body.applied || 0,
          changed: body.changed || 0,
          ok: res.ok,
          error: body.error,
        });
      } catch (err) {
        results.push({ shardId: b.shardId, repaired: send.length, ok: false, error: err.message });
      }
    }
    const record = { at: Date.now(), results };
    this.repairs.push(record);
    this.stats.repairs += 1;
    this.save();
    return { ok: true, ...record };
  }

  // --- reader state -------------------------------------------------------
  publicState() {
    const latest = this.versions[this.versions.length - 1] || null;
    const perShardStorage = new Map();
    let exactTotal = 0;
    for (const id of this.shardIds) {
      const b = this.bundles.get(id);
      const bytes = b ? Buffer.byteLength(JSON.stringify(b.payload.versions)) : 0;
      perShardStorage.set(id, bytes);
      exactTotal += bytes;
    }
    return {
      generatedAt: Date.now(),
      latest,
      history: this.versions.slice(-25).map(compactVersion),
      stats: this.stats,
      observations: this.observations.slice(-60),
      historyBundles: this.historyBundles.map((h) => ({ at: h.at, shards: Object.keys(h.stash) })),
      shards: this.shardIds.map((id) => {
        const b = this.bundles.get(id);
        return {
          shardId: id,
          url: this.shardUrls[id],
          hasBundle: !!b,
          generation: b ? b.payload.generation : null,
          incarnation: b ? b.payload.incarnation : null,
          liveCount: b ? b.payload.liveCount : null,
          tombstoneCount: b ? b.payload.tombstoneCount : null,
          summaryDigest: b ? b.payload.summaryDigest : null,
          exactDigest: b ? b.payload.exactDigest : null,
          receivedAt: b ? b.receivedAt : null,
          contentAgeMs: b ? Date.now() - b.receivedAt : null,
          approximateBytes: b ? serializeBytes(Summary.deserialize(b.payload.summary)) : null,
          exactVersionBytes: perShardStorage.get(id) || 0,
          summaryBuiltAt: b ? b.meta.builtAt : null,
        };
      }),
      storageTotals: { exactVersionBytes: exactTotal },
      interrupted: this.interrupted.slice(-8),
      repairs: this.repairs.slice(-5),
      verification: this.lastVerification,
      lastError: this.lastError,
      policy: POLICY_SUMMARY,
    };
  }

  writePublished() {
    const dir = path.resolve(this.publishDir);
    fs.mkdirSync(dir, { recursive: true });
    const state = this.publicState();
    fs.writeFileSync(path.join(dir, 'state.json'), JSON.stringify(state, null, 2));
    return state;
  }

  storageReport(bundles) {
    let exactBytes = 0;
    let approximateBytes = 0;
    let keys = 0;
    let tombstones = 0;
    let tombstoneKeyBytes = 0;
    for (const b of bundles) {
      const v = b.payload.versions;
      exactBytes += Buffer.byteLength(JSON.stringify(v));
      approximateBytes += serializeBytes(Summary.deserialize(b.payload.summary));
      for (const [key, e] of Object.entries(v)) {
        keys += 1;
        if (e.d === 1) {
          tombstones += 1;
          tombstoneKeyBytes += Buffer.byteLength(key);
        }
      }
    }
    return {
      exactVersionBytes: exactBytes,
      approximateSummaryBytes: approximateBytes,
      tombstoneKeyBytes,
      keys,
      tombstones,
      bytesPerKey: keys ? Math.round(exactBytes / keys) : 0,
      ratio: exactBytes ? Number((approximateBytes / exactBytes).toFixed(4)) : null,
      note:
        'exactVersionBytes is the LWW version map per shard (key + HLC + flags); approximateSummaryBytes is ' +
        'linear counter + KMV + HLL per shard. Summaries are O(parameters), not O(entities).',
    };
  }
}

const SHARD_WRITE_TIMEOUT = 8000;

// ---------------------------------------------------------------------------
function repairEvent(key, w) {
  return {
    id: `repair:${key}:${hlcKey(w.v)}:${w.d}`,
    op: w.d === 1 ? 'delete' : 'add',
    key,
    hlc: w.v,
    note: 'coordinator-repair',
  };
}

function choosePath({ mode, complete, lag, bundles }) {
  if (mode === 'exact') return 'exact';
  if (!bundles) return 'exact';
  if (mode === 'merge') return 'merge';
  if (mode === 'recompute') return complete ? 'recompute' : 'exact';
  if (!complete) return 'exact';
  if (lag > 0) return 'recompute';
  return 'merge';
}

function explain({ mode, path, complete, lag, merge, missing, stale, approximateAvailable }) {
  const reasons = [];
  if (mode !== 'auto') reasons.push(`mode=${mode} requested`);
  if (!complete) {
    reasons.push(`missing bundles: ${missing.join(', ') || 'none'}; exact baseline would be incomplete`);
  }
  if (path === 'merge') {
    if (stale.length) reasons.push(`stale bundles reused for: ${stale.join(', ')} (last known state, flagged)`);
    if (merge.stats.crossShardOverlap > 0) {
      reasons.push(`${merge.stats.crossShardOverlap} duplicate shard-level memberships deduplicated by the exact merge`);
    }
    reasons.push('no live/tombstone divergence and every summary sealed to its exact set: union merge is accepted');
  }
  if (path === 'recompute') {
    if (lag > 0) {
      reasons.push(
        `${lag} keys are live on at least one shard while the version merge tombstones them; a union of summaries cannot remove them`,
      );
    }
    reasons.push('rebuild one summary from the merged exact key set (whole-version recompute)');
  }
  if (path === 'exact') {
    reasons.push(approximateAvailable ? 'publishing the exact baseline only' : 'approximate merge not possible; exact baseline only');
  }
  return {
    mode,
    path,
    reasons,
    policy: POLICY_SUMMARY[path === 'merge' ? 'merge' : path === 'recompute' ? 'recompute' : 'exact'],
  };
}

const POLICY_SUMMARY = {
  merge:
    'cheap: union per-shard summaries. Preconditions: all shards present, each summary sealed to its exact set, no deletion divergence.',
  recompute:
    'corrective: rebuild one summary from the merged exact live set. Used when deletion divergence exists, on demand, and for verification.',
  exact: 'exact baseline only: no approximate number is published for this version.',
  localRebuild:
    'a shard rebuilds its summary from its own exact live set whenever that set changes; cost is linear in its live keys and is never a decrement.',
};

function emptyMerge() {
  return {
    mergedLiveKeys: [],
    mergedDeletedKeys: [],
    unionLive: new Set(),
    perShard: [],
    stats: {
      keys: 0,
      mergedLive: 0,
      mergedDeleted: 0,
      sumShardLive: 0,
      unionLive: 0,
      crossShardOverlap: 0,
      dedupSavings: 0,
      membershipConflicts: 0,
      versionConflicts: 0,
      deletionLagKeys: 0,
    },
    deletionLag: { keys: 0, byShard: {}, sample: [] },
    unionBias: 0,
  };
}

function bundleDigest(payload) {
  const rows = Object.entries(payload.versions || {})
    .map(([k, e]) => `${k}\u0001${hlcKey(e.v)}\u0001${e.d}\u0001${e.n}`)
    .sort();
  const h = createHash('sha256');
  h.update(`${payload.shardId}\u0000${payload.incarnation}\u0000${payload.generation}\u0000`);
  h.update(`${payload.exactDigest}\u0000${payload.summaryDigest}\u0000`);
  for (const r of rows) h.update(r + '\u0000');
  return h.digest('hex').slice(0, 20);
}

function watermarkOf(bundles) {
  const maxes = bundles.map((b) => maxVersionOf(b.payload.versions)).filter(Boolean);
  if (!maxes.length) return { min: null, max: null };
  let max = maxes[0];
  let min = maxes[0];
  for (const w of maxes) {
    if (compareHlc(w, max) > 0) max = w;
    if (compareHlc(w, min) < 0) min = w;
  }
  return { min, max };
}

function maxVersionOf(versions) {
  let max = null;
  for (const e of Object.values(versions || {})) if (!max || compareHlc(e.v, max) > 0) max = e.v;
  return max;
}

function errorAgainst(estimates, exact) {
  if (!estimates) return null;
  return {
    linear: round3(estimates.linear - exact),
    kmv: round3(estimates.kmv - exact),
    hll: round3(estimates.hll - exact),
  };
}

function relativeAgainst(estimates, exact) {
  if (!estimates || !exact) return null;
  const rel = (v) => Math.round(((v - exact) / exact) * 10000) / 10000;
  return { linear: rel(estimates.linear), kmv: rel(estimates.kmv), hll: rel(estimates.hll) };
}

function roundEstimates(e) {
  return { linear: round3(e.linear), kmv: round3(e.kmv), hll: round3(e.hll) };
}

function round3(v) {
  return Number.isFinite(v) ? Math.round(v * 1000) / 1000 : null;
}

// Estimates are rounded to 3 decimals before comparison, so equality means
// "identical at published precision" rather than exact float equality.
function nearlyEqual(d) {
  return Math.abs(d.linear) < 0.0011 && Math.abs(d.kmv) < 0.0011 && Math.abs(d.hll) < 0.0011;
}

function compactVersion(v) {
  const p = v.approximate.published;
  return {
    id: v.id,
    createdAt: v.createdAt,
    kind: v.kind,
    path: v.decision.path,
    trigger: v.trigger,
    note: v.note,
    exactLive: v.exact.mergedLive,
    complete: v.exact.complete,
    deletionLag: v.exact.deletionLag.keys,
    published: p ? p.estimates : null,
    publishedSource: p ? p.source : null,
    // Consistent yardstick across versions, whatever path produced the number:
    // published estimate minus merged exact live.
    error: v.approximate.errorAgainstMergedExact ? v.approximate.errorAgainstMergedExact.error : null,
    reasons: v.decision.reasons,
  };
}

function validateBundle(bundle, shardId) {
  if (!bundle || typeof bundle !== 'object') return 'bundle required';
  const p = bundle.payload;
  if (!p || typeof p !== 'object') return 'payload required';
  if (p.shardId !== shardId) return `shardId mismatch (${p.shardId} != ${shardId})`;
  if (typeof p.incarnation !== 'string' || !p.incarnation) return 'incarnation required';
  if (!Number.isSafeInteger(p.generation) || p.generation < 0) return 'generation required';
  if (!p.versions || typeof p.versions !== 'object') return 'versions required';
  if (!p.summary || typeof p.summary !== 'object') return 'summary required';
  if (typeof p.summaryDigest !== 'string' || !p.summaryDigest) return 'summaryDigest required';
  if (typeof p.exactDigest !== 'string' || !p.exactDigest) return 'exactDigest required';
  let count = 0;
  let tombstones = 0;
  for (const [k, e] of Object.entries(p.versions)) {
    if (!k) return 'empty key';
    if (!e || !isHlc(e.v) || (e.d !== 0 && e.d !== 1)) return `bad version entry for ${k}`;
    if (e.d === 0) count += 1;
    else tombstones += 1;
  }
  if (count !== p.liveCount) return `liveCount mismatch (${count} != ${p.liveCount})`;
  if (tombstones !== p.tombstoneCount) return `tombstoneCount mismatch (${tombstones} != ${p.tombstoneCount})`;
  return null;
}

module.exports = { Coordinator, bundleDigest, validateBundle, choosePath, POLICY_SUMMARY };
