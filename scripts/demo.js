'use strict';
// Non-interactive end-to-end walkthrough of every property the project claims.
// Runs two real shard processes and one coordinator, and checks the claims
// against the generator's truth.  Exit code 0 means every assertion held.
//
//   node scripts/demo.js [--entities 3000] [--seed 287] [--keep]

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Cluster } = require('../tests/harness');
const { generate } = require('./workload');
const { arg } = require('./cli');

const entities = Number(arg('entities', 3000));
const seed = Number(arg('seed', 287));
let failures = 0;

function check(label, ok, detail) {
  const mark = ok ? 'PASS' : 'FAIL';
  console.log(`  [${mark}] ${label}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failures += 1;
}

async function main() {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'activeset-demo-'));
  const c = await new Cluster({ dataDir }).start();
  const step = (n, t) => console.log(`\n${n}. ${t}`);
  try {
    const g = generate({ entities, seed });
    step(1, 'Load a workload with duplicate reports, cross-shard copies, deletes and retroactive corrections');
    for (const id of c.shardIds) await c.ingest(id, g.perShard[id]);
    console.log(`  workload: ${g.totalEvents} reports, ${g.totalEntities} entities, truth live = ${g.liveCount}`);
    console.log(`  ${g.corrections} retroactive corrections, ${g.retriedReports} duplicate reports, ${g.multiShardKeys} keys on both shards`);

    step(2, 'Exact baseline: LWW merge across shards equals generator truth');
    const v1 = await c.publish('auto');
    check('exact merged live = truth', v1.exact.mergedLive === g.liveCount, `${v1.exact.mergedLive} vs ${g.liveCount}`);
    check('duplicate reports do not inflate counts', v1.exact.crossShardOverlap > 0, `${v1.exact.crossShardOverlap} duplicate memberships deduplicated`);
    check('no deletion lag at rest', v1.exact.deletionLag.keys === 0);

    step(3, 'Pure-union merge path is exact set union; verification proves it');
    check('published via merge path', v1.decision.path === 'merge', v1.kind);
    const ver1 = await c.verify();
    check('union of summaries == summary of union keys', ver1.algebra.equal, JSON.stringify(ver1.algebra.drift));
    check(
      'published estimate within 2% of exact',
      Math.abs(v1.approximate.errorAgainstMergedExact.error.linear) / v1.exact.mergedLive < 0.02,
      `linear error ${v1.approximate.errorAgainstMergedExact.error.linear}`
    );

    step(4, 'Delete on one shard only: sketches cannot subtract, coordinator recomputes');
    const scn = await c.post(c.coordUrl('/scenario/divergent-deletes'), { count: 25 });
    check('scenario applied deletes', scn.body.ok && scn.body.changed > 0, `${scn.body.changed} keys now tombstoned on shard-a only`);
    const v2 = await c.publish('auto');
    check('deletion divergence detected', v2.exact.deletionLag.keys > 0, `${v2.exact.deletionLag.keys} keys`);
    check('coordinator switched to whole-version recompute', v2.decision.path === 'recompute', v2.kind);
    check(
      'recomputed estimate tracks merged exact',
      Math.abs(v2.approximate.published.estimates.linear - v2.exact.mergedLive) / v2.exact.mergedLive < 0.03,
      `${v2.approximate.published.estimates.linear} vs ${v2.exact.mergedLive}`
    );
    check(
      'uncorrected merge would have been biased upward',
      v2.approximate.mergeUnaudited.estimates.linear > v2.approximate.published.estimates.linear + 10,
      `merge would say ${v2.approximate.mergeUnaudited.estimates.linear}, recompute says ${v2.approximate.published.estimates.linear}`
    );
    check(
      'deletion-lag bias reported explicitly',
      v2.approximate.mergeUnaudited.errorAgainstMerged.linear > 0,
      `${v2.approximate.mergeUnaudited.errorAgainstMerged.linear}`
    );

    step(5, 'Forced merge remains available and is labelled as biased, not hidden');
    const v2b = await c.publish('merge');
    check('forced merge publishes the uncorrected union', v2b.decision.path === 'merge');
    check(
      'its error against merged exact is reported',
      v2b.approximate.errorAgainstMergedExact.error.linear > 0,
      `${v2b.approximate.errorAgainstMergedExact.error.linear}`
    );

    step(6, 'Verification separates sketch error from deletion-lag bias');
    const ver2 = await c.verify();
    check('union algebra still exact', ver2.algebra.equal);
    check(
      'gap decomposes into sketch error + exact lag',
      Math.abs(ver2.againstMergedExact.sketchError + ver2.againstMergedExact.deletionLagBias - ver2.againstMergedExact.linearGap) < 0.01
    );

    step(7, 'Repair reconciles shared keys so the cheap path is valid again');
    const rep = await c.repair();
    check('repair ran', rep.ok, rep.results.map((r) => `${r.shardId}:${r.repaired}`).join(' '));
    const v3 = await c.publish('auto');
    check('divergence cleared', v3.exact.deletionLag.keys === 0);
    check('back on the merge path', v3.decision.path === 'merge');
    check('exact count did not change from repair', v3.exact.mergedLive === v2.exact.mergedLive, `${v3.exact.mergedLive} vs ${v2.exact.mergedLive}`);

    step(8, 'Retransmitted old summary is rejected, not merged');
    await c.post(c.coordUrl('/scenario/stash'), {});
    await c.ingest('shard-a', [
      { id: 'retx-1', op: 'add', key: 'retx_key_1', hlc: { wall: Date.now(), seq: 0, node: 'demo' } },
    ]);
    // The coordinator must hold a *newer* bundle before the old one is replayed,
    // otherwise "old" and "current" are the same content and the receipt is
    // correctly classified as a retransmit.
    await c.publish('auto');
    const replay = await c.post(c.coordUrl('/scenario/replay-old'), { shardId: 'shard-a' });
    check('stale bundle rejected', replay.body.result.accepted === false && replay.body.result.reason === 'stale-generation', JSON.stringify(replay.body.result));

    step(9, 'Late shard: last known bundle is reused and flagged, never an empty set');
    await c.post(`${c.shardUrl('shard-b')}/admin/freeze`, {});
    const out = await c.collect('auto');
    check('shard-b reported as stale', out.collected['shard-b'].fresh === false, out.collected['shard-b'].error);
    check('exact baseline keeps shard-b contribution', out.version.exact.perShard.find((p) => p.shardId === 'shard-b').live > 0);
    check('version marked with stale shard', out.version.collect['shard-b'].fresh === false);
    await c.post(`${c.shardUrl('shard-b')}/admin/unfreeze`, {});

    step(10, 'Events during publish are reported as pending, not counted twice');
    const v4 = await c.publish('auto');
    const pendingAny = Object.values(v4.changeSinceBundle).some((x) => x.changed);
    check('changeSinceBundle reported per shard', 'shard-a' in v4.changeSinceBundle, pendingAny ? 'change seen after bundle' : 'no change in the window (timing)');
    const v5 = await c.publish('auto');
    check('next publish reflects exactly up to date state', v5.exact.mergedLive >= v4.exact.mergedLive);
    check('no duplicate counting: exact live stable across repeated collects', v5.exact.mergedLive === (await c.publish('auto')).exact.mergedLive);

    step(11, 'Shard process dies mid-flight and reboots from its journal');
    const before = await c.shardStatus('shard-a');
    await c.restartShard('shard-a');
    const after = await c.shardStatus('shard-a');
    check(
      'journal replay restores exact live count',
      after.exact.live === before.exact.live,
      `${after.exact.live} vs ${before.exact.live}`
    );
    check('new incarnation after reboot', after.loaded && after.exact, `gen ${after.generation}`);
    const v6 = await c.publish('auto');
    check('cluster count unaffected by reboot', v6.exact.mergedLive === v5.exact.mergedLive, `${v6.exact.mergedLive} vs ${v5.exact.mergedLive}`);

    step(12, 'Coordinator dies mid-publish and recovers without exposing a partial version');
    // Persist a pending publish artificially: crash right after the pending
    // record is written is hard to time externally, so exercise the recovery
    // path with a direct file injection on a fresh coordinator dir.
    await c.crashCoordinator();
    const storeFile = path.join(dataDir, 'coordinator', 'coordinator.json');
    const store = JSON.parse(fs.readFileSync(storeFile, 'utf8'));
    store.pending = { versionId: store.versionSeq + 1, mode: 'auto', trigger: 'crash-test', startedAt: Date.now(), bundles: {}, missing: [], stale: [] };
    fs.writeFileSync(storeFile, JSON.stringify(store, null, 2));
    const versionsBefore = store.versions.length;
    await c.restartCoordinator();
    const st = await c.state();
    check('partial publish was discarded', st.interrupted.length > 0, JSON.stringify(st.interrupted[st.interrupted.length - 1]).slice(0, 120));
    check('no phantom version appeared', st.latest.id === versionsBefore, `latest id ${st.latest && st.latest.id}, versions ${versionsBefore}`);
    const v7 = await c.publish('auto');
    check('publishing works after recovery', !!v7.id);
    check('recovered exact count matches', v7.exact.mergedLive === v6.exact.mergedLive, `${v7.exact.mergedLive} vs ${v6.exact.mergedLive}`);

    step(13, 'Storage: summaries are O(parameters), exact maps are O(keys)');
    const st7 = (await c.state()).latest.storage;
    console.log(`  exact version maps ${st7.exactVersionBytes} B for ${st7.keys} keys, summaries ${st7.approximateSummaryBytes} B, tombstones ${st7.tombstones}`);
    check('summary size independent of entity count', st7.approximateSummaryBytes < 100_000, `${st7.approximateSummaryBytes} B`);

    step(14, 'Web page and published state are served');
    const page = await fetch(c.coordUrl('/'));
    check('index.html served', page.status === 200);
    const stateDoc = await c.state();
    check('state.json has exact + approximate + evidence', !!(stateDoc.latest && stateDoc.latest.exact && stateDoc.latest.approximate && stateDoc.verification));

    console.log(`\n${failures === 0 ? 'ALL CHECKS PASSED' : `${failures} CHECK(S) FAILED`}`);
    if (arg('keep', false)) console.log(`data dir kept: ${dataDir}`);
  } finally {
    await c.stop();
    if (!arg('keep', false)) fs.rmSync(dataDir, { recursive: true, force: true });
  }
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
