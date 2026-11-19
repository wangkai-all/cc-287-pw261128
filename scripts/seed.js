'use strict';
// Seed the running cluster with a deterministic workload, then publish.
//
//   node scripts/seed.js [--url http://127.0.0.1:7000] [--entities 5000] [--seed 287]

const { arg, baseUrl, post } = require('./cli');

async function main() {
  const url = baseUrl();
  const entities = Number(arg('entities', 5000));
  const seed = Number(arg('seed', 287));
  const deleteFraction = Number(arg('delete-fraction', 0.12));

  const out = await post(`${url}/ingest`, { entities, seed, deleteFraction });
  const w = out.workload;
  const v = out.version;
  console.log('workload');
  console.log(`  entities generated      ${w.entities}`);
  console.log(`  live under generator truth ${w.truthLive}`);
  console.log(`  reports (add/delete)    ${w.events} (${w.events - w.deleteEvents}/${w.deleteEvents})`);
  console.log(`  retroactive corrections ${w.corrections}`);
  console.log(`  duplicate reports       ${w.retried}`);
  console.log(`  keys seen by both shards ${w.sharedKeys}`);
  console.log('ingest');
  for (const [id, r] of Object.entries(out.results)) {
    console.log(`  ${id}: ${r.reports} reports, ${r.applied} applied, ${r.changed} changed the live set`);
  }
  console.log('published');
  console.log(`  kind / path       ${v.kind} / ${v.decision.path}`);
  console.log(`  exact merged live ${v.exact.mergedLive}  (union ${v.exact.unionLive}, sum ${v.exact.sumShardLive}, duplicates removed ${v.exact.crossShardOverlap})`);
  console.log(`  deletion lag      ${v.exact.deletionLag.keys}`);
  if (v.approximate.published) {
    const p = v.approximate.published;
    const m = v.approximate.errorAgainstMergedExact;
    console.log(`  estimate          linear ${p.estimates.linear}  kmv ${p.estimates.kmv}  hll ${p.estimates.hll}`);
    console.log(`  error vs exact    linear ${m.error.linear}  (${(m.relativeError.linear * 100).toFixed(2)}%)`);
  }
  console.log(`  reasons:`);
  for (const r of v.decision.reasons) console.log(`    - ${r}`);
  console.log(`\nopen ${url}`);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
