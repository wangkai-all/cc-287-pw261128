# Active-set merge lab

Counting the **distinct active entities** across several shards, where the same
entity may be reported many times, deletes arrive separately, and some events are
retroactive corrections.

The interesting part is what happens to the approximate structures when a delete
arrives. Linear counting, KMV and HyperLogLog are union-only — there is no
`-1` — so this project never decrements a sketch. Deletion is handled on an exact
versioned plane, and the approximate number is produced either by a provably
correct union of per-shard summaries or by rebuilding one summary from the merged
exact key set. Every published number is shown next to the exact baseline and a
breakdown of its error.

No dependencies. Node.js 22+ only (`node:http`, `node:fs`, `node:crypto`,
`node:test`).

---

## Quick start

```bash
node scripts/start.js            # coordinator :7000, shard-a :7001, shard-b :7002
node scripts/seed.js             # in another terminal: load a workload and publish
open http://127.0.0.1:7000
```

`scripts/start.js` spawns three **separate OS processes**, each with its own
journal directory under `./data`. Killing any one of them and restarting it is
part of the demo; the journals are the source of truth on boot.

Options:

```bash
node scripts/start.js --port 8000 --data /tmp/activeset --sketch kmvK=256,hllP=10
node scripts/seed.js  --url http://127.0.0.1:8000 --entities 5000 --seed 287
```

## One-shot proof

```bash
node scripts/demo.js             # 14 steps, PASS/FAIL per claim, exit code 0 on success
```

The demo boots a real 2-shard cluster on free ports, loads a deterministic
workload, and checks every claim in this README against the generator's ground
truth: exact baseline, merge algebra, divergent deletes, recompute, repair,
stale-bundle rejection, frozen shard, crash recovery (shard and coordinator),
storage, and the served page. Use `--entities 3000 --seed 287` (the defaults) to
reproduce the numbers quoted in `docs/DESIGN.md`; add `--keep` to keep the data
directory.

## Tests

```bash
npm test                         # everything (39 tests)
npm run test:unit                # sketches, exact state, coordinator policy
npm run test:cluster             # real child processes, real HTTP, real journals
```

The cluster tests spawn shard and coordinator processes, `SIGKILL` them, restart
them on the same data directory, and assert the numbers survived.

---

## What to look at on the page

The page renders only from `/state.json`, which the coordinator writes after each
publish. Nothing is computed in the browser.

| card | meaning |
|---|---|
| **Exact merged live** | LWW merge of the shard version maps — the authoritative count of distinct live keys |
| **Published estimate** | what readers were actually given, and by which path |
| **Error vs exact** | published − exact, with the decomposition underneath |
| **Deletion divergence** | keys still live on some shard but tombstoned by the merge |
| **Summary storage** | summary bytes vs exact version-map bytes |

The **How this version was produced** panel shows the chosen path
(`merge` / `recompute` / `exact`), the reasons for it, and three side-by-side
columns: exact plane, approximate plane, and the error identity

```
published linear = exact live + sketch error + deletion-lag bias
```

The three charts plot every publish: estimate against exact over time (including
hollow dots for what an uncorrected merge would have said), deletion-divergence
impact, and storage for the two planes.

**Controls** run real operations against the real cluster:

- *Collect + auto* / *Force merge* / *Recompute* / *Exact only* — publish with an
  explicit path.
- *Verify against exact* — rebuild summaries from exact keys and report the
  algebra check plus the error decomposition.
- *Repair divergence* — push winning versions to the shard that disagrees, only
  for keys known by more than one shard.
- *Delete on one shard only*, *Late events*, *Stash bundle*, *Replay old bundle*
  — the failure scenarios.
- *Freeze / Unfreeze / Crash / Restart* per shard.

### Suggested walkthrough

1. Open the page and note the exact count, the estimate and the path (`merge`).
2. Press **Verify against exact** — algebra drift is zero, and the published
   number matches a fresh rebuild.
3. Press **Delete on one shard only**, then **Collect + auto**. The path flips to
   `recompute`, the divergence card becomes non-zero, and the decomposition
   attributes the whole gap to the exact lag term.
4. Press **Force merge** to see the uncorrected union, labelled as biased.
5. Press **Repair divergence**, then **Collect + auto** — lag returns to zero and
   the merge path is valid again, with the same exact count.
6. Press **Stash bundle**, publish, then **Replay old bundle** — rejected as a
   stale generation, count unchanged.
7. **Freeze** a shard and collect — the shard is reported stale with its last
   known set, never as empty.
8. **Crash** and **Restart** a shard — the journal restores its exact live count.

---

## Architecture

```
client events ──► shard-a process            shard-b process
                  exact: key→{version,d}      exact: key→{version,d}
                  journal (fsync per batch)   journal (fsync per batch)
                  summary over live keys      summary over live keys
                       │  bundle: versions + summary + digests + generation
                       ▼
                  coordinator process
                  LWW merge ──► exact active set (baseline)
                  union of summaries ──► cheap estimate  (valid when lag = 0)
                  rebuild from merged keys ──► corrected estimate (lag > 0)
                       │
                       ▼
                  /state.json ──► web page
```

- **Shards** own exact membership. Each event carries a hybrid logical clock
  `{wall, seq, node}`; the highest version per key wins, and a delete is a
  tombstone at a version — never a decrement. A generation increments whenever
  the live set changes, which invalidates the shard's own summary and forces a
  local rebuild on the next export.
- **Bundles** are atomic: the exact version map and the summary built from it
  arrive in one document, and the summary is sealed with a digest of the exact
  live keys. So a publish can never combine a summary from one instant with exact
  state from another.
- **The coordinator** merges version maps (LWW, deletion wins ties) to get the
  exact baseline, detects deletion divergence from those maps, and chooses the
  cheap or corrected path. It is also the verification oracle and the repair
  driver.
- **Publish is snapshot-based.** Events arriving mid-publish are not lost and not
  counted twice: they are journaled by the shard and appear in exactly one later
  bundle, reported meanwhile as `changeSinceBundle`.

Full rationale, the proofs behind the merge path, the recovery arguments and the
measured numbers are in [docs/DESIGN.md](docs/DESIGN.md).

---

## API

### Shard (`:7001`, `:7002`)

| method | path | description |
|---|---|---|
| `POST` | `/events` | `{events:[…]}` — validate, journal+fsync, apply. Idempotent by event id and version. |
| `GET` | `/bundle` | coherent `{versions, liveCount, summary, exactDigest, summaryDigest, generation, incarnation}` |
| `POST` | `/summary/rebuild` | force a local rebuild from the exact live set |
| `GET` | `/status` | counters, generation, summary validity and cost, journal bytes |
| `GET` | `/exact/keys` | sorted live and deleted keys (debug / verification) |
| `POST` | `/admin/freeze` `/unfreeze` | behave as a late shard (503 on data routes) |
| `POST` | `/admin/crash` `/restart` | simulate abrupt death, reload from journal |
| `GET` | `/health` | liveness |

Event shape:

```json
{ "id": "evt-1", "op": "add", "key": "ent_x", "hlc": { "wall": 1760000000000, "seq": 0, "node": "shard-a" } }
```

`op` is `add` or `delete`. `id` must be unique per logical event; a retry
re-using the id is a no-op.

### Coordinator (`:7000`)

| method | path | description |
|---|---|---|
| `GET` | `/` | the demo page |
| `GET` | `/state.json` | published reader state (also written to `web/public/state.json`) |
| `GET` | `/health`, `/shards` | liveness; shard status proxied |
| `POST` | `/publish` | `{mode: auto\|merge\|recompute\|exact}`, collects and publishes |
| `POST` | `/collect` | collect + publish, returns both |
| `POST` | `/collect-dry` | collect without publishing |
| `POST` | `/verify` | algebra proof + error decomposition + published-vs-rebuild |
| `POST` | `/repair` | anti-entropy for membership disagreements on shared keys |
| `POST` | `/bundles/:shardId` | shard push; idempotent by content digest |
| `POST` | `/ingest` | demo convenience: generate a workload, route it, collect, publish |
| `POST` | `/scenario/divergent-deletes` | `{count}` — delete keys on shard-a only |
| `POST` | `/scenario/late-events` | `{count}` — new keys with old HLCs |
| `POST` | `/scenario/stash` | snapshot the current bundles |
| `POST` | `/scenario/replay-old` | `{shardId}` — resend the stashed bundle |
| `POST` | `/admin/shards/:id/(freeze\|unfreeze\|crash\|restart)` | proxy to the shard |

---

## Guarantees, in one place

| property | mechanism |
|---|---|
| distinct entities, not reports | key-level exact state; `addReports` is never summed |
| deletes are not decrements | tombstone at a version; sketches are rebuilt, never patched |
| no double counting on retry | event-id dedup + version comparison; batches acknowledged only after fsync |
| no double counting on late bundles | content digest → `retransmit-ignored`; older generation → `stale-generation` |
| no resurrection by late adds | HLC order; a stale add always loses to the newer tombstone |
| no mixing of instants | summary and exact keys in one bundle, bound by `digestOfKeys` |
| a down shard is not an empty shard | 503 → coordinator reuses and flags the last bundle |
| deletion divergence is quantified | exact count from version maps, not a guess |
| merge path is provably correct | union-of-summaries vs summary-of-union-keys must match bitwise |
| crash recovery is honest | journal replay for shards; `pending` marker discarded on coordinator boot |
| failure is observable | `/state.json` carries decompositions, rejection counters, interrupted publishes |

## Repository layout

```
src/common/      hash, HLC, sketches, summary, merge, journal, http
src/shard/       state, shard (journal/rebuild/bundle), HTTP server
src/coordinator/ publish/verify/repair/recovery, HTTP server, scenarios
scripts/         start, seed, demo, workload generator, CLI helpers
web/public/      index.html, app.js, styles.css, published state.json
tests/           unit + algebra + coordinator + real-process integration
docs/DESIGN.md   design rationale, proofs, measured numbers
```
