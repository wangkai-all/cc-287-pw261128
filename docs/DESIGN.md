# Design: exact state and mergeable approximations, side by side

This document explains the decisions the code makes, why they are forced by the
data structures, and what evidence the system produces for every number it
publishes. Numbers quoted here come from `node scripts/demo.js` (seed 287,
3000 generated entities) and from a second run with 1500 entities; both are
reproducible.

---

## 0. The invariants the whole system is built to keep

1. **A key is in the active set at most once**, no matter how many add reports
   any shard received for it. Counting is over distinct keys, not reports.
2. **A delete removes the key from the set at a version.** It is never a
   decrement, and it removes the entity regardless of its occurrence count.
3. **No approximate structure is ever decremented.** Linear counting, KMV and
   HLL have no removal operation; the system never emulates one.
4. **A summary is only published together with the exact key set it was built
   from**, sealed by a digest, so a publish can never mix two instants.
5. **A late or retransmitted bundle cannot add a key twice or resurrect a
   deleted one.** Version comparison, not arrival order, decides.
6. **Every published number is reported with its exact baseline and a
   decomposition of its error.** Nothing is presented as exact that is not.

---

## 1. Why "count - 1" is not an option

The requirement forbids using `-1` to handle repeated occurrences of the same
entity. There are two independent reasons, and both are structural rather than
stylistic.

**Identity.** `alice` may report three times (phone, tablet, web). The count is
over *entities*, so three reports are one member. Decrementing per delete only
works if the delete can know how many occurrences it is cancelling, which it
cannot in general — reports and deletes arrive independently and out of order.

**Union-only structures.** The three sketches used here support only the union
operation:

| structure | element | merge | inverse |
|---|---|---|---|
| linear counting | one bit per hash bucket | bitwise OR | none |
| KMV / bottom-k | k smallest hashes | keep k smallest | none |
| HyperLogLog | max rank per register | register-wise max | none |

`LinearCounter.merge` is `a | b`. There is no value of `b` such that
`(a | b) & c == a` for an arbitrary removed `c` — once a bit is set it cannot be
cleared without knowing whether another live key also set it. `tests/sketches.test.js`
asserts that `remove`/`subtract` do not exist, so a later edit cannot quietly
introduce one.

The consequence is the design rule used everywhere below: **deletion is handled
above the sketch layer, by rebuilding from an exact live set — never inside it.**

---

## 2. Three planes, and where the boundary is

```
  client events
       │
       ▼
┌───────────────── shard process ─────────────────┐
│ exact plane   key → {version, deleted}          │  authoritative
│               journal (durable, fsync per batch)│
│               generation (bumps on set change)  │
│                                                 │
│ approximate   summary over *live keys only*     │  derived, disposable
│ plane         sealed to digest(live keys)       │
└────────────────────────┬────────────────────────┘
                         │ bundle = exact map + summary + digest + generation
                         ▼
┌────────────────── coordinator ──────────────────┐
│ LWW merge of version maps  → exact active set   │
│ union of summaries         → cheap estimate     │
│ rebuild from merged keys   → corrected estimate │
│ verification + repair                           │
└────────────────────────┬────────────────────────┘
                         ▼
                 published version + web page
```

**Exact state owns identity, membership and deletion.** It answers "which keys
are live", deduplicates across shards, and is the only thing allowed to say a
key is gone.

**Approximations own cardinality at bounded cost.** They answer "roughly how
many, in O(parameters) bytes", are rebuilt rather than edited, and are never
consulted for membership questions.

Anything that needs to *remove* a member is handled on the exact plane and the
approximation is re-derived. That boundary is the entire design.

---

## 3. Exact plane

### 3.1 Identity and occurrences

`ShardState.entities` is `key → { version, deleted, addReports, … }`.
`addReports` is kept for statistics and for showing that repeats collapse
(three adds of `u1` leave `liveKeys().length === 1`,
`tests/exact.test.js`). It is never used in a cardinality computation.

### 3.2 Versions: HLC and a total order

Every event carries `{wall, seq, node}`. Order is `(wall, seq, node)`, which is
total and deterministic (`compareHlc`). The node id is part of the order, so
two shards can produce concurrent timestamps without an ambiguous tie.

For each key the shard keeps only the **winning** version and whether it was an
add or a delete. Applying an event whose version is `<=` the stored one changes
nothing:

- retries are idempotent (same event id is also deduplicated),
- reordered delivery converges (tested by applying the same event list forward
  and backward),
- **a stale add cannot resurrect a deleted key**, because the delete has a
  higher version.

### 3.3 Tombstones

A delete stores a tombstone at its version and removes the key from the live
set. The tombstone is what makes later stale adds lose. It is *not* a counter
and is not decremented.

Tombstones are retained in the exact version map. That is why exact storage
grows with churn while summaries do not (§10). Garbage-collecting tombstones
safely needs a guarantee that every shard (and every in-flight bundle) has moved
past the tombstone's version; the coordinator computes a watermark
(`exact.watermark` = min/max of per-shard max versions) but GC is deliberately
not implemented — see §11.

### 3.4 Idempotency and retried batches

`Shard.ingest` validates the whole batch first, appends it to the journal with
one `write` + `fsync`, then applies it in memory. A retry of an acknowledged
batch is recognised twice over: identical event ids are `duplicate-report`, and
identical versions are `same-version`. Both return `applied: false`; neither
changes the live set, the generation, or any summary.

---

## 4. Approximate plane

### 4.1 The three sketches and their union operators

All three are union-only and merge exactly as set union requires:

- **Linear counter**: `m` bits; union is OR. Estimate `-m·ln(V/m)` where `V` is
  the number of empty buckets. Exact for small cardinalities up to saturation.
- **KMV**: the `k` smallest 64-bit hashes of the key set; union keeps the `k`
  smallest of the two heaps. `merge` sorts, drops hashes present in both heaps
  (same hash = same key), and truncates to `k`. This dedup step matters: without
  it, keys present on both shards occupy two of the `k` slots and inflate the
  estimate (`tests/sketches.test.js` covers both the overlap case and duplicate
  hash values inside one heap).
- **HyperLogLog**: `m = 2^p` registers of max rank; union is register-wise max,
  with linear-counting correction for small cardinalities.

The page shows all three on every publish, so the reader can see that they agree
within their expected error bands and that the reported error is not an artefact
of one estimator.

### 4.2 A summary is sealed to the key set it describes

`Summary.fromKeys(keys)` computes `exactDigest = digestOfKeys(keys)` (sorted
keys, SHA-256, 24 hex chars) and stores it in the summary payload. The bundle
carries `exactDigest` again at top level. On intake the coordinator recomputes
the digest from the bundle's own `versions` map and rejects the bundle if either
copy disagrees (`exact-digest-mismatch`).

This is what upgrades "trust me, this summary is current" to "this summary is a
function of exactly these keys". It is the precondition for §7.1.

### 4.3 Why the bundle is atomic

`Shard.bundle()` returns, as one JSON document:

```
{ shardId, incarnation, generation,
  versions: { key → {v, d, n} },      // exact, includes tombstones
  liveCount, tombstoneCount,
  summary: <linear + kmv + hll>,      // built from the live keys above
  summaryDigest, exactDigest }
```

The summary and the keys it was built from are produced in the same call and
therefore describe the same instant. If they were fetched separately, a shard
receiving an event between the two requests would hand the coordinator a summary
of 1000 keys with a version map of 1001 — and the "union of summaries equals
summary of the union" proof would silently be about nothing.

`generation` increments on every live-set transition (insert, delete, recreate —
not on extra occurrences, which cannot change a set of keys). `bundle()` rebuilds
the summary locally if its recorded generation no longer matches.

---

## 5. Division of labour

| question | answered by | cost | why not the other plane |
|---|---|---|---|
| is key `k` live, and since when? | exact version map | O(keys) | sketches cannot answer membership |
| how many distinct keys across shards? | exact LWW merge (baseline) / sketch union (estimate) | O(keys) / O(parameters) | exact is authoritative but must move keys |
| did a delete reach every shard? | comparison of version maps | O(keys) | a sketch cannot see a tombstone |
| did the estimate drift? | rebuild from exact keys | O(live keys) | only an exact rebuild is an oracle |
| how much space? | exact map bytes vs summary bytes | measured per version | — |

The approximate plane never decides membership, never resolves conflicts and is
never patched. The exact plane never uses the approximation to answer a
question it could answer itself.

---

## 6. Local rebuild vs whole-version recompute

Two different rebuilds, triggered by two different events. Conflating them is
the usual source of either stale summaries or needless full scans.

### 6.1 Local rebuild — a shard, about itself

**Trigger:** `this.generation` changed, i.e. that shard's own live set changed
(insert, delete, re-create). Also available on demand
(`POST /summary/rebuild`).

**What happens:** `Shard.rebuildSummary()` calls `Summary.fromKeys(liveKeys)`.
Tombstoned keys are simply not in the input, so deletion is handled by
construction. Cost is O(live keys on that shard); the duration and scan size are
recorded in `summaryMeta` and in the shard's `status()`.

**Why not patch:** a sketch has no subtraction. Even if the shard knows exactly
which key left, there is no safe operation that removes it. Rebuilding is the
only correct response, and it is cheap because it is scoped to one shard.

**What it does not do:** it cannot know that shard B still thinks a key is live.
That is divergence, and only the coordinator can see it.

### 6.2 Whole-version recompute — the coordinator, about the merged set

**Trigger:** `choosePath()` in `src/coordinator/coordinator.js`.

| mode | condition | path | why |
|---|---|---|---|
| `auto` | any shard has no bundle at all | `exact` | a count missing a shard is not a cluster count |
| `auto` | all shards present, `deletionLag == 0` | `merge` | union of summaries provably equals summary of union |
| `auto` | all shards present, `deletionLag > 0` | `recompute` | union summaries still contain keys the merge tombstoned |
| `merge` | forced | `merge` | available for comparison; the bias is reported, not hidden |
| `recompute` | forced | `recompute` | on-demand corrected number |
| `exact` | forced | `exact` | baseline only, no estimate published |

`deletionLag` is the set of keys with `d = 0` on at least one shard whose
last-writer-wins winner is a tombstone. It is computed from version maps, so it
is an **exact count of keys**, not an estimate:

```
unionLive   = |{ k : some shard has k with d=0 }|
mergedLive  = |{ k : LWW winner for k has d=0 }|
mergedLive ⊆ unionLive  ⇒  deletionLag = unionLive − mergedLive
```

(Live-on-a-shard implies the winner for that key cannot be "absent"; if the
winner is an add, the key is in both sets. So the only keys in `unionLive` but
not `mergedLive` are exactly the divergent ones.)

**What recompute does:** `Summary.fromKeys(merge.mergedLiveKeys)` — one summary
over the deduplicated, tombstone-corrected merged key set, on the coordinator,
with the same sketch parameters as the shards. The published number is that
summary's estimate, so the lag term is gone by construction rather than
corrected afterwards.

### 6.3 Cost model

| operation | work | where |
|---|---|---|
| local rebuild | O(live keys of one shard) | shard |
| bundle export | O(keys) serialization | shard |
| merge path publish | O(summary bytes) sketch merge + O(keys) LWW merge for the baseline | coordinator |
| recompute publish | O(merged live keys) hashing + the O(keys) merge above | coordinator |
| verification | one extra summary build from union keys and one from merged keys | coordinator |

The merge path's advantage is in *what the shards keep and what the sketches
have to touch*, not in eliminating the exact map at the coordinator: the demo
deliberately ships exact maps so the exact baseline and the lag count can be
produced at all. In a deployment where a key is owned by exactly one shard there
is no cross-shard overlap and no lag, and summaries alone suffice; overlap is
precisely what forces exact reconciliation. Keeping the exact plane here is what
makes the comparison honest — see §11.

### 6.4 Why the correction is a rebuild, not a "negative summary"

One could imagine building a summary of the divergent keys and subtracting it.
That does not work with union-only structures: subtracting a sketch is not
defined, and approximating a subtraction of two sketches compounds two
independent sampling errors with no bound. Enumerating the divergent keys is
required either way, and once they are enumerated the exact merged live set is
in hand — at which point rebuilding from it is O(n), exact, and certifiable
against a digest. So the system recomputes and never attempts sketch
subtraction.

---

## 7. Evidence: what proves the number, and what it costs

### 7.1 Merge algebra — proof that the cheap path is true set union

`verify()` computes two summaries:

```
U = union of the per-shard summaries          (sketch merge, O(bytes))
R = Summary.fromKeys(union of all shards' live keys)   (rehash, O(keys))
```

For linear counting, `U` and `R` must be **bitwise identical bitsets**; for KMV,
identical bottom-k sets; for HLL, identical registers. The reported drift is
`0, 0, 0` in every demo run. This is the check that makes the merge path
trustworthy: it demonstrates that the cheap operator implements real set union,
including deduplication of keys present on both shards.

It is a genuine proof for this data, not a statistical one: the digest sealing in
§4.2 ensures each input summary really is a function of the shard's exact keys.

### 7.2 Error decomposition — the published number is never unexplained

For a merge-path publication the linear estimate satisfies, by construction:

```
published linear = exact merged live
                 + (union estimate − union live)      ← sketch error
                 + (union live − merged live)         ← deletion lag, exact
```

`version.approximate.decomposition` stores exactly these four terms plus the
residual `published − exactLive`. For a recompute-path publication the lag term
is `0` and the identity reduces to estimate = exact + sketch error.

Crucially, §7.1 shows `union estimate` is exactly the estimate the counter gives
for the union *key set*; so the difference between the union estimate and an
estimate of the merged set is attributable to the lag keys, not to sloppy
merging. What remains against truth is ordinary sketch sampling error.

### 7.3 What `verify()` returns

| field | meaning |
|---|---|
| `algebra.equal`, `algebra.drift` | union-of-summaries vs summary-of-union-keys; must be zero |
| `againstMergedExact.sketchError` | union estimate − union live |
| `againstMergedExact.deletionLagBias` | union live − merged live (exact integer) |
| `againstMergedExact.linearGap` | union estimate − merged live = the two terms above |
| `publishedAtLastVersion`, `publishedSource` | what was actually shown to readers |
| `publishedMatchesRecomputed` | published number vs a fresh rebuild from merged keys |
| `bundlesUnchangedSincePublish` | whether any shard's generation moved since that publish |

The page prints these separately, because they answer different questions:
"does the cheap path compute true union" and "is the number users saw still
right".

### 7.4 Observed numbers (demo run, seed 287)

```
workload            9239 reports, 3000 entities, truth live 2665
                    55 retroactive corrections, 443 duplicate reports, 1788 shared keys
exact baseline      2665 = truth, 1453 duplicate memberships deduplicated
merge path          linear 2666.519, error +1.519 (0.057%), algebra drift 0/0/0
divergent deletes   25 keys tombstoned on shard-a only
  uncorrected merge would publish 2666.519   (+26.519 vs merged exact = 2640)
  recompute publishes            2640.486    (+0.486 vs merged exact)
  decomposition                  0.486 sketch + 26.519 exact lag
repair              25 events to shard-b, 0 to shard-a; lag 0; back on merge path
storage             403197 B exact version maps for 4789 keys, 52333 B summaries
```

---

## 8. Late data, retransmission, and events during publish

### 8.1 Intake rules (`Coordinator.receiveBundle`)

| situation | decision | effect |
|---|---|---|
| digest of the exact live keys ≠ `exactDigest`, or ≠ `summary.exactDigest` | reject `exact-digest-mismatch` | nothing stored |
| same incarnation, identical content digest | accept `retransmit-ignored` | receipt timestamp refreshed only |
| same incarnation, lower generation | reject `stale-generation` | current bundle kept |
| same incarnation, same generation, different summary digest | reject `summary-digest-conflict` | current bundle kept |
| different incarnation | accept as `stored` | `bootAccepted += 1`; if its generation is lower, `knowledgeRegressions += 1` |
| new incarnation with a valid, newer bundle | accept | normal path |

Retransmission is recognised by **content**, not by time, so an identical bundle
replayed at any later moment is a no-op. A genuinely old bundle needs an old
generation or an old digest to be rejected — which is exactly what a delayed
relay carries, because generations only move forward within one process
lifetime.

### 8.2 A late shard is stale, never empty

`GET /bundle` on a frozen or crashed shard returns `503`. The coordinator keeps
its last bundle for that shard, marks the collection result `fresh: false` with
an age, and marks the version's `collect[id].fresh` false. The alternative —
treating an unreachable shard as an empty set — would look like a mass deletion
and would be the single most dangerous failure mode for this system.

`exact.complete` is about *presence*: true when every configured shard
contributed something. A stale-but-present shard leaves `complete` true and is
surfaced separately through the `fresh` flags and the decision reasons.
`complete: false` only happens when a shard has never delivered a bundle at all.

### 8.3 Events arriving during a publish or recompute

Publish is snapshot-based: it collects one coherent bundle per shard, computes
from those bundles, and never consumes a delta. Events that arrive after a
bundle was taken are simply not in that snapshot.

- They are not lost: they are journaled by the shard and included in the next
  bundle.
- They cannot be counted twice: the next publish re-derives the count from
  bundles; there is no accumulator to double-add into.
- The delay is visible: `version.changeSinceBundle[shard]` reports
  `generationAtBundle` vs `generationNow` and `changed` (queried after the
  publish completes).

The same argument covers a recompute that takes seconds: the recompute reads
`merge.mergedLiveKeys`, which is an in-memory snapshot of the collected bundles.
Shards may accept writes during it; the next publish picks them up exactly once.

### 8.4 Reboot vs stale relay

`incarnation` is a random value per process lifetime. A restarted shard gets a
new one. Therefore:

- an old bundle from the *previous* process can never be mistaken for the
  current state, because the incarnation differs and the generation comparison
  across incarnations is not used to reject;
- a rebooted shard whose journal is shorter genuinely knows less. It is
  accepted (its state is real) and counted in `knowledgeRegressions`, so an
  operator can distinguish "restarted and lost a tail" from "quiet".

Clients are expected to retry unacknowledged batches; idempotency (§3.4) makes
that safe, and `repair` re-derives deleted keys from the merged version map.

---

## 9. Recovery

### 9.1 Shard

Every accepted batch is appended and `fsync`ed before the HTTP response. A crash
can leave at most a torn trailing line; `Journal.readAll` stops at the first
unparsable line and replays everything before it. Replay applies events through
the same `applyEvent`, so generation and live set come back identical —
`tests/cluster.test.js` asserts `after.exact.live === before.exact.live` and the
demo shows 2005 = 2005. Summaries are not journaled: they are derived state and
are rebuilt lazily on the next `bundle()`.

### 9.2 Coordinator

`publish()` writes a `pending` record (version id, mode, per-shard bundle
digests, missing and stale shards) and saves **before** appending the version.
A crash between the two leaves `pending` set and no version. On boot:

1. `pending` is moved to `interrupted` with the outcome
   "discarded before publication; readers never saw it";
2. no version is appended, so no phantom version id exists;
3. the next publish collects fresh bundles and starts again.

Readers only ever see complete versions, because `versions` is written after the
result is fully computed and `state.json` is written last.

### 9.3 Repair (anti-entropy)

`repair()` reconciles **membership disagreements only**, and only for keys known
by two or more shards:

```
if key known by < 2 shards          → skip (ordinary sharding, not divergence)
if this shard already agrees on d   → skip
otherwise                           → send the winning {version, d} as an event
```

Repair events reuse the winning version, so they are idempotent and cannot
resurrect anything: a stale add redelivered later still loses to the newer
tombstone. Keys that only one shard knows are deliberately *not* replicated —
pushing them everywhere would turn the cluster into full replicas and destroy the
storage advantage the project is demonstrating. In the demo, repair sends 25
events to `shard-b` and 0 to `shard-a`, and the merged exact count is unchanged
(2640 = 2640), because the merge already resolved those keys; repair changes
*who knows*, not *what is true*.

---

## 10. Storage

Per shard, with defaults (`linearBits = 2^16`, `kmvK = 512`, `hllP = 12`):

| component | bytes (approx.) | depends on |
|---|---|---|
| linear counter | 8192 raw → ~10.9 KB base64 | `linearBits` only |
| KMV | 512 × ~18 B hex → ~9.2 KB | `kmvK` only |
| HLL | 4096 registers → ~5.5 KB base64 | `hllP` only |
| **summary total** | **~25.5 KB per shard** | **parameters only** |

Measured in the demo: two shards → **52333 B** of summaries, against **403197 B**
of exact version maps for 4789 keys (**~84 B/key**). Summaries do not grow when
entities are added; the exact maps do, and they also grow with tombstones (720
tombstones in that run). The page's storage chart plots both series plus the
tombstone key bytes so the trade-off is visible rather than asserted.

Sketch accuracy at these parameters: linear counting is near-exact at these
cardinalities; HLL's standard error is `1.04/√4096 ≈ 1.6%`; KMV with `k = 512`
is comparable. The demo's observed errors are well inside those bands.

---

## 11. What is deliberately not implemented, and why

- **Tombstone GC.** Needs a guarantee that no shard, and no in-flight bundle,
  can still hold a version older than the tombstone. The watermark is computed
  and exposed, but deleting tombstones without that argument would allow
  resurrection. Leaving them is the safe choice.
- **Sketch subtraction.** See §6.4 — it is not a well-defined operation for
  these structures.
- **Approximation without exact keys at the coordinator.** The demo ships full
  version maps in every bundle because the exact baseline, cross-shard
  deduplication and the lag count all require them. A production design that
  owns each key on exactly one shard could ship summaries only; it would give up
  the ability to detect lag or to certify the union against an exact set. That
  trade-off is the subject of the exercise, so the exact side is kept and
  measured, not hidden.
- **A consensus protocol.** The coordinator is a single process. Shards are
  authoritative for their own state; the coordinator is authoritative for
  publication. Recovery is journal-based rather than coordinated.
- **Multi-coordinator takeover.** A second coordinator would need to agree on
  the version sequence; moving the coordinator store to a shared durable log
  would be the first step.

---

## 12. File map

| path | role |
|---|---|
| `src/common/hash.js` | 64-bit hash for sketches, key-set digest |
| `src/common/time.js` | HLC compare/order, canonical encoding |
| `src/common/sketches.js` | linear counter, KMV, HLL — union only |
| `src/common/summary.js` | the three sketches as one sealed summary; union |
| `src/common/merge.js` | per-key LWW merge, overlap, conflicts, deletion lag |
| `src/common/journal.js` | append + fsync batches, torn-tail-tolerant replay |
| `src/shard/state.js` | exact key→version state, tombstones, idempotency |
| `src/shard/shard.js` | journal, generation, local rebuild, bundle export |
| `src/shard/server.js` | shard HTTP API, freeze/crash/restart admin |
| `src/coordinator/coordinator.js` | intake rules, collect, publish, verify, repair, recovery |
| `src/coordinator/server.js` | coordinator HTTP API and static page |
| `src/coordinator/scenarios.js` | divergent deletes, late events, stash/replay |
| `scripts/workload.js` | deterministic workload with truth |
| `scripts/demo.js` | 14-step end-to-end assertion run |
| `web/public/` | page, renderer, charts, styles |
| `tests/` | unit, algebra, coordinator and real-process integration tests |

## 13. How each claim is tested

| claim | test |
|---|---|
| repeated reports count once; no decrement path | `exact.test.js`, `sketches.test.js` |
| stale add cannot resurrect a delete | `exact.test.js` |
| apply order is irrelevant | `exact.test.js` |
| summary rebuild excludes tombstoned keys | `exact.test.js` |
| KMV/HLL/linear unions are exact set unions | `sketches.test.js` |
| summaries are sealed to their key sets | `sketches.test.js`, `coordinator.test.js` |
| intake rejects stale, forged and duplicate bundles | `coordinator.test.js` |
| divergence forces recompute; lag is exact | `coordinator.test.js`, `cluster.test.js` |
| a missing shard blocks approximation | `coordinator.test.js` |
| crashes leave no phantom version | `coordinator.test.js`, `cluster.test.js` |
| two real shard processes converge to truth | `cluster.test.js` |
| frozen shard is stale, not empty | `cluster.test.js` |
| journal replay restores the exact set | `cluster.test.js` |
| events after a bundle are counted once | `cluster.test.js` |
| everything above, end to end, with numbers | `scripts/demo.js` |
