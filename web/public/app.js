'use strict';
// The demo page.  Everything is rendered from /state.json (server-published) and
// from explicit control calls.  No number on the page is computed only in the
// browser: exact counts, estimates, errors and storage all come from the
// coordinator, which derives them from shard responses.

const $ = (id) => document.getElementById(id);
const fmt = (n, d = 0) =>
  n === null || n === undefined || Number.isNaN(n)
    ? '—'
    : Number(n).toLocaleString(undefined, { maximumFractionDigits: d, minimumFractionDigits: 0 });
const pct = (v) => (v === null || v === undefined ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`);
const bytes = (n) => {
  if (n === null || n === undefined) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / 1024 / 1024).toFixed(2)} MiB`;
};

let state = null;
let timer = null;

async function api(path, body) {
  const res = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `HTTP ${res.status}`);
  return json;
}

function logLine(text) {
  const el = $('log');
  const time = new Date().toLocaleTimeString();
  el.textContent = `[${time}] ${text}\n` + el.textContent;
  if (el.textContent.length > 12000) el.textContent = el.textContent.slice(0, 12000);
}

async function refresh() {
  try {
    state = await api('/state.json');
    $('conn').textContent = 'live';
    $('conn').className = 'pill pill-ok';
    render(state);
  } catch (err) {
    $('conn').textContent = 'coordinator unreachable';
    $('conn').className = 'pill pill-bad';
  }
}

function render(s) {
  const v = s.latest;
  if (!v) {
    $('exactLive').textContent = 'no version yet';
    return;
  }
  const pub = v.approximate.published;
  const ex = v.exact;

  $('exactLive').textContent = fmt(ex.mergedLive);
  $('exactNote').textContent = ex.complete
    ? `${fmt(ex.keys)} keys known · ${fmt(ex.mergedDeleted)} tombstones · ${fmt(ex.crossShardOverlap)} duplicate memberships deduplicated`
    : 'INCOMPLETE: a shard has no bundle — this is not a cluster-wide count';

  if (pub) {
    // Headline error is always against the *merged exact* set: that is what a
    // user means by "how many people are there really".  On the merge path the
    // sketch error and the exact deletion-lag bias are shown separately, so a
    // number that is off is never presented as if the sketch were broken.
    $('published').textContent = fmt(pub.estimates.linear);
    $('publishedNote').textContent = `linear counter · ${pub.source.replace(/-/g, ' ')} · ${fmt(ex.mergedLive)} exact live keys`;
    const vsExact = pub.estimates.linear - ex.mergedLive;
    const relExact = ex.mergedLive ? vsExact / ex.mergedLive : null;
    $('error').textContent = `${vsExact > 0 ? '+' : ''}${fmt(vsExact, 3)}  (${pct(relExact)})`;
    const d = v.approximate.decomposition;
    const m = v.approximate.errorAgainstMergedExact;
    $('errorNote').textContent = d
      ? `linear = exact ${fmt(d.exactLive)} + sketch ${fmt(d.sketchError, 3)} + deletion lag ${fmt(d.deletionLagBias)}; KMV ${fmt(m.error.kmv, 2)}, HLL ${fmt(m.error.hll, 2)}`
      : `KMV ${fmt(m.error.kmv, 2)}, HLL ${fmt(m.error.hll, 2)}`;
  } else {
    $('published').textContent = 'exact only';
    $('publishedNote').textContent = 'no approximate number published for this version';
    $('error').textContent = '—';
    $('errorNote').textContent = '—';
  }

  $('lag').textContent = fmt(ex.deletionLag.keys);
  $('lagNote').textContent = ex.deletionLag.keys
    ? `by shard: ${Object.entries(ex.deletionLag.byShard).map(([k, n]) => `${k}=${n}`).join(', ')}`
    : 'no key is live on a surviving shard while tombstoned in the merge';

  const st = v.storage;
  $('storage').textContent = bytes(st.approximateSummaryBytes);
  $('storageNote').textContent = `vs ${bytes(st.exactVersionBytes)} exact version maps for ${fmt(st.keys)} keys · ratio ${st.ratio === null ? '—' : st.ratio}`;

  const pathPill = $('pathPill');
  pathPill.textContent = v.decision.path;
  pathPill.className = `pill ${v.decision.path === 'merge' ? 'pill-ok' : v.decision.path === 'recompute' ? 'pill-warn' : 'pill-muted'}`;
  $('decisionSummary').textContent = `${v.kind} · ${v.decision.policy || ''}`;
  $('reasons').innerHTML = v.decision.reasons.map((r) => `<li>${esc(r)}</li>`).join('');

  fillKv($('exactKv'), [
    ['Merged live (LWW)', fmt(ex.mergedLive)],
    ['Union of shard live sets', fmt(ex.unionLive)],
    ['Sum of shard live counts', fmt(ex.sumShardLive)],
    ['Cross-shard duplicates removed', fmt(ex.crossShardOverlap)],
    ['Tombstoned in merge', fmt(ex.mergedDeleted)],
    ['Membership conflicts', fmt(ex.membershipConflicts)],
    ['Version-only conflicts', fmt(ex.versionConflicts)],
    ['Watermark', ex.watermark && ex.watermark.max ? fmt(ex.watermark.max.wall) : '—'],
  ]);

  fillKv($('approxKv'), [
    ['Published source', pub ? pub.source : '—'],
    ['Linear counter', pub ? fmt(pub.estimates.linear, 3) : '—'],
    ['KMV', pub ? fmt(pub.estimates.kmv, 3) : '—'],
    ['HLL', pub ? fmt(pub.estimates.hll, 3) : '—'],
    ['Summary bytes (all shards)', pub ? bytes(v.approximate.storageBytes) : '—'],
    ['Uncorrected merge would say', v.approximate.mergeUnaudited ? fmt(v.approximate.mergeUnaudited.estimates.linear, 2) : '—'],
    ['…error vs merged exact', v.approximate.mergeUnaudited ? fmt(v.approximate.mergeUnaudited.errorAgainstMerged.linear, 2) : '—'],
    ['Recompute duration', v.approximate.recompute ? `${v.approximate.recompute.durationMs.toFixed(2)} ms` : '—'],
  ]);

  const d = v.approximate.decomposition;
  fillKv(
    $('decompKv'),
    d
      ? [
          ['Published linear', fmt(d.publishedLinear, 3)],
          ['Exact merged live', fmt(d.exactLive)],
          ['Union live', fmt(d.unionLive)],
          ['Sketch error', fmt(d.sketchError, 3)],
          ['Deletion-lag bias (exact)', fmt(d.deletionLagBias)],
          ['Residual vs merged exact', fmt(d.residualVsMergedExact, 3)],
        ]
      : [['—', 'no approximate number']]
  );

  renderShards(s);
  renderCharts(s);
  renderHistory(s);
  renderRecovery(s);
  renderVerification(s.verification);
}

function renderShards(s) {
  const body = $('shardsTable').querySelector('tbody');
  body.innerHTML = s.shards
    .map((sh) => {
      const c = s.latest && s.latest.collect ? s.latest.collect[sh.shardId] : null;
      const fresh = c && c.fresh;
      return `<tr>
        <td><strong>${esc(sh.shardId)}</strong><br><span class="mini">${esc((sh.incarnation || '').slice(0, 8))}</span></td>
        <td>${fmt(sh.liveCount)}</td>
        <td>${fmt(sh.tombstoneCount)}</td>
        <td>${fmt(sh.generation)}</td>
        <td>${c ? (fresh ? '<span class="pill pill-ok">fresh</span>' : `<span class="pill pill-warn">stale ${fmt((c.ageMs || 0) / 1000, 1)}s</span>`) : '<span class="pill pill-bad">no bundle</span>'}</td>
        <td>${bytes(sh.approximateBytes)}</td>
        <td>${bytes(sh.exactVersionBytes || 0)}</td>
        <td class="actions">
          <button class="btn btn-tiny" data-action="shard" data-shard="${esc(sh.shardId)}" data-shard-action="freeze" type="button">freeze</button>
          <button class="btn btn-tiny" data-action="shard" data-shard="${esc(sh.shardId)}" data-shard-action="unfreeze" type="button">unfreeze</button>
          <button class="btn btn-tiny" data-action="shard" data-shard="${esc(sh.shardId)}" data-shard-action="restart" type="button">restart</button>
        </td>
      </tr>`;
    })
    .join('');
}

function renderHistory(s) {
  const body = $('historyTable').querySelector('tbody');
  body.innerHTML = s.history
    .slice()
    .reverse()
    .map(
      (h) => `<tr>
      <td>${h.id}</td>
      <td><span class="pill ${h.path === 'merge' ? 'pill-ok' : h.path === 'recompute' ? 'pill-warn' : 'pill-muted'}">${esc(h.path)}</span></td>
      <td>${fmt(h.exactLive)}</td>
      <td>${h.published ? fmt(h.published.linear, 2) : '—'}</td>
      <td>${h.error ? fmt(h.error.linear, 2) : '—'}</td>
      <td>${fmt(h.deletionLag)}</td>
      <td class="mini">${esc(h.trigger)}${h.note ? ' · ' + esc(h.note) : ''}</td>
    </tr>`
    )
    .join('');
}

function renderRecovery(s) {
  const ints = $('interrupted');
  ints.innerHTML = (s.interrupted || []).length
    ? s.interrupted
        .map((i) => `<li>#${i.versionId} ${esc(i.trigger)} at ${new Date(i.startedAt).toLocaleTimeString()} → ${esc(i.outcome)}</li>`)
        .join('')
    : '<li class="mini">none</li>';

  const rej = $('rejects');
  const st = s.stats || {};
  rej.innerHTML = [
    `<li>retransmits ignored: ${fmt(st.retransmits)}</li>`,
    `<li>stale generations rejected: ${fmt(st.staleRejected)}</li>`,
    `<li>digest conflicts rejected: ${fmt(st.digestConflicts)}</li>`,
    `<li>shard reboots accepted: ${fmt(st.bootAccepted)}</li>`,
    `<li>knowledge regressions: ${fmt(st.knowledgeRegressions)}</li>`,
  ].join('');

  const rep = $('repairs');
  rep.innerHTML = (s.repairs || []).length
    ? s.repairs
        .slice()
        .reverse()
        .map(
          (r) =>
            `<li>${new Date(r.at).toLocaleTimeString()}: ${r.results
              .map((x) => `${esc(x.shardId)}: ${fmt(x.repaired)} sent, ${fmt(x.changed)} changed`)
              .join(' · ')}</li>`
        )
        .join('')
    : '<li class="mini">none</li>';
}

function renderVerification(v) {
  if (!v) return;
  $('verifyStamp').textContent = `verified ${new Date(v.verifiedAt).toLocaleTimeString()}`;
  const a = v.algebra;
  const cls = a.equal ? 'ok' : 'bad';
  $('verifyBox').innerHTML = `
    <p><strong class="${cls}">Merge algebra: ${a.equal ? 'exact' : 'MISMATCH'}</strong> — union of per-shard summaries vs one summary built from the union key set.
      Drift: linear ${fmt(a.drift.linear, 3)}, KMV ${fmt(a.drift.kmv, 3)}, HLL ${fmt(a.drift.hll, 3)}.</p>
    <p>Against merged exact (${fmt(v.exactLive)} live): rebuilt linear ${fmt(v.againstMergedExact.rebuilt.linear, 3)},
      union estimate ${fmt(v.againstMergedExact.unionEstimate.linear, 3)} = merged exact ${fmt(v.exactLive)}
      + sketch error ${fmt(v.againstMergedExact.sketchError, 3)}
      + deletion-lag bias ${fmt(v.againstMergedExact.deletionLagBias)}.</p>
    <p>Published number at last version: ${v.publishedAtLastVersion ? fmt(v.publishedAtLastVersion.linear, 3) : '—'}
      (${esc(v.publishedSource || '—')}) · matches recompute: <strong class="${v.publishedMatchesRecomputed ? 'ok' : 'bad'}">${v.publishedMatchesRecomputed === null ? '—' : v.publishedMatchesRecomputed}</strong>
      · bundles unchanged since publish: ${v.bundlesUnchangedSincePublish}.</p>`;
}

function fillKv(el, rows) {
  el.innerHTML = rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(String(v))}</dd>`).join('');
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// --- charts ---------------------------------------------------------------
// Small hand-rolled canvas plots: three series, no dependencies.

function setupCanvas(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth || canvas.width;
  const cssH = canvas.clientHeight || canvas.height;
  if (canvas.width !== Math.round(cssW * dpr) || canvas.height !== Math.round(cssH * dpr)) {
    canvas.width = Math.round(cssW * dpr);
    canvas.height = Math.round(cssH * dpr);
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);
  return { ctx, w: cssW, h: cssH };
}

const C = {
  grid: '#273043',
  axis: '#8b93a7',
  text: '#aeb7c9',
  exact: '#6ea8fe',
  merge: '#4ade80',
  recompute: '#fbbf24',
  biased: '#f472b6',
  exactBar: '#3b82f6',
  approxBar: '#22c55e',
};

function emptyChart(canvas, msg) {
  const { ctx, w, h } = setupCanvas(canvas);
  ctx.fillStyle = C.text;
  ctx.font = '13px system-ui, sans-serif';
  ctx.fillText(msg, 16, h / 2);
}

function axes(ctx, w, h, pad, yMin, yMax) {
  ctx.strokeStyle = C.grid;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let i = 0; i <= 4; i += 1) {
    const y = pad.t + ((h - pad.t - pad.b) * i) / 4;
    ctx.moveTo(pad.l, y);
    ctx.lineTo(w - pad.r, y);
  }
  ctx.stroke();
  return (v) => pad.t + (h - pad.t - pad.b) * (1 - (v - yMin) / Math.max(1e-9, yMax - yMin));
}

function renderCharts(s) {
  const obs = (s.observations || []).filter((o) => o.exactLive > 0);
  const acc = $('chartAccuracy');
  if (!obs.length) {
    emptyChart(acc, 'Publish a version to plot accuracy.');
    emptyChart($('chartDeletion'), 'Run “Delete on one shard only”, then collect.');
    emptyChart($('chartStorage'), 'Waiting for a version.');
    return;
  }
  drawAccuracy(acc, obs);
  drawDeletion($('chartDeletion'), obs);
  drawStorage($('chartStorage'), s);
}

function drawAccuracy(canvas, obs) {
  const { ctx, w, h } = setupCanvas(canvas);
  const pad = { l: 52, r: 14, t: 14, b: 26 };
  const vals = obs.flatMap((o) => [o.exactLive, o.publishedLinear, o.mergePathLinear || o.publishedLinear]);
  const yMin = Math.min(...vals) * 0.94;
  const yMax = Math.max(...vals) * 1.06;
  const Y = axes(ctx, w, h, pad, yMin, yMax);
  const X = (i) => pad.l + ((w - pad.l - pad.r) * i) / Math.max(1, obs.length - 1);

  // exact baseline line
  ctx.strokeStyle = C.exact;
  ctx.lineWidth = 2;
  ctx.beginPath();
  obs.forEach((o, i) => (i ? ctx.lineTo(X(i), Y(o.exactLive)) : ctx.moveTo(X(i), Y(o.exactLive))));
  ctx.stroke();

  // published markers
  obs.forEach((o, i) => {
    ctx.fillStyle = o.path === 'recompute' ? C.recompute : C.merge;
    ctx.beginPath();
    ctx.arc(X(i), Y(o.publishedLinear), 3.2, 0, Math.PI * 2);
    ctx.fill();
  });

  // biased merge dots (hollow): only where the merge path was not published
  obs.forEach((o, i) => {
    if (o.path === 'merge' || o.mergePathLinear === null) return;
    ctx.strokeStyle = C.biased;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.arc(X(i), Y(o.mergePathLinear), 5, 0, Math.PI * 2);
    ctx.stroke();
  });

  labelY(ctx, pad, w, h, yMin, yMax);
  ctx.fillStyle = C.text;
  ctx.font = '12px system-ui, sans-serif';
  ctx.fillText('linear counter estimate', pad.l, 12);
  // legend
  ctx.fillStyle = C.exact;
  ctx.fillText('exact', w - 210, 12);
  ctx.fillStyle = C.merge;
  ctx.fillText('published (merge)', w - 160, 12);
  ctx.fillStyle = C.recompute;
  ctx.fillText('published (recompute)', w - 70, 12);
}

function drawDeletion(canvas, obs) {
  const { ctx, w, h } = setupCanvas(canvas);
  const pad = { l: 52, r: 14, t: 14, b: 26 };
  const maxLag = Math.max(10, ...obs.map((o) => o.deletionLag));
  const maxErr = Math.max(5, ...obs.map((o) => Math.abs(o.exactLive ? (o.publishedLinear - o.exactLive) : 0)));
  const yMax = Math.max(maxLag, maxErr) * 1.15;
  const Y = axes(ctx, w, h, pad, 0, yMax);
  const X = (i) => pad.l + ((w - pad.l - pad.r) * i) / Math.max(1, obs.length - 1);

  // lag bars
  const bw = Math.max(2, ((w - pad.l - pad.r) / obs.length) * 0.5);
  obs.forEach((o, i) => {
    if (!o.deletionLag) return;
    ctx.fillStyle = C.biased;
    ctx.globalAlpha = 0.75;
    ctx.fillRect(X(i) - bw / 2, Y(o.deletionLag), bw, Y(0) - Y(o.deletionLag));
    ctx.globalAlpha = 1;
  });

  // published error line
  ctx.strokeStyle = C.recompute;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  obs.forEach((o, i) => {
    const e = Math.abs(o.publishedLinear - o.exactLive);
    if (i) ctx.lineTo(X(i), Y(e));
    else ctx.moveTo(X(i), Y(e));
  });
  ctx.stroke();

  labelY(ctx, pad, w, h, 0, yMax);
  ctx.fillStyle = C.text;
  ctx.font = '12px system-ui, sans-serif';
  ctx.fillText('keys', 8, 12);
  ctx.fillStyle = C.biased;
  ctx.fillText('deletion divergence (bars)', w - 190, 12);
  ctx.fillStyle = C.recompute;
  ctx.fillText('|published error|', w - 190, 26);
}

function drawStorage(canvas, s) {
  const v = s.latest;
  const { ctx, w, h } = setupCanvas(canvas);
  const st = v.storage;
  const pad = { l: 52, r: 14, t: 18, b: 40 };
  const series = [
    { label: 'exact version maps', value: st.exactVersionBytes, color: C.exactBar },
    { label: 'approximate summaries', value: st.approximateSummaryBytes, color: C.approxBar },
    { label: 'tombstone key bytes', value: st.tombstoneKeyBytes || 0, color: C.biased },
  ];
  const yMax = Math.max(1, ...series.map((x) => x.value)) * 1.15;
  const Y = axes(ctx, w, h, pad, 0, yMax);
  const slot = (w - pad.l - pad.r) / series.length;
  series.forEach((x, i) => {
    const bw = slot * 0.5;
    const cx = pad.l + slot * i + slot / 2;
    ctx.fillStyle = x.color;
    ctx.fillRect(cx - bw / 2, Y(x.value), bw, Y(0) - Y(x.value));
    ctx.fillStyle = C.text;
    ctx.font = '12px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(bytes(x.value), cx, Y(x.value) - 6);
    ctx.fillText(x.label, cx, h - 14);
    ctx.textAlign = 'left';
  });
  labelY(ctx, pad, w, h, 0, yMax);
}

function labelY(ctx, pad, w, h, yMin, yMax) {
  ctx.fillStyle = C.text;
  ctx.font = '11px system-ui, sans-serif';
  ctx.textAlign = 'right';
  for (let i = 0; i <= 4; i += 1) {
    const v = yMin + ((yMax - yMin) * (4 - i)) / 4;
    const y = pad.t + ((h - pad.t - pad.b) * i) / 4;
    ctx.fillText(compact(v), pad.l - 6, y + 4);
  }
  ctx.textAlign = 'left';
}

function compact(v) {
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  return Math.round(v).toString();
}

// --- controls -------------------------------------------------------------
async function onAction(e) {
  const btn = e.target.closest('button[data-action]');
  if (!btn) return;
  const action = btn.dataset.action;
  btn.disabled = true;
  try {
    if (action === 'collect') {
      const out = await api('/collect', { mode: btn.dataset.mode });
      logLine(`collect(${btn.dataset.mode}) → ${out.version.kind}/${out.version.decision.path}, exact ${out.version.exact.mergedLive}`);
    } else if (action === 'verify') {
      const v = await api('/verify', {});
      logLine(`verify → algebra equal=${v.algebra.equal}, publishedMatches=${v.publishedMatchesRecomputed}`);
    } else if (action === 'repair') {
      const r = await api('/repair', {});
      logLine(`repair → ${r.results.map((x) => `${x.shardId}:${x.repaired}`).join(' ')}`);
    } else if (action === 'scenario') {
      const name = btn.dataset.scenario;
      const r = await api(
        `/scenario/${name}`,
        name === 'replay-old' ? { shardId: 'shard-a' } : name === 'divergent-deletes' ? { count: 25 } : {}
      );
      logLine(`scenario ${name} → ${JSON.stringify(r).slice(0, 300)}`);
    } else if (action === 'shard') {
      const r = await api(`/admin/shards/${btn.dataset.shard}/${btn.dataset.shardAction}`, {});
      logLine(`shard ${btn.dataset.shard} ${btn.dataset.shardAction} → ${JSON.stringify(r)}`);
    }
    await refresh();
  } catch (err) {
    logLine(`ERROR ${action}: ${err.message}`);
  } finally {
    btn.disabled = false;
  }
}

function setAuto(on) {
  if (timer) clearInterval(timer);
  timer = null;
  if (on) timer = setInterval(refresh, 2000);
}

document.addEventListener('click', onAction);
$('refresh').addEventListener('click', refresh);
$('auto').addEventListener('change', (e) => setAuto(e.target.checked));
window.addEventListener('resize', () => {
  if (state) renderCharts(state);
});
refresh();
setAuto(true);
