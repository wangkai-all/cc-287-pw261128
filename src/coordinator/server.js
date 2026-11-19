'use strict';
// Coordinator HTTP service + static web UI.
//
// Routes:
//   GET  /                     demo page
//   GET  /state.json           published reader state
//   POST /publish              { mode: auto|merge|recompute|exact }
//   POST /verify               compare published number against exact recompute
//   POST /repair               anti-entropy: push winning versions to shards
//   POST /bundles/:shardId     shard push; idempotent by content
//   POST /collect              re-collect every shard's bundle and publish
//   POST /collect-dry          collect without publishing (show bundle state)
//   GET  /shards               per-shard status proxied from the shards

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Coordinator } = require('./coordinator');
const scenarios = require('./scenarios');
const { readJson, sendJson } = require('../common/http');

const DEFAULT_SHARD_URLS = {
  'shard-a': process.env.SHARD_A_URL || 'http://127.0.0.1:7001',
  'shard-b': process.env.SHARD_B_URL || 'http://127.0.0.1:7002',
};

function createCoordinatorServer({
  dataDir,
  publishDir = path.join(__dirname, '../../web/public'),
  shardUrls = DEFAULT_SHARD_URLS,
  port = 7000,
  sketchOptions = {},
  webDir = path.join(__dirname, '../../web/public'),
}) {
  const coordinator = new Coordinator({ dataDir, publishDir, shardUrls, sketchOptions });

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    try {
      if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
        return sendFile(res, path.join(webDir, 'index.html'), 'text/html; charset=utf-8');
      }
      if (req.method === 'GET' && /^\/(app\.js|styles\.css)$/.test(url.pathname)) {
        const type = url.pathname.endsWith('.css') ? 'text/css; charset=utf-8' : 'text/javascript; charset=utf-8';
        return sendFile(res, path.join(webDir, url.pathname.slice(1)), type);
      }
      if (req.method === 'GET' && url.pathname === '/state.json') {
        return sendJson(res, 200, coordinator.publicState());
      }
      if (req.method === 'GET' && url.pathname === '/health') {
        return sendJson(res, 200, { ok: true, shards: shardUrls, coordinatorPid: process.pid });
      }
      if (req.method === 'GET' && url.pathname === '/shards') {
        const out = {};
        await Promise.all(
          Object.entries(shardUrls).map(async ([id, base]) => {
            try {
              const r = await fetch(`${base}/status`, { signal: AbortSignal.timeout(3000) });
              out[id] = r.ok ? await r.json() : { error: `HTTP ${r.status}` };
            } catch (err) {
              out[id] = { error: err.message, unreachable: true };
            }
          })
        );
        return sendJson(res, 200, out);
      }
      if (req.method === 'POST' && url.pathname === '/publish') {
        const body = await readJson(req);
        const version = await coordinator.publish({
          mode: body.mode || 'auto',
          note: body.note || '',
          trigger: 'manual',
        });
        return sendJson(res, 200, version);
      }
      if (req.method === 'POST' && url.pathname === '/verify') {
        return sendJson(res, 200, coordinator.verify());
      }
      if (req.method === 'POST' && url.pathname === '/repair') {
        return sendJson(res, 200, await coordinator.repair());
      }
      const admin = url.pathname.match(/^\/admin\/shards\/([A-Za-z0-9_-]+)\/(freeze|unfreeze|crash|restart)$/);
      if (req.method === 'POST' && admin) {
        const [, shardId, action] = admin;
        if (!shardUrls[shardId]) return sendJson(res, 404, { error: `unknown shard ${shardId}` });
        try {
          const r = await fetch(`${shardUrls[shardId]}/admin/${action}`, { method: 'POST', signal: AbortSignal.timeout(5000) });
          return sendJson(res, r.status, await r.json().catch(() => ({})));
        } catch (err) {
          return sendJson(res, 502, { error: err.message });
        }
      }
      if (req.method === 'POST' && url.pathname === '/scenario/divergent-deletes') {
        const body = await readJson(req);
        return sendJson(res, 200, await scenarios.divergentDeletes({ cluster: { shardUrl: (id) => shardUrls[id] }, coordinator, count: body.count }));
      }
      if (req.method === 'POST' && url.pathname === '/scenario/late-events') {
        const body = await readJson(req);
        return sendJson(res, 200, await scenarios.lateEvents({ cluster: { shardUrl: (id) => shardUrls[id] }, coordinator, count: body.count }));
      }
      if (req.method === 'POST' && url.pathname === '/scenario/stash') {
        return sendJson(res, 200, scenarios.stashBundle({ coordinator }));
      }
      if (req.method === 'POST' && url.pathname === '/scenario/replay-old') {
        const body = await readJson(req);
        return sendJson(res, 200, await scenarios.replayStashedBundle({ coordinator, shardId: body.shardId }));
      }
      if (req.method === 'POST' && url.pathname === '/collect-dry') {
        const collected = await coordinator.collect();
        return sendJson(res, 200, describeCollected(collected));
      }
      if (req.method === 'POST' && url.pathname === '/ingest') {
        // Demo convenience: route a workload batch to the right shards and then
        // collect + publish.  Real clients talk to shards directly.
        const body = await readJson(req);
        const g = body.workload || {};
        const { generate } = require('../../scripts/workload');
        const generated = generate({ entities: Number(body.entities) || 2000, seed: Number(body.seed) || 287, deleteFraction: body.deleteFraction });
        const results = {};
        for (const [shardId, evs] of Object.entries(generated.perShard)) {
          const url = shardUrls[shardId];
          if (!url) continue;
          let applied = 0;
          let changed = 0;
          for (let i = 0; i < evs.length; i += 400) {
            const r = await fetch(`${url}/events`, {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({ events: evs.slice(i, i + 400) }),
              signal: AbortSignal.timeout(30000),
            });
            const out = await r.json();
            applied += out.applied || 0;
            changed += out.changed || 0;
          }
          results[shardId] = { reports: evs.length, applied, changed };
        }
        const collected = await coordinator.collect();
        const version = await coordinator.publish({ mode: 'auto', trigger: 'ingest', collected });
        return sendJson(res, 200, {
          workload: {
            entities: generated.totalEntities,
            truthLive: generated.liveCount,
            events: generated.totalEvents,
            deleteEvents: generated.deleteReports,
            corrections: generated.corrections,
            retried: generated.retriedReports,
            sharedKeys: generated.multiShardKeys,
          },
          results,
          version,
        });
      }
      if (req.method === 'POST' && url.pathname === '/collect') {
        const collected = await coordinator.collect();
        const body = await readJson(req).catch(() => ({}));
        const version = await coordinator.publish({
          mode: body.mode || 'auto',
          note: body.note || '',
          trigger: 'collect',
          collected,
        });
        return sendJson(res, 200, { collected: describeCollected(collected), version });
      }
      const m = url.pathname.match(/^\/bundles\/([A-Za-z0-9_-]+)$/);
      if (req.method === 'POST' && m) {
        const shardId = m[1];
        if (!shardUrls[shardId]) return sendJson(res, 404, { error: `unknown shard ${shardId}` });
        const body = await readJson(req);
        const rec = coordinator.receiveBundle(shardId, body, 'push');
        return sendJson(res, rec.accepted ? 200 : 409, rec);
      }
      return sendJson(res, 404, { error: 'not found' });
    } catch (err) {
      return sendJson(res, err.statusCode || 500, { error: err.message });
    }
  });

  return {
    coordinator,
    server,
    listen: (cb) => server.listen(port, '127.0.0.1', cb),
    close: () => new Promise((r) => server.close(r)),
  };
}

function describeCollected(collected) {
  const out = {};
  for (const [id, c] of Object.entries(collected)) {
    out[id] = {
      ok: c.ok,
      fresh: c.fresh,
      generation: c.generation == null ? null : c.generation,
      incarnation: c.payload ? c.payload.incarnation : null,
      liveCount: c.payload ? c.payload.liveCount : null,
      tombstoneCount: c.payload ? c.payload.tombstoneCount : null,
      error: c.error || null,
      ageMs: c.ageMs || 0,
    };
  }
  return out;
}

function sendFile(res, file, type) {
  let body;
  try {
    body = fs.readFileSync(file);
  } catch {
    return sendJson(res, 404, { error: `missing ${path.basename(file)}` });
  }
  res.writeHead(200, { 'content-type': type, 'cache-control': 'no-store' });
  res.end(body);
}

function main() {
  const port = Number(process.env.PORT || 7000);
  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data', 'coordinator');
  const publishDir = process.env.PUBLISH_DIR || path.join(process.cwd(), 'web', 'public');
  // Must match the shards' SKETCH_OPTIONS: the coordinator rebuilds summaries
  // for the recompute path and for verification, and the union-of-summaries
  // proof is only meaningful when every summary has the same parameters.
  const sketchOptions = JSON.parse(process.env.SKETCH_OPTIONS || '{}');
  const app = createCoordinatorServer({ dataDir, publishDir, port, sketchOptions });
  app.listen(() => {
    console.log(`[coordinator] pid=${process.pid} http://127.0.0.1:${port} shards=${JSON.stringify(DEFAULT_SHARD_URLS)}`);
  });
  process.on('SIGINT', () => app.close().then(() => process.exit(0)));
  process.on('SIGTERM', () => app.close().then(() => process.exit(0)));
}

if (require.main === module) main();

module.exports = { createCoordinatorServer, DEFAULT_SHARD_URLS };
