'use strict';
// Shard HTTP service.  Two of these run as independent OS processes in the demo.
//
// Consistency contract with the coordinator:
//   GET /bundle returns one coherent snapshot: exact version map + live count +
//   summary built from *that* version map, plus the generation they belong to.
//   The coordinator never has to stitch together two different points in time.
//
// Routes:
//   POST /events         {events:[...]}       append+apply a batch, idempotent
//   GET  /bundle                              coherent exact+summary bundle
//   GET  /status                              counters, generations, storage
//   POST /admin/freeze /unfreeze             simulate a late/unreachable shard
//   POST /admin/crash                        simulate abrupt process death
//   POST /admin/restart                      reload from journal
//   GET  /health

const http = require('node:http');
const path = require('node:path');
const { Shard } = require('./shard');
const { readJson, sendJson } = require('../common/http');

function createShardServer({ id, dataDir, sketchOptions = {} }) {
  let shard = new Shard(id, { dataDir, sketchOptions });
  shard.load();

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    try {
      if (req.method === 'GET' && url.pathname === '/health') {
        return sendJson(res, 200, { ok: true, shardId: id, frozen: shard.frozen, generation: shard.generation });
      }
      if (req.method === 'GET' && url.pathname === '/status') {
        return sendJson(res, 200, { ...shard.status(), servedByPid: process.pid });
      }
      // Admin routes stay available while frozen, so the demo can recover.
      if (req.method === 'POST' && url.pathname === '/admin/freeze') {
        shard.frozen = true;
        return sendJson(res, 200, { ok: true, frozen: true });
      }
      if (req.method === 'POST' && url.pathname === '/admin/unfreeze') {
        shard.frozen = false;
        return sendJson(res, 200, { ok: true, frozen: false });
      }
      if (req.method === 'POST' && url.pathname === '/admin/crash') {
        // Simulated abrupt death: stop journal writes, refuse everything else.
        shard.close();
        shard.crashCount += 1;
        shard.frozen = true;
        shard.crashed = true;
        return sendJson(res, 200, { ok: true, crashed: true });
      }
      if (req.method === 'POST' && url.pathname === '/admin/restart') {
        // Reload from journal as a fresh process would.
        shard.close();
        shard = new Shard(id, { dataDir, sketchOptions });
        const info = shard.load();
        return sendJson(res, 200, { ok: true, restarted: true, loaded: info, pid: process.pid });
      }

      if (shard.frozen) {
        // A late shard must look late, not empty: returning 503 leaves the
        // coordinator's previous bundle for this shard untouched.
        return sendJson(res, 503, { error: 'shard unavailable (frozen/crashed)', shardId: id });
      }

      if (req.method === 'POST' && url.pathname === '/events') {
        const body = await readJson(req);
        if (!Array.isArray(body.events)) return sendJson(res, 400, { error: 'events[] required' });
        const results = shard.ingest(body.events);
        const applied = results.filter((r) => r.applied).length;
        const changed = results.filter((r) => r.changed).length;
        return sendJson(res, 200, {
          shardId: id,
          generation: shard.generation,
          applied,
          changed,
          results,
          liveCount: shard.state.liveKeys().length,
        });
      }
      if (req.method === 'GET' && url.pathname === '/bundle') {
        return sendJson(res, 200, shard.bundle('requested'));
      }
      if (req.method === 'POST' && url.pathname === '/summary/rebuild') {
        const meta = shard.rebuildSummary('requested');
        return sendJson(res, 200, { shardId: id, meta });
      }
      if (req.method === 'GET' && url.pathname === '/exact/keys') {
        return sendJson(res, 200, {
          shardId: id,
          generation: shard.generation,
          live: shard.state.liveKeys().sort(),
          deleted: shard.state.deletedKeys().sort(),
        });
      }
      return sendJson(res, 404, { error: 'not found' });
    } catch (err) {
      return sendJson(res, err.statusCode || 500, { error: err.message });
    }
  });

  return {
    get shard() {
      return shard;
    },
    server,
    listen: (port, cb) => server.listen(port, '127.0.0.1', cb),
    close: async () => {
      shard.close();
      await new Promise((r) => server.close(r));
    },
  };
}

function main() {
  const id = process.env.SHARD_ID || process.argv[2];
  const port = Number(process.env.PORT || process.argv[3] || 7001);
  const dataDir = process.env.DATA_DIR || process.argv[4] || path.join(process.cwd(), 'data', id || 'shard');
  const sketchOptions = JSON.parse(process.env.SKETCH_OPTIONS || '{}');
  if (!id) {
    console.error('usage: SHARD_ID=shard-a PORT=7001 DATA_DIR=./data node src/shard/server.js');
    process.exit(2);
  }
  const app = createShardServer({ id, dataDir, sketchOptions });
  app.listen(port, () => {
    const s = app.shard.status();
    console.log(`[${id}] pid=${process.pid} :${port} live=${s.exact.live} gen=${s.generation} data=${dataDir}`);
  });
  process.on('SIGINT', () => app.close().then(() => process.exit(0)));
  process.on('SIGTERM', () => app.close().then(() => process.exit(0)));
}

if (require.main === module) main();

module.exports = { createShardServer };
