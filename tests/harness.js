'use strict';
// Integration harness: runs two real shard processes and one coordinator
// process (actual child processes, actual HTTP), for tests and the demo script.

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');

const ROOT = path.resolve(__dirname, '..');

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

async function freePorts(n) {
  const out = [];
  for (let i = 0; i < n; i += 1) out.push(await freePort());
  return out;
}

function waitForHttp(url, timeoutMs = 15000) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const attempt = async () => {
      try {
        const res = await fetch(url, { signal: AbortSignal.timeout(1000) });
        if (res.ok || res.status === 503) return resolve(true);
      } catch {
        /* retry */
      }
      if (Date.now() - start > timeoutMs) return reject(new Error(`timeout waiting for ${url}`));
      setTimeout(attempt, 100);
    };
    attempt();
  });
}

class Cluster {
  constructor({ dataDir, sketchOptions = {}, shardCount = 2 }) {
    this.dataDir = dataDir;
    this.shardIds = ['shard-a', 'shard-b'].slice(0, shardCount);
    this.sketchOptions = sketchOptions;
    this.procs = new Map();
    this.shardPorts = {};
    this.coordinatorPort = null;
    this.logs = [];
    this.coordinatorStarted = 0;
  }

  async start() {
    fs.mkdirSync(this.dataDir, { recursive: true });
    const ports = await freePorts(3);
    this.shardPorts = { 'shard-a': ports[0], 'shard-b': ports[1] };
    this.coordinatorPort = ports[2];
    this.shardUrls = Object.fromEntries(
      this.shardIds.map((id) => [id, `http://127.0.0.1:${this.shardPorts[id]}`])
    );
    for (const id of this.shardIds) this.spawnShard(id);
    this.spawnCoordinator();
    await Promise.all([
      ...this.shardIds.map((id) => waitForHttp(`${this.shardUrls[id]}/health`)),
      waitForHttp(this.coordUrl('/health')),
    ]);
    return this;
  }

  spawnShard(id) {
    this.spawn(`shard:${id}`, [path.join(ROOT, 'src/shard/server.js')], {
      SHARD_ID: id,
      PORT: String(this.shardPorts[id]),
      DATA_DIR: path.join(this.dataDir, id),
      SKETCH_OPTIONS: JSON.stringify(this.sketchOptions),
    });
  }

  spawnCoordinator() {
    const env = {
      PORT: String(this.coordinatorPort),
      DATA_DIR: path.join(this.dataDir, 'coordinator'),
      PUBLISH_DIR: path.join(this.dataDir, 'publish'),
      SHARD_A_URL: this.shardUrls['shard-a'],
      SHARD_B_URL: this.shardUrls['shard-b'] || 'http://127.0.0.1:1',
      SKETCH_OPTIONS: JSON.stringify(this.sketchOptions),
    };
    this.coordinatorStarted += 1;
    this.spawn('coordinator', [path.join(ROOT, 'src/coordinator/server.js')], env);
  }

  spawn(name, args, env) {
    const proc = spawn(process.execPath, args, { cwd: ROOT, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
    const entry = { proc, name };
    this.procs.set(name, entry);
    const capture = (chunk) => {
      this.logs.push(`[${name}] ${chunk.toString().trimEnd()}`);
      if (this.logs.length > 400) this.logs.shift();
    };
    proc.stdout.on('data', capture);
    proc.stderr.on('data', capture);
    return proc;
  }

  // Abrupt death, then a fresh process on the same data directory.
  async crashShard(id) {
    const name = `shard:${id}`;
    const entry = this.procs.get(name);
    if (!entry || entry.proc.exitCode !== null || entry.proc.signalCode !== null) return;
    entry.proc.kill('SIGKILL');
    await new Promise((r) => entry.proc.once('exit', r));
  }

  async restartShard(id) {
    await this.crashShard(id);
    this.spawnShard(id);
    await waitForHttp(`${this.shardUrls[id]}/health`);
  }

  async crashCoordinator() {
    const entry = this.procs.get('coordinator');
    entry.proc.kill('SIGKILL');
    await new Promise((r) => entry.proc.once('exit', r));
  }

  async restartCoordinator() {
    this.spawnCoordinator();
    await waitForHttp(this.coordUrl('/health'));
  }

  async stop() {
    const procs = [...this.procs.values()].map((e) => e.proc);
    for (const p of procs) {
      try {
        p.kill('SIGTERM');
      } catch {
        /* ignore */
      }
    }
    await new Promise((r) => setTimeout(r, 250));
    for (const p of procs) {
      try {
        p.kill('SIGKILL');
      } catch {
        /* ignore */
      }
    }
  }

  async post(url, body) {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body || {}),
      signal: AbortSignal.timeout(30000),
    });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  }

  async get(url) {
    const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
    return { status: res.status, body: await res.json().catch(() => ({})) };
  }

  shardUrl(id) {
    return this.shardUrls[id];
  }

  coordUrl(p = '') {
    return `http://127.0.0.1:${this.coordinatorPort}${p}`;
  }

  // Ingest in batches; returns the merged per-event results.
  async ingest(shardId, events, batchSize = 200) {
    const results = [];
    for (let i = 0; i < events.length; i += batchSize) {
      const r = await this.post(`${this.shardUrl(shardId)}/events`, { events: events.slice(i, i + batchSize) });
      if (r.status !== 200) throw new Error(`ingest failed on ${shardId}: ${r.status} ${JSON.stringify(r.body)}`);
      results.push(...r.body.results);
    }
    return results;
  }

  async publish(mode = 'auto') {
    return (await this.post(this.coordUrl('/publish'), { mode })).body;
  }

  async collect(mode = 'auto') {
    return (await this.post(this.coordUrl('/collect'), { mode })).body;
  }

  async state() {
    return (await this.get(this.coordUrl('/state.json'))).body;
  }

  async verify() {
    return (await this.post(this.coordUrl('/verify'), {})).body;
  }

  async repair() {
    return (await this.post(this.coordUrl('/repair'), {})).body;
  }

  async shardStatus(id) {
    return (await this.get(`${this.shardUrl(id)}/status`)).body;
  }
}

module.exports = { Cluster, freePort, freePorts, waitForHttp, ROOT };
