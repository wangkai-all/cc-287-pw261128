'use strict';
// Start the whole demo: coordinator + two independent shard processes.
//
//   node scripts/start.js [--data ./data] [--port 7000] [--sketch linearBits=65536,kmvK=512,hllP=12]
//
// Each process is separate (own pid, own journal directory).  Kill any one of
// them and restart it; the journals are the source of truth on boot.

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const { arg } = require('./cli');

const dataDir = path.resolve(arg('data', process.env.DATA_DIR || path.join(ROOT, 'data')));
const basePort = Number(arg('port', process.env.PORT || 7000));
const sketch = normalizeSketchArg(arg('sketch', process.env.SKETCH_OPTIONS || '{}'));

// Accept either JSON (`{"kmvK":256}`) or the friendlier `kmvK=256,hllP=10`.
function normalizeSketchArg(raw) {
  const text = String(raw || '').trim();
  if (!text) return '{}';
  if (text.startsWith('{')) {
    JSON.parse(text); // fail loudly on malformed JSON
    return text;
  }
  const out = {};
  for (const pair of text.split(',')) {
    const [k, v] = pair.split('=').map((x) => (x || '').trim());
    if (!k) continue;
    out[k] = Number(v);
    if (!Number.isFinite(out[k])) throw new Error(`sketch option ${k} must be a number`);
  }
  return JSON.stringify(out);
}

const shards = [
  { id: 'shard-a', port: basePort + 1 },
  { id: 'shard-b', port: basePort + 2 },
];

fs.mkdirSync(dataDir, { recursive: true });

const procs = [];
function launch(name, script, env) {
  const proc = spawn(process.execPath, [script], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  procs.push({ name, proc });
  proc.on('exit', (code, signal) => {
    console.log(`[start] ${name} exited (code=${code} signal=${signal})`);
  });
  return proc;
}

for (const s of shards) {
  launch(`shard ${s.id}`, path.join(ROOT, 'src/shard/server.js'), {
    SHARD_ID: s.id,
    PORT: String(s.port),
    DATA_DIR: path.join(dataDir, s.id),
    SKETCH_OPTIONS: sketch,
  });
}

launch('coordinator', path.join(ROOT, 'src/coordinator/server.js'), {
  PORT: String(basePort),
  DATA_DIR: path.join(dataDir, 'coordinator'),
  PUBLISH_DIR: path.join(ROOT, 'web/public'),
  SHARD_A_URL: `http://127.0.0.1:${shards[0].port}`,
  SHARD_B_URL: `http://127.0.0.1:${shards[1].port}`,
  SKETCH_OPTIONS: sketch,
});

console.log(`
  coordinator  http://127.0.0.1:${basePort}
  shard-a      http://127.0.0.1:${shards[0].port}   data: ${path.join(dataDir, 'shard-a')}
  shard-b      http://127.0.0.1:${shards[1].port}   data: ${path.join(dataDir, 'shard-b')}
  data dir     ${dataDir}

  Open the coordinator URL in a browser.  Seed data with:
    node scripts/seed.js --url http://127.0.0.1:${basePort}

  Ctrl-C stops all three processes.
`);

const stop = () => {
  for (const { proc } of procs) {
    try {
      proc.kill('SIGTERM');
    } catch {
      /* ignore */
    }
  }
  setTimeout(() => process.exit(0), 300);
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
