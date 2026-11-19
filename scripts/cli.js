'use strict';
// Shared command-line plumbing for scripts.

const path = require('node:path');

function arg(name, fallback) {
  const hit = process.argv.find((a) => a === `--${name}` || a.startsWith(`--${name}=`));
  if (!hit) return fallback;
  const [, value] = hit.split('=');
  if (value !== undefined) return value;
  const idx = process.argv.indexOf(hit);
  return process.argv[idx + 1] || fallback;
}

function baseUrl() {
  return arg('url', process.env.COORD_URL || 'http://127.0.0.1:7000');
}

async function post(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body || {}),
    signal: AbortSignal.timeout(120000),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status} ${JSON.stringify(json).slice(0, 300)}`);
  return json;
}

async function get(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(30000) });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
  return json;
}

function log(...a) {
  console.log(...a);
}

module.exports = { arg, baseUrl, post, get, log, path };
