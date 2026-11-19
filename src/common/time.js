'use strict';
// Hybrid logical clock helpers and canonical encoding.
//
// Version order is (wall, seq, node), which is total and deterministic.  The
// node id is part of the order, so two shards or the coordinator can generate
// concurrent observations without ever producing an ambiguous tie.

function compareHlc(a, b) {
  if (a.wall !== b.wall) return a.wall < b.wall ? -1 : 1;
  if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
  if (a.node !== b.node) return a.node < b.node ? -1 : 1;
  return 0;
}

function hlcKey(h) {
  return `${h.wall}.${h.seq}.${h.node}`;
}

function isHlc(h) {
  return (
    !!h &&
    typeof h === 'object' &&
    Number.isSafeInteger(h.wall) &&
    h.wall >= 0 &&
    Number.isSafeInteger(h.seq) &&
    h.seq >= 0 &&
    typeof h.node === 'string' &&
    h.node.length > 0
  );
}

// Monotonic clock.  `observe` lets a component accept a client-supplied HLC
// and guarantee that its own next timestamp is strictly larger.
function createClock(node) {
  let wall = 0;
  let seq = 0;
  return {
    node,
    now() {
      const t = Date.now();
      if (t > wall) {
        wall = t;
        seq = 0;
      } else {
        seq += 1;
      }
      return { wall, seq, node };
    },
    observe(h) {
      if (!isHlc(h)) return;
      if (h.wall > wall) {
        wall = h.wall;
        seq = h.seq;
      } else if (h.wall === wall && h.seq > seq) {
        seq = h.seq;
      }
    },
    peek() {
      return { wall, seq, node };
    },
  };
}

// Deterministic JSON encoding with sorted object keys.  Used for content
// digests so a retransmitted summary is recognisable regardless of property
// order.
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return '{' + keys.map((k) => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  }
  return JSON.stringify(value);
}

module.exports = { compareHlc, hlcKey, isHlc, createClock, canonical };
