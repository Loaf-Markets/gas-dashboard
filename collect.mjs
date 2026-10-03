#!/usr/bin/env node
// Settlement gas collector — Arbitrum (Nitro) chains. Zero dependencies, Node >= 20.
//
// Scans blocks incrementally and writes one JSON file per UTC day under `data/days/`,
// plus `data/index.json` (summary + metadata the page loads first). Everything is
// derived from public chain data — no secrets, no bridge access. Every chunk is
// checkpointed in `data/state.json`, so a run that hits its time budget resumes.
//
// Two cursors, so the page is CURRENT after the first run and history fills in behind:
//   live      — starts LIVE_HOURS before the tip on the first run, then follows the tip;
//               always processed first.
//   backfill  — starts BACKFILL_DAYS before the tip and walks forward until it meets the
//               live cursor's start block; processed with whatever time budget is left.
//
// Per block it collects
//   - header:        timestamp, gasUsed (chain-wide), baseFeePerGas
//   - tracked txs:   every tx that emitted a log on a TRACKED contract (eth_getLogs) →
//                    receipt (gasUsed, effectiveGasPrice, gasUsedForL1, from)
//   - sample blocks: 1-in-SAMPLE_EVERY blocks get full receipts, which is what the
//                    "other gas consumers" ranking is built from (scaled ×SAMPLE_EVERY)
//   - model:         the ArbOS-51 six-constraint pricing model
//                    (Smart-Contracts/docs/gas-aware-dispatch-governor.md §1.1) replayed
//                    with the real gas stream and with each tracked contract's gas
//                    removed, so the page can show how much of the base-fee elevation
//                    our own load is responsible for.
//
// Env (all optional):
//   RPC_URLS           comma-separated JSON-RPC endpoints (round-robin, adaptive pacing)
//   TRACKED            name=address[,name=address…]; first entry is the primary (default: prod + staging)
//   DATA_DIR           output dir (default ./data)
//   LIVE_HOURS         how far behind the tip the live cursor starts on a fresh state (default 6)
//   BACKFILL_DAYS      history depth the backfill cursor walks (default 3; <= LIVE_HOURS/24 disables it)
//   MAX_RUN_MINUTES    stop cleanly after this long (default 45; GH Actions job budget)
//   SAMPLE_EVERY       full-receipt sampling stride (default 200)
//   HEADER_BATCH       blocks per eth_getBlockByNumber batch request (default 100)
//   TX_RECEIPT_BATCH   eth_getTransactionReceipt items per batch request (default 100)
//   BLOCK_RECEIPT_BATCH eth_getBlockReceipts items per batch request (default 10)
//   CONCURRENCY        parallel batch requests per endpoint (default 1; public RPCs throttle above that)
//   CHUNK_BLOCKS       blocks per checkpoint (default 4000)
//   RETENTION_DAYS     day files kept under data/days (default 45; 0 = keep all)

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Measured 2026-09-08: all four serve batched headers ×100, tx receipts ×100, block
// receipts ×10 and eth_getLogs; each throttles per IP at roughly one batch per second.
const ENDPOINTS = (process.env.RPC_URLS ||
  "https://sepolia-rollup.arbitrum.io/rpc,https://arbitrum-sepolia-rpc.publicnode.com,https://arbitrum-sepolia.rpc.thirdweb.com,https://api.zan.top/arb-sepolia")
  .split(",").map((s) => s.trim()).filter(Boolean);
// Validate every endpoint eagerly: an unparseable RPC_URLS entry used to surface as an
// opaque "Invalid URL" from inside the retry loop, hours into a run.
for (const ep of ENDPOINTS) {
  let u;
  try { u = new URL(ep); } catch { throw new Error(`RPC_URLS entry is not a valid URL: ${ep}`); }
  if (u.protocol !== "http:" && u.protocol !== "https:") throw new Error(`RPC_URLS entry must be http(s): ${ep}`);
}
if (ENDPOINTS.length === 0) throw new Error("RPC_URLS resolved to zero endpoints");

const TRACKED = (process.env.TRACKED ||
  "prod=0xA1467Ffdc95CD2821736ee5b044E14E72CB80FD4,staging=0x3f29c4ac2f18a6963dfe3dddb53efefeb3c8e594")
  .split(",").map((s) => s.trim()).filter(Boolean).map((kv) => { const [name, addr] = kv.split("="); return { name: name.trim(), addr: (addr || "").trim().toLowerCase() }; });
// Reject malformed entries here rather than letting an `undefined` address reach
// eth_getLogs, where it silently returns nothing and every metric reads zero.
if (TRACKED.length === 0) throw new Error("TRACKED resolved to zero contracts");
for (const t of TRACKED) {
  if (!t.name) throw new Error(`TRACKED entry has no name: ${JSON.stringify(t)}`);
  if (!/^0x[0-9a-f]{40}$/.test(t.addr)) throw new Error(`TRACKED entry ${t.name} has an invalid address: ${t.addr || "(empty)"}`);
}
if (new Set(TRACKED.map((t) => t.name)).size !== TRACKED.length) throw new Error("TRACKED has duplicate names");
if (new Set(TRACKED.map((t) => t.addr)).size !== TRACKED.length) throw new Error("TRACKED has duplicate addresses");
const NAMES = TRACKED.map((t) => t.name);
const CKEYS = [...NAMES, "all"];
const ADDR_TO_NAME = new Map(TRACKED.map((t) => [t.addr, t.name]));
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, "data"));

/** Read a positive-integer env var, falling back to `dflt` on absent/garbage/<=0 values.
 *  Without this a stray `MAX_RUN_MINUTES=abc` yields NaN, which makes `budgetLeft()`
 *  NaN — never <= 0, so the run never stops and the Actions job dies at timeout. */
function envPositiveInt(name, dflt) {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return dflt;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    console.warn(`WARNING: ${name}=${JSON.stringify(raw)} is not a positive number; using ${dflt}`);
    return dflt;
  }
  return Math.floor(n);
}

const LIVE_HOURS = envPositiveInt("LIVE_HOURS", 6);
const BACKFILL_DAYS = envPositiveInt("BACKFILL_DAYS", 3);
const MAX_RUN_MS = envPositiveInt("MAX_RUN_MINUTES", 45) * 60_000;
const SAMPLE_EVERY = envPositiveInt("SAMPLE_EVERY", 200);
const HEADER_BATCH = envPositiveInt("HEADER_BATCH", 100);
const TX_RECEIPT_BATCH = envPositiveInt("TX_RECEIPT_BATCH", 100);
const BLOCK_RECEIPT_BATCH = envPositiveInt("BLOCK_RECEIPT_BATCH", 10);
const CONCURRENCY = envPositiveInt("CONCURRENCY", 1);
const CHUNK = envPositiveInt("CHUNK_BLOCKS", 4000);
const CONFIRMATIONS = 20; // stay behind the tip; Arbitrum reorgs are rare but the sequencer feed can lag

const ARB_GAS_INFO = "0x000000000000000000000000000000000000006c";
const SYSTEM_TX_TO = "0x00000000000000000000000000000000000a4b05"; // ArbOS internal tx sink
const TOPIC_TRADE_SETTLED = "0x5dc05ad95a5368e74347b28654eb0bedd6816a1de88fd3c636d86296bc225ef5";
const TOPIC_SETTLEMENT_FAILED = "0x6d38686d11eb51642fcb53228261031dd253be4dad70080bab3afaf941a8394f";
const TOP_PER_HOUR = 120; // addresses kept per hour in the sampled ranking

const t0 = Date.now();
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);
const budgetLeft = () => MAX_RUN_MS - (Date.now() - t0);

// ───────────────────────────── RPC layer ─────────────────────────────
let rr = 0;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
// Per-endpoint pacing (AIMD): a throttle response multiplies the endpoint's minimum
// request interval, every success decays it. Consecutive throttles put the endpoint in
// a penalty box that doubles each time (a provider on a daily quota drops out of the
// rotation instead of eating retry attempts). Public RPCs limit per-IP request rate.
const pace = new Map(ENDPOINTS.map((ep) => [ep, { minInterval: 250, nextAt: 0, cooldownUntil: 0, strikes: 0 }]));
function pickEndpoint() {
  let best = null;
  for (let i = 0; i < ENDPOINTS.length; i++) {
    const ep = ENDPOINTS[(rr + i) % ENDPOINTS.length];
    const p = pace.get(ep);
    const readyAt = Math.max(p.nextAt, p.cooldownUntil);
    if (!best || readyAt < best.readyAt) best = { ep, readyAt };
  }
  rr = (rr + 1) % ENDPOINTS.length;
  return best;
}
let throttleEvents = 0;
let untrackedLogs = 0; // logs an endpoint returned outside the tracked-address filter
/** Send a JSON-RPC batch; returns replies in request order. Retries on 429 / 5xx / network
 *  errors (and hops endpoints on per-item errors, e.g. a method one provider lacks). */
async function rpcBatch(calls) {
  if (calls.length === 0) return [];
  const body = JSON.stringify(calls.map((c, i) => ({ jsonrpc: "2.0", id: i, method: c.method, params: c.params })));
  const MAX_ATTEMPTS = 20;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    // Never start a request that cannot finish inside the run budget: sleeping through
    // a 10-minute penalty box used to burn the whole budget on a single call.
    const picked = pickEndpoint();
    if (!picked) break;
    const { ep, readyAt } = picked;
    const p = pace.get(ep);
    const wait = readyAt - Date.now();
    if (wait > 0) {
      if (wait > budgetLeft()) { log(`  would need ${Math.round(wait / 1000)}s to reach ${new URL(ep).host}, past the run budget — giving up`); break; }
      await sleep(Math.min(wait, 30_000));
    }
    p.nextAt = Date.now() + p.minInterval;
    try {
      const res = await fetch(ep, { method: "POST", headers: { "content-type": "application/json" }, body, signal: AbortSignal.timeout(120_000) });
      let throttled = res.status === 429 || res.status >= 500 || res.status === 403;
      let arr = null;
      if (!throttled) {
        let json;
        try { json = await res.json(); } catch (e) { throw new Error(`non-JSON response from ${ep}: HTTP ${res.status}`); }
        arr = Array.isArray(json) ? json : [json];
        throttled = arr.some((r) => r.error && (r.error.code === 429 || r.error.code === -32005 || r.error.code === -32012 || /rate|too many|limit exceeded|no longer/i.test(r.error.message || "")));
      }
      if (throttled) {
        throttleEvents++;
        p.strikes++;
        p.minInterval = Math.min(8000, p.minInterval * 1.6);
        p.cooldownUntil = Date.now() + Math.min(600_000, 1500 * 2 ** Math.min(p.strikes, 9));
        if (p.strikes >= 5 && p.strikes % 5 === 0) log(`  ${new URL(ep).host}: ${p.strikes} consecutive throttles (${calls[0].method} ×${calls.length}); penalty ${((p.cooldownUntil - Date.now()) / 1000) | 0}s`);
        continue;
      }
      p.strikes = 0;
      p.minInterval = Math.max(150, p.minInterval * 0.93);
      // Allocate exactly calls.length slots: a hostile or buggy endpoint replying with an
      // out-of-range id used to grow `out` to that id, i.e. a ~1e9-element sparse array.
      const out = new Array(calls.length);
      let malformed = false;
      for (const r of arr) {
        if (!r || !Number.isInteger(r.id) || r.id < 0 || r.id >= calls.length) { malformed = true; break; }
        if (out[r.id] !== undefined) { malformed = true; break; } // duplicate id
        out[r.id] = r;
      }
      if (!malformed) for (let i = 0; i < calls.length; i++) if (!out[i]) { malformed = true; break; }
      if (malformed) {
        // A malformed reply is the endpoint's fault, not the data's — penalise it briefly
        // and retry elsewhere rather than surfacing a misleading "missing id" error.
        p.strikes++; p.cooldownUntil = Date.now() + 2000;
        log(`  ${new URL(ep).host}: malformed batch reply (${calls[0].method} ×${calls.length}) — retrying on another endpoint`);
        continue;
      }
      // per-item errors (unsupported method, "metadata not found", …): try another endpoint first
      if (out.some((r) => r.error) && attempt < 3 && ENDPOINTS.length > 1) { p.cooldownUntil = Date.now() + 2000; continue; }
      return out;
    } catch (e) {
      p.strikes++;
      p.cooldownUntil = Date.now() + Math.min(600_000, 2000 * 2 ** Math.min(p.strikes, 8));
      if (attempt === MAX_ATTEMPTS - 1) throw e;
    }
  }
  throw new Error(`rpc batch (${calls[0].method} ×${calls.length}) exhausted retries: rate-limited on every endpoint`);
}
async function rpc(method, params) {
  const [r] = await rpcBatch([{ method, params }]);
  if (r.error) throw new Error(`${method}: ${r.error.message}`);
  return r.result;
}
/** Run batches with bounded concurrency. A batch that comes back with per-item errors
 *  (e.g. "response too large" from a size-capped provider) is split in half and retried;
 *  only a single-item batch that still errors is fatal. */
async function runBatches(calls, size, worker) {
  const queue = [];
  for (let i = 0; i < calls.length; i += size) queue.push(calls.slice(i, i + size));
  const lanes = Array.from({ length: Math.max(1, CONCURRENCY * ENDPOINTS.length) }, async () => {
    while (queue.length) {
      const g = queue.shift();
      const res = await rpcBatch(g);
      if (res.some((r) => r.error || !r.result)) {
        if (g.length > 1) { const mid = Math.ceil(g.length / 2); queue.push(g.slice(0, mid), g.slice(mid)); log(`  batch of ${g.length} ${g[0].method} had item errors (${res.find((r) => r.error)?.error?.message || "null result"}); splitting`); continue; }
        // let the worker raise a precise error for the single failing item
      }
      worker(g, res);
    }
  });
  await Promise.all(lanes);
}

// ───────────────────────────── helpers ─────────────────────────────
const hex = (n) => "0x" + n.toString(16);
const toInt = (h) => Number.parseInt(h, 16);
const toBig = (h) => BigInt(h);
const WEI = 1e18;
/** Convert an exact wei BigInt to ETH without first routing it through a JS float.
 *  Splitting at 1e9 keeps the division in BigInt and preserves ~18 significant digits,
 *  which a direct `Number(wei) / 1e18` does not once wei exceeds 2^53 (~9.0e15, i.e.
 *  just 9 gwei — a normal Arbitrum base fee is already past that). */
export function weiToEth(wei) {
  const v = wei < 0n ? -wei : wei;
  const neg = wei < 0n;
  const scaled = v / 1000000000n;
  const frac = v % 1000000000n;
  return (neg ? -1 : 1) * (Number(scaled) + Number(frac) / 1e9) / 1e9;
}
/** floor of `state.minBaseFee` expressed in wei, for exact BigInt comparisons. */
const minBaseFeeWei = () => BigInt(Math.round(state?.minBaseFee ?? 0));
const dayOf = (ts) => new Date(ts * 1000).toISOString().slice(0, 10);
const readJson = (p, fallback) => (fs.existsSync(p) ? JSON.parse(fs.readFileSync(p, "utf8")) : fallback);
function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  const tmp = p + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(obj));
  fs.renameSync(tmp, p);
}

/** Round detector: a new round opens when an id falls below a quarter of the running
 *  maximum, after at least 5,000 ids (ids are not strictly monotone inside a round). */
export function observeTradeId(cur, k, idN, h) {
  const rs = cur.rounds[k];
  if (rs.length === 0) rs.push({ n: 1, startBlock: h.n, startTs: h.ts, firstId: idN, maxId: idN, startKnown: false });
  else if (cur.lastMaxTradeId[k] > 5000 && idN < cur.lastMaxTradeId[k] / 4) {
    const prev = rs[rs.length - 1];
    prev.endBlock = h.n - 1; prev.endTs = h.ts;
    rs.push({ n: prev.n + 1, startBlock: h.n, startTs: h.ts, firstId: idN, maxId: idN, startKnown: true });
    cur.lastMaxTradeId[k] = 0;
  }
  const r = rs[rs.length - 1];
  if (idN > r.maxId) r.maxId = idN;
  if (idN > cur.lastMaxTradeId[k]) cur.lastMaxTradeId[k] = idN;
}

/** ArbOS-51 quartic: Q(E) ≈ 1 + E + E²/2 + E³/6 + E⁴/24 (arbmath.ApproxExpBasisPoints(E, 4)). */
export const quartic = (E) => 1 + E + (E * E) / 2 + (E * E * E) / 6 + (E * E * E * E) / 24;
/** Inverse of the quartic on E >= 0 (monotone), by Newton iteration. */
export function invQuartic(q) {
  if (q <= 1) return 0;
  let E = Math.log(q);
  for (let i = 0; i < 30; i++) {
    const f = quartic(E) - q, d = 1 + E + (E * E) / 2 + (E * E * E) / 6;
    const step = f / d; E -= step;
    if (Math.abs(step) < 1e-12) break;
  }
  return Math.max(0, E);
}

async function readChainParams() {
  const [minRes, conRes] = await rpcBatch([
    { method: "eth_call", params: [{ to: ARB_GAS_INFO, data: "0xf918379a" }, "latest"] }, // getMinimumGasPrice()
    { method: "eth_call", params: [{ to: ARB_GAS_INFO, data: "0x232027d1" }, "latest"] }, // getGasPricingConstraints()
  ]);
  // Neither reply was error-checked: a failing eth_call produced `Number(BigInt(undefined))`
  // -> a thrown TypeError, or an empty slice that made .match() return null -> a TypeError
  // on .map. Both aborted the whole run instead of just this refresh.
  if (minRes.error || minRes.result == null) throw new Error(`getMinimumGasPrice: ${minRes.error?.message || "empty result"}`);
  if (conRes.error || conRes.result == null) throw new Error(`getGasPricingConstraints: ${conRes.error?.message || "empty result"}`);
  const minBaseFee = Number(BigInt(minRes.result));
  if (!Number.isFinite(minBaseFee) || minBaseFee <= 0) throw new Error(`getMinimumGasPrice returned an unusable value: ${minRes.result}`);
  const hexBody = String(conRes.result);
  const words = hexBody.slice(2).match(/.{64}/g);
  if (!words || words.length < 5) throw new Error(`getGasPricingConstraints returned an undecodable payload: ${hexBody.slice(0, 80)}`);
  const decoded = words.map((w) => Number(BigInt("0x" + w)));
  const n = decoded[1];
  // The count is chain-supplied; a corrupt value must not drive a huge allocation/loop.
  if (!Number.isInteger(n) || n < 0 || decoded.length < 2 + n * 3) throw new Error(`getGasPricingConstraints: inconsistent count ${n} for ${decoded.length} words`);
  const constraints = [];
  for (let i = 0; i < n; i++) {
    const [target, window, backlog] = decoded.slice(2 + i * 3, 5 + i * 3);
    // A zero window would make every E term a division by zero (Infinity in the model).
    if (!target || !window) throw new Error(`constraint ${i} has target=${target} window=${window}`);
    constraints.push({ target, window, backlog });
  }
  if (constraints.length === 0) throw new Error("getGasPricingConstraints returned no constraints; cannot price the model");
  return { minBaseFee, constraints };
}

// ───────────────────────────── state ─────────────────────────────
const statePath = path.join(DATA_DIR, "state.json");
let state = isMainCheck() ? readJson(statePath, null) : null;
function isMainCheck() { return process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url); }
const labels = readJson(path.join(__dirname, "labels.json"), {});

function newCursor(name, from, endBlock, nCons) {
  const zeros = () => Array.from({ length: nCons }, () => 0);
  return {
    name, from, lastBlock: from - 1, endBlock, // endBlock null = follow the tip
    model: { backlogs: zeros(), without: Object.fromEntries(CKEYS.map((n) => [n, zeros()])), lastTs: null, warmAt: null },
    prevHeader: null,
    rounds: Object.fromEntries(NAMES.map((n) => [n, []])), // per contract: [{ n, startBlock, startTs, firstId, maxId, startKnown, endBlock?, endTs? }]
    lastMaxTradeId: Object.fromEntries(NAMES.map((n) => [n, 0])),
    fit: null,
  };
}
async function chainTipAndRate() {
  const latest = toInt(await rpc("eth_blockNumber", []));
  const tip = latest - CONFIRMATIONS;
  const probe = 100_000;
  const [a, b] = await rpcBatch([
    { method: "eth_getBlockByNumber", params: [hex(tip - probe), false] },
    { method: "eth_getBlockByNumber", params: [hex(tip), false] },
  ]);
  return { tip, rate: probe / (toInt(b.result.timestamp) - toInt(a.result.timestamp)) }; // blocks per second
}
async function initState() {
  const { tip, rate } = await chainTipAndRate();
  const { minBaseFee, constraints } = await readChainParams();
  const chainId = toInt(await rpc("eth_chainId", []));
  const liveFrom = Math.max(1, tip - Math.round(rate * 3600 * LIVE_HOURS));
  const backFrom = Math.max(1, tip - Math.round(rate * 86400 * BACKFILL_DAYS));
  return {
    version: 2,
    chainId,
    tracked: TRACKED,
    minBaseFee,
    constraints: constraints.map(({ target, window }) => ({ target, window })),
    sampleEvery: SAMPLE_EVERY,
    eventStats: { tradeSettledByShape: 0, failedByShape: 0, other: 0 },
    cursors: {
      live: newCursor("live", liveFrom, null, constraints.length),
      backfill: backFrom < liveFrom ? newCursor("backfill", backFrom, liveFrom - 1, constraints.length) : null,
    },
  };
}
/** v1 state (single forward cursor) → v2: the old cursor becomes the backfill, a fresh live cursor starts near the tip. */
async function migrateV1(old) {
  const { tip, rate } = await chainTipAndRate();
  const liveFrom = Math.max(old.lastBlock + 1, tip - Math.round(rate * 3600 * LIVE_HOURS));
  const backfill = { name: "backfill", from: old.startBlock, lastBlock: old.lastBlock, endBlock: liveFrom - 1, model: old.model, prevHeader: old.prevHeader, rounds: old.rounds, lastMaxTradeId: old.lastMaxTradeId, fit: old.fit };
  log(`migrating v1 state: backfill ${old.startBlock}-${liveFrom - 1} (at ${old.lastBlock}), live from ${liveFrom}`);
  return {
    version: 2, chainId: old.chainId, tracked: old.tracked, minBaseFee: old.minBaseFee, constraints: old.constraints, sampleEvery: old.sampleEvery,
    eventStats: old.eventStats || { tradeSettledByShape: 0, failedByShape: 0, other: 0 },
    cursors: { live: newCursor("live", liveFrom, null, old.constraints.length), backfill: backfill.lastBlock >= backfill.endBlock ? null : backfill },
  };
}

// ───────────────────────────── day file cache ─────────────────────────────
const dayCache = new Map();
function dayFile(date) {
  if (!dayCache.has(date)) {
    const p = path.join(DATA_DIR, "days", `${date}.json`);
    const d = readJson(p, { date, rows: {}, topHours: {} });
    for (const k of Object.keys(d.rows)) d.rows[k] = toAccum(d.rows[k]);
    dayCache.set(date, d);
  }
  return dayCache.get(date);
}
function checkpointDays() {
  for (const [date, d] of dayCache) writeJson(path.join(DATA_DIR, "days", `${date}.json`), { ...d, rows: Object.fromEntries(Object.entries(d.rows).map(([k, r]) => [k, "baseFeeAvg" in r ? r : finalizeRow(r)])) });
}
function newContractAcc() {
  return { txs: 0, gas: 0, l1Gas: 0, trades: 0, failed: 0, feeEth: 0, premiumEth: 0, selfPremiumEth: 0, causedEth: 0, qNoSum: 0 };
}
function newRow(t) {
  return {
    t, blocks: 0, chainGas: 0, chainTxs: 0, sampled: 0,
    baseFeeSum: 0, baseFeeMax: 0, baseFeeMin: 0, qActSum: 0, qModelSum: 0, qMax: 0,
    c: Object.fromEntries(CKEYS.map((k) => [k, newContractAcc()])),
  };
}
/** Day files persist finalized rows; a later run that touches the same minute needs the accumulator shape back. */
export function toAccum(r) {
  if (!("baseFeeAvg" in r)) return r;
  const b = r.blocks || 1;
  const c = {};
  for (const k of CKEYS) {
    const x = r.c?.[k] || {};
    c[k] = { txs: x.txs || 0, gas: x.gas || 0, l1Gas: x.l1Gas || 0, trades: x.trades || 0, failed: x.failed || 0, feeEth: x.feeEth || 0, premiumEth: x.premiumEth || 0, selfPremiumEth: x.selfPremiumEth || 0, causedEth: x.causedEth || 0, qNoSum: (x.qNo || 0) * b };
  }
  return {
    t: r.t, blocks: r.blocks, chainGas: r.chainGas, chainTxs: r.sampledTxs, sampled: r.sampled,
    baseFeeSum: r.baseFeeAvg * b, baseFeeMax: r.baseFeeMax, baseFeeMin: r.baseFeeMin, qActSum: r.qAct * b, qModelSum: (r.qModel ?? r.qAct) * b, qMax: r.qMax, c,
  };
}
export function finalizeRow(r) {
  const b = r.blocks || 1;
  const c = {};
  for (const k of CKEYS) {
    const x = r.c[k];
    c[k] = { txs: x.txs, gas: x.gas, l1Gas: x.l1Gas, trades: x.trades, failed: x.failed, feeEth: x.feeEth, premiumEth: x.premiumEth, selfPremiumEth: x.selfPremiumEth, causedEth: x.causedEth, qNo: x.qNoSum / b };
  }
  return {
    t: r.t, blocks: r.blocks, chainGas: r.chainGas, sampled: r.sampled, sampledTxs: r.chainTxs,
    baseFeeAvg: r.baseFeeSum / b, baseFeeMax: r.baseFeeMax, baseFeeMin: r.baseFeeMin,
    qAct: r.qActSum / b, qModel: r.qModelSum / b, qMax: r.qMax, c,
  };
}

// ───────────────────────────── chunk processing ─────────────────────────────
async function processChunk(cur, from, to) {
  // 1. headers for every block
  const headers = new Array(to - from + 1);
  const hcalls = [];
  for (let n = from; n <= to; n++) hcalls.push({ method: "eth_getBlockByNumber", params: [hex(n), false] });
  await runBatches(hcalls, HEADER_BATCH, (g, res) => {
    for (let i = 0; i < g.length; i++) {
      const r = res[i];
      if (r.error || !r.result) throw new Error(`header ${g[i].params[0]}: ${r.error?.message || "null"}`);
      const h = r.result;
      const n = toInt(h.number);
      // A provider that answers with a block outside the requested range used to write
      // outside the array, leaving a hole that later crashed the walk with "cannot read
      // properties of undefined". Ignore it here; the hole check below turns it into a
      // precise, retryable error for the whole chunk.
      if (n < from || n > to) continue;
      headers[n - from] = { n, ts: toInt(h.timestamp), gasUsed: toInt(h.gasUsed), baseFee: Number(toBig(h.baseFeePerGas)) };
    }
  });
  for (let i = 0; i < headers.length; i++) {
    if (!headers[i]) throw new Error(`no header returned for block ${from + i} (of ${from}-${to})`);
  }

  // 2. tracked txs via logs (split on the provider's result limit)
  const logs = [];
  async function getLogs(a, b) {
    const [r] = await rpcBatch([{ method: "eth_getLogs", params: [{ address: TRACKED.map((t) => t.addr), fromBlock: hex(a), toBlock: hex(b) }] }]);
    if (r.error) {
      if (/limit|too many|exceed|range/i.test(r.error.message) && b > a) {
        const mid = Math.floor((a + b) / 2);
        await getLogs(a, mid); await getLogs(mid + 1, b); return;
      }
      throw new Error(`eth_getLogs: ${r.error.message}`);
    }
    logs.push(...r.result);
  }
  await getLogs(from, to);
  const txs = new Map(); // hash → { n, who, trades, failed, ids }
  for (const l of logs) {
    const h = l.transactionHash;
    // eth_getLogs is filtered by address, but a provider that ignores the filter (or a
    // reorg/misindexed node) can return logs from anywhere. Without this guard the lookup
    // returned undefined, which then became a key on the accumulator objects and threw
    // "Cannot read properties of undefined (reading 'gas')" and lost the whole chunk.
    const who = ADDR_TO_NAME.get(String(l.address || "").toLowerCase());
    if (!who) { untrackedLogs++; continue; }
    let t = txs.get(h);
    if (!t) { t = { n: toInt(l.blockNumber), who, trades: 0, failed: 0, ids: [] }; txs.set(h, t); }
    // Match by signature hash first; fall back to event SHAPE so a proxy upgrade that
    // re-declares TradeSettled (topic0 changes) cannot silently zero the trade count.
    // TradeSettled: 3 indexed (tradeId, buyer, seller) + 6 words of data.
    // SettlementFailed: 1 indexed (tradeId) + ABI-encoded string.
    const nTopics = l.topics.length, dataWords = (l.data.length - 2) / 64;
    if (l.topics[0] === TOPIC_TRADE_SETTLED || (nTopics === 4 && dataWords === 6)) {
      t.trades++; t.ids.push(BigInt(l.topics[1]));
      if (l.topics[0] !== TOPIC_TRADE_SETTLED) state.eventStats.tradeSettledByShape++;
    } else if (l.topics[0] === TOPIC_SETTLEMENT_FAILED || (nTopics === 2 && dataWords >= 3)) {
      t.failed++;
      if (l.topics[0] !== TOPIC_SETTLEMENT_FAILED) state.eventStats.failedByShape++;
    } else state.eventStats.other++;
  }
  const rcalls = [...txs.keys()].map((h) => ({ method: "eth_getTransactionReceipt", params: [h] }));
  await runBatches(rcalls, TX_RECEIPT_BATCH, (g, res) => {
    for (let i = 0; i < g.length; i++) {
      const r = res[i];
      if (r.error || !r.result) throw new Error(`receipt ${g[i].params[0]}: ${r.error?.message || "null"}`);
      const rc = r.result;
      const t = txs.get(g[i].params[0]);
      t.gasUsed = toInt(rc.gasUsed);
      t.l1Gas = rc.gasUsedForL1 ? toInt(rc.gasUsedForL1) : 0;
      // effectiveGasPrice is wei, which routinely exceeds 2^53 (9e15) once expressed as a
      // float. Keep the exact BigInt alongside it and do all wei math on the BigInt, so a
      // receipt's fee is bit-exact instead of order-dependent across float summation.
      t.priceWei = rc.effectiveGasPrice == null ? 0n : toBig(rc.effectiveGasPrice);
      t.price = Number(t.priceWei);
      t.from = rc.from.toLowerCase();
    }
  });
  const byBlock = new Map();
  for (const t of txs.values()) { if (!byBlock.has(t.n)) byBlock.set(t.n, []); byBlock.get(t.n).push(t); }

  // 3. sampled full receipts for the "who else" ranking
  const sampleBlocks = [];
  for (let n = from; n <= to; n++) if (n % SAMPLE_EVERY === 0) sampleBlocks.push(n);
  const sampled = new Map();
  await runBatches(sampleBlocks.map((n) => ({ method: "eth_getBlockReceipts", params: [hex(n)] })), BLOCK_RECEIPT_BATCH, (g, res) => {
    for (let i = 0; i < g.length; i++) {
      const r = res[i];
      if (r.error || !r.result) throw new Error(`blockReceipts ${g[i].params[0]}: ${r.error?.message || "null"}`);
      sampled.set(toInt(g[i].params[0]), r.result.map((rc) => ({
        to: rc.to ? rc.to.toLowerCase() : (rc.contractAddress ? "contract-creation" : "unknown"),
        gasUsed: toInt(rc.gasUsed),
      })).filter((x) => x.to !== SYSTEM_TX_TO || x.gasUsed > 0));
    }
  });

  // 4. walk blocks in order: model, rounds, minute rows
  const m = cur.model;
  const cons = state.constraints;
  const fitErr = [];
  for (const h of headers) {
    const ours = byBlock.get(h.n) || [];
    const gasBy = Object.fromEntries(CKEYS.map((k) => [k, 0]));
    for (const t of ours) { gasBy[t.who] += t.gasUsed; gasBy.all += t.gasUsed; }

    // Model check BEFORE applying this block: the state so far predicts THIS block's base fee.
    if (m.lastTs !== null && m.warmAt !== null && h.ts >= m.warmAt) {
      let E = 0; for (let i = 0; i < cons.length; i++) E += m.backlogs[i] / (cons[i].window * cons[i].target);
      fitErr.push(Math.abs((state.minBaseFee * quartic(E)) / h.baseFee - 1));
    }
    const dt = m.lastTs === null ? 0 : Math.max(0, h.ts - m.lastTs);
    const l2gas = h.gasUsed; // gasUsedForL1 is ~0 on Arb Sepolia (pricePerUnit = 0); see README
    const qMeas = h.baseFee / state.minBaseFee;
    if (m.lastTs === null) {
      // Cold start: no archive node to read historical backlogs, but the measured base fee
      // gives the total exponent E. Park it on the longest window; shorter windows re-converge
      // within their own length, the long one decays over a day.
      const Emeas = invQuartic(qMeas);
      const li = cons.reduce((bi, c, i) => (c.window > cons[bi].window ? i : bi), 0);
      m.backlogs[li] = Emeas * cons[li].window * cons[li].target;
      for (const k of CKEYS) m.without[k][li] = m.backlogs[li];
    }
    let E = 0;
    const Eno = Object.fromEntries(CKEYS.map((k) => [k, 0]));
    for (let i = 0; i < cons.length; i++) {
      const { target, window } = cons[i];
      m.backlogs[i] = Math.max(0, m.backlogs[i] - dt * target) + l2gas;
      E += m.backlogs[i] / (window * target);
      for (const k of CKEYS) {
        m.without[k][i] = Math.max(0, m.without[k][i] - dt * target) + Math.max(0, l2gas - gasBy[k]);
        Eno[k] += m.without[k][i] / (window * target);
      }
    }
    m.lastTs = h.ts;
    if (m.warmAt === null) m.warmAt = h.ts + Math.max(...cons.map((c) => c.window));
    const qModel = quartic(E);
    const Emeas = invQuartic(qMeas);
    // Counterfactual anchored to the MEASURED fee: measured exponent minus the replayed
    // difference this contract's gas makes, so cold-start error cancels out.
    const qNo = {}, uplift = {};
    for (const k of CKEYS) { qNo[k] = quartic(Math.max(0, Emeas - (E - Eno[k]))); uplift[k] = Math.max(0, 1 - qNo[k] / qMeas); }

    // rounds: raw uint64 trade ids restart near 1 on a competition round reset (per contract)
    for (const t of ours) for (const id of t.ids) observeTradeId(cur, t.who, id > 2n ** 53n ? Number.MAX_SAFE_INTEGER : Number(id), h);

    // minute row
    const minute = Math.floor(h.ts / 60) * 60;
    const d = dayFile(dayOf(h.ts));
    const row = d.rows[minute] || (d.rows[minute] = newRow(minute));
    row.blocks++;
    row.chainGas += h.gasUsed;
    row.baseFeeSum += h.baseFee;
    row.baseFeeMax = Math.max(row.baseFeeMax, h.baseFee);
    row.baseFeeMin = row.baseFeeMin === 0 ? h.baseFee : Math.min(row.baseFeeMin, h.baseFee);
    row.qActSum += qMeas; row.qModelSum += qModel; row.qMax = Math.max(row.qMax, qMeas);
    for (const t of ours) for (const k of [t.who, "all"]) {
      const c = row.c[k];
      c.txs++; c.gas += t.gasUsed; c.l1Gas += t.l1Gas; c.trades += t.trades; c.failed += t.failed;
      // Wei math is exact: gasUsed * effectiveGasPrice routinely exceeds 2^53, so summing
      // it as a float silently lost precision and made the published ETH totals depend on
      // the order transactions happened to be visited in. The division is done in BigInt
      // and scaled to ETH once, keeping ~18 significant digits of the wei total.
      const minWei = minBaseFeeWei();
      const feeEth = weiToEth(BigInt(t.gasUsed) * t.priceWei);
      c.feeEth += feeEth;
      c.premiumEth += weiToEth(BigInt(t.gasUsed) * (t.priceWei > minWei ? t.priceWei - minWei : 0n));
      // selfPremiumEth is an attribution share of the fee we just computed exactly, so it
      // is derived from that value rather than re-running the wei product with a float ratio.
      c.selfPremiumEth += feeEth * uplift[k];
    }
    for (const k of CKEYS) {
      row.c[k].qNoSum += qNo[k];
      // The gas-not-ours * baseFee product also passes 2^53 on a busy block (30M gas * 10 gwei
      // wei is ~3e17), so it is computed in BigInt and the uplift share is applied as a
      // fixed-point millionth to stay exact rather than multiplying wei by a float ratio.
      row.c[k].causedEth += weiToEth(BigInt(Math.max(0, h.gasUsed - gasBy[k])) * BigInt(Math.round(h.baseFee)) * BigInt(Math.round(uplift[k] * 1e6))) / 1e6;
    }

    const s = sampled.get(h.n);
    if (s) {
      row.sampled++;
      row.chainTxs += s.length; // sampled count; page scales by sampleEvery
      const hourKey = String(Math.floor(h.ts / 3600) * 3600);
      const hm = d.topHours[hourKey] || (d.topHours[hourKey] = {});
      for (const x of s) {
        const e = hm[x.to] || (hm[x.to] = [0, 0]);
        e[0] += x.gasUsed; e[1] += 1;
      }
    }
  }
  // prune hour maps to TOP_PER_HOUR (+ an "_other" bucket)
  for (const d of dayCache.values()) for (const [hk, hm] of Object.entries(d.topHours)) {
    const entries = Object.entries(hm).filter(([k]) => k !== "_other");
    if (entries.length <= TOP_PER_HOUR) continue;
    entries.sort((a, b) => b[1][0] - a[1][0]);
    const keep = Object.fromEntries(entries.slice(0, TOP_PER_HOUR));
    const other = hm._other || [0, 0];
    for (const [, v] of entries.slice(TOP_PER_HOUR)) { other[0] += v[0]; other[1] += v[1]; }
    keep._other = other;
    d.topHours[hk] = keep;
  }
  if (fitErr.length > 50) {
    fitErr.sort((a, b) => a - b);
    cur.fit = { blocks: fitErr.length, medianAbsErr: fitErr[Math.floor(fitErr.length / 2)], p90AbsErr: fitErr[Math.floor(fitErr.length * 0.9)], asOfBlock: to };
  }
  cur.lastBlock = to;
  cur.prevHeader = { n: to, ts: headers[headers.length - 1].ts };
  return { txs: txs.size, logs: logs.length, sampled: sampleBlocks.length };
}

/** Walk one cursor forward until `until` (inclusive) or the time budget runs out. */
async function runCursor(cur, until) {
  let from = cur.lastBlock + 1, chunks = 0;
  while (from <= until) {
    if (budgetLeft() <= 0) { log(`[${cur.name}] time budget reached at ${cur.lastBlock}; will resume next run`); break; }
    const to = Math.min(until, from + CHUNK - 1);
    const t1 = Date.now();
    let info;
    try { info = await processChunk(cur, from, to); }
    catch (e) {
      // Do not take the whole run (and the deploy) down with one bad chunk: keep the last
      // checkpoint, record the error for the page, stop this cursor for now.
      state.lastError = { at: Math.floor(Date.now() / 1000), cursor: cur.name, from, to, message: String(e.message || e).slice(0, 300) };
      log(`[${cur.name}] chunk ${from}-${to} FAILED: ${state.lastError.message} — stopping this cursor for this run`);
      // in-memory day rows may hold partial minutes from the failed chunk: reload from disk
      dayCache.clear();
      return chunks;
    }
    state.lastError = null;
    checkpointDays();
    writeJson(statePath, state);
    chunks++;
    log(`[${cur.name}] blocks ${from}-${to} (${to - from + 1}) trackedTxs=${info.txs} logs=${info.logs} sampled=${info.sampled} in ${((Date.now() - t1) / 1000).toFixed(1)}s`);
    from = to + 1;
  }
  return chunks;
}

// ───────────────────────────── index / summary ─────────────────────────────
/** Join one contract's backfill and live round lists at the seam (pure). */
export function mergeRoundLists(bfRounds, lvRounds, seamClosed) {
  const bf = bfRounds.map((r) => ({ ...r })), lv = lvRounds.map((r) => ({ ...r }));
  if (seamClosed && bf.length && lv.length) {
    const last = bf[bf.length - 1], first = lv[0];
    if (!first.startKnown && first.firstId >= last.maxId / 4) {
      // same round continues across the seam
      last.maxId = Math.max(last.maxId, first.maxId);
      if (first.endTs) { last.endTs = first.endTs; last.endBlock = first.endBlock; }
      lv.shift();
    }
  }
  let n = 0;
  return [...bf, ...lv].map((r) => ({ ...r, n: ++n }));
}
/** Rounds as the page sees them: backfill rounds first, then live, joined at the seam. */
function mergedRounds() {
  const { live, backfill } = state.cursors;
  const seamClosed = !backfill || backfill.lastBlock >= backfill.endBlock;
  return Object.fromEntries(NAMES.map((k) => [k, mergeRoundLists(backfill ? backfill.rounds[k] : [], live.rounds[k], seamClosed)]));
}
/** Days of minute rows kept on disk. Older day files are pruned after this many, so RAM,
 *  per-run I/O, the published _site bundle and the repo's git history stay bounded instead
 *  of growing forever at ~1440 rows per day. Set RETENTION_DAYS=0 to keep everything. */
const RETENTION_DAYS = envPositiveInt("RETENTION_DAYS", 45);

/** Delete day files older than the retention window (pure decision, side-effecting write). */
function pruneOldDays(daysDir, days) {
  if (RETENTION_DAYS === 0 || days.length <= RETENTION_DAYS) return [];
  const doomed = days.slice(0, days.length - RETENTION_DAYS);
  for (const date of doomed) {
    // The in-memory cache is only this run's work; drop the entry so a later write in the
    // same process cannot resurrect a file we just deleted.
    dayCache.delete(date);
    try { fs.unlinkSync(path.join(daysDir, `${date}.json`)); } catch { /* already gone */ }
  }
  log(`pruned ${doomed.length} day file(s) older than ${RETENTION_DAYS} days (${doomed[0]}..${doomed[doomed.length - 1]})`);
  return doomed;
}

function writeIndex() {
  const daysDir = path.join(DATA_DIR, "days");
  const days = fs.existsSync(daysDir) ? fs.readdirSync(daysDir).filter((f) => f.endsWith(".json")).map((f) => f.slice(0, -5)).sort() : [];
  pruneOldDays(daysDir, days);
  const summaries = days.map((date) => {
    const rows = Object.values(dayCache.get(date)?.rows || readJson(path.join(daysDir, `${date}.json`), { rows: {} }).rows);
    // `firstT` must never stay Infinity for a day with no rows: the page computes
    // Math.min(...days.map(d => d.firstT)), and Math.min(Infinity) -> 0, which made the
    // range span from the epoch and iterate ~29.8M minute buckets (a frozen tab).
    const s = { date, minutes: rows.length, blocks: 0, chainGas: 0, baseFeeMax: 0, firstT: null, lastT: 0, c: Object.fromEntries(CKEYS.map((k) => [k, { gas: 0, txs: 0, trades: 0, feeEth: 0 }])) };
    for (const r of rows) {
      const f = "baseFeeAvg" in r ? r : finalizeRow(r);
      s.blocks += f.blocks; s.chainGas += f.chainGas; s.baseFeeMax = Math.max(s.baseFeeMax, f.baseFeeMax);
      s.firstT = s.firstT === null ? f.t : Math.min(s.firstT, f.t);
      s.lastT = Math.max(s.lastT, f.t);
      for (const k of CKEYS) { const x = f.c[k]; s.c[k].gas += x.gas; s.c[k].txs += x.txs; s.c[k].trades += x.trades; s.c[k].feeEth += x.feeEth; }
    }
    return s;
  });
  const { live, backfill } = state.cursors;
  writeJson(path.join(DATA_DIR, "index.json"), {
    updatedAt: Math.floor(Date.now() / 1000),
    chainId: state.chainId,
    tracked: state.tracked,
    startBlock: backfill ? backfill.from : live.from,
    lastBlock: live.lastBlock,
    lastBlockTs: live.prevHeader?.ts ?? null,
    liveFrom: live.from,
    liveFromTs: live.model.warmAt === null ? null : live.model.warmAt - Math.max(...state.constraints.map((c) => c.window)),
    backfill: backfill ? { from: backfill.from, lastBlock: backfill.lastBlock, endBlock: backfill.endBlock, done: backfill.lastBlock >= backfill.endBlock, lastTs: backfill.prevHeader?.ts ?? null } : null,
    minBaseFee: state.minBaseFee,
    constraints: state.constraints,
    sampleEvery: state.sampleEvery,
    modelWarmAt: live.model.warmAt,
    fit: live.fit,
    eventStats: state.eventStats,
    lastError: state.lastError || null,
    // Diagnostics surfaced by renderMeta(): a non-zero value means an endpoint returned logs
    // outside the tracked-address filter and we discarded them rather than crashing.
    untrackedLogs,
    rounds: mergedRounds(),
    labels,
    days: summaries,
  });
}

// ───────────────────────────── main ─────────────────────────────
const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) (async () => {
  if (!state) { state = await initState(); log(`fresh state: chain ${state.chainId}, tracking ${NAMES.join("+")}, live from ${state.cursors.live.from} (${LIVE_HOURS}h), backfill ${state.cursors.backfill ? `${state.cursors.backfill.from}-${state.cursors.backfill.endBlock} (${BACKFILL_DAYS}d)` : "off"}`); }
  else {
    const same = state.tracked && state.tracked.length === TRACKED.length && state.tracked.every((t, i) => t.addr === TRACKED[i].addr && t.name === TRACKED[i].name);
    if (!same) throw new Error(`TRACKED changed since data/state.json was created (${JSON.stringify(state.tracked)}) — delete data/ to restart`);
    if (!state.cursors) state = await migrateV1(state);
    // refresh the chain params each run (an ArbOwner change to the schedule would silently skew the model)
    const { minBaseFee, constraints } = await readChainParams();
    if (constraints.length === state.constraints.length) state.constraints = constraints.map(({ target, window }) => ({ target, window }));
    // Guard against a zero floor: every "x times min" ratio on the page divides by this
    // value, and a persisted 0 would render as Infinity and persist across every later run.
    if (!Number.isFinite(minBaseFee) || minBaseFee <= 0) throw new Error(`chain reported an unusable minimum base fee: ${minBaseFee}`);
    state.minBaseFee = minBaseFee;
    state.eventStats ||= { tradeSettledByShape: 0, failedByShape: 0, other: 0 };
  }
  const { tip } = await chainTipAndRate();
  const { live, backfill } = state.cursors;
  log(`live lastBlock=${live.lastBlock} tip=${tip} behind=${tip - live.lastBlock}; backfill ${backfill ? `${backfill.lastBlock}/${backfill.endBlock} (${backfill.endBlock - backfill.lastBlock} left)` : "done"}; endpoints=${ENDPOINTS.map((e) => new URL(e).host).join(",")}`);
  let chunks = await runCursor(live, tip);
  if (backfill && backfill.lastBlock < backfill.endBlock && budgetLeft() > 0) chunks += await runCursor(backfill, backfill.endBlock);
  for (const d of dayCache.values()) for (const k of Object.keys(d.rows)) if ("baseFeeSum" in d.rows[k]) d.rows[k] = finalizeRow(d.rows[k]);
  for (const [date, d] of dayCache) writeJson(path.join(DATA_DIR, "days", `${date}.json`), d);
  writeIndex();
  const roundsSummary = NAMES.map((n) => `${n}:${mergedRounds()[n].length}`).join(" ");
  log(`throttle events this run: ${throttleEvents}`);
  log(`done: ${chunks} chunks, live=${live.lastBlock}, backfill=${backfill ? `${backfill.lastBlock}/${backfill.endBlock}` : "done"}, rounds ${roundsSummary}, ${((Date.now() - t0) / 1000).toFixed(0)}s`);
})().catch((e) => { console.error(e); process.exit(1); });
