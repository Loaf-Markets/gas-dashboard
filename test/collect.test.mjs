import test from "node:test";
import assert from "node:assert/strict";
import { quartic, invQuartic, toAccum, finalizeRow, observeTradeId, mergeRoundLists, weiToEth } from "../collect.mjs";
import { resolveRequest, isInside, site, data } from "../serve.mjs";
import path from "node:path";

test("quartic matches the ArbOS-51 table from the governor design doc", () => {
  assert.ok(Math.abs(quartic(0.5) - 1.65) < 0.01);
  assert.ok(Math.abs(quartic(2) - 7.0) < 0.01);
  assert.ok(Math.abs(quartic(4) - 34.3) < 0.1);
  assert.ok(Math.abs(quartic(8.5) - 365) < 1);
});

test("invQuartic inverts quartic across the useful range", () => {
  for (const E of [0, 0.01, 0.5, 1, 2.5, 4, 8.5, 12]) assert.ok(Math.abs(invQuartic(quartic(E)) - E) < 1e-9, `E=${E}`);
  assert.equal(invQuartic(0.5), 0);
});

test("finalizeRow → toAccum round-trips a minute row", () => {
  const acc = { t: 60, blocks: 4, chainGas: 1000, chainTxs: 7, sampled: 1, baseFeeSum: 400, baseFeeMax: 120, baseFeeMin: 90, qActSum: 48, qModelSum: 44, qMax: 13,
    c: { prod: { txs: 2, gas: 300, l1Gas: 3, trades: 5, failed: 1, feeEth: 1e-4, premiumEth: 9e-5, selfPremiumEth: 1e-6, causedEth: 2e-6, qNoSum: 47 },
         staging: { txs: 0, gas: 0, l1Gas: 0, trades: 0, failed: 0, feeEth: 0, premiumEth: 0, selfPremiumEth: 0, causedEth: 0, qNoSum: 48 },
         all: { txs: 2, gas: 300, l1Gas: 3, trades: 5, failed: 1, feeEth: 1e-4, premiumEth: 9e-5, selfPremiumEth: 1e-6, causedEth: 2e-6, qNoSum: 47 } } };
  const fin = finalizeRow(acc);
  assert.equal(fin.baseFeeAvg, 100); assert.equal(fin.qAct, 12); assert.equal(fin.c.prod.qNo, 11.75); assert.equal(fin.sampledTxs, 7);
  const back = toAccum(fin);
  for (const k of ["blocks", "chainGas", "chainTxs", "sampled", "baseFeeMax", "baseFeeMin", "qMax"]) assert.equal(back[k], acc[k], k);
  assert.ok(Math.abs(back.baseFeeSum - acc.baseFeeSum) < 1e-9);
  assert.ok(Math.abs(back.qActSum - acc.qActSum) < 1e-9);
  assert.ok(Math.abs(back.c.prod.qNoSum - acc.c.prod.qNoSum) < 1e-9);
  assert.equal(back.c.prod.trades, 5);
  assert.deepEqual(finalizeRow(back), fin);
  assert.deepEqual(toAccum(back), back, "toAccum is idempotent on accumulator rows");
});

function cursor() { return { rounds: { prod: [] }, lastMaxTradeId: { prod: 0 } }; }

test("round detector: first id opens round 1 with unknown start; ids may wobble", () => {
  const c = cursor();
  observeTradeId(c, "prod", 100, { n: 10, ts: 1000 });
  observeTradeId(c, "prod", 103, { n: 11, ts: 1001 });
  observeTradeId(c, "prod", 101, { n: 12, ts: 1002 });
  assert.equal(c.rounds.prod.length, 1);
  assert.equal(c.rounds.prod[0].startKnown, false);
  assert.equal(c.rounds.prod[0].maxId, 103);
});

test("round detector: a reset after >5000 ids opens a new round and closes the previous one", () => {
  const c = cursor();
  observeTradeId(c, "prod", 20000, { n: 10, ts: 1000 });
  observeTradeId(c, "prod", 20001, { n: 11, ts: 1001 });
  observeTradeId(c, "prod", 3, { n: 50, ts: 1100 });
  assert.equal(c.rounds.prod.length, 2);
  assert.equal(c.rounds.prod[0].endBlock, 49); assert.equal(c.rounds.prod[0].endTs, 1100);
  assert.equal(c.rounds.prod[1].startKnown, true); assert.equal(c.rounds.prod[1].firstId, 3);
});

test("round detector: a small dip is not a reset; a reset before 5000 ids is ignored", () => {
  const c = cursor();
  observeTradeId(c, "prod", 20000, { n: 10, ts: 1000 });
  observeTradeId(c, "prod", 6000, { n: 11, ts: 1001 }); // > 1/4 of max → same round
  assert.equal(c.rounds.prod.length, 1);
  const d = cursor();
  observeTradeId(d, "prod", 4000, { n: 10, ts: 1000 });
  observeTradeId(d, "prod", 1, { n: 11, ts: 1001 });    // running max below 5000 → not a reset
  assert.equal(d.rounds.prod.length, 1);
});

test("mergeRoundLists joins the same round across a closed seam and renumbers", () => {
  const bf = [{ n: 1, startBlock: 1, startTs: 10, firstId: 5, maxId: 9000, startKnown: false }];
  const lv = [{ n: 1, startBlock: 500, startTs: 900, firstId: 9100, maxId: 9500, startKnown: false }, { n: 2, startBlock: 800, startTs: 1400, firstId: 2, maxId: 40, startKnown: true }];
  const merged = mergeRoundLists(bf, lv, true);
  assert.equal(merged.length, 2);
  assert.equal(merged[0].maxId, 9500); assert.equal(merged[0].startTs, 10);
  assert.deepEqual(merged.map((r) => r.n), [1, 2]);
  // reset at the seam: keep both, live round stays "start unknown"
  const lv2 = [{ n: 1, startBlock: 500, startTs: 900, firstId: 12, maxId: 40, startKnown: false }];
  const m2 = mergeRoundLists(bf, lv2, true);
  assert.equal(m2.length, 2); assert.equal(m2[1].startKnown, false);
  // open seam: never merge
  assert.equal(mergeRoundLists(bf, lv, false).length, 3);
});

// --- exact wei arithmetic (M4) ------------------------------------------------
// A base fee of 10 gwei is 1e16 wei, already past 2^53 (~9.007e15). Doing the fee
// math as floats loses low-order wei and makes totals order-dependent.
test("weiToEth is exact past the 2^53 float boundary", () => {
  const gwei = 1000000000n;
  assert.equal(weiToEth(0n), 0);
  assert.equal(weiToEth(1n), 1e-18);
  assert.equal(weiToEth(gwei), 1e-9);
  assert.equal(weiToEth(gwei * 1000000000n), 1);
  // 3e23 wei is exactly representable as a double, so it round-trips either way. Use a
  // gas count whose wei product is NOT representable: the float path drifts in the tail.
  const exact = 12345678901n * (10n * gwei);
  assert.equal(weiToEth(exact), 123.45678901);
  const approx = Number(exact) / 1e18;
  assert.equal(approx, 123.45678901000001, "float path is the imprecise one");
  assert.notEqual(approx, weiToEth(exact), "the BigInt path must be the exact one");
  // A value with a long tail must keep every digit it can.
  assert.equal(weiToEth(1234567890123456789n), 1.2345678901234567);
});

test("weiToEth handles negative values", () => {
  assert.equal(weiToEth(-1000000000000000000n), -1);
  assert.equal(weiToEth(-1000000000n), -1e-9);
  assert.ok(weiToEth(-1n) < 0);
  assert.equal(weiToEth(-1500000000000000000n), -1.5);
});

// --- preview-server path resolution (C1, C2) ---------------------------------
test("isInside respects path-separator boundaries, not string prefixes", () => {
  assert.ok(isInside("C:\\a\\data", "C:\\a\\data"));
  assert.ok(isInside("C:\\a\\data", "C:\\a\\data\\index.json"));
  // A sibling whose name merely STARTS WITH "data" must not count as inside "data".
  assert.ok(!isInside("C:\\a\\data", "C:\\a\\database\\secret.txt"));
  assert.ok(!isInside("C:\\a\\site", "C:\\a\\siteX\\index.html"));
});

test("serve: traversal out of data/ and site/ is refused", () => {
  // The two shapes the old startsWith() prefix check let through.
  assert.equal(resolveRequest("/data/../database/secret.txt"), null);
  assert.equal(resolveRequest("/../siteX/secret.txt"), null);
  assert.equal(resolveRequest("/data/../../package.json"), null);
  assert.equal(resolveRequest("/../../../../Windows/win.ini"), null);
  // A /data/ request may not re-root itself into site/.
  assert.equal(resolveRequest("/data/../site/app.js"), null);
});

test("serve: malformed percent-encoding is refused instead of throwing", () => {
  // decodeURIComponent("%") throws URIError; uncaught, it killed the whole server.
  for (const bad of ["/%", "/%zz", "/data/%", "/%e0%a4%a"]) {
    assert.equal(resolveRequest(bad), null, bad);
  }
  // NUL must never reach node:fs (ERR_INVALID_ARG_VALUE).
  assert.equal(resolveRequest("/%00"), null);
  assert.equal(resolveRequest("/data/index.json%00.txt"), null);
});

test("serve: legitimate requests still resolve", () => {
  assert.equal(resolveRequest("/"), path.join(site, "index.html"));
  assert.equal(resolveRequest("/app.js"), path.join(site, "app.js"));
  assert.equal(resolveRequest("/data/index.json"), path.join(data, "index.json"));
  assert.equal(resolveRequest("/data/days/2026-09-08.json"), path.join(data, "days", "2026-09-08.json"));
  // A query string is not part of the filesystem path.
  assert.equal(resolveRequest("/app.js?v=2"), path.join(site, "app.js"));
  // A percent-escaped but legitimate name still decodes.
  assert.equal(resolveRequest("/data/index%2Ejson"), path.join(data, "index.json"));
});

test("serve: /data with no file after it is refused", () => {
  assert.equal(resolveRequest("/data"), null);
  assert.equal(resolveRequest("/data/"), null);
});
