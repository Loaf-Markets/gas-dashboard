import test from "node:test";
import assert from "node:assert/strict";
import { quartic, invQuartic, toAccum, finalizeRow, observeTradeId, mergeRoundLists } from "../collect.mjs";

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
