/* Settlement gas dashboard — static page over data/ produced by collect.mjs. No build step. */
(() => {
  const DATA = "data/";
  const $ = (s) => document.querySelector(s);
  const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
  const state = { meta: null, days: new Map(), range: "24h", gran: "hour", focus: null, custom: null, charts: {} };

  // ───────── formatting ─────────
  const fmtGas = (g) => g >= 1e9 ? (g / 1e9).toFixed(2) + " Ggas" : g >= 1e6 ? (g / 1e6).toFixed(2) + " Mgas" : g >= 1e3 ? (g / 1e3).toFixed(1) + " kgas" : Math.round(g) + " gas";
  const fmtGasAxis = (g) => g >= 1e9 ? (g / 1e9).toFixed(1) + "G" : g >= 1e6 ? (g / 1e6).toFixed(0) + "M" : g >= 1e3 ? (g / 1e3).toFixed(0) + "k" : String(g);
  const fmtEth = (e) => e >= 1 ? e.toFixed(3) + " ETH" : e >= 1e-3 ? e.toFixed(5) + " ETH" : (e * 1e6).toFixed(1) + " µETH";
  const fmtPct = (x, d = 1) => (x * 100).toFixed(d) + "%";
  const fmtNum = (n) => n.toLocaleString("en-US", { maximumFractionDigits: 0 });
  const fmtGwei = (wei) => (wei / 1e9).toFixed(3) + " gwei";
  const pad = (n) => String(n).padStart(2, "0");
  const fmtTs = (t, gran) => {
    const d = new Date(t * 1000);
    const date = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
    const hm = `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
    return gran === "day" ? date : `${date.slice(5)} ${hm}`;
  };
  const fmtDateTime = (t) => new Date(t * 1000).toISOString().replace("T", " ").slice(0, 16) + " UTC";
  const fmtDur = (s) => s >= 86400 ? (s / 86400).toFixed(1) + " d" : s >= 3600 ? (s / 3600).toFixed(1) + " h" : Math.round(s / 60) + " min";
  const short = (a) => a.length > 20 ? a.slice(0, 6) + "…" + a.slice(-4) : a;
  const label = (a) => (state.meta.labels && state.meta.labels[a]) || null;
  const explorer = (a) => `https://sepolia.arbiscan.io/address/${a}`;
  const focusName = () => state.focus === "all" ? "Loaf (all contracts)" : `Loaf ${state.focus}`;
  const trackedAddrs = () => new Set(state.meta.tracked.map((t) => t.addr));
  const focusAddrs = () => state.focus === "all" ? trackedAddrs() : new Set(state.meta.tracked.filter((t) => t.name === state.focus).map((t) => t.addr));

  // ───────── data loading ─────────
  async function getJson(p) { const r = await fetch(DATA + p, { cache: "no-cache" }); if (!r.ok) throw new Error(`${p}: HTTP ${r.status}`); return r.json(); }
  async function loadDays(dates) {
    await Promise.all(dates.map(async (d) => { if (!state.days.has(d)) { try { state.days.set(d, await getJson(`days/${d}.json`)); } catch { state.days.set(d, null); } } }));
  }
  const dayOf = (t) => new Date(t * 1000).toISOString().slice(0, 10);

  function currentRange() {
    const m = state.meta;
    const end = m.lastBlockTs;
    const first = Math.min(...m.days.map((d) => d.firstT));
    if (state.range === "custom" && state.custom) return [Math.max(first, state.custom[0]), Math.min(end + 60, state.custom[1])];
    const span = { "6h": 6 * 3600, "24h": 86400, "3d": 3 * 86400, "7d": 7 * 86400 }[state.range];
    if (!span) return [first, end + 60];
    return [Math.max(first, end - span), end + 60];
  }
  function datesBetween(a, b) { const out = []; for (let t = Math.floor(a / 86400) * 86400; t < b; t += 86400) out.push(dayOf(t)); return out; }

  const EMPTY_C = { txs: 0, gas: 0, l1Gas: 0, trades: 0, failed: 0, feeEth: 0, premiumEth: 0, selfPremiumEth: 0, causedEth: 0, qNo: 0 };
  /** Minute rows in [a, b), zero-filled where the chain produced no block. */
  function minuteRows(a, b) {
    const rows = [];
    for (let t = Math.floor(a / 60) * 60; t < b; t += 60) {
      const d = state.days.get(dayOf(t));
      const r = d && d.rows[t];
      rows.push(r || { t, blocks: 0, chainGas: 0, sampled: 0, sampledTxs: 0, baseFeeAvg: 0, baseFeeMax: 0, baseFeeMin: 0, qAct: 0, qModel: 0, qMax: 0, c: {} });
    }
    return rows;
  }
  const cf = (r) => r.c[state.focus] || EMPTY_C;
  function emptyBucket(t, key) {
    return { t, key, seconds: 0, blocks: 0, chainGas: 0, sampled: 0, sampledTxs: 0, ourTxs: 0, ourGas: 0, ourTrades: 0, ourFailed: 0, ourFeeEth: 0, ourPremiumEth: 0, ourSelfPremiumEth: 0, othersPremiumCausedEth: 0, bfW: 0, qActW: 0, qNoUsW: 0, qModelW: 0, baseFeeMax: 0, qMax: 0, upliftW: 0, per: {} };
  }
  function addRow(b, r) {
    const c = cf(r);
    b.seconds += 60; b.blocks += r.blocks; b.chainGas += r.chainGas; b.sampled += r.sampled; b.sampledTxs += r.sampledTxs;
    b.ourTxs += c.txs; b.ourGas += c.gas; b.ourTrades += c.trades; b.ourFailed += c.failed;
    b.ourFeeEth += c.feeEth; b.ourPremiumEth += c.premiumEth; b.ourSelfPremiumEth += c.selfPremiumEth; b.othersPremiumCausedEth += c.causedEth;
    b.bfW += r.baseFeeAvg * r.blocks; b.qActW += r.qAct * r.blocks; b.qNoUsW += c.qNo * r.blocks; b.qModelW += r.qModel * r.blocks;
    b.upliftW += (r.qAct > 0 ? 1 - c.qNo / r.qAct : 0) * r.blocks;
    b.baseFeeMax = Math.max(b.baseFeeMax, r.baseFeeMax); b.qMax = Math.max(b.qMax, r.qMax);
    for (const [k, x] of Object.entries(r.c)) { const p = b.per[k] || (b.per[k] = { gas: 0, txs: 0, trades: 0, feeEth: 0 }); p.gas += x.gas; p.txs += x.txs; p.trades += x.trades; p.feeEth += x.feeEth; }
  }
  function finish(b) {
    const w = b.blocks || 1;
    b.baseFeeAvg = b.bfW / w; b.qAct = b.qActW / w; b.qNoUs = b.qNoUsW / w; b.qModel = b.qModelW / w; b.uplift = b.upliftW / w;
    b.share = b.chainGas ? b.ourGas / b.chainGas : 0;
    b.ourRate = b.ourGas / (b.seconds || 1); b.chainRate = b.chainGas / (b.seconds || 1);
    return b;
  }
  function roundsForFocus() {
    const r = state.meta.rounds || {};
    const primary = state.meta.tracked[0].name;
    return r[state.focus === "all" ? primary : state.focus] || [];
  }
  function bucketize(rows, gran) {
    const out = new Map();
    const rounds = roundsForFocus();
    for (const r of rows) {
      let key, t;
      if (gran === "minute") { key = r.t; t = r.t; }
      else if (gran === "hour") { t = Math.floor(r.t / 3600) * 3600; key = t; }
      else if (gran === "day") { t = Math.floor(r.t / 86400) * 86400; key = t; }
      else {
        const rd = rounds.find((x) => r.t >= x.startTs && (x.endTs == null || r.t < x.endTs));
        if (!rd) continue;
        key = "round-" + rd.n; t = rd.startTs;
      }
      let b = out.get(key); if (!b) { b = emptyBucket(t, key); out.set(key, b); }
      addRow(b, r);
    }
    return [...out.values()].map(finish).sort((x, y) => x.t - y.t);
  }
  function total(rows) { const b = emptyBucket(rows[0]?.t || 0, "total"); for (const r of rows) addRow(b, r); return finish(b); }

  // ───────── charts ─────────
  function theme() {
    return { ours: css("--ours"), oursSoft: css("--ours-soft"), rest: css("--rest"), restSoft: css("--rest-soft"), s2: css("--s2"), s3: css("--s3"), s4: css("--s4"), text: css("--text-2"), grid: css("--grid") };
  }
  function baseOpts(yFmt, extra = {}) {
    const th = theme();
    return {
      responsive: true, maintainAspectRatio: false, animation: false, normalized: true,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ` ${c.dataset.label}: ${(c.dataset.fmt || yFmt)(c.parsed.y)}` } } },
      scales: {
        x: { stacked: !!extra.stacked, ticks: { color: th.text, maxTicksLimit: 10, maxRotation: 0, autoSkip: true }, grid: { display: false }, border: { color: th.grid } },
        y: { stacked: !!extra.stacked, ticks: { color: th.text, callback: (v) => yFmt(v), maxTicksLimit: 6 }, grid: { color: th.grid }, border: { display: false }, beginAtZero: true, ...(extra.y || {}) },
      },
      elements: { point: { radius: 0, hitRadius: 8, hoverRadius: 4 }, line: { borderWidth: 2, tension: 0 } },
    };
  }
  function mount(id, cfg) {
    const host = $(id); if (state.charts[id]) state.charts[id].destroy();
    host.innerHTML = "<canvas></canvas>";
    state.charts[id] = new Chart(host.firstChild, cfg);
  }
  const useBars = (n) => n <= 400;
  const bucketLabels = (buckets, gran) => buckets.map((b) => gran === "round" ? "Round " + b.key.slice(6) : fmtTs(b.t, gran));

  function renderGas(buckets, gran) {
    const th = theme(); const labels = bucketLabels(buckets, gran); const bars = useBars(buckets.length);
    mount("#c-gas", {
      type: bars ? "bar" : "line",
      data: { labels, datasets: [
        { label: focusName(), data: buckets.map((b) => b.ourGas), backgroundColor: bars ? th.ours : th.oursSoft, borderColor: th.ours, fill: bars ? undefined : "origin", borderRadius: 2, borderSkipped: false, fmt: fmtGas },
        { label: "Rest of chain", data: buckets.map((b) => Math.max(0, b.chainGas - b.ourGas)), backgroundColor: bars ? th.rest : th.restSoft, borderColor: th.rest, fill: bars ? undefined : "-1", borderRadius: 2, borderSkipped: false, fmt: fmtGas },
      ] },
      options: baseOpts(fmtGasAxis, { stacked: true }),
    });
    $("#lg-gas-ours").textContent = focusName();
  }
  function renderShare(buckets, gran) {
    const th = theme(); const labels = bucketLabels(buckets, gran); const bars = useBars(buckets.length);
    mount("#c-share", { type: bars ? "bar" : "line", data: { labels, datasets: [{ label: `${focusName()} share of chain gas`, data: buckets.map((b) => b.share * 100), backgroundColor: bars ? th.ours : th.oursSoft, borderColor: th.ours, fill: !bars, borderRadius: 2, borderSkipped: false, fmt: (v) => v.toFixed(1) + "%" }] }, options: baseOpts((v) => v + "%", { y: { suggestedMax: 30 } }) });
  }
  function renderRate(buckets, gran) {
    const th = theme(); const labels = bucketLabels(buckets, gran); const bars = useBars(buckets.length);
    const cons = state.meta.constraints || [];
    const floor = cons.length ? Math.min(...cons.map((c) => c.target)) : null;
    const ds = [
      { label: `${focusName()} gas/s`, data: buckets.map((b) => b.ourRate), backgroundColor: bars ? th.ours : th.oursSoft, borderColor: th.ours, fill: !bars, borderRadius: 2, borderSkipped: false, fmt: (v) => fmtGas(v) + "/s" },
      { label: "Whole chain gas/s", data: buckets.map((b) => b.chainRate), type: "line", borderColor: th.rest, backgroundColor: th.rest, fill: false, fmt: (v) => fmtGas(v) + "/s" },
    ];
    if (floor) ds.push({ label: "Indefinite target", data: buckets.map(() => floor), type: "line", borderColor: th.s2, borderDash: [6, 4], borderWidth: 1.5, fill: false, fmt: (v) => fmtGas(v) + "/s" });
    mount("#c-rate", { type: bars ? "bar" : "line", data: { labels, datasets: ds }, options: baseOpts((v) => fmtGasAxis(v) + "/s") });
    $("#lg-rate-ours").textContent = `${focusName()} gas/s`;
  }
  function renderFee(buckets, gran) {
    const th = theme(); const labels = bucketLabels(buckets, gran); const min = state.meta.minBaseFee;
    mount("#c-fee", { type: "line", data: { labels, datasets: [
      { label: "Measured base fee (× min)", data: buckets.map((b) => b.qAct), borderColor: th.s2, backgroundColor: th.s2, fill: false, fmt: (v) => v.toFixed(2) + "× (" + fmtGwei(v * min) + ")" },
      { label: "Model replay, all gas (× min)", data: buckets.map((b) => b.qModel), borderColor: th.ours, backgroundColor: th.ours, fill: false, borderWidth: 1.5, fmt: (v) => v.toFixed(2) + "×" },
      { label: `Without ${focusName()} gas (× min)`, data: buckets.map((b) => b.qNoUs), borderColor: th.s3, backgroundColor: th.s3, borderDash: [6, 4], fill: false, fmt: (v) => v.toFixed(2) + "×" },
    ] }, options: baseOpts((v) => v + "×", { y: { beginAtZero: false, suggestedMin: 1 } }) });
    $("#lg-fee-nous").textContent = `Without ${focusName()} gas`;
    const warm = state.meta.modelWarmAt; const notes = [];
    if (warm && buckets.length && buckets[0].t < warm) notes.push(`Warm-up: the model needs 24 h of scanned history before the "without our gas" line and the elevation figures are complete; before ${fmtDateTime(warm)} they are understated.`);
    if (state.meta.fit) notes.push(`Replay fit vs measured base fee (warmed blocks): median error ${fmtPct(state.meta.fit.medianAbsErr, 1)}, p90 ${fmtPct(state.meta.fit.p90AbsErr, 1)} over ${fmtNum(state.meta.fit.blocks)} blocks.`);
    $("#fee-note").textContent = notes.join(" ");
  }
  function renderCost(buckets, gran) {
    const th = theme(); const labels = bucketLabels(buckets, gran); const bars = useBars(buckets.length);
    const mk = (l, f, color, soft) => ({ label: l, data: buckets.map(f), backgroundColor: bars ? color : soft, borderColor: color, fill: bars ? undefined : true, borderRadius: 2, borderSkipped: false, fmt: fmtEth });
    mount("#c-cost", { type: bars ? "bar" : "line", data: { labels, datasets: [
      mk("At minimum base fee", (b) => Math.max(0, b.ourFeeEth - b.ourPremiumEth), th.ours, th.oursSoft),
      mk("Premium from others' load", (b) => Math.max(0, b.ourPremiumEth - b.ourSelfPremiumEth), th.s4, th.s4 + "55"),
      mk("Premium from our own load", (b) => b.ourSelfPremiumEth, th.s2, th.s2 + "55"),
    ] }, options: baseOpts((v) => fmtEth(v), { stacked: true }) });
  }

  function renderTop(a, b, rows) {
    const th = theme(); const m = state.meta; const N = m.sampleEvery;
    const agg = new Map(); let sampledBlocks = 0, otherGas = 0;
    for (const r of rows) sampledBlocks += r.sampled;
    for (const d of datesBetween(a, b)) {
      const day = state.days.get(d); if (!day) continue;
      for (const [hk, hm] of Object.entries(day.topHours)) {
        const h = Number(hk); if (h + 3600 <= a || h >= b) continue;
        for (const [addr, [gas, txs]] of Object.entries(hm)) {
          if (addr === "_other") { otherGas += gas; continue; }
          const e = agg.get(addr) || [0, 0]; e[0] += gas; e[1] += txs; agg.set(addr, e);
        }
      }
    }
    const tot = total(rows);
    const tracked = trackedAddrs(); const focus = focusAddrs();
    const list = [...agg.entries()].sort((x, y) => y[1][0] - x[1][0]);
    const top = list.slice(0, 15);
    const sampledTotal = list.reduce((s, [, v]) => s + v[0], 0) + otherGas;
    $("#top-sub").textContent = `Estimated from full receipts of 1 in ${N} blocks (${fmtNum(sampledBlocks)} sampled blocks in range, scaled ×${N}). ${focusName()}'s exact total in this range is ${fmtGas(tot.ourGas)} (${fmtPct(tot.share)} of chain gas). Loaf contracts are highlighted.`;
    mount("#c-top", { type: "bar", data: { labels: top.map(([addr]) => label(addr) || short(addr)), datasets: [{ label: "Estimated gas", data: top.map(([, v]) => v[0] * N), backgroundColor: top.map(([addr]) => focus.has(addr) ? th.ours : tracked.has(addr) ? th.s3 : th.rest), borderRadius: 3, borderSkipped: false, fmt: fmtGas }] },
      options: { responsive: true, maintainAspectRatio: false, animation: false, indexAxis: "y", plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ` ${fmtGas(c.parsed.x)}` } } }, scales: { x: { ticks: { color: th.text, callback: (v) => fmtGasAxis(v) }, grid: { color: th.grid }, border: { display: false }, beginAtZero: true }, y: { ticks: { color: th.text, autoSkip: false }, grid: { display: false } } } } });
    const rowsHtml = list.slice(0, 40).map(([addr, v], i) => `<tr class="${tracked.has(addr) ? "ours" : ""}"><td class="num">${i + 1}</td><td class="mono"><a href="${explorer(addr)}" target="_blank" rel="noopener">${short(addr)}</a></td><td>${label(addr) || ""}</td><td class="num">${fmtGas(v[0] * N)}</td><td class="num">${sampledTotal ? fmtPct(v[0] / sampledTotal) : "–"}</td><td class="num">${fmtNum(v[1] * N)}</td><td class="num">${v[1] ? fmtGas(v[0] / v[1]) : "–"}</td></tr>`).join("");
    $("#t-top").innerHTML = `<thead><tr><th class="num">#</th><th>Address</th><th>Label</th><th class="num">Est. gas</th><th class="num">Share (sampled)</th><th class="num">Est. txs</th><th class="num">Gas / tx</th></tr></thead><tbody>${rowsHtml}</tbody>`;
  }

  function renderRounds(rowsAll) {
    const rounds = roundsForFocus(); const min = state.meta.minBaseFee;
    const body = rounds.map((rd) => {
      const rs = rowsAll.filter((r) => r.t >= rd.startTs && (rd.endTs == null || r.t < rd.endTs));
      const b = rs.length ? total(rs) : null;
      const end = rd.endTs ? fmtDateTime(rd.endTs) : `<span class="pill good">ongoing</span>`;
      const dur = (rd.endTs || state.meta.lastBlockTs) - rd.startTs;
      return `<tr><td>${rd.n}${rd.startKnown ? "" : " <span class='pill'>start = data start</span>"}</td><td>${fmtDateTime(rd.startTs)}</td><td>${end}</td><td class="num">${fmtDur(dur)}</td><td class="num">${b ? fmtNum(b.ourTrades) : "–"}</td><td class="num">${b ? fmtNum(b.ourTxs) : "–"}</td><td class="num">${b ? fmtGas(b.ourGas) : "–"}</td><td class="num">${b && b.ourTrades ? fmtGas(b.ourGas / b.ourTrades) : "–"}</td><td class="num">${b ? fmtPct(b.share) : "–"}</td><td class="num">${b ? fmtEth(b.ourFeeEth) : "–"}</td><td class="num">${b ? (b.baseFeeAvg / min).toFixed(1) + "×" : "–"}</td><td class="num">${b ? fmtPct(b.uplift) : "–"}</td><td class="num">${fmtNum(rd.maxId)}</td></tr>`;
    }).join("");
    const which = state.focus === "all" ? ` (rounds are detected per contract; showing ${state.meta.tracked[0].name})` : "";
    $("#rounds-sub").textContent = `A round boundary is detected on-chain when trade ids restart from ~1 (competition round reset)${which}. The first round's start is where our data begins, not the true round start. Totals are over the loaded range only.`;
    $("#t-rounds").innerHTML = `<thead><tr><th>Round</th><th>Start (UTC)</th><th>End (UTC)</th><th class="num">Length</th><th class="num">Trades</th><th class="num">Batches</th><th class="num">Gas</th><th class="num">Gas / trade</th><th class="num">Chain share</th><th class="num">ETH spent</th><th class="num">Avg base fee</th><th class="num">Our elevation</th><th class="num">Max trade id</th></tr></thead><tbody>${body || "<tr><td colspan=13>No rounds detected yet.</td></tr>"}</tbody>`;
  }

  function renderBucketTable(buckets, gran) {
    const min = state.meta.minBaseFee; const N = state.meta.sampleEvery;
    const body = buckets.slice().reverse().slice(0, 500).map((b) => `<tr><td>${gran === "round" ? "Round " + b.key.slice(6) : fmtDateTime(b.t)}</td><td class="num">${fmtNum(b.blocks)}</td><td class="num">${fmtGas(b.chainGas)}</td><td class="num">${fmtGas(b.ourGas)}</td><td class="num">${fmtPct(b.share)}</td><td class="num">${fmtNum(b.ourTxs)}</td><td class="num">${fmtNum(b.ourTrades)}</td><td class="num">${b.ourTrades ? fmtGas(b.ourGas / b.ourTrades) : "–"}</td><td class="num">${fmtGas(b.ourRate)}/s</td><td class="num">${(b.baseFeeAvg / min).toFixed(2)}×</td><td class="num">${(b.baseFeeMax / min).toFixed(1)}×</td><td class="num">${fmtPct(b.uplift)}</td><td class="num">${fmtEth(b.ourFeeEth)}</td><td class="num">${fmtEth(b.ourPremiumEth)}</td><td class="num">${fmtNum(b.sampledTxs * N)}</td></tr>`).join("");
    $("#t-buckets").innerHTML = `<thead><tr><th>Bucket start</th><th class="num">Blocks</th><th class="num">Chain gas</th><th class="num">Loaf gas</th><th class="num">Share</th><th class="num">Batches</th><th class="num">Trades</th><th class="num">Gas / trade</th><th class="num">Loaf gas/s</th><th class="num">Base fee avg</th><th class="num">Base fee max</th><th class="num">Our elevation</th><th class="num">ETH spent</th><th class="num">Premium paid</th><th class="num">≈ chain txs</th></tr></thead><tbody>${body}</tbody>`;
  }

  function renderTiles(rows) {
    const m = state.meta; const min = m.minBaseFee; const t = total(rows);
    const last = [...rows].reverse().find((r) => r.blocks > 0);
    const floor = m.constraints?.length ? Math.min(...m.constraints.map((c) => c.target)) : null;
    const nowMult = last ? last.baseFeeAvg / min : 0;
    const feePill = nowMult < 2 ? "good" : nowMult < 8 ? "warn" : "bad";
    const perLine = m.tracked.map((tr) => `${tr.name} ${fmtGas(t.per[tr.name]?.gas || 0)}`).join(" · ");
    const tiles = [
      [`${focusName()} gas used`, fmtGas(t.ourGas), `${fmtPct(t.share)} of chain gas · ${fmtNum(t.blocks)} blocks${state.focus === "all" ? ` · ${perLine}` : ""}`],
      ["Trades settled", fmtNum(t.ourTrades), `${fmtNum(t.ourTxs)} batch txs · ${t.ourTrades ? fmtGas(t.ourGas / t.ourTrades) : "–"} per trade · ${t.ourTxs ? (t.ourTrades / t.ourTxs).toFixed(2) : "–"} trades/batch${t.ourFailed ? ` · ${fmtNum(t.ourFailed)} failed legs` : ""}`],
      ["ETH spent on gas", fmtEth(t.ourFeeEth), `${fmtEth(t.ourPremiumEth)} (${t.ourFeeEth ? fmtPct(t.ourPremiumEth / t.ourFeeEth) : "–"}) above the floor price · ${t.seconds ? fmtEth(t.ourFeeEth / t.seconds * 86400) + "/day pace" : ""}`],
      ["Base fee now", `${nowMult.toFixed(1)}× min <span class="pill ${feePill}">${last ? fmtGwei(last.baseFeeAvg) : "–"}</span>`, `range avg ${(t.baseFeeAvg / min).toFixed(1)}× · max ${(t.baseFeeMax / min).toFixed(1)}× · min ${fmtGwei(min)}`],
      ["Elevation we cause", fmtPct(t.uplift), `self-inflicted premium ${fmtEth(t.ourSelfPremiumEth)} · imposed on others ${fmtEth(t.othersPremiumCausedEth)}`],
      [`${focusName()} gas rate`, fmtGas(t.ourRate) + "/s", `chain ${fmtGas(t.chainRate)}/s${floor ? ` · indefinite target ${fmtGas(floor)}/s` : ""}`],
    ];
    $("#tiles").innerHTML = tiles.map(([k, v, s]) => `<div class="tile"><div class="k">${k}</div><div class="v">${v}</div><div class="s">${s}</div></div>`).join("");
  }

  function renderMeta() {
    const m = state.meta;
    const age = Math.floor(Date.now() / 1000) - m.updatedAt;
    const lag = Math.floor(Date.now() / 1000) - (m.lastBlockTs || 0);
    const stale = age > 3 * 3600;
    $("#meta").innerHTML = [
      m.tracked.map((t) => `${t.name} <a class="mono" href="${explorer(t.addr)}" target="_blank" rel="noopener">${short(t.addr)}</a>`).join(" · "),
      `chain ${m.chainId}`,
      `<span class="${stale ? "stale" : ""}">updated ${fmtDur(age)} ago${stale ? " (stale)" : ""}</span>`,
      `data through block ${fmtNum(m.lastBlock)} (${fmtDur(lag)} behind now)`,
      `history since ${fmtDateTime(Math.min(...m.days.map((d) => d.firstT)))}`,
      m.fit ? `replay fit ±${fmtPct(m.fit.medianAbsErr, 1)}` : `model replay warming up`,
    ].join(" · ");
  }
  function renderLimits() {
    const cons = state.meta.constraints || [];
    $("#t-limits").innerHTML = `<thead><tr><th>Window</th><th class="num">Target gas/s</th><th class="num">Trades/s at 177k gas</th></tr></thead><tbody>${cons.map((c) => `<tr><td>${fmtDur(c.window)} (${c.window} s)</td><td class="num">${fmtGas(c.target)}/s</td><td class="num">${(c.target / 177000).toFixed(0)}</td></tr>`).join("")}</tbody>`;
  }

  // ───────── orchestration ─────────
  async function render() {
    const [a, b] = currentRange();
    await loadDays(datesBetween(a, b));
    const rows = minuteRows(a, b);
    const buckets = bucketize(rows, state.gran);
    $("#rangeInfo").textContent = `${fmtDateTime(a)} → ${fmtDateTime(b - 60)} · ${buckets.length} buckets`;
    renderTiles(rows);
    renderGas(buckets, state.gran); renderShare(buckets, state.gran); renderRate(buckets, state.gran);
    renderFee(buckets, state.gran); renderCost(buckets, state.gran);
    renderTop(a, b, rows); renderRounds(rows); renderBucketTable(buckets, state.gran); renderLimits();
  }

  function seg(id, attr, onPick) {
    for (const btn of document.querySelectorAll(`${id} button`)) btn.addEventListener("click", () => {
      document.querySelectorAll(`${id} button`).forEach((x) => x.setAttribute("aria-pressed", "false")); btn.setAttribute("aria-pressed", "true");
      onPick(btn.dataset[attr]);
    });
  }
  function wire() {
    seg("#range", "r", (v) => { state.range = v; $("#customRange").hidden = v !== "custom"; if (v !== "custom") render().catch(showErr); });
    seg("#gran", "g", (v) => { state.gran = v; render().catch(showErr); });
    const fz = $("#focus");
    fz.innerHTML = [...state.meta.tracked.map((t) => t.name), "all"].map((n, i) => `<button data-f="${n}" aria-pressed="${i === 0}">${n === "all" ? "All Loaf" : n[0].toUpperCase() + n.slice(1)}</button>`).join("");
    seg("#focus", "f", (v) => { state.focus = v; render().catch(showErr); });
    $("#applyCustom").addEventListener("click", () => {
      const f = Date.parse($("#from").value + "Z") / 1000, t = Date.parse($("#to").value + "Z") / 1000;
      if (Number.isFinite(f) && Number.isFinite(t) && t > f) { state.custom = [f, t]; render().catch(showErr); }
    });
    matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => render().catch(showErr));
  }
  function showErr(e) { $("#error").style.display = "block"; $("#error").textContent = "Failed to load dashboard data: " + e.message; console.error(e); }

  (async () => {
    try {
      state.meta = await getJson("index.json");
      state.focus = state.meta.tracked[0].name;
      renderMeta(); wire();
      const lastT = state.meta.lastBlockTs; const iso = (t) => new Date(t * 1000).toISOString().slice(0, 16);
      $("#from").value = iso(lastT - 86400); $("#to").value = iso(lastT);
      $("#controls").hidden = false; $("#app").hidden = false; $("#loading").remove();
      await render();
    } catch (e) { showErr(e); }
  })();
})();
