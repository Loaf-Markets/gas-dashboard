# Loaf settlement gas dashboard (Arbitrum Sepolia)

A team-visible, no-login dashboard of what the Loaf settlement contracts burn on
chain: gas per minute / hour / day / competition round, our share of all chain gas,
how much of the base-fee elevation we cause ourselves, what that costs in ETH, and
who else is burning gas on the chain. Prod and staging are tracked as separate
contracts (they live on the same chain) with a Prod / Staging / All Loaf switch.

Hosted on **GitHub Pages** from a public repo, refreshed every 10 minutes by a GitHub
Action, fed only by **public RPC endpoints** — no Grafana, no VPN, no secrets, no
bridge box. On top of the collected history the page has a **"Right now" strip** that
the browser reads straight from a public RPC every 30 s (base fee, trades and
batches per minute, gas rate and share, spend pace), so the live picture never waits
for the collector.

```
collect.mjs ──(cron */10)──▶ data/days/YYYY-MM-DD.json + data/index.json ──▶ site/ on Pages
                               │ state in the Actions cache; git checkpoint every 6 h
browser ──(every 30 s)──▶ public RPC: feeHistory + getLogs + sampled headers + receipts
```

## Why this shape

| Constraint | Decision |
|---|---|
| Team Grafana is IP-allowlisted | Static site on GitHub Pages; anyone with the link can open it |
| `Loaf-Markets/Smart-Contracts` is **private on a free org plan** — GitHub Pages is not available for private repos on Free; and every merge to its `main` images and rolls the bridge fleets | This is its own repo (public, so Pages works); it contains only public chain data and this code, and never touches the bridge deploy path |
| Public RPCs throttle at ~1 batch/s per IP (a GitHub runner scans ~11–20 blocks/s across four endpoints) | Collector is incremental + checkpointed, uses batched calls with per-endpoint adaptive pacing, and runs two cursors: a **live** cursor that follows the tip (processed first, so the page is current after the first run) and a **backfill** cursor that fills older history behind it |
| No archive node (historical `eth_call` fails) | ArbOS pricing model is anchored to the *measured* base fee, so cold-start error cancels out of the counterfactual |
| Ranking "everyone else" needs every receipt on the chain (~350k blocks/day) | Full receipts for 1-in-200 blocks, scaled — clearly labelled as sampled. Our own numbers are exact |

## Publish it (one time, ~5 minutes)

The directory is already a local git repo (`~/Code/loaf/gas-dashboard`, branch `main`, nothing committed yet):

```bash
cd ~/Code/loaf/gas-dashboard
git add -A && git commit -m "Settlement gas dashboard: collector + Pages site"
# 1. create the public repo from it (name is yours to pick)
gh repo create Loaf-Markets/gas-dashboard --public --source=. --push \
  --description "Loaf settlement gas dashboard (Arbitrum Sepolia)"

# 2. enable Pages with the Actions source
gh api -X POST repos/Loaf-Markets/gas-dashboard/pages -f build_type=workflow

# 3. kick the first run (backfills BACKFILL_DAYS=3 across a few hourly runs)
gh workflow run gas-dashboard --repo Loaf-Markets/gas-dashboard
```

The site appears at `https://loaf-markets.github.io/gas-dashboard/` after the first
successful run. The live cursor makes the page current within the first run; the
collector then keeps running back-to-back (45 min budget per run) until the 3-day
backfill is done, after which each 10-minute run takes a minute or two.

Measured alternatives that did NOT help: the Etherscan V2 `txlist` API (63 s per
1,000 transactions on this chain, capped at 1,000 rows — slower than batched RPC
receipts), and larger RPC batches (every public endpoint returns 429 above ~100
items). Optional speed-up: add a repo secret `RPC_URLS` with a comma-separated list that
includes a keyed provider (Alchemy / QuickNode / …). Do not reuse the bridge's
production Alchemy key — the dashboard would eat the bridge's quota. The collector round-robins
across every endpoint listed, so keyed + public together is fastest. Never put a
keyed URL in the workflow file or this README (P0 Rule 6).

Tracked contracts default to prod `0xA1467F…` + staging `0x3f29c4…`. To change
them set `TRACKED=name=0x…,name=0x…` in the workflow `env:` and delete `data/` so
the state restarts (the collector refuses to mix state from a different set).

Scheduled workflows on GitHub are paused after 60 days without repo activity; the
hourly data commits count as activity, so this does not bite unless the collector
itself stops committing.

## Run locally

```bash
cd ~/Code/loaf/gas-dashboard
BACKFILL_DAYS=0.05 node collect.mjs      # ~1.2 h of history, ~12 min on public RPCs (≈19 blocks/s)
node serve.mjs                           # http://127.0.0.1:8123/
```

Environment knobs are documented at the top of `collect.mjs`.

## What each number means

**Our gas** — every transaction that emitted a log on the selected settlement contract
(`eth_getLogs` by address, then the receipt's `gasUsed`). Batches that revert at
the top level emit nothing and are not counted; per-trade failures inside a batch
*are* counted (they emit `SettlementFailed`).

**Chain gas** — exact sum of block `gasUsed`. Share = ours / chain.

**Trades / batches** — `TradeSettled` events / distinct transactions. Gas per trade
is the ratio, so it includes the fixed per-batch overhead.

**Rounds** — trade ids on chain are raw `uint64`s that restart from ~1 on a
competition round reset. A new round opens when an id drops below a quarter of the
running maximum (after at least 5,000 ids). The first round's start is simply where
the data begins.

**Base fee multiple** — `baseFeePerGas / ArbGasInfo.getMinimumGasPrice()`
(currently 0.02 gwei on Arbitrum Sepolia). Above 1× means the chain is congested.

**Elevation we cause** — Arbitrum (ArbOS 51) prices gas from six constraints read
live from the chain (the older single `speedLimitPerSecond` of 7 M gas/s from
`getGasAccountingParams()` is dead storage and no longer what the chain prices
against — the live indefinite target is 10 M gas/s). Each constraint is a
(target gas/s, window s) pair with a backlog that drains at its target and grows
by each block's gas; `baseFee = min × Q(Σ backlog_i / (window_i × target_i))` with
`Q` a quartic approximation of `exp`. The collector replays this from block headers
twice — with all gas and with our gas removed — and reports
`1 − Q(E_measured − ΔE) / Q(E_measured)`, i.e. the fraction of the *measured* price
that exists because of our backlog. The replay is validated continuously: the page
header shows the median error of the cold replay against the measured fee (see
`Smart-Contracts/docs/gas-aware-dispatch-governor.md` §1 for the model derivation and its
wei-exact validation). The counterfactual needs 24 h of scanned history before it
is complete — until then "elevation we cause" is understated.

**Cost split** — floor = gas × min fee; premium = gas × (paid − min). The premium
is split by the elevation fraction above into "our own load" vs "others' load".
"Imposed on others" = the rest of the chain's gas × base fee × our fraction.

**Speed limits** — the live constraint targets, e.g. 60 M gas/s for 9 s down to
10 M gas/s indefinitely. Sustained chain load above the lowest target is what keeps
the base fee elevated even when we are idle.

**L1 component** — Arbitrum Sepolia currently posts with `pricePerUnit = 0`, so
`gasUsedForL1 ≈ 0` and all figures are pure L2 execution. On Arbitrum One the L1
share would add on top and is not affected by congestion or by deferring batches.

## Data layout

- `data/.checkpoint` — epoch of the last git checkpoint (the cache always has fresher data).
- `data/state.json` — the two cursors (`live`, `backfill`), each with its scan position,
  model backlogs and round-detector state. Delete to restart. A v1 single-cursor state is
  migrated automatically (it becomes the backfill; a fresh live cursor starts near the tip).
- `data/days/YYYY-MM-DD.json` — `rows` keyed by minute epoch: chain-wide fields plus
  `c.<name>` / `c.all` per-contract blocks (txs, gas, trades, ETH, premiums, counterfactual);
  `topHours` keyed by hour epoch: sampled per-address `[gas, txs]` (top 120 + `_other`).
- `data/index.json` — metadata, per-day totals, rounds, labels; the page loads this first.

Roughly 1 MB per day of history, committed by the Action (git-scraping pattern).
Prune `data/days/` older than what you care about if the repo grows; the page only
loads the days in the selected range.

## Known limits

- Sampled ranking: an address that is only active in a burst shorter than ~1 min
  can be missed or over-weighted. Widen the range to smooth it, or lower
  `SAMPLE_EVERY` (more RPC calls).
- The public RPCs sometimes rate-limit hard for minutes at a time; the run then
  simply ends at its time budget and resumes on the next tick. "updated N h ago" in
  the header turns red past 3 h. The "Right now" strip has its own status pill and
  falls back across three endpoints.
- If the Actions cache is evicted (it is LRU with a 10 GB cap and expires after 7 days
  unused), the next run resumes from the last git checkpoint, at most 6 h behind.
- Timestamps are block timestamps (UTC). Minutes with no block on the chain are
  shown as zero.
- While the backfill is still running there is a gap between the oldest backfilled
  minute and the live cursor's start; the header says how far along it is. Buckets
  inside the gap show as zero until it closes.
- The live cursor's pricing-model replay starts cold, so "elevation we cause" is
  understated for the first 24 h of the live segment even after the backfill lands.
