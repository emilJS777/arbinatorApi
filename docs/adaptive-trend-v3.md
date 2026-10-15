# Adaptive Trend v3: fixed paper experiment, 2026-10-15

Status: **unvalidated / inconclusive**, not a profitable preset. Baseline, v1 and v2 are unchanged.

## Rule fixed before outcome testing

Use all existing v2 entry gates, plus the sign of configured futures exchange mid-price change over 60 seconds. Long requires positive change, short negative. Use the latest anchor aged 60..65 seconds, distinct source timestamps, no inter-observation gap over 5 seconds, fresh linear USDT futures books. Missing history, wrong identity or stale data blocks entry. No threshold search, unanimity rule or future-price reconstruction. This is a directional consistency hypothesis, NOT a predicted executable return or cost-coverage estimate.

History is bounded in process memory, isolated by config/session/market. Restart, Pause or data gaps require warmup again. Duplicate timestamps do not advance history or replace their price. The filter runs again at pending-entry revalidation. Existing v2 renewal, persistence, fees, feedback, risk, latency/depth execution and exits remain authoritative. TP 1.8% / SL 0.9% of margin are not modified.

## Evidence

`trend-v3-export-check.json` records input hashes and separates version/session/config. Four exports have 42 repeated IDs without accounting conflicts, 161 unique records including cancellations. V1: 16 executed. V2: 36 executed, 7 wins, net -1.1173369 USDT, fees 0.9786369, expectancy -0.03103714, round-trip turnover 978.6369 USDT.

Latest subset: 24 executed, 6 wins, gross -0.0953, fees 0.6531061, net -0.7484061; expectancy -0.03118359, turnover 653.1061, closed-trade peak-to-trough drawdown 0.7897279 USDT. Drawdown is NOT mark-to-market drawdown. Turnover includes entry and exit notionals. Execution PnL already reflects simulated bid/ask/depth prices; adding spread again would double-count. Exact funding is not established by these exports.

No continuous pre-entry 60-second source-price series exists in these exports. Candidate retained/rejected counts and expectancy are **unknown**, not zero. This is retrospective accounting, not a filter backtest or full replay. The 2026-10-08 smoke data are outside frozen development and provide no performance evidence. No evaluation data were opened.

## Bounded forward comparison (not started automatically)

One public collection run of 1 hour, no automatic extension. Keep input strictly within frozen development `[2026-10-09T00:00:00Z, 2026-10-23T00:00:00Z)`; `--until` excludes October 23. It starts immediately. The existing collector covers five pairs; comparison uses VELVET only. Missing futures markets/API restrictions must be reported, never bypassed or replaced with spot.

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
export LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false AUTO_RUN_MIGRATIONS=false
mkdir -p research/local-trend-v3
caffeinate -i .venv/bin/python scripts/collect_perpetual_books.py \
  --output research/local-trend-v3/development.jsonl --duration 3600 --interval 1 \
  --notional 14 --exchanges mexc bybit gateio huobi --until 2026-10-23T00:00:00Z
PYTHONPATH=. .venv/bin/python scripts/compare_adaptive_book_v1.py \
  --development-input research/local-trend-v3/development.jsonl \
  --reference adaptive_book_v2 --candidate adaptive_trend_v3 \
  --config docs/trend-v3-comparison-config.json \
  --output research/local-trend-v3/comparison.json
```

Stop collector with Ctrl-C; resume with the same command plus `--resume` and only the unused duration budget. Do not exceed one hour of collection or the UTC boundary. Both versions replay identical chronological rows in an isolated in-memory DB. The frozen comparison config derives from the last exported executed trade, with runtime IDs removed and live disabled; it is NOT applied to hosted settings. Replay runs baseline cost .1% per fill / 250ms latency and adverse >=.15% / >=1000ms, plus explicitly assumed adverse funding .1% per 8h prorated, NOT actual settled cashflows.

Review criteria fixed in advance: verify futures identity/depth units; report source gaps and blocked warmups, actual source/receipt cadence, signal funnel and pending cancellations. Seek >=95% fresh execution-venue observations and >=30 v2 fills for an exploratory comparison; otherwise inconclusive, no tuning or automated longer run. Report both net expectancy and total net PnL, both fees, turnover, exposure and drawdown. Fewer trades alone is not improvement. Only consider further paper study if net expectancy improves AND is positive under the adverse scenario, with no safety regression; this is not statistical proof of edge. If losses remain or evidence is sparse, report it without adjusting thresholds.

REST >=1s collection cannot identify a precise 250ms fill or all intrabook events. Replay waits for available post-delay depth; flag this resolution limitation. Exact account fees, settled funding/mark prices and queue/partial-fill effects remain limitations. Do not use final evaluation or infer profitability from synthetic tests.

## Manual deploy / optional UI trial

Review and commit only listed source/test/docs files in the two actual push repositories, excluding unrelated pycache changes; push both, then run your existing manual redeploy. No new migration; existing single head remains q0b5c8e2f709. Keep server hard-disable true. No deployment was performed here.

After deployment, verify matching frontend/backend builds. To opt in: Pause entries; manage/close any existing paper position normally; create a new paper session, choose `Adaptive Trend v3 / Unvalidated`, review unchanged TP/SL and costs, Save, refresh and verify the saved version before starting paper. Never override an open trade's immutable settings. The current selection is NOT automatically changed. Export snapshots include the trend's anchor/current timestamps and prices for audit. A sequential hosted v3 session is forward evidence, not a paired comparison with earlier v2 sessions.

Checks: backend pytest, frontend npm test/build, isolated migration-head inspection. No hosted access, orders or database migrations were performed.
