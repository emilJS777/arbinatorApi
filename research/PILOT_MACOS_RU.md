# UTC boundaries и 24-hour macOS pilot

Frozen v1.1 НЕ изменён: даты корректны как полуоткрытые интервалы.

| Период | Включительно от UTC | Исключительно до UTC | Полные календарные дни UTC |
|---|---|---|---|
| Development | 2026-10-09 00:00:00 | 2026-10-23 00:00:00 | 9–22 октября, 14 суток |
| Gap | 2026-10-23 00:00:00 | 2026-10-24 00:00:00 | 23 октября |
| Evaluation | 2026-10-24 00:00:00 | 2026-11-21 00:00:00 | 24 октября–20 ноября, 28 суток |

`--until 2026-10-23T00:00:00Z` правильно исключает 23 октября из development. Если понадобится включить 23 октября или передвинуть даты — требуется НОВАЯ версия с новым gap/evaluation, не edit frozen JSON. Сейчас такого изменения нет. UTC midnight = 04:00 Asia/Yerevan.

Старая development-команда запускается сразу, поскольку collector не имеет start scheduler. Исправление инструкции: для ТОЧНОГО development window запускать её 9 октября в 00:00 UTC, а не сразу. Поздний старт означает неполное окно и должен быть раскрыт; не имитировать отсутствующие часы. До этой даты можно запускать ТОЛЬКО отдельный pilot ниже. Он не становится development/evaluation автоматически. Pilot сейчас пересечёт начало development по календарю, но остаётся отдельным диагностическим набором и не используется в итоговых файлах/метриках.

## Запуск (вручную; здесь не выполнен)

macOS Terminal, local backend. Нужны питание и открытая крышка: caffeinate предотвращает idle sleep, но не lid-close, power loss, manual sleep или reboot. Экран может погаснуть. Wrapper не запускает scanner/bot, public collector не использует ключи/orders.

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
mkdir -p research/pilot
PILOT="research/pilot/pilot-$(date -u +%Y%m%dT%H%M%SZ).jsonl"
UNTIL="$(date -u -v+24H +%Y-%m-%dT%H:%M:%SZ)"
PLAN=research/protocols/orderbook-v1.1.frozen.json

# Refuse a pilot extending into the untouched evaluation period.
if .venv/bin/python -c 'import json,sys; from datetime import datetime,timezone; p=json.load(open(sys.argv[1])); end=datetime.fromisoformat(sys.argv[2].replace("Z","+00:00")); boundary=datetime.fromisoformat(p["chronology"]["evaluation_start_utc"]).replace(tzinfo=timezone.utc); assert end < boundary, "pilot overlaps evaluation; choose a new future protocol before collecting"' "$PLAN" "$UNTIL"; then

nohup env LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false \
  .venv/bin/python -u -B scripts/collect_perpetual_books.py \
  --output "$PILOT" --duration 86400 --until "$UNTIL" \
  --interval 2 --notional 20 --funding-interval 300 \
  > "$PILOT.log" 2>&1 &
PID=$!
printf '%s\n' "$PID" > "$PILOT.pid"
printf '%s\n' "$PILOT" > research/pilot/current.path
printf '%s\n' "$UNTIL" > "$PILOT.until"
nohup caffeinate -ims -w "$PID" > "$PILOT.sleep.log" 2>&1 &
fi
```

Абсолютный `--until` ставит максимум 24 часа от подготовки команды, включая initialization; фактический полезный capture чуть меньше 24 часов. `--duration` ещё один верхний предел. Новое UTC имя отделяет pilot от всех прежних наборов. Скрипт намеренно не используется как scheduler/supervisor.

## Progress (можно из нового Terminal)

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
PILOT="$(cat research/pilot/current.path)"
PID="$(cat "$PILOT.pid")"
ps -p "$PID" -o pid,etime,command
.venv/bin/python -m json.tool "${PILOT%.jsonl}.checkpoint.json"
wc -l "$PILOT" "${PILOT%.jsonl}.funding.jsonl"
du -h "$PILOT" "${PILOT%.jsonl}.funding.jsonl"
df -h .
tail -n 20 "$PILOT.log"
pmset -g assertions
```

Checkpoint появляется после initialization/первого законченного batch и обновляется после flush/fsync. Log полный summary печатает при остановке. Это не повод запускать второй writer. `pmset` должен показывать assertion caffeinate; после завершения collector assertion снимется. Pilot не выполняет авто-restart после ошибок: при exited процессе проверить log.

## Safe stop

```sh
PILOT="$(cat research/pilot/current.path)"
PID="$(cat "$PILOT.pid")"
case "$(ps -p "$PID" -o command=)" in
  *scripts/collect_perpetual_books.py*"$PILOT"*) kill -TERM "$PID" ;;
  *) echo "PID absent or does not match this pilot; no signal sent" ;;
esac
ps -p "$PID" -o pid,etime,command
tail -n 20 "$PILOT.log"
```

Дождаться выхода (текущий batch может занять несколько timeout/rate-limit интервалов). Не применять kill -9, не удалять lock/checkpoint/JSONL. Проверка command предотвращает signal чужому reused PID. После стопа caffeinate завершится сам.

Optional resume ДО исходного cutoff: убедиться, что старый process вышел, прочитать `UNTIL="$(cat "$PILOT.until")"` и повторить launch для ТОГО ЖЕ `$PILOT` с `--resume --until "$UNTIL" --duration 86400`, обновить PID и привязать caffeinate к новому PID. Не устанавливать новый cutoff +24h при resume: иначе pilot удлинится. Если cutoff уже прошёл, collector откажет — создать новый отдельный pilot, не продлевать старый незаметно. Record gaps остаются в quality report.

## Validation после выхода collector

```sh
PILOT="$(cat research/pilot/current.path)"
PLAN=research/protocols/orderbook-v1.1.frozen.json

.venv/bin/python -B scripts/research_quality.py \
  --input "$PILOT" --funding-input "${PILOT%.jsonl}.funding.jsonl" \
  --plan "$PLAN" --output "$PILOT.quality.json"

.venv/bin/python -B scripts/replay_orderbooks.py \
  --input "$PILOT" --funding-input "${PILOT%.jsonl}.funding.jsonl" \
  --research-plan "$PLAN" --all-symbols --diagnostic \
  --output "$PILOT.replay.json"

.venv/bin/python -c 'import json,sys; q=json.load(open(sys.argv[1]+".quality.json")); r=json.load(open(sys.argv[1]+".replay.json")); print("pairs quality:",q["pairs"]); print("missing funding:",q["funding"]); print("book GB:",q["book_bytes"]/1e9,"projected GB/day:",(q["estimated_continuous_book_bytes_per_day"] or 0)/1e9); print("verdict:",r["conclusion"]); [print(s,"trades:",p["improved"]["trades_count"],"net:",p["improved"]["total_net_pnl"],"funnel:",p["improved"]["signal_funnel"]) for s,p in r["pairs"].items()]' "$PILOT"
du -h "$PILOT" "${PILOT%.jsonl}.funding.jsonl" "$PILOT.replay.json"
df -h .
```

Quality показывает timestamps/futures identity, contract metadata, quantity/depth conversion, executable size, stale/gaps/cross-exchange coverage. Проверить также `venues` и `rejections` в JSON. Pilot funding/fees/mark gaps явно раскрываются; fee/funding stress scenarios в replay — assumptions, не exact costs. Primary net PnL считается по CLOSED trades, незакрытая exposure отмечается отдельно, не означает break-even.

Disk estimate прошлого короткого smoke ~0.54 GB/day пока предварительная; обновить по этому 24h pilot, отдельно прибавить funding/logs/outputs, минимум 3x raw и временный replay index. Quality/replay сортируют весь файл через дисковый SQLite index, запускать после остановки, не постоянным polling. Replay 5 pairs x scenarios может занять значительное CPU-время.

`--diagnostic` = exploratory split пилота, НЕ official untouched evaluation. Даже если pilot показывает прибыль, это не evidence edge и не выбор победителя. Не append/concat pilot в `research/data/development.jsonl` или `evaluation.jsonl`, не использовать как `--evaluation-input`. Если после pilot понадобится изменить параметры/даты/code, сначала новая версия и новый pre-evaluation seal. Frozen v1.1 здесь не перезаписывался; программа не запускалась.
