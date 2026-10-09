# Research stage ArbiNator: готовность и запуск

## Что зафиксировано

Активный протокол: `research/protocols/orderbook-v1.1.frozen.json`, version `orderbook-recovery-research-v1.1`. Внутри полный resolved strategy/config и SHA256 исходников strategy, feedback, market data normalization, execution, collector/replay/quality tools. `v1.frozen.json` сохранён как superseded: исправлен только хронологический обход validator, без просмотра evaluation. Для запуска использовать ТОЛЬКО v1.1. Применённых migrations/production изменений нет.

- Candidate universe: BTC, ETH, SOL, XRP, VELVET linear USDT perpetual; public sources MEXC/Binance/Bybit, где доступны, без spot substitution.
- Paper execution venue MEXC; независимый виртуальный счёт 10000 USDT для каждой пары, НЕ общий portfolio backtest.
- Margin cap/base 10 USDT, leverage 2, maximum requested notional 20 USDT; quantity только round DOWN, никогда не повышается ради exchange minimum. Risk budget 0.25% equity с более строгим margin cap; максимум 1 position, daily loss 25, total loss 100 USDT. Геометрического повышения нет.
- TP 10%, SL 5% от margin, instant entry и остальные существующие thresholds/feedback без изменений. Все поля зафиксированы в JSON; ML disabled, execution paper, live confirmation false, kill switch true.
- Development UTC `[2026-10-09 00:00, 2026-10-23 00:00)`, purge/gap сутки. Untouched evaluation `[2026-10-24 00:00, 2026-11-21 00:00)` — 28 суток. UTC ISO в протоколе намеренно naive, timezone явно UTC. В Ереване boundaries = 04:00.
- Никакой настройки/выбора пары на evaluation. Все пять кандидатов отчётны, победитель автоматически не выбирается. Изменения по development требуют НОВОЙ версии плана ДО evaluation; не переписывать v1.1. Если даты пропущены, согласовать новые будущие даты до сбора evaluation.
- Initial plan заморожен сейчас. Data seal ещё НЕ создан: его можно создать только после окончания development и до evaluation. Он фиксирует development book/funding hashes; resume-повторы того же funding settlement не меняют economic hash, конфликтующая ревизия вызывает отказ.

## Cost scenarios, не котировки биржи

Primary replay: taker 0.10% от notional на каждую сторону, latency 250 ms, фактические observed spread/depth. Funding только по settled rates + settlement mark, если есть. Это fee ASSUMPTION, не подтверждённый private tier. Missing funding/mark не является доказательством нулевого funding.

Заранее заданная sensitivity, одинаковая для baseline/improved:

| Сценарий | Taker % / side | Latency ms | Adverse funding % / 8h |
|---|---:|---:|---:|
| conservative_assumption | 0.10 | 250 | 0.10 |
| fee_latency_stress | 0.15 | 1000 | 0.10 |
| high_cost_stress | 0.20 | 2000 | 0.30 |

Funding scenarios моделируют отрицательную стоимость пропорционально holding time от entry notional для обеих сторон, ВМЕСТО public settlement cashflows. Это stress assumptions, НЕ реальные settlement schedules, НЕ гарантированные worst-case bounds. Latency сценарии реально переигрывают fills на последующей доступной книге; fees/funding могут изменить net outcomes и последующее поведение существующего feedback/risk. Strategy rules при этом не меняются.

## Проверка smoke

Основной short sample: 180 book rows, 15 venue/pair combinations, по 12 rows на combination, несколько коротких сессий и resume. Все 180 имеют futures identity/contract metadata/ordered positive depth, без fetch errors. На configured MEXC ticks в этой малой выборке fresh/cross-venue coverage 100%; это НЕ длительная availability гарантия.

Первые 120 rows получены до добавления explicit depth provenance fields: их единицы подтверждаются кодом CCXT, но raw quantity audit невозможен по одним rows. Последние 30 rows сохраняют first raw bid/ask, `depth_amount_unit=base`; проверено 30 conversions `base amount = raw amount * contractSize` ровно один раз. Промежуточные 30 имеют unit annotation без raw examples. Installed CCXT = 4.4.85; перед unattended run не обновлять зависимости. Python/CCXT dependencies не закрепляются hash guard автоматически — сохранять текущую venv/lock отдельно.

Реальный SIGTERM smoke: выход 0, 15 valid JSONL rows = 15 checkpoint rows; долгий процесс не оставлен. Dataset resume успешно append-ит, не перезаписывает, требует `--resume`; file lock предотвращает двух writers. При interrupted tail удаляется только последняя оборванная строка, внутреннее повреждение вызывает отказ.

Contract feasibility при notional <=20 USDT:

- MEXC BTC/SOL/XRP/VELVET проходят quantity minimum и depth; MEXC ETH в этой выборке не проходит `below_contract_min_amount`. ETH сохраняется в universe и источниках, но replay НЕ увеличит size. Это ограничение текущей цены/лотности/бюджета, НЕ оценка качества стратегии.
- Bybit BTC/ETH не проходят quantity minimum; Binance BTC quantity minimum, ETH min notional. Другие venues нужны для signal evidence, НЕ переключения execution venue.
- Непубликуемый в metadata explicit min notional показывается отдельно, а не приравнивается к нулю; известные quantity minimums соблюдаются.
- Public taker estimates есть, private account tier неизвестен. 84 funding rows в основном smoke, без fetch errors, но во ВСЕХ 84 отсутствует settlement mark price. Исторические mark prices/точные account fees ещё не собраны. Нельзя считать public текущий funding projection исполненным cashflow.

Из-за намеренных stop/resume пауз есть gaps до ~342 секунд: этот smoke НЕ соответствует frozen uninterrupted evaluation quality. Evaluation требует fresh configured coverage >=95%, >=2 fresh sources coverage >=90%, max configured gap <=30s, observed duration >=27.5 days и >=100 CLOSED trades на пару; 100 trades — только screening minimum, не доказательство значимости.

Smoke replay совместим: реальный service path создал paper fill VELVET; закрытых trades 0, BTC остался pending на границе. Нулевой realized net PnL не означает break-even/profitability: есть незакрытая/pending exposure. Отчёт включает funnel, rejection reasons, realized/mark-to-market DD, exposure seconds/fraction, notional-time, open boundary и cost scenarios. Mark-to-market DD использует observable top bid/ask и estimated exit fee, не гарантирует глубинную liquidation/partial-fill цену. Итог `inconclusive`, winning pair = null.

Артефакты: `research-v1-smoke.quality.json`, `research-v1-smoke.replay.json`, JSONL/funding/checkpoint и отдельный `research-v1-stop-smoke.jsonl`. Ни development, ни untouched evaluation ещё не собраны. Запуск protocol evaluation преждевременно не является future validation.

## Диск и эксплуатация

Основной JSONL 285745 bytes. Оценка continuous throughput по median per venue/pair intervals <=30s (длинные resume idle gaps исключены): ~0.54 GB/day, ~23 GB raw books за 42 дня. Это краткая экстраполяция, не capacity guarantee: минимум 3x raw reserve + funding/logs + временный SQLite sort index + outputs; рекомендуемый стартовый запас ~100 GB. При проверке локально доступно ~177 GiB, но это меняется. Обновить оценку после первых 24 часов.

REST capture с interval=2 означает паузу после цикла, НЕ 2s гарантированный SLA каждой пары. Clock sync, sleep, сеть, rate limits и disk pressure контролировать. Ограничения API не обходятся. Большой replay с 2 variants x 3 scenarios x 5 pairs CPU-intensive; не обещается realtime. Funding/trades пока не полностью streaming. Лаптоп должен не спать; предпочтительна выделенная НЕ production машина.

## Точные команды

Все команды выполнять в local backend, не на production. Ничего ниже не запущено автоматически. Collector standalone, без app import/API credentials/order methods. Development можно начать сейчас как дополнительный warmup, но метрики period начнутся только 9 октября UTC.

### 1. Unattended development collection

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
mkdir -p research/data research/logs research/run
export LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false

nohup env LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false .venv/bin/python -u -B scripts/collect_perpetual_books.py --output research/data/development.jsonl --duration 1296000 --until 2026-10-23T00:00:00Z --interval 2 --notional 20 --funding-interval 300 > research/logs/development.log 2>&1 &
echo $! > research/run/development.pid
```

Duration 15 days — только верхний предел; `--until` ограничивает development boundary, включая фильтрацию поздно полученных rows. На macOS при необходимости отдельно: `caffeinate -ims -w "$(cat research/run/development.pid)"` (держит эту terminal command до завершения collector). Не обновлять файлы/библиотеки, зафиксированные протоколом.

### 2. Progress / quality

```sh
ps -p "$(cat research/run/development.pid)" -o pid,etime,command
.venv/bin/python -m json.tool research/data/development.checkpoint.json
wc -l research/data/development.jsonl research/data/development.funding.jsonl
du -h research/data/development.jsonl research/data/development.funding.jsonl
df -h .
tail -n 20 research/logs/development.log

# Full quality scan — вручную, не частый polling миллионов rows.
.venv/bin/python -B scripts/research_quality.py --input research/data/development.jsonl --funding-input research/data/development.funding.jsonl --plan research/protocols/orderbook-v1.1.frozen.json --output research/data/development.quality.json
```

Log итоговый summary появляется при остановке; checkpoint обновляется после flush/fsync каждой полной scan batch. Во время initialization checkpoint может ещё отсутствовать. Для consistency full quality/replay лучше запускать ПОСЛЕ safe stop, не при частично записываемой строке. Не считать checkpoint timing SLA. Скрипт не является supervisor: failures/выход нужно проверять вручную или внешним process manager вне production.

### 3. Safe stop / resume

Сначала `ps` выше: command обязан содержать `collect_perpetual_books.py` и `research/data/development.jsonl`. Не отправлять signal чужому процессу при reused/stale PID.

```sh
kill -TERM "$(cat research/run/development.pid)"
# Дождаться завершения ps/log; текущий batch завершается и fsync-ится.
ps -p "$(cat research/run/development.pid)" -o pid,etime,command
```

Не применять `kill -9`, не удалять dataset/checkpoint/lock. После завершения прежнего writer, ДО development end:

```sh
nohup env LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false .venv/bin/python -u -B scripts/collect_perpetual_books.py --output research/data/development.jsonl --duration 1296000 --until 2026-10-23T00:00:00Z --interval 2 --notional 20 --funding-interval 300 --resume > research/logs/development-resume.log 2>&1 &
echo $! > research/run/development.pid
```

После cutoff resume development НЕ делать. Quality gaps не прятать и не заполнять spot/forward data.

### 4. Development replay / seal (23 октября, до 24 октября UTC)

```sh
.venv/bin/python -B scripts/replay_orderbooks.py --input research/data/development.jsonl --funding-input research/data/development.funding.jsonl --output research/data/development.replay.json --research-plan research/protocols/orderbook-v1.1.frozen.json --period development

.venv/bin/python -B scripts/replay_orderbooks.py --input research/data/development.jsonl --funding-input research/data/development.funding.jsonl --output /tmp/research-seal-unused.json --research-plan research/protocols/orderbook-v1.1.frozen.json --prepare-protocol research/protocols/orderbook-v1.1.sealed.json

chmod a-w research/data/development.jsonl research/data/development.funding.jsonl research/protocols/orderbook-v1.1.frozen.json research/protocols/orderbook-v1.1.sealed.json
```

Seal до окончания development/после начала evaluation запрещён. Даже недостаточную development выборку не выдавать за достаточную: при крупных gaps/неработающих metadata сначала согласовать НОВЫЙ future protocol, не подглядывать evaluation. Изменения параметров не допускаются с `--config` при plan/protocol; новые параметры только новой pre-evaluation версией, не текущим edit.

### 5. Untouched evaluation (24 октября UTC, только после seal)

```sh
nohup env LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false .venv/bin/python -u -B scripts/collect_perpetual_books.py --output research/data/evaluation.jsonl --duration 2419200 --until 2026-11-21T00:00:00Z --interval 2 --notional 20 --funding-interval 300 > research/logs/evaluation.log 2>&1 &
echo $! > research/run/evaluation.pid
```

Данные физически отдельно. Progress/stop те же команды с `evaluation` вместо `development`; resume та же evaluation-команда с `--resume`, НЕ писать в development файл. До cutoff проверять ТОЛЬКО capture integrity/availability; не читать outcomes, не выбирать параметры/пары. Не стартовать evaluation раньше 24 октября; сам collector не является scheduler. Ни cron, ни долгий процесс этой работой не создавались.

### 6. Evaluation report после 21 ноября UTC

```sh
.venv/bin/python -B scripts/replay_orderbooks.py --input research/data/development.jsonl --funding-input research/data/development.funding.jsonl --evaluation-input research/data/evaluation.jsonl --evaluation-funding-input research/data/evaluation.funding.jsonl --output research/data/untouched-evaluation.json --protocol research/protocols/orderbook-v1.1.sealed.json --period evaluation

.venv/bin/python -B -m pytest tests -q -p no:cacheprovider
```

Отчёт выдаёт все кандидаты и sensitivity, не выбирает победителя. Missing exact costs/insufficient trades/coverage/incomplete period/open boundary ведут к `inconclusive`. Даже прохождение screening означает только готовность statistical review, не profitability: нужны dependence-aware uncertainty, regimes и correction для multiple comparisons, не тюнинг на просмотренном test.

## Проверка реализации и ограничения

Backend 285 passed, 1 skipped. Новые tests проверяют UTC boundaries, ordered paper-only plan, independent development/evaluation files, event ordering, resume-invariant funding hash, conflict rejection, signal evidence verdict, exposure/cost scenario, graceful-stop flag; реальный public SIGTERM отдельно проверен. Frontend/application strategy/execution в этом этапе не изменялись, migrations не добавлялись.

Reproducibility guard проверяет локальные source hashes, а не remote dependencies/account state. Короткая выборка не покрывает funding settlement marks, trade outcomes, regimes или tail latency. REST не наблюдает queue/intervening events/полные partial fills. Exposure/DD в sample с незакрытой позицией не являются полноценной performance assessment. Никаких реальных orders/production изменений, никакого ML influence.

Определения contract size/min volume сверены с [официальной MEXC Contract Info](https://www.mexc.com/api-docs/futures/market-endpoints/get-contract-info) и [CCXT FAQ](https://docs.ccxt.com/docs/faq). Installed MEXC parser inspected: depth quantity не умножается на contractSize внутри `fetch_order_book`; collector делает одну явную conversion. Market metadata/public rates не подтверждают private fee tier.

**Текущий ответ о стратегии и парах: inconclusive. Готовы инструменты, протокол и команды; достаточных development/evaluation данных ещё нет.**
