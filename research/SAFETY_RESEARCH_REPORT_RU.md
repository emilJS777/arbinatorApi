# ArbiNator: safety и подготовка исследования

## Границы работы

Только локальный ArbiNator, не FuturesML. Production, deployment и реальные ордера не затрагивались. Public-сбор работает без ключей. Replay принудительно использует отдельную in-memory SQLite, paper execution и отключённый ML. LiveExecutionService по умолчанию hard-disabled; команды ниже дополнительно задают оба запрета.

## Проверенные изменения

Финальная verification: backend `277 passed, 1 skipped`; frontend `17 passed`; `npm run build` успешен (существующее предупреждение о крупном bundle). Отдельный PostgreSQL runner: `11 passed`, результат в `research/postgres-execution-tests.json`. `flask db heads` и `current` тестовой PostgreSQL: единственный `n7e2f5b9c406 (head)`.

- PostgreSQL: миграции от пустой БД до единственного head `n7e2f5b9c406`. Новая additive migration после `m6d1e4a8b305`; ранее применённые revisions не изменялись. Добавлены мониторинг защиты, funding/legacy статусы и уникальный ledger funding-событий.
- 11 отдельных PostgreSQL/process/HTTP сценариев: конкурентные open/close, crash до commit, до submit и после принятого submit, crash после первого защитного плана, expiry/cancel защиты, quarantine legacy без доказательств, восстановление с оригинальным config/fill, signed funding без повторного учёта. Это фактические пути сервиса/ccxt signing/requests/DB через loopback HTTP-эмулятор, НЕ реальная MEXC и НЕ доказательство корректности аккаунта биржи.
- Неизвестный submit не повторяется вслепую. Durable execution slot остаётся заблокирован до read-only reconciliation. Сохранённые частичные TP/SL IDs не теряются при восстановлении.
- Guardian проверяет TP/SL каждые 5 секунд при работающем reconciliation loop: два различных plan IDs, контракт, close-side, объём, trigger prices, active state и expiry. `id` защитного плана не путается с `orderId` исполненного ордера. Отмена, expiry, missing/unconfirmed защита включают emergency entry block; автоматического повторного создания после неопределённого результата нет.
- Legacy trade без сохранённых исходных параметров/доказательства entry fill не управляется по сегодняшнему config: quarantine и block. При наличии evidence проверяются venue, symbol, side, leverage, amount, entry price и fee; исходные параметры фиксируются, новый entry order не отправляется.
- Funding: signed cashflow сохраняется с уникальным event ID, пагинацией и position scope. Ошибка/непонятный ответ не превращается в нулевой funding. Closed net PnL = verified gross - fees + signed funding. Settlement учитывает задержку; пока результат pending/unavailable, новые входы заблокированы. Win/loss state для нового live close применяется однократно после funding reconciliation, не по предварительному gross результату. Emergency block не снимается автоматически.
- Future/stale/incompatible snapshots отсекаются ДО записи momentum history. Paper entry/exit после latency требуют нового source timestamp, а не использования старой книги после ожидания.
- Paper quantity округляется вниз по contract size/amount precision; недостаточный min quantity/min notional отвергается, размер не увеличивается ради минималки. Strict replay требует contract metadata.
- Signal funnel считает evaluation, snapshot/risk/consensus/feedback/entry/confirmation/execution/fill этапы; отдельно сохраняет точные причины отказа для long/short. Никакие signal thresholds ради сделок не ослаблялись.
- Public collection: parallel venue workers с ccxt rate limiting, append/resume, exclusive file lock, fsync + checkpoint, восстановление только оборванной последней JSONL строки. Есть контрактные minimums/precision, source/receive timestamps, latency/depth и отдельный funding stream. Реальный smoke: 30 строк, затем resume ещё 15; итог 45 book rows, 69 funding rows. Это проверка сбора, не исследование доходности.
- Replay сортирует JSONL через временный дисковый индекс. Protocol фиксирует полные параметры, development data/funding hashes и исходники execution/signal/replay. Freeze отвергается, если evaluation уже присутствует; изменения config/code/development после freeze делают evaluation недействительной.

## Почему старый replay дал ноль

Старый 90-секундный файл даёт всего четыре evaluation ticks на configured venue для каждой пары в exploratory test-части. Полный результат: `research/signal-funnel-diagnostic.json`.

| Пара | Evaluation | Consensus passed | Execution rejected: contract_metadata_missing | Другие остановки |
|---|---:|---:|---:|---|
| BTC | 4 | 2 | 2 | no_consensus x2 |
| ETH | 4 | 2 | 2 | not_enough_valid_exchanges x1, no_consensus x1; anomaly в источнике |
| SOL | 4 | 1 | 1 | no_consensus x3 |
| XRP | 4 | 1 | 1 | no_consensus x3 |
| VELVET | 4 | 0 | 0 | not_enough_valid_exchanges x2, no_consensus x1, spread_too_high x1 |

Отделение причин:

1. Недостаточные данные: старый файл не содержит `precision_mode`; strict execution не может подтвердить lot sizing. Мало synchronized fresh sources и evaluation ticks. Settled funding/mark history отсутствует.
2. Config/рынок: реальные consensus, imbalance, momentum и spread проверки не проходят; это не автоматически bug. Нужна распределённая по времени статистика funnel, а не снижение порогов.
3. Найденные implementation defects: history могла загрязняться future/stale источниками; paper latency могла использовать ту же старую книгу. Исправлено и проверено regression tests. Контролируемый synthetic replay через настоящий service создаёт и закрывает сделки с depth/fees/funding: ноль в старой выборке не означает, что execution безусловно недостижим.

`baseline` сохранён как raw-price/snapshot-count momentum comparator при том же реалистичном execution. Это НЕ полная реконструкция старого production поведения. 70/30 diagnostic уже просмотрен и НЕ untouched evaluation.

## Пары: что действительно известно

Public smoke обнаружил USDT linear swaps BTC/ETH/SOL/XRP/VELVET на трёх venues без замены spot. Пример единичного последнего MEXC snapshot: spread BTC 0.000121%, ETH 0.000394%, SOL 0.008868%, XRP 0.007130%, VELVET 0.027461%. Это НЕ рейтинг пригодности: даже два наблюдения не описывают tails, volatility, regimes или executable slippage.

Наблюдаемый Binance BTC min notional = 50 USDT: continuous depth на 20 USDT не означает допустимый exchange order. MEXC amount minimum в smoke = один контракт; реальное base amount зависит от contractSize. Проверка minimums встроена в strict replay.

Недостаёт длительной синхронной L2 истории, settled funding + mark price в точке settlement, account-tier fees, распределения REST/submit/fill latency, partial fills/queue/intervening events, достаточного числа independent trades и regime coverage. Public funding rate является projection, пока не получено settled событие. Отсутствующий mark price не заменяется фиктивным нулём. До закрытия этих пробелов нет доказательства положительного ожидания ни одной пары.

## Оставшиеся риски

- Приватные read responses, plan expiry semantics и funding sign проверены документацией/эмулятором, но не реальным read-only аккаунтом в этой работе. У MEXC funding documentation пример противоречит описанию `resultList`; parser намеренно fail-closed. Данные аккаунта потребуют отдельной read-only проверки с разрешением пользователя.
- Guardian зависит от работающего scanner/reconciliation процесса. При остановке процесса локальный монитор не работает; exchange-side защита должна оставаться. Нет независимого watchdog/HA/OCO supervisor и безопасного автоматического renewal. Удаление/expiry защитных orders блокирует новые входы, но не может само гарантировать сохранность уже открытой позиции.
- Legacy quarantine требует ручной проверки исходного fill/config и ownership. Соседние ручные позиции/ордера на том же контракте нельзя считать принадлежащими боту без доказательства.
- Не проверен concurrent first-ever config creation. Доказанные PostgreSQL сценарии используют один уже созданный config; это не сертификат всех возможных DB races/HA paths.
- Funding finality после 30 секунд — conservative policy, не обещание биржи. Более поздняя корректировка уже reconciled записи потребует отдельной сверки statement; текущий цикл прекращает polling reconciled trades. Emergency block требует явного review перед снятием.
- Depth replay AON/observed-book, не queue/event simulator; REST пропускает промежуточные события, partial fills не моделируются полноценно. Slippage может менять фактическую margin после зарезервированного budget. Нельзя считать этот replay точной live симуляцией.
- Capture errors/coverage gaps и funding pagination limits фиксируются, а не обходятся. Длительные файлы требуют контроля диска и rotation. Replay funding rows и итог trades пока собираются в RAM; очень длинные периоды потребуют streaming этих небольших относительно L2 наборов.

## Точные следующие действия (не выполнены заранее)

Основные изменённые файлы: `OrderBookRecoveryService.py`, `LiveExecutionService.py`, `OrderBookRecoveryModel.py`, `SignalRules.py`, `CcxtService.py`; новые `PositionGuardian.py`, `PaperContractRules.py`, `SignalFunnel.py`, migration `n7e2f5b9c406_guardian_accounting.py`, `scripts/test_postgres_execution.py`, `tests/test_research_safety.py`. Обновлены public collector/replay и существующие execution tests. Frontend в этом этапе не изменялся.

Команды ниже для локального backend. Пример дат UTC необходимо выбрать ДО evaluation; это план, а не уже собранные недели.

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
export LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false
PY=.venv/bin/python

# Development: public books всех пяти пар + funding, без credentials/orders.
# Остановить до заранее выбранной evaluation boundary; 604800 = семь суток.
$PY -B scripts/collect_perpetual_books.py --output research/long-books.jsonl --duration 604800 --interval 2 --notional 20 --funding-interval 300
# После сбоя: та же команда с --resume; существующий файл НЕ перезаписывается.
$PY -B scripts/collect_perpetual_books.py --output research/long-books.jsonl --duration 86400 --interval 2 --notional 20 --funding-interval 300 --resume

# Development diagnostics/tuning ONLY. Не просматривать будущую evaluation.
$PY -B scripts/replay_orderbooks.py --input research/long-books.jsonl --funding-input research/long-books.funding.jsonl --output research/development-diagnostic.json --all-symbols --diagnostic

# Перед первым evaluation snapshot: freeze code/config/development books/funding.
# Пример boundary/end UTC, заменить на заранее согласованные даты.
$PY -B scripts/replay_orderbooks.py --input research/long-books.jsonl --funding-input research/long-books.funding.jsonl --output /tmp/protocol-unused.json --prepare-protocol research/frozen-protocol.json --development-end 2026-10-16T00:00:00 --evaluation-end 2026-10-23T00:00:00

# Только после freeze: продолжить сбор в тот же append-only dataset.
$PY -B scripts/collect_perpetual_books.py --output research/long-books.jsonl --duration 604800 --interval 2 --notional 20 --funding-interval 300 --resume

# После окончания evaluation: одно заранее описанное сравнение, без перенастройки.
$PY -B scripts/replay_orderbooks.py --input research/long-books.jsonl --funding-input research/long-books.funding.jsonl --output research/untouched-evaluation.json --protocol research/frozen-protocol.json --all-symbols --period evaluation

$PY -B -m pytest tests -q -p no:cacheprovider
.venv/bin/flask --app src db heads
```

Перед replay отчётности проверить полноту timestamp/lot/fees/funding inputs, fresh cross-venue coverage, отсутствие gaps и неполных labels. Если данных нет, сначала добрать, не интерпретировать отсутствующие costs как ноль. Замороженные файлы и конфигурацию сохранить до evaluation; не тюнить на evaluation после чтения результатов. Несколько недель — старт исследования, а не достаточное универсальное число: нужен заранее заданный sample-size/regime план, uncertainty intervals и sensitivity к costs/latency.

## Источники

- [MEXC Get Plan Order List](https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-plan-order-list): состояния, required time bounds, pagination, plan id vs executed order id, executeCycle hours.
- [MEXC Get Funding Fee Details](https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-funding-fee-details): position-scoped funding и pagination; пример response не соответствует описанной схеме.
- [MEXC funding rules](https://www.mexc.com/support/article/mexc-futures-funding-rate-305432020820705280): settlement и возможная задержка обработки.

Вывод: устранены проверенные safety/data-path ошибки и подготовлен воспроизводимый research pipeline. Прибыльность, подходящая пара и статистическое преимущество пока НЕ установлены. Live остаётся запрещён; production не изменялся.
