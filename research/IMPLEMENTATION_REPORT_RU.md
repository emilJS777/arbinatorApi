# ArbiNator: execution safety и исследование order-book стратегии

Дата проверки: 8 октября 2026. Проект ArbiNator, не FuturesML.

## Границы работы

Изменены локальные backend/frontend. Production, Kubernetes, deploy-файлы и .env не изменялись. Бот не запускался; реальные private/order запросы не отправлялись. Сетевые запросы использовались только для публичных market data и документации. Backend-тесты запрещают немокированные HTTP-запросы.

Это этап hardening и подготовки исследования, а не разрешение на live и не заключение о прибыльности. Новая миграция включает emergency_entry_block=true для существующих конфигураций. Без явно заданной настройки LIVE_TRADING_HARD_DISABLED live-входы теперь запрещены по умолчанию.

## 1. Исполнение и жизненный цикл

- Submission acknowledgment больше не считается fill. После MEXC submit читаются данные ордера: статус, dealVol, dealAvgPrice, комиссия и валюта комиссии. Количество contracts переводится в base amount через contractSize.
- Отсутствующие fill/fee не заменяются текущей ценой или нулевой комиссией. Ордер остаётся unresolved, новые входы блокируются.
- ExecutionSlot резервирует единственный слот стратегии в БД до отправки. Уникальный client order ID сохраняется до сетевого запроса. После timeout/HTTP 5xx выполняется read-only reconciliation по externalOid, без автоматической повторной отправки.
- Полный конфиг исполнения сохраняется в execution_config_json. Закрытие, TP/SL и reconciliation используют параметры открытой сделки, а не изменённую форму настроек.
- Stop запрещает новые входы, но не отключает управление открытой позицией. Периодическая сверка с биржей вынесена в отдельную задачу scanner, в том числе без свежей книги.
- Stop-loss plan отправляется раньше take-profit. Каждый полученный plan ID сохраняется сразу; частичная ошибка не теряет уже созданный SL. Неподтверждённая защита устанавливает emergency block.
- Защитные планы отменяются только после подтверждённого полного закрытия. Запрос отмены исправлен на orders:[{symbol,orderId}]. Ошибки cleanup сохраняются как предупреждения.
- Если позиция закрыта вне приложения или MEXC отвечает 2009, проверяются orders/history по positionId. При невозможности подтвердить fill/PnL локальная позиция не превращается в вымышленную закрытую сделку по market price; сохраняется external_close_unreconciled.
- Net PnL использует подтверждённые комиссии входа/выхода; exchange profit применяется, когда доступен. Funding не сверяется и явно отмечается предупреждением.
- Неуспешная попытка входа хранится как rejected/open_failed, не участвует в win/loss и не считается открытой позицией.

### Документация MEXC

На дату проверки актуальный market order type = **5**, а не исторический type=6. Endpoint: POST https://api.mexc.com/api/v1/private/order/create; обязательный price присутствует, для market используется 0. Submit возвращает orderId/ts, не подтверждённый fill. [Place Order](https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order), [Change Log](https://www.mexc.com/api-docs/futures/update-log).

Контрактный volume округляется по contractSize/volUnit/volScale/minVol/maxVol; целое количество сериализуется как integer. Совпадение price=0 с поведением реального аккаунта не проверялось live-запросом и не считается доказанным только документацией.

Запрос отмены plan orders использует документированный список symbol/orderId. [Cancel Planned Orders](https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/cancel-planned-orders).

## 2. Рыночные данные

- Отдельный FuturesSnapshotStore: spot scanner/dashboard/arbitrage продолжают использовать прежний store.
- Для OrderBookRecovery собираются active linear USDT swaps. Spot и несовместимые инструменты не используются как замена.
- Contract book amounts переводятся в base amounts. Проверяются source timestamp, возраст, spread, корректность книги и аномальный imbalance.
- Momentum рассчитывается как относительное изменение mid-price по timestamp действительно новых данных, а не по повторному чтению одного snapshot. Окно ограничено по времени. Debug reload не создаёт искусственный momentum.
- Public futures fetch и strategy evaluation отделены от event loop; timeout не запускает второй одновременный запрос на том же клиенте, пока предыдущий worker ещё работает.
- Consensus-функция выделена для переиспользования; стратегия остаётся OrderBookRecovery, новые индикаторы или ML-влияние не добавлены.

## 3. Ограниченный риск вместо geometric recovery

- Loss никогда не увеличивает margin. current_step остаётся 0; multiplier не используется для sizing.
- Margin ограничен base margin, max_position_margin_usdt, equity, live_max_margin_usdt и денежным риском на SL с оценкой roundtrip fee.
- Risk budget = equity * risk_per_trade_percent / 100; дополнительно учитывается остаток daily/total loss budget.
- Defaults: risk_per_trade_percent=0.25, max_position_margin_usdt=10, max_leverage=2, max_consecutive_losses=3, emergency_entry_block=true.
- Только одна позиция/незавершённая попытка на стратегию; проверяется также существующая позиция выбранного контракта на бирже.
- После серии потерь применяется pause, без увеличения следующего размера. Emergency block не мешает закрытию/сверке уже открытой позиции.
- В Config UI добавлены risk budget, cap margin/leverage, consecutive-loss limit, emergency block, paper fees и latency. Старые payload keys сохранены для совместимости.

Стоп-лосс не гарантирует предельный фактический убыток: gap, slippage, недоступность API и liquidation остаются рисками. Дневные лимиты ограничивают новые входы, а не отменяют рыночные риски открытой позиции.

## 4. Публичное сравнение USDT perpetuals

Файлы: public-books.jsonl и public-books.comparison.json. Сбор около 90 секунд, размер диагностического исполнения 20 USDT, до 20 уровней книги, MEXC/Binance/Bybit. Для MEXC по 14 snapshots на пару. Это малая выборка для оценки friction, не статистическая проверка стратегии.

| MEXC pair | Median spread % | Минимальная двусторонняя глубина 20 уровней, USDT | Std наблюдаемых returns | Funding, доля / интервал | Median fresh venues |
|---|---:|---:|---:|---|---:|
| BTC | 0.000121 | 1 182 732 | 0.0002260 | 0.000017 / 8h | 3 |
| ETH | 0.000395 | 622 748 | 0.0003349 | -0.000010 / 8h | 3 |
| SOL | 0.008872 | 4 324 089 | 0.0002955 | 0.000040 / 8h | 2.5 |
| XRP | 0.007177 | 1 720 582 | 0.0003572 | -0.000007 / 8h | 2 |
| VELVET | 0.027189 | 9 046 | 0.0005920 | 0.000050 / 4h | 2 |

В наблюдавшихся книгах непрерывное количество на 20 USDT полностью помещалось в depth; дополнительный median depth slippage относительно best ask/bid был 0. Это **не** означает нулевой spread, отсутствие комиссии или гарантированное исполнение. Lot/min-notional feasibility отдельна: например, Binance BTC имеет min cost 50 USDT, поэтому 20 USDT не является допустимым ордером там. Contract precision/limits сохранены в сравнении.

В финальной выборке получены все 15 сочетаний биржа/пара, включая VELVET swap на Bybit: по 14 snapshots. Median cross-exchange price dispersion: BTC 0.00959%, ETH 0.02054%, SOL 0.01775%, XRP 0.02150%, VELVET 0.03397%. Минимальное число одновременно свежих книг в выборке было 1 для каждой пары: median coverage не гарантирует непрерывный consensus. Collector делает последовательные публичные запросы, поэтому измеренная coverage зависит также от его расписания; это не независимый тест скорости production scanner.

**Вывод по парам:** BTC/ETH разумно исследовать первыми из-за меньшего наблюдаемого spread и более широкой свежей coverage, не из-за предполагаемой доходности. VELVET имеет меньшую наблюдаемую глубину, более высокий spread и median fresh coverage 2 при трёх доступных биржах. SOL/XRP промежуточны по spread; сам по себе больший depth не доказывает более качественный signal.

Недостающие данные: многодневные синхронизированные книги с частотой достаточной для signal horizon, latency распределения, длительность stale episodes, fee tiers реального аккаунта, исторические funding cashflows, lot feasibility при конкретном sizing, mark/index/basis, стабильность depth и возможный spoofing. Наблюдаемый std не annualized volatility; funding только point-in-time. Public APIs не обходились при ограничениях.

## 5. Paper/replay и проверяемость

- Paper buy исполняется против asks, sell против bids; VWAP по доступной глубине, комиссии на обеих сторонах, задержка исполнения. При недостаточной глубине fill не выдумывается.
- Replay изолирован в SQLite memory; forcibly paper, ML disabled, hard-disable live. Свечи не используются.
- Первые 70% времени - warmup; последние 30% - отдельный chronological test. Параметры не подбирались на test.
- Baseline в replay восстанавливает только прежний raw-price momentum; не всю историческую production-реализацию. Обе версии имеют одинаковую cost/depth/latency модель и bounded risk.
- Для VELVET получены 4 test evaluations, **0 закрытых сделок в обеих версиях**, открытых позиций в конце нет. Expectancy/PF = null, а не доказанное нулевое или положительное ожидание.

**Статистическое преимущество не установлено.** Нельзя заключить ни прибыльность, ни отсутствие преимущества по этой короткой выборке. Инфраструктура позволяет продолжить запись книг и повторить честное сравнение на достаточной отдельной выборке.

## 6. Проверки и миграция

- Backend: 258 passed, 1 skipped в финальном полном прогоне непосредственно в локальном backend после переноса изменений.
- Frontend: 17 passed; build успешен. Существующее предупреждение Vite о большом bundle остаётся.
- Empty SQLite: вся цепочка upgrade head успешно применена; новый head m6d1e4a8b305 после l5c9e3a7b204. Старые migrations не изменены. Production/PostgreSQL миграция не выполнялась.
- Критические tests: unknown submit без повторного order; durable slot; реальный fill/fee; partial TP/SL; защита сохраняется при failed close; Stop управляет позицией; immutable config; bounded risk; timestamp momentum; incompatible/stale sources; paper latency/depth; внешнее закрытие по подтверждённой истории.

Перед применением к рабочей БД: backup, остановить новые входы, проверить отсутствие незавершённых live submissions, применить migration и сверить legacy positions. Не переключать emergency block/live во время этого этапа.

```bash
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false .venv/bin/flask --app src db upgrade head
.venv/bin/flask --app src db heads
.venv/bin/python -B -m pytest tests -q -p no:cacheprovider
cd /Users/emilhambardzumyan/WebstormProjects/arbinator
npm test
npm run build
```

Сбор и replay (только публичные книги, без запуска бота):

```bash
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
.venv/bin/python -B scripts/collect_perpetual_books.py --output research/new-books.jsonl --duration 3600 --interval 2 --notional 20
.venv/bin/python -B scripts/replay_orderbooks.py --input research/new-books.jsonl --output research/new-replay.json --symbol VELVET/USDT
```

## 7. Оставшиеся риски и следующие gates

1. Live не проверен реальными orders намеренно. Нужны PostgreSQL multi-process concurrency/crash tests и exchange sandbox/account-specific integration review до любого live допуска.
2. TP/SL IDs подтверждают принятие запроса, но пока нет полной непрерывной проверки активного состояния, expiry и sibling cancellation на бирже. Это не гарантированная OCO-защита; применять live сейчас нельзя считать безопасным.
3. Если fill известен, но fee неизвестна, или protection submission ambiguous, система блокирует новые входы и требует review. Автоматического blind retry нет, но требуется operational процедура разбора unresolved states.
4. Legacy trades без execution_config_json/client IDs не могут автоматически считаться безопасными: нужен отдельный импорт/сверка immutable параметров. Уже существующие live позиции нельзя просто забыть после deploy.
5. Полный reconciliation адаптер сделан для MEXC. Другие биржи используются как market-data sources; их generic execution не прошло равноценную проверку. Не включать их live автоматически.
6. Funding, liquidation fees и funding history не включены в итоговый PnL; marks, margin mode и фактический leverage требуют сверки с аккаунтом.
7. Paper/replay не моделирует очередь, все промежуточные изменения книги и биржевые частичные исполнения. При недостаточной depth pending paper fill ждёт, а не симулирует exchange IOC rejection; это ограничение модели.
8. Reconciliation task требует работающего scanner process; отдельного always-on position guardian сервиса пока нет. Несколько backend workers имеют раздельные market-data buffers; sticky ownership/leader coordination не реализованы.
9. Baseline/holdout исследование ещё не завершено: короткий сбор и отсутствие сделок не позволяют принять стратегию. Следующий этап - длительный public book capture, immutable train/test protocol, fees/funding/min-lot modeling и статистика на независимом периоде.

## Изменённые файлы

Backend: src/OrderBookRecovery/{LiveExecutionService,OrderBookRecoveryService,OrderBookRecoveryModel,OrderBookNormalizer,DepthExecution,FuturesSnapshotStore,SignalRules}.py; src/Scanner/ScannerService.py; src/Ccxt/CcxtService.py; migration m6d1e4a8b305_execution_safety.py; scripts/{collect_perpetual_books,replay_orderbooks}.py; tests/{conftest,test_execution_safety,test_mexc_live_diagnostics,test_orderbook_recovery_strategy}.py.

Frontend: src/utils/orderBookRecoveryConfig.js; src/views/orderBookRecovery/v-order-book-recovery.vue; tests/orderBookRecoveryConfig.test.js.

Артефакты исследования: этот отчёт, public-books.jsonl, public-books.comparison.json, replay-comparison.json.
