# Adaptive Book v1: paper-эксперимент

Отдельная opt-in версия `adaptive_book_v1`. По умолчанию `baseline`.
Никакие сохранённые TP/SL, данные, исторические сделки или frozen protocols не изменяются.
Для сравнения явно задано TP 1.8% / SL 0.9% маржи; это не изменение рабочего config.
API, Start и open guard запрещают experimental live. Новые поля конфигурации:
`strategy_version`, `experiment_settings`. При открытой позиции/slot менять их нельзя.

Вход использует существующие median imbalance, timestamp momentum, cross-exchange consensus,
feedback и bounded risk. Дополнительно требует >=2 valid futures venues, подтверждение
биржи исполнения, минимум 3 новых временных метки и 3 секунды устойчивости.
Повторная доставка книги не продлевает подтверждение. Противоречие, gap и рестарт
требуют нового прогрева. Pending-entry повторно проверяет эксперимент после latency.
Источник executed trade flow не подтверждён: явно unavailable, в решении не используется.

Порог затрат по умолчанию: gross TP >= 2 * бюджет roundtrip.
Бюджет: разница VWAP покупки/продажи на текущей глубине + комиссии обеих сторон
+ резерв funding 5 bps от notional за максимальный holding period. Спред отдельно
не прибавляется повторно. Это эвристический бюджет, не прогноз ожидаемой доходности.
Цена будущего закрытия неизвестна; funding reserve не является реальным начислением.
`net_target_after_cost_budget_usdt` — остаток бюджета, НЕ обещанный net PnL на TP.
Фактический paper PnL считает существующий executor по двум fills/fees и funding ledger;
резерв повторно из PnL не вычитается. При отсутствии settlements funding непроверен.

Hard TP/SL первичны. Дополнительные выходы: >=3 новых наблюдения ухудшения/разворота
за 3 секунды; максимум 120 секунд; optional trailing (% margin), default 0/off.
Нет выхода по одному шумному update. Max-hold защёлкивает close intent даже без depth;
без свежего post-delay стакана исполнения нет. Stop не расширяется, fees не блокируют
защитный выход. Параметры берутся из immutable execution config сделки.
Состояние мониторинга выходов сохраняется в decision JSON. Pause продолжает management.
Фиксированная latency, all-or-none depth; queue и partial fills не моделируются.

## Функциональная диагностика, НЕ performance evidence

`research/research-v1-smoke.jsonl`: 180 существующих строк public collector,
15 venue/pair combinations, checkpoint подтверждает 180 строк. Время receipt UTC:
2026-10-08 17:03:28.467 — 17:13:43.888. Это resume-smoke с паузами, не непрерывный рынок.
Происхождение соответствует существующим collector/checkpoint/RESEARCH_STAGE_RU.md;
независимой криптографической верификации исходных биржевых ответов нет.
Frozen development начинается 2026-10-09; файл вне окна. Протокол НЕ изменён.
Validation/evaluation не открывались, параметры по результатам не подбирались.

Воспроизведение только функциональной проверки в in-memory SQLite, HTTP/orders не нужны:

```sh
AUTO_RUN_MIGRATIONS=false LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false \
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/compare_adaptive_book_v1.py \
  --functional-smoke --development-input research/research-v1-smoke.jsonl \
  --output /tmp/adaptive-book-functional-new.json
```

На VELVET 12 evaluations: baseline зарезервировал один pending, затем TTL expired;
experimental отверг единственный проходящий сигнал как недостаточно устойчивый.
Остальное: no_consensus, spread_too_high, недостаточно valid venues.
Обе версии: 0 закрытых сделок, нет открытой позиции в конце; net PnL 0 не доказывает edge.
Проверены два заранее выбранных сценария fee/side 0.10% + 250ms и 0.15% + 1000ms,
funding adverse 0.10%/8h prorated (допущение, не settlement).
Нужен длительный development-only dataset с непрерывной глубиной, fills/fees assumptions,
funding settlement/mark и достаточным числом trades. Итог: **недостаточно доказательств**.

## Ручной deploy

Из фактических push repositories проверьте diff и коммитьте только перечисленные файлы,
не `__pycache__`, .env или данные. Backend:

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
git add src/OrderBookRecovery/AdaptiveBookV1.py src/OrderBookRecovery/OrderBookRecoveryModel.py \
  src/OrderBookRecovery/OrderBookRecoveryService.py tests/test_adaptive_book_v1.py \
  migrations/versions/q0b5c8e2f709_adaptive_book_paper.py scripts/compare_adaptive_book_v1.py \
  docs/adaptive-book-v1-ru.md
git commit -m "Add opt-in adaptive book v1 paper experiment"
git push
```

Frontend:

```sh
cd /Users/emilhambardzumyan/WebstormProjects/arbinator
git add src/utils/orderBookRecoveryConfig.js src/utils/dashboardPresentation.js \
  src/plugins/locale.js src/views/orderBookRecovery/v-order-book-recovery.vue tests/adaptiveBookConfig.test.js
git commit -m "Expose versioned paper experiment settings and cost diagnostics"
git push
```

На сервере после backup и проверки сохранённых safety flags:
`cd /root/arbinator && ./deploy/redeploy.sh`.
Migration Job должен выполнить `flask --app src db upgrade head` **до backend rollout**.
Новый единственный head: `q0b5c8e2f709`, parent `p9a4b7d1e608`; только 2 новые колонки.
`LIVE_TRADING_HARD_DISABLED=true`, `LIVE_TRADING_ENABLED=false` оставить включёнными.
После deploy проверить `db current`/`db heads`, GET config: baseline и прежние TP/SL.
Для эксперимента: Pause, нет открытой позиции/pending, новая paper session, выбрать
adaptive_book_v1, Review/Save; проверить сохранённые значения, затем явно Start paper.
Не переключать эксперимент на существующей позиции. Здесь hosted операции НЕ выполнялись.

Проверки: `PYTHONDONTWRITEBYTECODE=1 AUTO_RUN_MIGRATIONS=false .venv/bin/python -m pytest -q -p no:cacheprovider`,
frontend `npm test` и `npm run build`. Миграция проверяется только в отдельной тестовой БД.
