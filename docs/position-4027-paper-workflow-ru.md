# Paper position #4027: Close 409 и безопасное снятие с учёта

## Что подтверждено на хосте

Read-only GET https://arbinator.api.deneon.net/api/orderbook-recovery/state вернул 200,
success=true, open_position.id=4027, execution_mode=paper, enabled=true.
paper_exit_diagnostics.exit_block_reason=legacy_paper_execution_config_review_required,
missing_fields=[paper_latency_ms, paper_taker_fee_percent]. Эти исторические параметры
исполнения отсутствуют; текущие настройки не являются их доказательством.

В этом hosted response нет abandon_allowed, accounting_status отсутствует,
build_revision=null. Локальный backend возвращает эти поля. Это подтверждает
несовпадение доступных возможностей; SHA/image/schema сервера пока не подтверждены.
Один ответ не исключает смешанные версии replicas. На хосте POST Close, Pause,
Abandon, миграции и SSH не выполнялись. Позиция на хосте пока НЕ снята с учёта.

Сам hosted POST response не получен. Чтобы окончательно сопоставить отказ, в
DevTools → Network выбрать ранее выполненный POST positions/4027/close-manual:
сохранить только UTC-время, URL, status и Response JSON. Нужны obj.code/msg,
obj.fields, obj.incident_id, если есть. Не передавать HAR, Authorization, cookies,
API keys, полный config или env. Для ожидаемого 409 traceback не требуется;
при неожиданном 500 нужен sanitized lifecycle log по incident_id.

## Реализованное поведение

Close с проверяемыми frozen execution parameters работает обычным delayed paper
исполнением: новый свежий futures book после задержки, достаточная глубина, реальные
симулированные fill/fees. При отсутствии этих данных позиция остаётся unresolved.
Missing execution config возвращает 409 с полями position_id, missing fields,
abandon_allowed, abandon_requires_pause и abandon_block_reason. Такой ответ
не меняет позицию и не создаёт фиктивный close request/fill.

На Positions & History рядом с заблокированным Close расположены отдельные кнопки
«Снять legacy paper-позицию с учёта #ID» и, когда требуется, «Приостановить новые
входы». UI использует свежий успешный state и серверный abandon_allowed=true.
Старая версия backend вызывает понятное предупреждение об обновлении. Ошибки
HTTP сохраняют code/fields/incident_id. После Close, Pause и Abandon state, metrics
и history обновляются сразу; более старый polling не возвращает прежнюю позицию.

Abandon требует explicit execution_mode=paper, отсутствия live evidence,
неполного исторического execution config, paused entries и подтверждения ID.
Неизвестный mode, order IDs, live fills/raw responses, protection/funding evidence
или неопределённый associated slot приводят к отказу. Свежие параметры не
подставляются вместо исторических. Сервер повторно проверяет условия под config
и trade locks, отменяет pending-поля только данной записи и освобождает только
ExecutionSlot с её trade_id. Чужой slot сохраняется.

Запись остаётся в БД: abandoned_at, abandonment_reason, result=abandoned,
live_status=paper_abandoned, accounting_status=abandoned_unverified. Исторические
поля сохранены. closed_at, exit_price, fill, fees и PnL не создаются; неподтверждённый
PnL в DTO равен null. Запись исключена из active positions, equity, performance,
win/loss и signal feedback. Повторный запрос идемпотентен, stale worker после
lock/refresh не может исполнить запись. Exchange API не вызываются.

Стратегия VELVET и значения TP 1.8% / SL 0.9% не менялись. Live не включался.

## Что изменено в push-репозиториях

Backend /Users/emilhambardzumyan/PycharmProjects/ArbiNator:
- src/OrderBookRecovery/OrderBookRecoveryService.py
- src/OrderBookRecovery/LifecycleSafety.py
- src/OrderBookRecovery/RuntimeDiagnostics.py
- tests/test_legacy_paper_abandon.py
- scripts/ui_paper_fixture_server.py (только isolated test fixture)
- docs/position-4027-paper-workflow-ru.md

Frontend /Users/emilhambardzumyan/WebstormProjects/arbinator:
- src/views/orderBookRecovery/v-order-book-recovery.vue
- src/utils/paperAbandonment.js
- src/store/modules/orderBookRecovery.js
- src/store/request.js
- src/plugins/locale.js
- tests/paperAbandonment.test.js
- tests/lifecycleErrorResponse.test.js
- tests/ui-paper-abandonment.mjs

Существующая migration p9a4b7d1e608 уже tracked, не менялась. Новая migration не нужна.
Не выкладывать новый ORM на schema без abandoned_at/abandonment_reason.

## Ручной push/redeploy

Сначала проверить diff; не добавлять pycache, env, secrets или чужие файлы.

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
git diff -- src/OrderBookRecovery scripts/ui_paper_fixture_server.py tests/test_legacy_paper_abandon.py
git add src/OrderBookRecovery/LifecycleSafety.py src/OrderBookRecovery/OrderBookRecoveryService.py src/OrderBookRecovery/RuntimeDiagnostics.py scripts/ui_paper_fixture_server.py tests/test_legacy_paper_abandon.py docs/position-4027-paper-workflow-ru.md
git commit -m "Make legacy paper close rejection actionable and verify abandonment"
git push

cd /Users/emilhambardzumyan/WebstormProjects/arbinator
git diff -- src tests
git add src/plugins/locale.js src/store/modules/orderBookRecovery.js src/store/request.js src/utils/paperAbandonment.js src/views/orderBookRecovery/v-order-book-recovery.vue tests/paperAbandonment.test.js tests/lifecycleErrorResponse.test.js tests/ui-paper-abandonment.mjs
git commit -m "Expose safe legacy paper abandonment beside blocked Close"
git push
```

Убедиться, что push включает предыдущий commit с моделями, PaperAbandonment,
route abandon-legacy-paper и migration p9a4b7d1e608. Отдельного Service.py недостаточно.

На сервере владелец выполняет штатную выкладку, предварительно сохранив backup БД:

```sh
cd /root/arbinator
./deploy/redeploy.sh
kubectl -n arbinator-prod get deployments
kubectl -n arbinator-prod get pods -o wide
kubectl -n arbinator-prod logs job/arbinator-migrate --timestamps
```

Hosted deploy script/Dockerfiles здесь не читались; использовать эту команду только
если штатный script действительно запускает migration job нового image до rollout
и останавливается при failed migration. Сохранить LIVE_TRADING_ENABLED=false и
LIVE_TRADING_HARD_DISABLED=true в deployment. Не reset/stamp/drop БД/позиции/slot.

В новом backend container (если deployment называется иначе, подставить его имя):

```sh
kubectl -n arbinator-prod exec deploy/arbinator-backend -- flask --app src db heads
kubectl -n arbinator-prod exec deploy/arbinator-backend -- flask --app src db current
kubectl -n arbinator-prod exec deploy/arbinator-backend -- python scripts/check_paper_lifecycle_schema.py
kubectl -n arbinator-prod logs deploy/arbinator-backend --since=15m --timestamps
```

Heads/current: единственный p9a4b7d1e608, compatible=true. Migration выполняется
штатной job (`flask --app src db upgrade head`), не вручную из каждого pod.
Проверить imageID всех backend replicas и новый frontend bundle, сделать hard refresh.
BUILD_REVISION желательно задавать SHA image commit; отсутствие SHA не заменяет
проверку imageID и источников. app.py запускает management/scanner; запуск только
HTTP без management worker не обеспечивает сопровождение. Snapshot store
process-local: WebSocket connected не доказывает наличие свежего execution book.

## Проверка #4027 после выкладки

1. GET state: прежние ID/история сохранены, runtime_diagnostics.paper_abandonment_supported=true.
   paper_exit_diagnostics содержит missing_fields и abandon_allowed.
2. Если abandon_allowed=false, посмотреть abandon_block_reason; не удалять evidence
   и не обходить проверку. Обновлённый UI должен объяснить отказ.
3. Если true: Positions → Pause new entries → кнопка «Снять legacy paper-позицию
   с учёта #4027» → подтвердить именно #4027. Это необратимое признание истории
   неподтверждённой, а не рыночное закрытие.
4. GET state: #4027 отсутствует среди активных; Trades/details сохраняют запись
   с abandoned_at, accounting_status=abandoned_unverified, pnl/net_pnl=null,
   closed_at/exit_price не сфабрикованы. Metrics/equity/feedback её не учитывают.
5. Повторное действие через API возвращает тот же abandoned_at. Новая paper session
   и новый Start — отдельные действия пользователя, ничего не запускается автоматически.

Endpoint при явном подтверждении владельцем:
POST /api/orderbook-recovery/positions/4027/abandon-legacy-paper
body {"position_id":4027,"confirm_abandon":true}.

## Проверки

Все проверки запускались из фактических push-repositories. Backend: 353 passed,
1 skipped; frontend: 43 passed; build passed (существующее предупреждение о больших chunks).
Миграции с нуля применены только к двум новым БД в отдельном /tmp PostgreSQL,
head/schema проверены. Конкурентные Abandon, stale worker и настоящий restart
протестированы на PostgreSQL. Полный UI paper lifecycle и UI Close 409 → Pause →
cancel confirmation → confirm Abandon → preserved history прошли через Flask +
PostgreSQL, без API interception. RU/EN и desktop/mobile проверены.

Данные книги/позиции в этих UI-тестах синтетические. Hosted #4027 не изменялась,
локальная репродукция не является доказательством её разрешения на сервере.
