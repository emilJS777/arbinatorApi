# Общая проверка paper lifecycle и hosted incidents

Дата: 2026-10-10. Hosted Start/Close tracebacks в этой проверке недоступны.
Причина текущих hosted HTTP 500 остаётся неустановленной до сопоставления
Response incident_id с traceback именно запущенного backend image.

## Подтверждённые локальные ошибки и изменения

На persisted legacy short без execution snapshot вызов Start проходил commit,
затем state_payload -> trade_config выбрасывал LiveExecutionError.
Неполный snapshot с null paper_latency_ms тоже не мог использоваться для выхода.
Исправлено: state показывает legacy_paper_execution_config_review_required и
missing_fields; Pause и сохранение доступны; Close возвращает 409. Сохранённые
параметры сделки не достраиваются из текущего config. Позиция/история сохраняются.
JSON array/невалидный JSON больше не используется как dict.

Save/Start/Pause/Session сериализуют ответ до commit. Неожиданная ошибка ответа
откатывает переход. Post-commit notification и запуск ML worker логируются
отдельно; не превращают committed Start/Pause в failed HTTP.
Повторный Start работающей paper-сессии не создаёт второй run/session.
Повторный Close закрытой paper-сделки идемпотентен, включая legacy null mode.
Emergency block остаётся gate: Start при нём возвращает 400.
Его переключение не запускает entries; включение отменяет pending paper entries.
Guard Save сравнивает изменённые values, а не наличие market/mode в полном payload.

Start/Save/Pause/Close/Session и связанные config/state/trades/metrics/debug имеют
общий error wrapper. Неожиданный failure: HTTP 500, success=false,
obj.msg=<operation>_failed_internal, obj.incident_id. Лог содержит operation,
класс исключения, PostgreSQL SQLSTATE при наличии, traceback frames без SQL,
payload, locals, сообщений исключений или credentials. Validation: 400/409/404.
Worker query failures используют ту же безопасную incident-запись.
Rollback не отменяет уже committed close intent или fill: после любого failure
надо перечитать state/trade перед повтором. Нельзя удалять записи для исправления.

## Получение hosted доказательств

В DevTools Network для Start и Close отдельно сохранить только:
- UTC timestamp, URL, method, HTTP status;
- Response JSON, особенно incident_id;
- state.config.execution_mode, emergency_entry_block, enabled, position id/mode;
- сохранённые exchange_id/trading_pair_id и показанную причину блокировки Start.

Не отправлять HAR, cookies, Authorization, полный env/config, ключи или secrets.
Снимать Close только для подтверждённой paper-позиции. Для сравнения версии:

```sh
kubectl -n arbinator-prod get pods
POD='<фактический-backend-pod>'
kubectl -n arbinator-prod get pod "$POD" -o jsonpath='{range .status.containerStatuses[*]}{.name}{" "}{.imageID}{"\n"}{end}'
kubectl -n arbinator-prod logs "$POD" --since=15m --timestamps
# Только если контейнер перезапускался:
kubectl -n arbinator-prod logs "$POD" --previous --since=15m --timestamps
```

Перед передачей вырезать посторонние старые логи с возможными credentials.
Нужна запись operation=start/close с тем же incident_id. Если несколько pod —
проверить каждый, который обслуживал запрос. HTML без incident_id может приходить
от старого image или ingress; это требует проверки, а не догадки о причине.

## Schema/version и ручной redeploy

1. Проверить git diff в /Users/emilhambardzumyan/PycharmProjects/ArbiNator. Push
   service, controller, LifecycleSafety.py, ScannerService.py, новые regression tests
   и scripts вместе. Не включать pycache/secrets. Frontend API/payload не менялся.
2. Rebuild backend image с уникальным тегом; проверить deployed imageID. Checkout
   Git SHA на сервере не доказывает содержимое контейнера. Сверить SHA256 файлов:

```sh
# В local push repo:
shasum -a 256 src/OrderBookRecovery/OrderBookRecoveryService.py src/OrderBookRecovery/OrderBookRecoveryController.py src/OrderBookRecovery/LifecycleSafety.py
# В image с правильным working directory:
kubectl -n arbinator-prod exec "$POD" -- python -c 'import hashlib,pathlib; files=["src/OrderBookRecovery/OrderBookRecoveryService.py","src/OrderBookRecovery/OrderBookRecoveryController.py","src/OrderBookRecovery/LifecycleSafety.py"]; [print(f,hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest()) for f in files]'
```

3. Сохранить LIVE_TRADING_ENABLED=false, LIVE_TRADING_HARD_DISABLED=true. Не удалять
   позиции, ExecutionSlot, историю, volume или БД. Hosted БД здесь не изменялась.
4. Новых моделей/миграций в данном исправлении нет. Ранее проверенный единый head:
   o8f3a6c0d507. Backup и штатный migration job из нового image должны завершиться
   до rollout. Не выполнять stamp/reset. В среде migration job проверять:
   `flask --app src db heads`, `flask --app src db upgrade head`, `flask --app src db current`.
5. Для read-only сверки deployed DB schema с этим image:

```sh
kubectl -n arbinator-prod exec "$POD" -- python scripts/check_paper_lifecycle_schema.py
```

Скрипт не запускает миграции и не создаёт/изменяет records. Он проверяет head и
наличие всех model columns в config/recovery/run/trade/slot/session. compatible=true
не гарантирует полноту legacy execution snapshot — это отдельная runtime-проверка.
6. Проверить entrypoint: app.py запускает HTTP и scanner/reconciliation. Один WSGI
   HTTP process без отдельно организованного worker не обеспечивает paper management.
   Snapshots/heartbeat process-local; учитывать это при нескольких pod/processes.
7. Использовать существующий ручной deploy script после проверки этих условий.
   Hosted /root/arbinator/deploy/redeploy.sh здесь не читался и не запускался.
8. После rollout обновить browser cache. Save -> GET state -> Start; при emergency
   block ожидается 400. Legacy review status требует проверки сохранённых данных,
   а не автоматической подстановки config. Pause сохраняет management. Close без
   свежего book/depth остаётся unresolved. Повторный closed paper Close — 200.

## Локальная верификация

Полный migration chain применён только к arbinator_safety_test_lifecycle_shared
на Unix socket /tmp:55439. schema compatible=true, head o8f3a6c0d507.
scripts/test_postgres_shared_lifecycle.py проходит Start после Save, Pause, Close
без данных, worker fill по новому controlled book, repeated Close и новую session
в трёх отдельных процессах, сохраняя pre-session trade.
Playwright tests/ui-paper-lifecycle.mjs прошёл Vue -> Flask -> PostgreSQL: 6
наблюдений, errors=[], включая latency/depth, pending TTL/Pause, session isolation,
legacy/repeated Close. HTTP API не mock-ился; order books и история синтетические.
Это локальные доказательства корректности; hosted cause остаётся unresolved.

## Дополнение: сначала проверить GET /state

Сообщения Unknown / unavailable / waiting не доказывают причину HTTP 500.
Подтверждено по коду: при провале GET state store сохранял последний успешный
STATE, а open_position мог браться из metrics, если STATE не загрузился.
Теперь отдельно показываются HTTP status (0 = network error), last attempt,
last successful state, stale flag, incident_id и отсутствующие поля контракта.
При ошибке старые данные сохраняются, но явно не считаются текущим runtime.
`paper_exit_diagnostics=null` допустимо при отсутствии открытой paper позиции;
отсутствие самого ключа — другой случай. Новая версия backend возвращает
runtime_diagnostics с process_id, hostname, generated_at и state contract v2.
BUILD_REVISION показывается лишь если действительно задан в image/env; null
не является доказательством версии. Для версии обязательно сверять imageID/хэши.

Без Start/Close и без изменения данных собрать 5 ответов public GET state:

```sh
for i in 1 2 3 4 5; do
  curl --silent --show-error --max-time 15 \
    -w '\nHTTP=%{http_code}\n' \
    https://arbinator.api.deneon.net/api/orderbook-recovery/state
  sleep 2
done
```

Сохранить локально, перед отправкой оставить только success, incident_id,
obj.msg/code/incident_id, obj.config.execution_mode/enabled/exchange/symbol,
obj.enabled/status, obj.recovery_state.stop_reason, obj.open_position.id,
obj.open_position.execution_mode/paper_exit_status/paper_close_requested_at,
obj.paper_exit_diagnostics и obj.runtime_diagnostics. Не присылать cookies,
Authorization, полный HAR, env или credentials. Также сохранить статус/структуру
ответа Network именно GET state; Save/Start/Close — отдельные запросы.

Для каждой backend replica проверить imageID, схемный скрипт и хэши файлов,
не только одну случайно выбранную pod:

```sh
kubectl -n arbinator-prod get pods -o wide
kubectl -n arbinator-prod get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.imageID}{" "}{end}{"\n"}{end}'
# Подставить имя КАЖДОЙ backend pod, не frontend/database:
POD='<backend-pod>'
kubectl -n arbinator-prod exec "$POD" -- python scripts/check_paper_lifecycle_schema.py
kubectl -n arbinator-prod exec "$POD" -- python -c 'import hashlib,pathlib; names=["src/OrderBookRecovery/OrderBookRecoveryService.py","src/OrderBookRecovery/OrderBookRecoveryController.py","src/OrderBookRecovery/RuntimeDiagnostics.py"]; [(print(n,hashlib.sha256(pathlib.Path(n).read_bytes()).hexdigest())) for n in names]'
kubectl -n arbinator-prod logs "$POD" --all-containers --since=15m --timestamps
```

Логи могут содержать старые сообщения; перед отправкой очистить secrets и raw
responses. Искать incident_id конкретного запроса, operation=state/start/close,
position_worker, Futures snapshot rejected. Не присылать весь production log.

Подтверждён путь данных: app.py запускает scanner + HTTP threads одного процесса;
ScannerService.fetch_futures_snapshot записывает в FuturesSnapshotStore, затем
вызывает recovery hook. Этот store и worker heartbeat — память процесса, не БД
и не shared cache. Spot order_book dashboard использует отдельный store.
Reconciliation/paper management планируется раз в 5 секунд, независимо от
config.enabled, но только когда scanner cycle работает. Поэтому 250 ms — минимум
latency, не обещание fill через 250 ms. Futures task требует enabled exchange,
enabled trading pair и symbol, совпадающий с текущим config.symbol. Клиент swap
запрашивает book отдельно от spot; нужны source timestamp, linear USDT identity
и глубина. Наличие spot WS данных недостаточно.

Гипотезы, требующие hosted подтверждения: WSGI без scanner; worker/HTTP в разных
процессах или pods; mixed frontend/backend или backend replica versions;
отключённые pair/exchange; ошибка fetch futures; отсутствующий timestamp;
legacy position symbol отличается от текущего config.symbol. Проверить args/
entrypoint контейнера и enabled exchange/pair через UI, не выводить env/secrets.
Позиции и настройки не менять для диагностики. Новый state показывает exchange/
symbol открытой позиции, market type, resolved symbol, source/received timestamps
в exit diagnostics именно отвечающего процесса. Без этих hosted фактов ни одна
гипотеза не объявляется причиной 500. Shared snapshot transport здесь не менялся.
