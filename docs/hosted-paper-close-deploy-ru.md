# Paper Close / Start: проверка и deployment

Проверено 2026-10-10 в финальных репозиториях PycharmProjects/ArbiNator и
WebstormProjects/arbinator. Доступ к hosted instance не использовался.

## Подтверждённая локальная причина

POST /api/orderbook-recovery/positions/<id>/close-manual, body
`{"reason":"manual_close"}` вызывает close_manual -> close_trade ->
apply_recovery_after_close -> bounded_margin. В старом execution/decision config
нет risk_per_trade_percent. Получен AttributeError SimpleNamespace.risk_per_trade_percent.
Exit использовал старый snapshot для расчёта размера следующего входа.
Исправлено: fill/fees/latency берутся из сохранённых параметров сделки;
следующий risk sizing — из актуальной конфигурации. Pause сохраняется.
Повторный Close закрытой paper-сделки идемпотентно возвращает 200, без нового fill.
Полностью отсутствующие/некорректные execution snapshots не восстановлены автоматически:
их параметры нельзя угадывать; требуется отдельный разбор безопасного traceback.

## Start

UI вызывает POST /api/orderbook-recovery/start-paper только по сохранённому Paper.
Причины disabled: нет config, не Paper, отсутствуют exchange_id/trading_pair_id,
emergency_entry_block, unsaved form, pending Start request, state.enabled=true,
или config/state расходятся. UI показывает причины RU/EN.
Save PATCH /api/orderbook-recovery/config теперь делает post-write GET state
независимо от polling backoff. Ответы LOAD_STATUS до Save отбрасываются.
Включённые entry blocks не препятствуют защитному закрытию.

## Что собрать на hosted instance

В DevTools Network: URL/метод Close, UTC время, HTTP status, Response body.
Не отправлять HAR, cookies, Authorization, request headers или полный config/env.
Для Start достаточно execution_mode, exchange_id, trading_pair_id,
emergency_entry_block из config/state, enabled, stop_reason и показанной причины.

На сервере (команды выполняет владелец; здесь не запускались):

```sh
kubectl -n arbinator-prod get pods
kubectl -n arbinator-prod get deployments
# Подставить имя именно backend pod:
kubectl -n arbinator-prod logs <backend-pod> --all-containers --since=15m --timestamps
# Если контейнер перезапускался:
kubectl -n arbinator-prod logs <backend-pod> --previous --since=15m --timestamps
```

Перед передачей убрать credentials, DB URL, подписи, headers и посторонние payloads.
Нужен блок traceback вокруг Close: файл/строка/класс исключения, не весь лог.
Если ingress 500 не имеет backend traceback, проверить ingress logs за тот же UTC.

## Ручной push/redeploy

1. В обоих финальных репозиториях проверить git diff/status; не включать secrets,
   pycache и посторонние правки. Commit/push исправлений вместе с зависимыми
   ранее сделанными paper-session/worker изменениями. Один новый файл service
   без моделей/предыдущих миграций недостаточен.
2. На сервере получить нужные commits обоих repo. Собрать новые images с
   однозначными тегами; проверить, что deployment использует эти images.
3. Сохранить backup рабочей PostgreSQL штатным способом. Не удалять позиции,
   ExecutionSlot, историю, persistent volumes; не выполнять stamp/reset/drop.
4. Сохранить LIVE_TRADING_ENABLED=false и LIVE_TRADING_HARD_DISABLED=true.
5. Migration job должен использовать новый backend image и существующие DB secrets.
   Команды внутри этого image: `flask --app src db heads`, `flask --app src db upgrade head`,
   `flask --app src db current`. Ожидаемый единственный head o8f3a6c0d507.
   Новых миграций в данном исправлении нет. Не rollout при failed job.
6. Запустить штатный проверенный `/root/arbinator/deploy/redeploy.sh`, если он
   действительно существует и реализует шаги выше. Его hosted содержимое здесь
   недоступно; локальных Dockerfile/deploy/redeploy.sh в backend нет.
7. Проверить backend entrypoint: app.py запускает scanner/reconciliation и HTTP.
   Один `flask run`/WSGI HTTP без отдельного worker этого не делает. Snapshot store
   process-local: worker и обслуживающий runtime должны иметь согласованную архитектуру.
8. Обновить browser cache. GET state: прежний position id/history сохранены;
   при отсутствии свежего book выход unresolved, не fake closed. Повторять Close
   только после проверки, что это paper. Проверить 200, затем новое состояние.
9. Save валидной Paper config -> GET state должен совпасть -> Start доступен,
   если enabled=false и отсутствуют показанные gates. Emergency block снимать
   только отдельным осознанным действием пользователя, не для обхода ошибки.

## Проверки

305 backend tests passed, 1 skipped; 35 frontend tests passed; build passed.
Новая пустая отдельная PostgreSQL мигрирована до head. Playwright: 6 lifecycle
наблюдений, no page errors, frontend -> Flask -> PostgreSQL без API interception.
Synthetic book/history, не реальные биржевые наблюдения. Настоящий restart
тестового процесса сохранил pre-session short и pending close; отсутствие book
не породило fill; новый book позволил закрыть; повторный Close 200.
Никаких реальных ордеров. Тестовая БД не является рабочей/production.
