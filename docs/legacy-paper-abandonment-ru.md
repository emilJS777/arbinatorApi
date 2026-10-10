# Снятие legacy paper-позиции с учёта

Действие не закрывает сделку на рынке. Оно сохраняет запись как abandoned/unverified,
исключает её из позиции, equity, performance и feedback и не вычисляет новые fills,
цены, комиссии или PnL. Исторические поля остаются в БД; неподтверждённый PnL
в API показывается null. closed_at не заполняется: отдельный abandoned_at обозначает
момент административного действия, а не fill. Новая paper-сессия не создаётся автоматически.

Доступно только для явно execution_mode=paper, открытой позиции с ошибкой
legacy_paper_execution_config_review_required и без live evidence. Null/unknown mode,
order ids, raw exchange responses, client order ids, live fill prices/amount,
protection/funding evidence и конфликтующий frozen live mode запрещают действие.
Это намеренно консервативно: client order id сам по себе может быть неоднозначным.
При отказе нужна отдельная проверка evidence; нельзя удалять эти поля ради обхода.

Сначала Pause new entries. В диагностике открытой позиции появится кнопка
«Снять legacy paper-позицию с учёта #ID». Confirm явно показывает ID и необратимость.
UI требует свежего успешного state с abandon_allowed=true. Backend независимо
повторно проверяет условия под PostgreSQL config/trade locks. Повторное действие
идемпотентно. Сбрасываются только pending-поля этой записи и удаляется только slot
с совпадающим trade_id. Чужой slot сохраняется. Workers перечитывают terminal state
после получения того же config lock; abandoned запись больше не исполняется.
Настройки, emergency block, recovery state, TP 1.8% и SL 0.9% не изменяются.

Endpoint: POST /api/orderbook-recovery/positions/{id}/abandon-legacy-paper

```json
{"confirm_abandon":true,"position_id":123}
```

## Ручной push/redeploy

Финальные репозитории: /Users/emilhambardzumyan/PycharmProjects/ArbiNator и
/Users/emilhambardzumyan/WebstormProjects/arbinator. Добавить новые файлы в commit,
не добавлять __pycache__, env, secrets или файлы research protocols. Push обоих repo.
Перед выкладкой получить backup БД и Pause entries через текущий UI.

Новая аддитивная миграция p9a4b7d1e608 зависит от o8f3a6c0d507. Старые миграции не
менялись. Migration job должен использовать новый backend image и закончиться
успешно до rollout: новый ORM требует двух новых columns. Не запускать новый
backend со старой schema и не делать stamp/reset/drop. Существующий deploy script
на сервере в этой задаче не читался/не запускался. В его штатной migration job:

```sh
flask --app src db heads
flask --app src db upgrade head
flask --app src db current
python scripts/check_paper_lifecycle_schema.py
```

Ожидается единственный head/current p9a4b7d1e608 и compatible=true. Убедиться, что
все backend replicas имеют один новый imageID; пересобрать/развернуть frontend и
обновить cache браузера. LIVE_TRADING_ENABLED=false и LIVE_TRADING_HARD_DISABLED=true
сохранить. После redeploy проверить GET state, Pause и условия кнопки. Действие
пользователь выполняет сам с подтверждением ID. Затем проверить историю: abandoned
запись видна, pnl=null, closed_at=null, open_position отсутствует для этой записи,
slot снят только если принадлежал ей. Новая сессия — отдельное ручное действие.

## Проверки

```sh
.venv/bin/python -m pytest -q
# Только фиксированная isolated PostgreSQL после её миграции:
.venv/bin/python scripts/test_postgres_paper_abandonment.py
```

Frontend: npm test; npm run build. Hosted БД не мигрировалась, exchange requests
не выполнялись. Локальные tests не доказывают соответствие развёрнутой версии.
