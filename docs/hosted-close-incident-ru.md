# Повторный hosted Close 500: необходимые доказательства

Доступа к серверу в этой проверке не было. Причина текущего hosted 500 пока
не установлена. Ранее воспроизведённый missing risk_per_trade_percent не следует
автоматически считать причиной нового сбоя.

## Запрос и traceback

1. В DevTools -> Network сохранить только URL, method, UTC время, HTTP status и
   Response JSON для POST /api/orderbook-recovery/positions/<id>/close-manual.
   Body штатного запроса: {"reason":"manual_close"}. Не пересылать HAR, cookies,
   Authorization и полный config. Проверить по state, что позиция именно paper.
2. После этого исправления неожиданная ошибка возвращает msg=close_failed_internal,
   incident_id и position_id. Backend пишет incident_id + error_class + список
   traceback frames. Никакого сообщения исключения/SQL params в этой записи нет.
3. На сервере выполнить (POD выбрать по фактическому get pods, не угадывать):

```sh
kubectl -n arbinator-prod get pods
POD='<backend-pod>'
kubectl -n arbinator-prod logs "$POD" --since=15m --timestamps
kubectl -n arbinator-prod logs "$POD" --previous --since=15m --timestamps
kubectl -n arbinator-prod get pod "$POD" -o jsonpath='{range .status.containerStatuses[*]}{.name}{" "}{.imageID}{"\n"}{end}'
```

Последняя logs-команда нужна лишь при рестарте контейнера. Если несколько pod,
найти тот, который обработал запрос. Перед передачей вручную убрать секреты из
старых логов. Достаточно соответствующего incident_id/traceback, не всех логов.
Если response остался HTML/старого формата — проверить image/version и ingress;
это не доказательство, что новая версия обработала запрос.

## Проверка кода в image

В рабочем каталоге backend image получить SHA256 двух файлов, без импорта app:

```sh
kubectl -n arbinator-prod exec "$POD" -- python -c 'import hashlib,pathlib; files=["src/OrderBookRecovery/OrderBookRecoveryService.py","src/OrderBookRecovery/OrderBookRecoveryController.py"]; [(print(f,hashlib.sha256(pathlib.Path(f).read_bytes()).hexdigest())) for f in files]'
```

Если cwd image другой, сначала проверить `kubectl ... exec "$POD" -- pwd` и
использовать фактические пути. Сравнить с теми же файлами в push repo через
`shasum -a 256`. Git SHA на серверном checkout не доказывает содержимое image.

## Исправленные подтверждённые ошибки

- Full-form PATCH раньше блокировался просто при наличии market/mode полей.
  Теперь сравниваются значения после разрешения exchange/pair IDs; unchanged
  поля допустимы. Открытая позиция, execution slot и pending confirmation
  продолжают запрещать реальную смену рынка/режима.
- Emergency toggle не меняет fill/TP/SL открытой сделки. Включение отменяет
  pending paper entries под config row lock; выключение не запускает entries.
  Для запуска нужен отдельный Start. Защита от произвольных строк вместо boolean.
- Paper close, уже закоммиченный, не отвечает 500 из-за notification failure.
- Неожиданная ошибка Close явно rollback-ит текущую транзакцию. Ранее committed
  close intent остаётся. Уже committed close тоже нельзя откатить rollback-ом;
  всегда перечитать state/trade перед повтором. Не удалять position/history.

## Ручной redeploy

1. В /Users/emilhambardzumyan/PycharmProjects/ArbiNator проверить git diff и добавить
   только service, controller, новый regression test и этот документ. Не добавлять
   pycache/secrets. Commit/push в нужную ветку. Frontend в этом исправлении не менялся.
2. На сервере pull именно этого commit; rebuild backend image с новым тегом;
   применить его штатным deploy script. Проверить imageID и SHA256 файлов выше.
3. LIVE_TRADING_ENABLED=false, LIVE_TRADING_HARD_DISABLED=true оставить.
   Не удалять БД/volume/позицию/ExecutionSlot. Новых миграций нет; ожидаемый ранее
   проверенный head o8f3a6c0d507. Existing migration job должен успешно завершиться
   до rollout. Не менять применённые миграции и не использовать stamp/reset.
4. Проверить state: сохранены id/история. PATCH с неизменным рынком и toggle должен
   дать 200; enabled остаётся false после переключения. Смена symbol/mode при
   позиции/pending даёт 400. Paper Close без fresh book остаётся unresolved.
5. При 500 собрать incident_id и совпадающий trace; без него новый hosted root cause
   остаётся неизвестным. Полностью отсутствующий execution snapshot не достраивается
   автоматически из текущих настроек: нужна отдельная проверка сохранённых данных.
