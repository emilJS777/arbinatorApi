# Entry quality: проверка 2026-10-11

## Подтверждено

Read-only GET hosted state/config и non-archived JSON export вернули 200.
UI загрузился: PAPER, running, нет открытой позиции, session PnL около -0.19 USDT.
В runtime_diagnostics.build_revision нет commit SHA: совпадение deployed кода
с локальным commit не доказано. Никакие hosted настройки/данные не менялись.
Архивный export оказался большим (~38.8 MB) и оборвался по timeout: его
неполное содержимое не использовалось. V1 прочитан из ранее приложенного файла.

| Выборка | Сделки | Gross USDT | Fees USDT | Net USDT | Net / trade | Оборот обеих сторон |
|---|---:|---:|---:|---:|---:|---:|
| v1: 16 исполненных shorts | 16 | -0.0547000 | 0.4367997 | -0.4914997 | -0.0307187 | 436.7997 |
| v2: #4105-4108 | 4 | -0.0648000 | 0.1078344 | -0.1726344 | -0.0431586 | 107.8344 |
| v2: актуальный export #4105-4109 | 5 | -0.0576000 | 0.1346904 | -0.1922904 | -0.0384581 | 134.6904 |

Все сделки этих выборок net-negative. Просадка по закрытым сделкам равна
модулю соответствующего net total; это не внутрисделочная/MTM просадка.
V1: выход lost confirmation. V2: все пять experimental_max_hold;
удержание 121.95-125.10s. Порог 120s означает запрос выхода, а не гарантию
fill ровно на 120s: worker, fresh post-delay book и depth остаются обязательны.
Наблюдавшийся gross MFE каждой v2 сделки ниже её reported roundtrip fee.
Это редкие наблюдения во время позиции, не непрерывный путь и не post-exit данные.
Funding в этих итогах независимо не подтверждён.

## Пороги и горизонты

Hosted Long imbalance >3.0, Short <=0.65. При ratio bid/ask это соответственно
bid >3*ask и ask >=1.538*bid. Пороги НЕ симметричны. Зеркалом 3.0 было бы
1/3, но оснований менять пользовательский 0.65 по этим сделкам нет.
Проверки momentum имеют противоположные знаки; short branch достижим.
Код: src/OrderBookRecovery/SignalRules.py, AdaptiveBookV1.py, AdaptiveBookV2.py.

V2 persistence минимум 6s/4 новых книги, max hold 120s. Нет модели/доказательства,
что короткий сигнал предсказывает исполнимое движение на таком горизонте.
cost_hurdle сравнивает выбранный TP с same-book VWAP roundtrip costs +
heuristic funding reserve. Это бюджет, НЕ expected return. Executed trade flow
недоступен и не используется. TP 1.8%/SL 0.9% of margin не менялись.

## Одно реализованное изменение: измерение, не новая стратегия

Нет достаточных непрерывных development books для честного same-period replay.
Поэтому не создан v3 и не изменены baseline/v1/v2 или thresholds/max hold.
Добавлен PaperEntryMeasurement, вызываемый только для открытых paper v1/v2
после существующих futures/freshness/depth проверок. JSON в decision snapshot:

- horizons 15/30/60/120: первая наблюдаемая исполнимая exit quote в [h,h+5]s;
- gross, обе комиссии, net excluding funding; spread/depth уже в VWAP;
- observed net MFE/MAE, sample count, first positive net age;
- пропущенные окна missing; нет интерполяции или исторического backfill;
- duplicate/out-of-order/future timestamps не создают coverage;
- отсутствующие fees не заменяются нулём;
- это quotes, НЕ delayed fills, не predicted profit; только время открытой сделки.

Данные уже попадают в decision details/export через существующий JSON.
RU/EN блок деталей явно показывает отсутствие измерений у старых сделок.
Нет миграции, фонового collector, нового API, изменений ML или execution.
Выборка всё ещё selection-biased: отвергнутые сигналы и время после выхода
требуют existing public collection. Это минимальный шаг, не готовая валидация.

## Проверки из actual push repositories

Backend: 383 passed, 1 skipped; targeted lifecycle/measurement/v1/v2: 28 passed.
Frontend: 51 tests passed; npm run build passed (существующее large-chunk warning).
Migration head: q0b5c8e2f709, один head. Миграции/рабочая БД не применялись.
Hosted UI осмотрен read-only; новые изменения ещё НЕ deployed.

## Manual deploy

Backend /Users/emilhambardzumyan/PycharmProjects/ArbiNator:

```sh
git add src/OrderBookRecovery/PaperEntryMeasurement.py src/OrderBookRecovery/OrderBookRecoveryService.py tests/test_paper_entry_measurement.py docs/entry-quality-audit-2026-10-11.md
git diff --cached
git commit -m "Measure paper entry horizons without changing decisions"
git push
```

Frontend /Users/emilhambardzumyan/WebstormProjects/arbinator:

```sh
git add src/views/orderBookRecovery/v-order-book-recovery.vue src/plugins/locale.js tests/entryQualityPresentation.test.js
git diff --cached
git commit -m "Show paper entry measurements and missing coverage"
git push
```

Не включайте unrelated staged changes/pycache/secrets. На сервере вручную ваш
обычный `cd /root/arbinator && ./deploy/redeploy.sh`; live hard-disable сохранить.
Сопоставьте backend/frontend checkout commits с pushed commits и image versions:
build_revision=null сейчас не подтверждает версию API.
После redeploy в новых v1/v2 paper сделках проверить
decision_snapshot.entry_quality_measurement в decision-details и новый RU/EN блок.
Старые сделки не заполняются искусственно; новые данные могут отсутствовать,
если стратегия остановлена/нет позиции/нет свежей глубины. Не запускать автоматически.

Следующий исследовательский шаг: непрерывные futures depth/source timestamps,
contract metadata, fees/funding и post-exit путь на development-only периоде.
Запустить baseline/v1/v2 на одном потоке без подбора evaluation. Не сравнивать
два разных времени торгов как причинное доказательство улучшения. До этого
вывод: insufficient evidence; в проверенных выборках net отрицательный.
