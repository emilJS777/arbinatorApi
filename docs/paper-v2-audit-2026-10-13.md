# V2: pending revalidation и наблюдения, 2026-10-13

Источник: orderbook-recovery-trades-2026-10-13-17-44.json.
SHA256: 5e19d2fa142187afe05d4de0e78f58790a476a1e2e8864eff99acae7821504b0.
Hosted GET /state проверен read-only: 200, running, paper; build_revision=null.
Это не подтверждение deployed commit. Hosted данные/параметры не менялись.

## Факты

- 12 shorts: 1 win, 11 losses; 11 max-hold, 1 stop_loss.
- Gross -0.0434, fees 0.3255308, net -0.3689308 USDT.
- Sample net expectancy -0.03074423 USDT/trade; win rate 8.33%.
- Двусторонний turnover 325.5308 USDT. Drawdown по закрытым сделкам 0.3689308 USDT,
  не внутрисделочный/mark-to-market drawdown. Funding отдельно не подтверждён.
- 8 сделок не имели положительного observed net MFE; это sampled depth quotes,
  не непрерывная траектория и не утверждение, что прибыльный exit был невозможен.
- 26 отмен: 23 no_consensus, 2 expired, 1 adaptive_consensus_ratio_not_met.

## Pending

В actual service после fixed latency сигнал проверяется заново по свежему
post-delay стакану; затем feedback, profit/risk checks и contract/depth sizing.
No consensus при повторной проверке корректно отменяет заявку. Старый export
содержит entry decision, но не точную новую consensus matrix, поэтому приписывать
23 отмены конкретно spread, stale, momentum или median нельзя.
Теперь в decision JSON сохраняются pending_revalidation: source/request/evaluation
timestamps, expected/selected side, full bounded consensus, direction_rejections,
final_reject_reason. pending_cancellation отдельно сохраняет TTL/paused/reason/time.
TTL без повторной оценки не выдаётся за measured consensus failure.

## Горизонт 120s

Только 2/12 наблюдены, 10 unobserved. Один SL закрыт до 120s; это естественное
цензурирование. У 9 max-hold нет 120s observation. Во всех имеющихся observations
source_time - opened_at совпадает с observed_age_seconds и попадает в [h,h+5].
opened_at перезаписывается при реальном paper fill, не остаётся reservation time.

Подтверждён пробел кода: pending close обходил обычный observer. Max-hold использует
worker wall time, а measurement source time; в момент запроса source может быть
ещё младше 120s. Исправлено наблюдение в close flow после refresh и до latency
return, без изменения времени/правил выхода. Ошибка optional measurement не
блокирует close. Отсутствие свежей глубины по-прежнему не создаёт ни fill, ни label.
Теперь также сохраняется paper_entry_timing: request, actual fill и book source.
Исторические пропуски не заполняются искусственно.

## Минимальный bounded shadow

Для НОВЫХ cancelled pending / closed v1/v2 paper records создаётся
paper_shadow_prices в существующем decision JSON. Existing worker просматривает
до 20 последних закрытых paper records за последние 125s; row lock skip_locked
предотвращает потерю JSON обновления между worker/архивацией. Нет exchange requests.
Reference: первая реально наблюдённая exit-side depth quote в [event,event+5s].
Далее 15/30/60/120s окна, signed returns и MFE/MAE returns. Source/receipt/freshness,
future, duplicate/out-of-order и depth проверки сохраняются. Состояние переживает
restart; missed windows не интерполируются. Нет ledger/fees/fills/feedback mutation.

Это ЦЕНЫ, не counterfactual net PnL: комиссий/funding/гипотетических исполнений
этот shadow не создаёт. Ограничение: только зарезервированные pending кандидаты,
не все ранние rejected signals. При >20 закрытиях/125s часть кандидатов не покрыта;
при остановленном worker/недоступных books/restart после окна данные останутся missing.
Нужен continuous public collector для полного потока и реалистичного replay.
RU/EN inspector показывает эти блоки как раскрываемые diagnostics; export уже
включает их через decision_snapshot_json. Новых таблиц/миграций нет.

## Качество входа: вывод без подгонки

Long bid/ask >3.0 против Short <=0.65: short требует ask/bid >=1.538, а long bid/ask >3.
Это асимметрия сохранённых настроек, не основание молча менять их. Momentum sign
и cross-exchange guards остаются прежними. V2 использует >=6s/4 books persistence,
но predictive horizon до 120s не установлен. TP/cost hurdle — бюджет, не forecast.

Available research/*.jsonl — короткие public smoke 2026-10-08, до frozen development.
Экспорт сделок 11-13 октября находится в development, но не содержит непрерывного
стакана и rejected/post-exit путей. Same-data comparison: INCONCLUSIVE / insufficient
data. Новый кандидат не создан; baseline/v1/v2 неизменны. Evaluation не использовалась.
Следующий шаг: collect непрерывный development-only поток с metadata, depth, source
timestamps, fee/funding assumptions; replay обеих версий на одном потоке без подбора
по evaluation. До этого эти 12 исходов не дают подтверждённого edge.

## Проверки / ручной deploy

Actual backend: 388 passed, 1 skipped, изолированный SQLite в pytest. Не PostgreSQL
concurrency proof. Frontend: 52 passed, npm run build passed, прежний large-chunk warning.
Head q0b5c8e2f709; миграций нет, рабочая БД не тронута. Hard SL/latency/session tests passed.

Backend /Users/emilhambardzumyan/PycharmProjects/ArbiNator:

```sh
git add src/OrderBookRecovery/OrderBookRecoveryService.py src/OrderBookRecovery/PaperShadowMeasurement.py tests/test_paper_shadow_measurement.py docs/paper-v2-audit-2026-10-13.md
git diff --cached
git commit -m "Observe pending failures and paper exit horizons"
git push
```

Frontend /Users/emilhambardzumyan/WebstormProjects/arbinator:

```sh
git add src/views/orderBookRecovery/v-order-book-recovery.vue src/plugins/locale.js tests/pendingMeasurement.test.js
git diff --cached
git commit -m "Expose pending and shadow price evidence"
git push
```

Проверьте staged diff: не включайте unrelated изменения/pycache/secrets.
Затем ваш обычный ручной `cd /root/arbinator && ./deploy/redeploy.sh` на сервере.
Сверьте frontend/backend image commits: null build_revision сам по себе недостаточен.
Новые записи: проверить pending_revalidation/pending_cancellation, paper_entry_timing,
entry_quality_measurement.horizons.120, paper_shadow_prices в decision-details/export.
Нет гарантий нового label без fresh data или активного worker. Настройки, TP1.8%/SL0.9%,
live hard-disable и текущую сессию не менять ради этой диагностики. Не стартовать автоматически.
