# Проверка #4081 / #4083 / #4086 / #4092

Hosted DB не читалась. Runtime DB_CONNECTION_STRING в локальном shell отсутствует;
достоверный статус конкретных записей, session inclusion и feedback eligibility не подтверждены.
Присланный текст drawer не содержит timestamps, execution config и exit-time evidence.

Подтверждено кодом:
- `detail()` возвращает `"-"` для null. `v-if="detail('trade.abandoned_at')"` показывал
  abandonment даже у обычной сделки. Исправлен nullable fallback; реальные abandoned не переименовываются.
- `utcTime()` добавлял Z к Flask RFC/GMT datetime. Исправлено: Z только для naive ISO.
- Paper close включал exit fee в total_fee, но не заполнял live_exit_fee. Теперь отдельная
  комиссия сохраняется для НОВЫХ paper closes. История не переписывается. Старый прочерк
  не доказывает отсутствие комиссии: по присланным суммам total-entry = 0.013242,
  0.013923, 0.013865, 0.013786 USDT соответственно (арифметика, не DB-проверка).
- Metrics и feedback отбрасывают abandoned; фильтруют execution mode и paper_session_id.
  Feedback также требует closed, non-archived и result win/loss. Неактуальная сессия
  не означает abandonment. UI badge не влияет на серверные запросы учёта.

Совместные gross ~-0.0033, fees 0.109629, net ~-0.1129 USDT согласуются с текстом.
Это сумма четырёх выбранных примеров, не оценка expectancy/drawdown всей стратегии.
Разница 7/14 в decision snapshot и фактических margin/notional объяснима пересчётом
размера при fill/contract rounding, но требует execution_config и amount для подтверждения.
TP/SL считаются от фактической margin с frozen percentages, не от карточки intended size.

## Выход v1 без изменения правил

`AdaptiveBookV1.exit_decision`: weakness = consensus_side != position_side.
Поэтому direction=None (потеря подтверждения, в том числе нулевой momentum) может
вызвать выход, как и противоположное направление: default >=3 новых books и >=3s.
При недостаточной доступности данных service не вызывает этот сигнал; hard SL,
max-hold и ранее защёлкнутые protective exits остаются отдельными путями.
Без последовательности книг нельзя отличить шум, выход источников из valid pool
и реальный разворот. Entry snapshot этого не показывает.

Добавлен bounded history из 12 последних exit observations в decision_snapshot:
classification, time/source time, direction, reject reason и consensus features.
`unavailable` НЕ выдаётся за observed reversal. Исторические свидетельства не восстанавливаются.
Правила v1, baseline, TP/SL, delayed execution и frozen protocols не менялись.

## Сравнение: недостаточно данных

Доступный public smoke датирован 2026-10-08, frozen development начинается 2026-10-09.
Его нельзя использовать как development performance evidence; evaluation не открывался.
Net return, turnover, drawdown и adverse movement after exit для альтернативы: **не измерены**.
Сравнение отложено, а не заменено синтетической доходностью или четырьмя примерами.

Единственная кандидатная альтернатива для предварительной регистрации будущего paper v2:
entry persistence >=6s и >=4 новых books; soft exit только после >=6s / >=4 books
подтверждённого противоположного консенсуса. None/unavailable не считаются reversal;
разрыв данных сбрасывает подтверждение. Hard SL/max-hold неизменны, прибыль для выхода
НЕ требуется. Это непроверенная гипотеза hysteresis, не улучшение с доказанной доходностью;
она НЕ реализована/включена и не подбиралась по этим четырём сделкам.

## Нужен обезличенный hosted export

В браузере DevTools → Network → открыть View Details каждой сделки → запрос
`GET /api/orderbook-recovery/trades/4081/decision-details` (также 4083, 4086, 4092)
→ вкладка Response → скопировать JSON. Не копировать Request Headers, cookies,
Authorization или HAR. После deploy ответ содержит allowlisted execution_config.
Дополнительно из `/state`: только config.paper_session_id/execution_mode и runtime/version.
Нужны сохранённые exit observations/worker logs, books до входа и до +60s после выхода.
Если их нет, причинную реконструкцию и post-exit adverse movement подтвердить нельзя.

## Deploy

Из фактических backend/frontend репозиториев проверить diff, commit только task files,
push обоих. На сервере вручную `cd /root/arbinator && ./deploy/redeploy.sh`.
Новых миграций нет; head остаётся q0b5c8e2f709. LIVE_TRADING_HARD_DISABLED=true,
LIVE_TRADING_ENABLED=false оставить. Никаких backfill/update исторических сделок.
После reload проверить nullable abandoned_at, даты и новую exit fee у следующего
закрытия paper позиции. Не считать hosted исправленным до проверки JSON.
