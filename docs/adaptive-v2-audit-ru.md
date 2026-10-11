# Paper v2 и проверка loss streak

Экспорт 16 сделок не доступен во вложениях/локальной рабочей папке. Запрошен CSV/JSON.
По агрегатам пользователя: gross -0.0547, fees 0.4368, net -0.4915 USDT без иных начислений,
sample net expectancy -0.03071875 USDT/сделку, средняя комиссия 0.0273 USDT/сделку.
Это арифметика заявленных сумм, не независимый audit файла и не OOS результат.
16 completed shorts — count, НЕ денежный turnover. Turnover, drawdown и post-exit
adverse movement без хронологии/размеров/книг не измерены. Funding неизвестен.

## Enforcement

Исторический short_loss_streak и runtime consecutive_losses — разные величины.
Первый ограничен feedback lookback текущей paper session, второй сбрасывается при
max_recovery_pause. Side cooldown отсчитывается от последнего закрытого loss;
после истечения возможен следующий вход даже при streak=15. Start может снять runtime stop.
Следовательно число 15 само по себе не подтверждает обход защиты.
Regression: 15 short losses блокируют side внутри 600s, после cooldown пропускают
только при выполнении остальных adaptive checks. Три runtime losses при limit=3
останавливают entries. Эти проверки не доказывают hosted enforcement #4103.
Нужны frozen config, время входа/предыдущего закрытия, state/session, Start/Pause logs.
Теперь feedback snapshot сохраняет checked_side, evaluated_at, last_loss_closed_at,
cooldown_until, limit/duration и runtime count/limit. Правила feedback не менялись.

Подтверждено: v1 использует direction != position.side, поэтому neutral/None может
вызвать deterioration. Это дизайн v1, а не доказанный обход лимита. V1 оставлен неизменным.
Предыдущие исправления UI abandonment/date/exit-fee сохраняются. Новых подтверждённых
hosted accounting/enforcement bugs без экспорта нет.

## Один кандидат: adaptive_book_v2

Только paper, opt-in; baseline default, TP/SL неизменны.
Минимум entry persistence 6s и 4 distinct fresh books. Если configured persistence
больше 6s, используется большее значение; effective minimum показан в diagnostics/UI.
Neutral/lost confirmation не является reversal. Недоступные данные сбрасывают
подтверждение soft exit. Противоположный valid consensus закрывает после >=6s и >=4 книг.
Optional trailing также требует устойчивого подтверждения; hard SL/max-hold независимы
от signal и profitability. Держать позицию до прибыли не требуется.
После закрытия нужна устойчивая neutral/opposite фаза >=6s/4books, затем новый
полный entry persistence. После process restart эта фаза нужна повторно: fail closed.
Существующие cost hurdle, feedback/risk, contract/depth, latency, TTL, session accounting
используются без нового execution engine. Реальные orders не отправляются.
Новых DB колонок/миграций нет; версия хранится в существующем strategy_version,
параметры и exit monitor — в immutable config/decision JSON.

## Доказательства и следующий эксперимент

Доступные smoke books — 2026-10-08, до frozen development window. Повторный performance
тест на них не запускался. Validation/evaluation и frozen protocols не изменялись.
Сравнение v1/v2 **inconclusive / insufficient data**, ни одна версия не объявляется лучше.
Не оптимизировали по 16 исходам. Пока добавлены только тесты механики на fixtures.

Для будущей paper session сохраняются last 12 exit observations и observed_excursions:
gross MFE/MAE по реально наблюдавшимся исполнимым exit VWAP, sample count/source time.
Это не непрерывная траектория и не post-exit MFE/MAE. После закрытия история цен пока
не собирается этим монитором; для counterfactual exit нужен public collector.

Одинаковый development-only файл для обеих версий, без настройки evaluation:

```sh
AUTO_RUN_MIGRATIONS=false LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false \
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/compare_adaptive_book_v1.py \
 --development-input /path/to/development-only.jsonl --reference adaptive_book_v1 \
 --candidate adaptive_book_v2 --output /tmp/v1-v2-development-new.json
```

Допускается только UTC [2026-10-09,2026-10-23); output не перезаписывается.
Scenarios: fee/side .10% и latency250ms; .15% и1000ms; adverse funding .10%/8h prorated.
Это cost assumptions, не verified exchange fees/funding. VWAP учитывает spread/depth
без повторного вычитания. Для exact funding нужны settlement/mark records.

Ручной deploy: проверить diffs actual repos, commit только source/tests/docs (не pycache),
push backend/frontend, затем `cd /root/arbinator && ./deploy/redeploy.sh` на сервере.
Head остаётся q0b5c8e2f709. Live hard-disabled. Не переключать текущую открытую позицию;
после её штатного закрытия Pause → новая paper session → v2 → Review/Save → Start paper.
