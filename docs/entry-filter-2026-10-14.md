# Один фиксированный entry-filter: unanimity

Гипотеза сформулирована перед расчётом: вход разрешён, только если все valid
exchanges подтверждают выбранную side в original pre-entry decision snapshot.
Обоснование: cross-exchange disagreement может сопровождать кратковременные
сигналы. Это гипотеза, не эмпирический прогноз прибыли. Никаких threshold searches.
Не используются MFE/MAE, выходы, последующие pending revalidation или shadow prices
для выбора retained trades. Outcomes используются исключительно для оценки.

## Источники / разделение

Три экспорта из Downloads: 2026-10-11-12-05, 2026-10-13-17-44, 2026-10-14-18-56.
140 уникальных IDs, повторов нет; 43 executed, 97 cancelled. Ранее прочитанные hosted
5 сделок не включены: их нет в этих трёх файлах, нельзя незаметно расширять sample.
V1: session a22584ca, config hash 8c87d599efd7, 16 executed, отдельно, не тестируем фильтр.
V2: session ac2b2d86, config hash 1f7f168120fe, 27 executed, два последовательных блока:
- ранний: 2026-10-11 12:30:28 — 2026-10-13 09:33:15 UTC, 12;
- поздний: 2026-10-13 15:14:42 — 2026-10-14 13:47:52 UTC, 15.
Границы соответствуют непересекающимся экспортам, не подбору выгодной даты.
Внутри каждой версии config отличается только updated_at; это поле исключено из
fingerprint, остальные поля учтены. Периоды development, не untouched evaluation.
Поздний блок уже был доступен при постановке гипотезы: не называем его blind OOS.

## Результат, USDT

| Блок / subset | N | Gross | Fees | Net | Net/trade | Turnover | Closed-trade DD |
|---|---:|---:|---:|---:|---:|---:|---:|
| ранний v2 |12|-0.043400|0.325531|-0.368931|-0.030744|325.5308|0.368931|
| retained |2|0.020900|0.054217|-0.033317|-0.016658|54.2165|0.033317|
| rejected |10|-0.064300|0.271314|-0.335614|-0.033561|271.3143|0.335614|
| поздний v2 |15|0.100900|0.408675|-0.307775|-0.020518|408.6747|0.355788|
| retained |5|-0.020900|0.136194|-0.157094|-0.031419|136.1939|0.157094|
| rejected |10|0.121800|0.272481|-0.150681|-0.015068|272.4808|0.210763|

Поздние retained IDs: 4170,4171,4182,4183,4189, все losses. Все 4 winners отклонены.
Ранние retained: 4129,4145, тоже losses. Поздняя expectancy хуже оригинала, а
сокращение total loss / turnover / DD связано с меньшим числом сделок. Нет edge.
Не пробовали второй фильтр или другой threshold после этого результата.

## Что эта проверка НЕ доказывает

Это retrospective filter исполненных сделок, не full strategy replay.
Использованы исходные paper fill prices (с уже включённым bid/ask/depth), обе
экспортированные комиссии и исходные задержки/выходы; spread повторно не вычитался.
Exact historical depth/latency не восстановлены и не независимо подтверждены.
Funding completeness не установлена. DD только по closed trades, не MTM.
Удаление сделок изменило бы equity/risk sizing, feedback, renewal и последующие
входы; неизвестные альтернативные сделки здесь не моделируются. Нет модели
частичных fills/очереди. Нельзя использовать observed MFE как достижимый exit.

Вывод: гипотеза НЕ поддержана, credible improvement не найден. Малый sample не
доказывает отсутствие любых преимуществ, но оснований создавать v3 нет.
Baseline/v1/v2, TP1.8%/SL0.9%, exits и safety не изменены. Frontend не менялся.

## Воспроизведение в actual backend

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
.venv/bin/python scripts/check_unanimous_entry.py \
 /Users/emilhambardzumyan/Downloads/orderbook-recovery-trades-2026-10-11-12-05.json \
 /Users/emilhambardzumyan/Downloads/orderbook-recovery-trades-2026-10-13-17-44.json \
 /Users/emilhambardzumyan/Downloads/orderbook-recovery-trades-2026-10-14-18-56.json \
 --output /tmp/unanimity-new-output.json
AUTO_RUN_MIGRATIONS=false LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false \
 PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider tests/test_unanimous_entry_check.py -q
```

Output не перезаписывается. Скрипт не импортирует trading app, не обращается к DB
или сети. Reject outside development, conflicting duplicates, invalid counts,
decision after fill. SHA256 источников и IDs retained/rejected сохраняются.

## Ограниченный следующий сбор, без запуска сейчас

Недостаёт непрерывных одновременных futures books MEXC/Bybit/Gate/HTX, точных
source/receipt timestamps, контрактных minimums/step sizes, fee tier, исторических
funding settlements/mark, full pending/entry funnel, gaps и post-exit path.
План: один 6-hour development pilot, без настройки порогов по результату; затем
проверить coverage до любого расширения. Existing collector собирает все 5 BASES,
а не только VELVET; анализ этого опыта ограничить VELVET. Нет выбора winning pair.

```sh
cd /Users/emilhambardzumyan/PycharmProjects/ArbiNator
LIVE_TRADING_HARD_DISABLED=true LIVE_TRADING_ENABLED=false AUTO_RUN_MIGRATIONS=false \
 caffeinate -i .venv/bin/python scripts/collect_perpetual_books.py \
 --output research/unanimity-development-pilot-2026-10-14.jsonl \
 --duration 21600 --interval 2 --notional 14 --funding-interval 300 \
 --exchanges mexc bybit gateio htx --until 2026-10-23T00:00:00Z
```

Команда начинает сразу; development end exclusive, 23 октября не включает.
Ctrl-C — безопасная остановка; --resume с тем же файлом возобновляет без потери
завершённых строк, duration задаёт новый бюджет запуска. Не продлевать pilot
за суммарные 6h автоматически. Unsupported markets/API restrictions записывать
как missing, не обходить. Проверить bytes/rows/checkpoint и source gaps.
Интервал 2s НЕ разрешает точный 250ms replay: нужна более частая depth history
либо честно маркированный coarse-next-book latency scenario. Funding snapshots
не заменяют подтверждённые settlement records. Даже полный pilot не валидация edge.
Frozen protocol/evaluation untouched. Ни collector, ни live сейчас не запущены.

Deploy не требуется: изменены только offline script/tests/report. При желании
включить их в ручной push: git add только scripts/check_unanimous_entry.py,
tests/test_unanimous_entry_check.py и этот report; review/commit/push. Нет миграций,
нового candidate или причин перезапускать hosted bot ради этого расчёта.
