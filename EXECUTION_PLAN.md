# EXECUTION_PLAN.md

# Crypto Research Workbench — Personal Execution Plan

> Это **моя персональная рабочая версия плана исполнения проекта**.
> 
> В отличие от `ROADMAP.md`, этот документ нужен не для публичного GitHub-описания, а для **ежедневной и еженедельной работы**:
> - что делать сейчас;
> - в каком порядке двигаться;
> - что считается завершением этапа;
> - что не трогать раньше времени.

---

## 1. Главная цель проекта

Собрать **self-hosted open-source crypto research workbench**, который:

1. будет сильным кейсом в резюме;
2. будет реально полезен мне после развертывания;
3. будет понятен и интересен разработчикам;
4. будет иметь понятную пользовательскую ценность;
5. будет выглядеть как продукт, а не просто как набор сервисов.

---

## 2. Мой принцип работы над проектом

Перед каждой новой задачей я проверяю 5 вопросов:

1. Это делает платформу полезнее?
2. Это ускоряет мой будущий ресерч?
3. Это улучшает воспроизводимость?
4. Это делает систему понятнее для других?
5. Это усиливает кейс в резюме?

Если на большинство вопросов ответ **«нет»**, задача не в приоритете.

---

## 3. Что для меня сейчас является главным deliverable

Мой целевой MVP = это не просто поднятый стек.

### MVP должен включать:
- локальный self-hosted стек, который запускается одной командой;
- historical market data backbone;
- bronze / silver / gold слой;
- ClickHouse serving layer;
- dbt models + tests;
- хотя бы 1 Airflow DAG;
- 10 saved research queries;
- 3 notebook workflows;
- 3–5 dashboard pages;
- 1 минимальный live streaming path;
- 2–3 realtime сигнала;
- понятную документацию.

---

## 4. Что НЕ делать слишком рано

Пока MVP не собран, не уходить в:

- слишком много бирж;
- слишком много сигналов;
- сложный UI;
- продвинутый auth / RBAC;
- «идеальную» продовость;
- избыточную оптимизацию того, чем еще никто не пользуется;
- фичи, которые не улучшают research experience.

---

## 5. Мой порядок исполнения проекта

Я иду по 8 блокам:

1. Смысл и рамки
2. Репозиторий и документация
3. Инфраструктура
4. Исторический data backbone
5. Аналитический слой и research layer
6. Оркестрация
7. Streaming и realtime value
8. Упаковка под open-source и резюме

---

# 6. Поэтапный план исполнения

# Этап 0 — Зафиксировать проект как продукт

## Цель
Перестать думать о проекте как о «наборе технологий» и закрепить его как **research workbench**.

## Что должно быть готово
- `PROJECT_SCOPE.md`
- `ROADMAP.md`
- `ARCHITECTURE.md`
- `DECISIONS.md`
- `README.md` skeleton
- `CONTRIBUTING.md`

## Definition of Done
- у проекта есть четкое название и позиционирование;
- есть понимание аудиторий;
- зафиксирован MVP;
- есть архитектурная логика;
- есть объяснение ключевых решений.

## Статус
- [x] Сделано

---

# Этап 1 — Собрать рабочий каркас репозитория

## Цель
Сделать репозиторий удобным для реальной разработки, а не только для описания идеи.

## Задачи
- [x] создать фактическую структуру каталогов;
- [x] добавить `.gitignore`;
- [x] добавить `LICENSE`;
- [x] добавить `.env.example`;
- [x] добавить `Makefile`;
- [x] добавить `docker-compose.yml`;
- [x] создать базовые папки:
  - [x] `docker/`
  - [x] `ingestion/historical/`
  - [x] `ingestion/realtime/`
  - [x] `ingestion/metadata/`
  - [x] `spark_jobs/`
  - [x] `dbt/`
  - [x] `airflow/dags/`
  - [x] `clickhouse/ddl/`
  - [x] `clickhouse/views/`
  - [x] `notebooks/`
  - [x] `queries/`
  - [x] `dashboards/`
  - [x] `tests/`
  - [x] `.github/workflows/`

## Definition of Done
- репозиторий выглядит как реальный инженерный проект;
- понятно, где какой тип логики должен лежать;
- документация и кодовая база не конфликтуют по структуре.

## Приоритет
**Очень высокий**

## Статус
- [x] Сделано

---

# Этап 2 — Поднять локальную инфраструктуру

## Цель
Получить reproducible local environment, который можно запускать и проверять одной командой.

## Сервисы MVP
- Kafka
- Spark
- MinIO
- ClickHouse
- Airflow
- PostgreSQL
- Superset

## Задачи
- [x] описать `docker-compose.yml`;
- [x] завести общую сеть;
- [x] настроить volumes;
- [x] прописать healthchecks;
- [x] задать переменные окружения;
- [x] унифицировать `TZ=UTC`;
- [x] избежать конфликтов портов;
- [x] добавить `make up` / `make down` / `make logs`.

## Проверка
После запуска должны быть доступны:
- [ ] ClickHouse
- [ ] MinIO
- [ ] Airflow
- [ ] Superset
- [ ] Kafka / streaming runtime

## Definition of Done
- одна команда поднимает стек;
- сервисы реально стартуют;
- есть диагностируемость через healthchecks и логи;
- базовая инфраструктура повторяемо разворачивается.

## Приоритет
**Очень высокий**

---

# Этап 3 — Historical Data Backbone

## Цель
Собрать историческую основу проекта, без которой все остальное бессмысленно.

## Что делаю первым
### Источники
- historical OHLCV / klines;
- coin metadata;
- exchange metadata (минимально).

### Слои
- Bronze — raw landed data;
- Silver — cleaned / typed / normalized;
- Gold — marts для serving и research.

## Задачи
- [ ] написать historical ingestion script;
- [ ] написать metadata ingestion script;
- [ ] сохранять raw в bronze;
- [ ] сделать silver-normalization;
- [ ] загрузить базовый gold в ClickHouse;
- [ ] проверить dedup / idempotency;
- [ ] выбрать начальный universe symbols для MVP.

## Минимальный стартовый scope
На MVP достаточно:
- 10–20 символов;
- 1–3 биржи максимум;
- 1m historical candles;
- базовый metadata layer.

## Что должно появиться
- [ ] `fact_candles`
- [ ] `dim_coin`
- [ ] `dim_exchange`
- [ ] `dim_time`

## Definition of Done
- я могу исторически анализировать рынок;
- данные лежат в правильных слоях;
- rerun не создает хаос и дубли;
- data backbone уже реально полезен.

## Приоритет
**Критически высокий**

---

# Этап 4 — dbt и аналитический слой

## Цель
Сделать gold-слой не кустарным, а инженерно оформленным.

## Задачи
- [ ] инициализировать dbt project;
- [ ] сделать staging models;
- [ ] сделать marts;
- [ ] добавить dbt tests;
- [ ] собрать docs / lineage.

## Обязательные модели
- [ ] `stg_klines`
- [ ] `stg_coins`
- [ ] `dim_coin`
- [ ] `dim_exchange`
- [ ] `dim_time`
- [ ] `fact_candles`
- [ ] `mart_volatility`
- [ ] `mart_volume_profile`
- [ ] `mart_market_regime`

## Обязательные тесты
- [ ] unique
- [ ] not_null
- [ ] relationships
- [ ] accepted_values
- [ ] business rule test для OHLC consistency

## Definition of Done
- `dbt build` проходит;
- gold-слой понятен;
- есть линия от сырых данных к marts;
- этот слой пригоден для dashboards и research workflows.

## Приоритет
**Очень высокий**

---

# Этап 5 — Research Layer, чтобы платформа была полезна мне самому

## Цель
Сделать систему не просто хранилищем, а рабочим исследовательским инструментом.

## Deliverables
### Saved SQL queries
- [ ] unusual volume scan
- [ ] realized volatility scan
- [ ] movers scan
- [ ] price-volume divergence scan
- [ ] intraday range anomalies
- [ ] active symbols scan
- [ ] exchange spread snapshot
- [ ] long-wick scan
- [ ] breakout candidate scan
- [ ] market regime summary

### Notebooks
- [ ] `daily_market_scan.ipynb`
- [ ] `asset_deep_dive.ipynb`
- [ ] `cross_exchange_comparison.ipynb`

### Watchlists
- [ ] majors
- [ ] high-volatility assets
- [ ] research candidates

## Главное правило
Каждый артефакт должен экономить мне время в реальном ресерче.

## Definition of Done
- я могу запускать типовые исследования без ручной сборки с нуля;
- platform outputs реально помогают формировать гипотезы;
- есть хотя бы 1–2 workflow, которыми я бы пользовался сам.

## Приоритет
**Очень высокий**

---

# Этап 6 — Dashboards и user-facing value

## Цель
Сделать платформу понятной и полезной не только мне, но и внешнему пользователю.

## Обязательные страницы
- [ ] Market Overview
- [ ] Asset Research Page
- [ ] Anomaly Feed
- [ ] Exchange Comparison
- [ ] Data Health / Freshness

## Что должна давать каждая страница
### Market Overview
Понять, что происходит на рынке сейчас.

### Asset Page
Понять, что происходит с конкретным активом.

### Anomaly Feed
Быстро видеть, что выбивается.

### Exchange Comparison
Находить расхождения и market structure insights.

### Data Health
Понимать, насколько можно доверять текущим данным.

## Definition of Done
- человек без знания всех таблиц может использовать платформу;
- dashboards отвечают на реальные вопросы;
- проект начинает выглядеть как продукт.

## Приоритет
**Высокий**

---

# Этап 7 — Orchestration

## Цель
Автоматизировать платформу как систему, а не запускать все руками.

## Обязательные DAGs
- [ ] `historical_backfill`
- [ ] `daily_incremental_update`
- [ ] `coin_metadata_refresh`
- [ ] `dbt_build`
- [ ] `data_quality_validation`

## Что важно
- retries;
- rerun-safe logic;
- понятные task boundaries;
- видимость ошибок.

## Definition of Done
- запуск пайплайнов происходит системно;
- есть хотя бы 1 полностью зеленый путь end-to-end;
- поддержка платформы становится проще.

## Приоритет
**Высокий**

---

# Этап 8 — Streaming и realtime value

## Цель
Добавить live-слой, который реально повышает полезность платформы.

## Что входит в MVP realtime
- [x] WebSocket producer
- [x] Kafka topic(s)
- [ ] Spark Structured Streaming consumer
- [x] Bronze/Silver streaming sink
- [ ] Gold realtime outputs

## Первые сигналы
- [x] volume spike
- [x] large trade detector
- [ ] accelerated activity
- [ ] exchange divergence
- [ ] freshness lag

## Что обязательно не забыть
- [x] checkpointing
- [ ] deduplication
- [ ] watermarking
- [ ] restart safety

## Definition of Done
- live данные доходят end-to-end;
- есть видимый user-facing результат;
- realtime не просто «существует», а реально помогает notice unusual behavior.

## Приоритет
**Средне-высокий**, но только после solid historical backbone

---

# Этап 9 — CI и инженерная зрелость

## Цель
Повысить надежность и воспринимаемую зрелость проекта.

## Задачи
- [ ] linting
- [ ] formatting
- [ ] unit tests
- [ ] dbt validation in CI
- [ ] docker build checks
- [ ] smoke checks

## Definition of Done
- проект не выглядит как хрупкий черновик;
- базовые regressions ловятся автоматически;
- контрибьютить безопаснее.

## Приоритет
**Средний**, но желательно не откладывать слишком надолго

---

# Этап 10 — Финальная упаковка проекта

## Цель
Сделать проект сильным кейсом для GitHub, портфолио и резюме.

## Deliverables
- [ ] архитектурная диаграмма;
- [ ] screenshots;
- [ ] polished README;
- [ ] consistent docs;
- [ ] example demo flow;
- [ ] resume bullets;
- [ ] short public case narrative.

## Definition of Done
- проект легко объяснить;
- ценность видна быстро;
- сильные инженерные решения вынесены наружу;
- это реально выглядит как сильный portfolio case.

## Приоритет
**Высокий ближе к финалу**, но не «когда-нибудь потом»

---

# 7. Мой фактический порядок выполнения (без лишней романтики)

Вот в каком порядке мне действительно стоит работать:

## Блок A — foundation
1. [x] создать структуру репозитория
2. [x] оформить `.env.example`
3. [x] сделать `docker-compose.yml`
4. [x] сделать `Makefile`
5. [ ] запустить инфраструктуру локально

## Блок B — historical spine
6. [ ] historical ingest
7. [ ] metadata ingest
8. [ ] bronze landing
9. [ ] silver normalization
10. [ ] gold loading into ClickHouse

## Блок C — modeling
11. [ ] dbt setup
12. [ ] staging models
13. [ ] marts
14. [ ] tests
15. [ ] lineage/docs

## Блок D — value for myself
16. [ ] 10 saved queries
17. [ ] 3 notebooks
18. [ ] watchlists

## Блок E — user-facing value
19. [ ] Market Overview
20. [ ] Asset Page
21. [ ] Anomaly Feed
22. [ ] Exchange Comparison
23. [ ] Data Health

## Блок F — orchestration
24. [ ] Airflow DAG for historical backfill
25. [ ] daily refresh DAG
26. [ ] dbt DAG
27. [ ] validation DAG

## Блок G — realtime
28. [ ] WebSocket producer
29. [ ] Kafka line
30. [ ] Spark streaming
31. [ ] real-time marts/signals
32. [ ] realtime dashboard integration

## Блок H — polish
33. [ ] CI
34. [ ] screenshots
35. [ ] architecture diagram
36. [ ] resume bullets
37. [ ] final cleanup

---

# 8. Что делать прямо сейчас

## Мой текущий ближайший фокус
Переходить от документов к фактической реализации.

### Следующие 5 практических шагов
- [x] создать реальный skeleton repo;
- [x] собрать `docker-compose.yml`;
- [x] подготовить `.env.example`;
- [x] сделать минимальный `Makefile`;
- [ ] проверить, что базовая инфраструктура поднимается.

### После этого сразу
- [ ] писать historical ingestion;
- [ ] делать bronze/silver/gold;
- [ ] подключать ClickHouse;
- [ ] запускать первый usable data flow.

---

# 9. Что считать неделей хорошего прогресса

Хорошая неделя = это не “я долго ковырялся”, а когда закрыт measurable output.

## Примеры хорошего weekly outcome
- инфраструктура реально стартует;
- есть working historical ingestion;
- есть загруженные candles в ClickHouse;
- есть первый dbt mart;
- есть первая research query;
- есть первый dashboard.

Если результат нельзя показать или проверить, значит прогресс слабый.

---

# 10. Что считать плохим отклонением

Это признаки, что я ушел не туда:

- слишком рано занят «красотой» вместо spine;
- добавляю много фич без usable MVP;
- усложняю realtime до того, как готов historical baseline;
- ухожу в редкие edge-cases без user value;
- строю сложную архитектуру, которой пока нечего обслуживать;
- не фиксирую done criteria.

---

# 11. Мой Definition of Done для MVP

MVP готов, если одновременно выполнены условия:

- [x] стек поднимается одной командой;
- [x] historical data загружаются и доступны для анализа;
- [x] bronze / silver / gold реализованы;
- [ ] gold models проходят базовые проверки;
- [x] есть ClickHouse serving layer;
- [x] есть dbt models + tests;
- [x] есть минимум 1 Airflow DAG;
- [x] есть 10 saved research queries;
- [x] есть 3 notebooks;
- [ ] есть хотя бы 3 dashboard pages;
- [x] есть хотя бы 1 live path;
- [x] есть 2–3 realtime сигнала;
- [ ] есть README + docs, достаточные для понимания проекта.

---

# 12. Мой Definition of Done для «проект реально удался»

Проект можно считать по-настоящему удавшимся, если:

1. Я реально могу использовать его для собственного ресерча;
2. Другой человек может понять, зачем он нужен;
3. Разработчик может относительно безболезненно в него войти;
4. Репозиторий выглядит как зрелый инженерный продукт;
5. Я могу уверенно показывать его как один из своих сильных кейсов.

---

# 13. Короткий operational summary для меня

Если забываю, на чем фокус:

## Сначала
**Запустить working local stack**

## Потом
**Сделать historical data backbone**

## Потом
**Сделать analytical marts и research workflows**

## Потом
**Сделать dashboards и orchestration**

## Потом
**Добавить realtime**

## Потом
**Дошлифовать open-source упаковку и портфолио**

---

# 14. Самая короткая версия плана в одной фразе

> Сначала я строю работающую локальную исследовательскую платформу с historical backbone и useful analytical outputs, потом добавляю orchestration и realtime, а затем довожу проект до уровня сильного open-source portfolio product.
