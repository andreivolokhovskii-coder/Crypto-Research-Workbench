# Crypto Research Workbench — Архитектура и обучающий гайд

> Эталонный проект по Data Engineering: от сырых биржевых данных до аналитического дашборда за одну команду.

---

## Содержание

1. [Идея проекта](#1-идея-проекта)
2. [Технологический стек](#2-технологический-стек)
3. [Медальонная архитектура данных](#3-медальонная-архитектура-данных)
4. [Компоненты системы](#4-компоненты-системы)
5. [Потоки данных](#5-потоки-данных)
6. [Деплой за одну команду](#6-деплой-за-одну-команду)
7. [Оркестрация через Airflow](#7-оркестрация-через-airflow)
8. [Трансформации: dbt и Spark](#8-трансформации-dbt-и-spark)
9. [Аналитика: Superset](#9-аналитика-superset)
10. [Ключевые архитектурные решения](#10-ключевые-архитектурные-решения)

---

## 1. Идея проекта

### Что мы строим

Полноценная платформа сбора, хранения и анализа криптовалютных данных. Проект охватывает весь жизненный цикл данных:

```
Биржа Binance
    ↓
Сбор данных (REST + WebSocket)
    ↓
Хранение сырых данных (MinIO + ClickHouse)
    ↓
Стриминг (Kafka)
    ↓
Трансформации (dbt + Spark)
    ↓
Аналитика (Superset)
    ↓
Оркестрация (Airflow)
```

### Зачем это нужно

Большинство учебных проектов по data engineering показывают один-два инструмента в изоляции. Этот проект демонстрирует как все инструменты работают **вместе** в реальной архитектуре — именно так строятся production data platforms в компаниях.

### Что умеет система

- Скачать 30 дней истории по 5 символам (BTC/ETH/SOL/BNB/XRP) одной командой
- Стримить live-данные с биржи в режиме реального времени
- Автоматически обнаруживать аномалии: всплески объёма, аномальные свечи
- Вычислять метрики волатильности через Apache Spark
- Классифицировать рыночные режимы (trending / volatile / ranging)
- Отображать всё на интерактивном дашборде без ручной настройки

---

## 2. Технологический стек

### Обзор инструментов

| Слой | Инструмент | Версия | Назначение |
|------|-----------|--------|-----------|
| Аналитическая СУБД | ClickHouse | 23.8 LTS | Главное хранилище, колоночная БД |
| Объектное хранилище | MinIO | latest | S3-совместимое хранилище Parquet-файлов |
| Операционная БД | PostgreSQL | 15 | Метаданные Airflow |
| Очередь сообщений | Apache Kafka | 7.6 (KRaft) | Стриминг событий без ZooKeeper |
| Пакетная обработка | Apache Spark | 3.5.3 | Сложные вычисления, горизонтальное масштабирование |
| Трансформации | dbt | 1.7.1 | SQL-трансформации с тестами и lineage |
| Оркестрация | Apache Airflow | 2.8 | Расписание и мониторинг пайплайнов |
| Аналитика | Apache Superset | 3.0.3 | BI-дашборды |
| Notebooks | JupyterLab | latest | Исследовательский анализ |
| Контейнеризация | Docker Compose | v2 | Весь стек одной командой |

### Почему именно эти инструменты

**ClickHouse, а не PostgreSQL для аналитики**

PostgreSQL — строчная СУБД, оптимизированная под OLTP (много мелких транзакций).
ClickHouse — колоночная СУБД для OLAP (агрегации по большим таблицам).
На запросе `SELECT AVG(close) FROM silver_klines GROUP BY symbol, toDate(open_time)`
ClickHouse в 10–100× быстрее, потому что читает только нужные колонки с диска.

**MinIO, а не локальный диск**

MinIO реализует S3 API. В production данные хранятся в AWS S3 или GCS.
Используя MinIO локально, мы пишем код, который без изменений работает в облаке — просто меняем endpoint в `.env`.

**Kafka, а не прямая запись в ClickHouse**

Kafka даёт: буферизацию при пиковой нагрузке, несколько независимых консьюмеров,
гарантию доставки и возможность replay данных с любого offset.
При падении ClickHouse данные не теряются — они ждут в топике.

**dbt, а не сырой SQL**

dbt — это не просто SQL, это SQL с версионированием, тестами, документацией и dependency graph.
`dbt build` запускает трансформации в правильном порядке и автоматически проверяет качество данных.

**Spark, а не SQL в ClickHouse**

Расчёт rolling volatility на 90 днях минутных данных по 5 символам ClickHouse выполнит.
Но при росте до сотен символов и лет истории Spark горизонтально масштабируется:
добавь workers — задача выполнится быстрее. Также Spark незаменим для cross-symbol корреляций.

---

## 3. Медальонная архитектура данных

### Концепция

Medallion Architecture — стандарт индустрии для организации данных в хранилище.
Данные проходят три уровня очистки:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │
│  Raw Data   │    │  Cleaned    │    │  Analytics  │
│  Append-only│    │  Normalized │    │  Aggregated │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Bronze — сырые данные

Данные как есть, без изменений. Принцип: **никогда не удалять, никогда не изменять**.

```sql
-- bronze_klines: временные метки в миллисекундах (как приходят с биржи)
CREATE TABLE bronze_klines (
    ingested_at     DateTime DEFAULT now(),
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    open_time       Int64,           -- миллисекунды Unix timestamp
    open            Float64,
    ...
    _source_file    String           -- путь к Parquet-файлу в MinIO
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, interval, open_time);
```

Зачем хранить сырые данные? Если в трансформации была ошибка — можно перезапустить с нуля.
Bronze — источник правды, из которого можно восстановить весь pipeline.

### Silver — нормализованные данные

Базовая очистка: правильные типы, UTC timestamps, дедупликация.

```sql
-- silver_klines: DateTime вместо Int64, ReplacingMergeTree для идемпотентности
CREATE TABLE silver_klines (
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    open_time   DateTime,            -- нормализованный UTC DateTime
    close_time  DateTime,
    open        Float64,
    ...
) ENGINE = ReplacingMergeTree(ingested_at)  -- дедупликация по ключу
ORDER BY (exchange, symbol, interval, open_time);
```

**ReplacingMergeTree** — ключевой паттерн идемпотентности. При повторной загрузке тех же данных
дубли не накапливаются: движок оставляет строку с максимальным значением `ingested_at`.
Запрос с `FINAL` форсирует слияние на лету.

### Gold — аналитический слой

Агрегированные, обогащённые данные для BI. Здесь живут dbt-модели и Spark-джобы.

```
gold/
├── fact_candles         — свечи с производными метриками (price_change_pct, is_bullish)
├── dim_coin             — справочник монет из CoinGecko
├── dim_exchange         — справочник бирж
├── mart_volatility      — rolling volatility 7d/30d, ATR-14  ← dbt
├── mart_market_regime   — классификация рынка                ← Spark
└── mart_volume_profile  — профиль объёма
```

### Real-time слой

Отдельный слой для данных с очень низкой латентностью:

```sql
rt_latest_kline  -- текущая цена по каждому символу (обновляется на каждом тике)
rt_signals       -- торговые сигналы с TTL 7 дней (MergeTree + TTL)
```

---

## 4. Компоненты системы

### Ingestion: исторические данные

**Файл:** `ingestion/historical/klines_backfill.py`

Использует библиотеку `ccxt` — универсальный клиент для 100+ бирж.
Один и тот же код работает с Binance, Bybit, OKX, Coinbase.

```python
# Паттерн: пагинированная загрузка с защитой от дублей
exchange = ccxt.binance()
seen = set()
while start < end:
    candles = exchange.fetch_ohlcv(symbol, timeframe, since=start, limit=1000)
    fresh = [c for c in candles if c[0] not in seen]
    seen.update(c[0] for c in fresh)
    start = candles[-1][0] + 1  # следующая страница
```

Результат идёт в два места:
- **MinIO** — Parquet с Snappy-компрессией, партиционированный по дате. Долгосрочное хранение, исходник для replay
- **ClickHouse** — bronze + silver для немедленных запросов

### Ingestion: стриминг

**WebSocket Producer** (`ingestion/realtime/ws_producer.py`)

```
Binance WS → [Pydantic validation] → Kafka topic: klines.raw
```

Pydantic-валидация на входе: невалидные сообщения уходят в Dead Letter Queue (`klines.dlq`), а не теряются.
Exponential backoff при реконнекте: 1s, 2s, 4s, 8s... до 60s максимум.

**Kafka Consumer** (`ingestion/realtime/klines_consumer.py`)

Ключевая особенность: **at-least-once delivery** с ручным commit offset.

```
klines.raw → [batch: 50 candles ИЛИ 10 секунд, что наступит раньше]
    ├── rt_latest_kline  — каждый тик, включая незакрытые свечи
    ├── bronze_klines    — только закрытые свечи
    ├── silver_klines    — только закрытые свечи
    └── rt_signals       — если сработал детектор аномалий
         ↓
    commit offset — только ПОСЛЕ успешной записи в ClickHouse
```

Детекция сигналов в реальном времени (rolling window 60 свечей):
- **volume_spike**: z-score объёма > 2.5σ
- **large_candle**: размах свечи > 3× ATR-14

### Ingestion: метаданные (CoinGecko)

**Файл:** `ingestion/metadata/coingecko_dims.py`

Топ-100 монет с рейтингом, категориями, market cap. Обогащает `dim_coin`.
Запускается ежедневно через Airflow с rate limiting: 1.5 сек между запросами к бесплатному API.

---

## 5. Потоки данных

### Исторический пайплайн (batch)

```
make deploy
    │
    ├── klines_backfill.py
    │   5 символов × 30 дней × 1440 свечей/день ≈ 216k строк
    │   ├──▶ MinIO: s3://bronze/klines/binance/BTCUSDT/1m/date=2024-05-01/data.parquet
    │   └──▶ ClickHouse: bronze_klines + silver_klines
    │
    ├── coingecko_dims.py
    │   └──▶ ClickHouse: bronze_coin_metadata + silver_coin_metadata
    │
    ├── dbt build
    │   ├──▶ stg_klines      (view над silver_klines FINAL)
    │   ├──▶ fact_candles    (enriched candles с производными метриками)
    │   ├──▶ dim_coin        (latest snapshot per coin_id)
    │   └──▶ mart_volatility (rolling vol 7d/30d + ATR-14)
    │
    └── spark-submit volatility_batch.py
        └──▶ mart_market_regime (OHLCV + vol + regime: trending/volatile/ranging)
```

### Real-time пайплайн (streaming)

```
Binance WebSocket (5 символов, combined stream)
    │
    ▼
ws_producer.py
    │  {"symbol": "BTCUSDT", "close": 67123.5, "volume": 12.4, "is_closed": true}
    ▼
Kafka topic: klines.raw  (retention: 7 дней)
    │
    ▼
klines_consumer.py
    ├── rt_latest_kline  ──▶ Superset "Live Prices"
    ├── silver_klines    ──▶ база для следующего dbt run
    └── rt_signals       ──▶ Superset "Trading Signals"
```

### Ежедневный пайплайн (Airflow)

```
Каждые 6 часов (00:00, 06:00, 12:00, 18:00):
    [incremental_klines, metadata_refresh] ──▶ dbt_build

Каждые 6 часов, сдвиг +30 мин (00:30, 06:30, 12:30, 18:30):
    spark_volatility_batch ──▶ mart_market_regime

Ежедневно в 02:00:
    freshness_check ──▶ dbt_test ──▶ row_count_check
```

---

## 6. Деплой за одну команду

### setup.sh — генерация секретов

```bash
# Криптографически стойкие пароли — никаких дефолтных "admin/admin"
gen_pass()   { openssl rand -hex 24; }    # 48 hex-символов
gen_fernet() { python3 -c "import base64, os;
    print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }

# sed с разделителем | вместо / — base64 содержит слэши
sed -i "s|change_me_clickhouse_password|${CLICKHOUSE_PASSWORD}|g" .env
```

Результат: `.env` с уникальными паролями для каждого деплоя.
`.env` в `.gitignore` — секреты никогда не попадают в git.

### Makefile deploy target — полный сценарий

```makefile
deploy:
    @bash setup.sh                        # генерируем секреты
    DOCKER_BUILDKIT=0 docker compose up --build -d
    # DOCKER_BUILDKIT=0 обходит IPv6 DNS-проблему при сборке образов

    @until docker inspect workbench-clickhouse \
        --format='{{.State.Health.Status}}' | grep -q healthy; do sleep 2; done
    # ждём ClickHouse (healthcheck: SELECT 1 via HTTP /ping)

    @until docker inspect workbench-minio-init \
        --format='{{.State.Status}}' | grep -qE 'exited'; do sleep 2; done
    # ждём создания MinIO buckets (one-shot контейнер)

    docker compose run --rm app python ingestion/historical/klines_backfill.py
    docker compose run --rm app python ingestion/metadata/coingecko_dims.py || true
    docker compose run --rm dbt dbt deps && dbt build
    docker compose exec spark-master spark-submit volatility_batch.py || true
```

### Порядок запуска сервисов (depends_on)

```yaml
# Airflow webserver запускается только после успешной миграции БД
airflow-webserver:
  depends_on:
    postgres:
      condition: service_healthy              # ждёт pg_isready
    airflow-init:
      condition: service_completed_successfully  # ждёт exit 0
```

---

## 7. Оркестрация через Airflow

### Архитектура

**LocalExecutor** — все задачи выполняются на одной машине в отдельных процессах.
Для production с большой нагрузкой: CeleryExecutor (несколько workers) или KubernetesExecutor (pod per task).

### DAG: daily_pipeline — паттерн fan-in

```python
COMMON_ENV = {
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    # os.environ.get() читается при загрузке DAG — пароль из env контейнера
}

# Параллельные задачи, потом зависимая
[incremental_klines, metadata_refresh] >> dbt_build
```

Fan-in: две независимые задачи выполняются параллельно, dbt запускается только когда обе успешны.

### DAG: spark_batch — SparkSubmitOperator

```python
volatility_batch = SparkSubmitOperator(
    conn_id="spark_default",   # connection создан в airflow-init автоматически
    application="/app/spark_jobs/volatility_batch.py",
    packages="com.clickhouse:clickhouse-jdbc:0.6.5",
    conf={"spark.executor.memory": "1g"},
)
```

Offset расписания +30 минут гарантирует что dbt-пайплайн уже завершился.

### DAG: data_quality — последовательные проверки

```python
freshness_check >> dbt_test >> row_count_check
```

Если данные устарели (>2 часов без обновления) — не тратим время на dbt tests.

### Автосозданные ресурсы в airflow-init

Чтобы не требовать ручных действий в UI после деплоя:

```bash
# Создаём Spark connection — без него spark_batch DAG падает сразу
airflow connections add spark_default \
    --conn-type spark --conn-host "spark://spark-master" --conn-port 7077

# Заполняем Variables для совместимости с Jinja-шаблонами в DAGs
airflow variables set CLICKHOUSE_PASSWORD "${CLICKHOUSE_PASSWORD}"
airflow variables set MINIO_ROOT_PASSWORD  "${MINIO_ROOT_PASSWORD}"
```

---

## 8. Трансформации: dbt и Spark

### dbt: SQL-трансформации с тестами

dbt не выполняет вычисления — он генерирует и запускает SQL в ClickHouse.

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_klines.sql    -- view: тонкая обёртка над silver_klines FINAL
│   │   └── sources.yml       -- декларация источников (silver_klines)
│   └── marts/
│       ├── fact_candles.sql  -- обогащённые свечи (price_change_pct, is_bullish)
│       ├── mart_volatility.sql
│       └── schema.yml        -- тесты: not_null, accepted_values
├── macros/
└── profiles.yml              -- credentials через env_var()
```

Граф зависимостей строится из `{{ ref('stg_klines') }}`:

```
silver_klines
    └── stg_klines (view)
            ├── fact_candles (table)   ← dbt build запустит именно в этом порядке
            └── mart_volatility (table)
```

Тесты в `schema.yml` — часть `dbt build`:

```yaml
- name: is_bullish
  tests:
    - accepted_values:
        values: [0, 1]   # dbt сгенерирует SQL-проверку автоматически
```

### mart_volatility: rolling window в ClickHouse SQL

```sql
-- Annualized realized volatility — стандарт в quant finance
round(
    stddevSamp(log_return) over (
        partition by exchange, symbol
        order by trade_date
        rows between 6 preceding and current row  -- 7-дневное окно
    ) * sqrt(365), 6                              -- аннуализация (252 trading days / 365 calendar)
) as realized_vol_7d
```

`log(close / prev_close)` — логарифмическая доходность.
Лучше аппроксимирует нормальное распределение, чем простое `(close - prev) / prev`.

### Spark: market regime classification

```python
# Два критерия классификации режима рынка

vol_condition = F.col("vol_7d") > F.lit(1.5) * F.col("vol_30d")
# Волатильность выше нормы: 7-дневная > 1.5× от 30-дневной

trend_up   = (F.col("close") - F.col("sma_20")) / F.col("sma_20") > 0.02
trend_down = (F.col("sma_20") - F.col("close")) / F.col("sma_20") > 0.02
# Отклонение от SMA-20 больше 2%

regime = F.when(vol_condition, "volatile") \
          .when(trend_up,      "trending_up") \
          .when(trend_down,    "trending_down") \
          .otherwise(          "ranging")
```

### Почему dbt И Spark одновременно — разделение ответственности

| | dbt | Spark |
|---|---|---|
| Тип вычислений | SQL трансформации | Распределённые вычисления |
| Масштабирование | Вертикальное (ClickHouse) | Горизонтальное (добавить workers) |
| Идеально для | ETL, агрегации, joins | Сложные window functions на больших объёмах |
| Тестирование | Встроенные тесты | Unit tests на PySpark |
| Используем для | fact_candles, mart_volatility | mart_market_regime |

---

## 9. Аналитика: Superset

### Автонастройка при старте контейнера

Superset полностью настраивается через Python-скрипт в `docker/superset/entrypoint.sh`.
Нет ручных кликов в UI после деплоя:

```python
# Шаг 1: пересоздаём DB connection (пароль мог измениться при redeploy)
database = Database(
    database_name="ClickHouse",
    sqlalchemy_uri=f"clickhouse+http://{user}:{pw}@{host}:{port}/{db}"
)

# Шаг 2: создаём pre-aggregated view для обхода double-grain бага
_ch_exec("""
    CREATE OR REPLACE VIEW crypto.v_daily_klines AS
    SELECT exchange, symbol,
           toDate(open_time) AS trade_date,
           argMin(open, open_time) AS day_open,
           max(high) AS day_high, min(low) AS day_low,
           argMax(close, open_time) AS day_close,
           sum(volume) AS day_volume
    FROM crypto.silver_klines WHERE interval = '1m'
    GROUP BY exchange, symbol, trade_date
""")

# Шаг 3: регистрируем датасеты, синхронизируем колонки
# Шаг 4: создаём дашборд (версионирован — пересоздаётся только при смене DASHBOARD_V)
```

### Решение проблемы double-grain

ClickHouse в strict mode: все не-агрегированные колонки SELECT должны быть в GROUP BY.
Superset с `clickhouse-sqlalchemy` накладывал time grain дважды:

```sql
-- Проблема: двойное оборачивание
GROUP BY toStartOfDay(toDateTime(toStartOfDay(toDateTime(open_time))))
--        ↑ grain применён к уже обёрнутому алиасу open_time ↑

-- Решение: pre-aggregated view + time_grain_sqla=None
-- Данные уже на нужной гранулярности, grain-функция не применяется
SELECT trade_date, symbol, AVG(day_close)
FROM v_daily_klines
GROUP BY trade_date, symbol   -- валидный ClickHouse GROUP BY
```

### Дашборд: 6 чартов в 3 рядах

| Ряд | Чарт 1 | Чарт 2 | Источник данных |
|-----|--------|--------|----------------|
| 1 | Price History (линейный, все символы) | Volume History (барный) | v_daily_klines |
| 2 | Realized Volatility 7d | Market Regime (таблица) | mart_volatility, mart_market_regime |
| 3 | Live Prices (таблица) | Trading Signals (таблица) | rt_latest_kline, rt_signals |

---

## 10. Ключевые архитектурные решения

### Идемпотентность — любой шаг можно повторить

- `ReplacingMergeTree` дедуплицирует повторные вставки в ClickHouse
- `dbt build` пересоздаёт таблицы через `EXCHANGE TABLES` (atomic swap без даунтайма)
- `CREATE OR REPLACE VIEW` — безопасная перезапись view
- Superset entrypoint версионирует дашборд через `json_metadata.init_version`

### Безопасность секретов

```
.env.example  → в git (заглушки: change_me_clickhouse_password)
.env          → в .gitignore (никогда в git)
setup.sh      → генерирует уникальные пароли при каждом деплое

dbt profiles.yml: credentials через {{ env_var('CLICKHOUSE_PASSWORD') }}
DAGs:             credentials через os.environ.get('CLICKHOUSE_PASSWORD')
Docker Compose:   передаёт из .env через environment: ${CLICKHOUSE_PASSWORD}
```

### Graceful degradation — деплой не прерывается при сбое второстепенных шагов

```makefile
$(COMPOSE) run --rm app python coingecko_dims.py \
    || echo "[warn] metadata failed — dim_coin will be empty"
    # CoinGecko может быть недоступен — это не блокирует основной пайплайн

$(COMPOSE) exec spark-master spark-submit volatility_batch.py \
    || echo "[warn] Spark failed — run 'make spark-volatility' manually"
    # mart_market_regime пустой, но остальной дашборд работает
```

### Lambda Architecture: batch + streaming в одном слое

```
Batch (historical):   ccxt REST API → MinIO Parquet → silver_klines
Streaming (live):     Binance WS → Kafka → klines_consumer → silver_klines

Общий слой: silver_klines
    ↓
dbt + Spark читают из него — неважно, откуда пришли данные
```

Это классический паттерн Lambda Architecture. Batch даёт полноту и корректность данных.
Streaming даёт актуальность. `silver_klines` — точка схождения обоих потоков.

### Infrastructure as Code

Весь стек описан декларативно:

```
docker-compose.yml  — все сервисы, сети, volumes, healthchecks
Makefile            — все операции (deploy, reset, backfill, logs)
setup.sh            — генерация секретов
clickhouse/ddl/     — схема БД (применяется при первом старте)
dbt/                — трансформации с тестами
airflow/dags/       — расписание и оркестрация
```

Новый разработчик клонирует репо и запускает `make deploy` — всё поднимается само.

---

## Структура файлов проекта

```
Crypto-Research-Workbench/
│
├── docker-compose.yml           # главный оркестратор сервисов
├── Makefile                     # точки входа для всех операций
├── setup.sh                     # генерация секретов и .env
├── .env.example                 # шаблон переменных окружения
│
├── docker/
│   ├── app/Dockerfile           # Python ingestion/processing
│   ├── dbt/Dockerfile           # dbt-clickhouse
│   └── superset/
│       ├── Dockerfile           # Superset + clickhouse-sqlalchemy
│       └── entrypoint.sh        # автонастройка: DB, datasets, dashboard
│
├── ingestion/
│   ├── historical/
│   │   └── klines_backfill.py   # REST API → MinIO + ClickHouse
│   ├── realtime/
│   │   ├── ws_producer.py       # WebSocket → Kafka
│   │   └── klines_consumer.py   # Kafka → ClickHouse + signals
│   └── metadata/
│       └── coingecko_dims.py    # CoinGecko → dim_coin
│
├── dbt/
│   ├── models/
│   │   ├── staging/stg_klines.sql
│   │   └── marts/
│   │       ├── fact_candles.sql
│   │       ├── mart_volatility.sql
│   │       └── schema.yml       # тесты и документация
│   └── profiles.yml             # credentials через env_var()
│
├── spark_jobs/
│   └── volatility_batch.py      # rolling vol + market regime classification
│
├── airflow/dags/
│   ├── daily_pipeline.py        # каждые 6ч: backfill + metadata + dbt
│   ├── spark_batch.py           # каждые 6ч (+30мин): Spark vol job
│   ├── data_quality.py          # ежедневно: freshness + dbt tests
│   └── historical_backfill.py   # ручной запуск с параметрами
│
└── clickhouse/ddl/
    ├── 01_init.sql              # bronze + silver + gold схемы
    └── 02_streaming.sql         # rt_latest_kline + rt_signals
```

---

## Итог

Этот проект — не просто набор связанных инструментов. Это демонстрация **принципов**:

1. **Medallion Architecture** — bronze → silver → gold, каждый уровень чище предыдущего
2. **Идемпотентность** — любой шаг можно перезапустить без side effects
3. **Observability** — Airflow DAGs, dbt tests, data quality checks
4. **Infrastructure as Code** — весь стек в git, `make deploy` и готово
5. **Separation of concerns** — ingestion / transformation / serving / orchestration

Именно так выглядят data platforms в реальных компаниях.
Масштаб может быть другим, принципы — те же.
