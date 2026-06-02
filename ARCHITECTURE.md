# ARCHITECTURE.md

# Crypto Research Workbench — Architecture

## 1. Architecture Overview

**Crypto Research Workbench** is a self-hosted, local-first, modular platform for crypto market research.

The architecture is designed to support four main goals:

1. **Reliable historical market analysis**
2. **Useful real-time monitoring**
3. **Reusable research workflows**
4. **Extension-friendly open-source development**

This system is intentionally designed as a **product-oriented analytics platform**, not just a collection of infrastructure tools.

---

## 2. Architectural Principles

### 2.1 Product-first Architecture
The architecture exists to support meaningful research workflows and user-facing value, not only data movement.

### 2.2 Local-first and Self-hosted
The platform should run in a reproducible local environment without depending on managed cloud services for its core functionality.

### 2.3 Separation of Historical and Real-Time Workloads
Historical and real-time pipelines solve different problems and should remain deliberately separated:
- **historical candles / OHLCV** provide stable analytical depth;
- **live trades / tick events** provide immediacy and anomaly detection.

### 2.4 Medallion Data Design
The system uses a layered data approach:
- **Bronze** = raw landed data;
- **Silver** = cleaned and normalized datasets;
- **Gold** = analytics-ready marts and serving models.

### 2.5 Reproducibility and Idempotency
Each batch and streaming path should be safe to rerun without corrupting results or creating uncontrolled duplication.

### 2.6 Modularity
New exchanges, signals, transformations, marts, notebooks, and dashboards should be addable with minimal architectural disruption.

### 2.7 Observability
Freshness, failures, lag, and data health should be visible.

### 2.8 Simplicity Before Scale
This architecture is designed to be technically credible and extensible, while still practical for local development.

---

## 3. High-Level System View

The platform is composed of seven major layers:

1. **Source Layer**
2. **Ingestion Layer**
3. **Storage Layer**
4. **Processing Layer**
5. **Orchestration Layer**
6. **Serving Layer**
7. **Research & Extension Layer**

---

## 4. Layer-by-Layer Architecture

# 4.1 Source Layer

## Purpose
Provide raw inputs to the system.

## Data Categories
- historical market data
- live market data
- coin metadata
- exchange metadata
- optional future external enrichments

## Typical Inputs
- historical candle / OHLCV data
- live trades / ticks
- metadata for assets and exchanges

## Responsibilities
- provide raw content for ingestion;
- remain external to platform logic;
- be abstracted behind ingestion modules.

---

# 4.2 Ingestion Layer

## Purpose
Fetch, receive, and land external data into the platform.

## Subcomponents
### Historical Ingestion
Used for:
- large backfills,
- periodic historical refreshes,
- metadata snapshots.

### Real-Time Ingestion
Used for:
- live event subscription,
- streaming event production,
- market activity monitoring.

### Metadata Ingestion
Used for:
- coin reference data,
- exchange info,
- enrichment dimensions.

## Responsibilities
- read from external sources;
- persist raw payloads to bronze;
- apply minimal transport-level validation;
- preserve source-level fidelity;
- separate batch and stream entry points.

## Design Rules
- raw source payloads should be preserved where useful;
- ingestion should not perform heavy analytical modeling;
- data contracts should be explicit;
- failures should be observable and retryable.

---

# 4.3 Storage Layer

## Purpose
Provide durable, structured, multi-stage storage for analytics.

The storage layer is split into **Bronze**, **Silver**, and **Gold** zones.

---

## 4.3.1 Bronze Layer

### Purpose
Store raw landed data as close to source shape as practical.

### Typical Contents
- raw OHLCV files;
- raw trade event payloads;
- raw metadata extracts;
- ingestion timestamps;
- source metadata.

### Characteristics
- append-oriented;
- low transformation;
- traceable to source;
- useful for reprocessing and debugging.

### Storage Form
Recommended as:
- object storage based files,
- partitioned raw datasets,
- schema-tolerant formats when practical.

---

## 4.3.2 Silver Layer

### Purpose
Create cleaned, typed, normalized datasets.

### Typical Operations
- column typing;
- timestamp normalization;
- symbol normalization;
- exchange normalization;
- deduplication;
- light quality controls.

### Characteristics
- more stable than bronze;
- structured for consistent downstream use;
- still fairly close to source semantics.

### Example Silver Outputs
- normalized candles
- normalized trades
- normalized coin metadata
- normalized exchange reference tables

---

## 4.3.3 Gold Layer

### Purpose
Provide analytics-ready, user-facing, query-friendly models.

### Typical Contents
- fact tables
- dimension tables
- derived marts
- anomaly outputs
- serving views for dashboards and notebooks

### Characteristics
- stable business-facing semantics;
- optimized for queries and dashboards;
- designed for repeated usage.

### Example Gold Models
- `fact_candles`
- `fact_trades`
- `dim_coin`
- `dim_exchange`
- `dim_time`
- `mart_volatility`
- `mart_volume_profile`
- `mart_market_regime`
- `mart_anomalies`
- `mart_exchange_spread`

---

# 4.4 Processing Layer

## Purpose
Transform raw landed data into analytics-ready assets.

## Main Processing Modes
### Batch Processing
Used for:
- historical backfill;
- incremental historical updates;
- analytical recomputation;
- dimensional modeling.

### Streaming Processing
Used for:
- near real-time event handling;
- live deduplication;
- rolling windows;
- anomaly detection;
- low-latency serving updates.

## Main Processing Responsibilities
- convert bronze to silver;
- derive gold marts;
- perform quality-oriented normalization;
- enforce model-level business logic;
- generate reusable analytical outputs.

## Processing Boundaries
- ingestion moves data into platform;
- processing turns platform data into reliable structured models;
- serving exposes processed outputs to users.

---

# 4.5 Orchestration Layer

## Purpose
Coordinate pipelines, schedules, dependencies, reruns, and validation.

## Responsibilities
- start batch ingestion;
- trigger transformations;
- run validations;
- coordinate model builds;
- manage refresh cadence;
- surface task failures.

## Workload Types
### Scheduled Workloads
- daily updates
- metadata refreshes
- periodic backfills
- model rebuilds

### Event-Driven / Operational Workloads
- post-load validations
- downstream refresh triggers
- operational checks

## Orchestration Principles
- tasks must be restart-safe;
- dependencies must be explicit;
- logs must be accessible;
- reruns should be possible by date or partition scope.

---

# 4.6 Serving Layer

## Purpose
Expose analytical outputs in a form usable by people.

## Main Consumers
- dashboards
- SQL users
- notebooks
- saved query workflows
- future API consumers

## Responsibilities
- serve curated gold models;
- expose dashboards and interactive analytical views;
- support ad hoc exploration;
- support repeatable research workflows.

## Typical Outputs
- market overview dashboards
- asset pages
- anomaly feeds
- spread comparison pages
- data freshness pages

---

# 4.7 Research & Extension Layer

## Purpose
Turn the platform from a backend into a reusable research product.

## Components
### Saved SQL Queries
Reusable queries for repeated market scans and investigations.

### Notebooks
Workflow-oriented research templates.

### Watchlists
Curated symbol groups for repeated monitoring.

### Extension Guides
Instructions for adding new:
- exchanges,
- signals,
- marts,
- dashboards,
- notebooks.

## Why This Layer Exists
A data platform becomes much more valuable when it helps users repeat high-value workflows instead of rebuilding analysis manually each time.

---

## 5. Core Services

Below is the recommended service model for the local self-hosted architecture.

---

## 5.1 Kafka

### Role
Event backbone for real-time market data.

### Responsibilities
- receive live trade events from producers;
- buffer stream input;
- decouple producers and consumers;
- support restart-safe streaming flows.

### Best Use in This System
- real-time trade stream transport
- optional event fan-out for future signal modules

### Not Intended For
- long-term analytical storage
- business-facing query use

---

## 5.2 Spark / PySpark

### Role
Distributed-capable processing engine for both batch and streaming workflows.

### Responsibilities
- process historical datasets from bronze;
- transform to silver/gold;
- consume stream input;
- apply windowed stream logic;
- write analytical outputs.

### Best Use in This System
- larger historical transforms
- stream processing logic
- rolling metrics and anomaly pipelines

### Not Intended For
- long-term serving storage
- dashboard visualization
- orchestration itself

---

## 5.3 MinIO / Object Storage

### Role
Durable file/object storage for raw and intermediate data.

### Responsibilities
- store bronze datasets;
- store silver datasets;
- store checkpoints and intermediate artifacts where needed;
- preserve landed raw files.

### Best Use in This System
- raw and partitioned dataset storage
- reprocessing source
- stream checkpoint location
- analytical file staging

### Not Intended For
- direct end-user analytics interface

---

## 5.4 ClickHouse

### Role
Primary analytical database and serving engine.

### Responsibilities
- store gold analytical models;
- support fast time-series queries;
- back dashboards;
- support ad hoc SQL exploration;
- support anomaly and signal serving.

### Best Use in This System
- analytical marts
- dashboard datasets
- market scans
- comparative queries
- serving research outputs

### Not Intended For
- raw object landing
- workflow orchestration

---

## 5.5 dbt

### Role
Transformation and analytical modeling layer.

### Responsibilities
- define staged models;
- define marts;
- run model tests;
- formalize analytical lineage;
- standardize transformations.

### Best Use in This System
- dimensional modeling
- analytical marts
- testable SQL transformations
- documentation generation

### Not Intended For
- raw ingestion
- stream transport
- dashboarding

---

## 5.6 Airflow

### Role
Workflow orchestration and operational coordination.

### Responsibilities
- schedule and trigger ingestion jobs;
- run transformations;
- coordinate validations;
- define operational workflows;
- support reruns and observability.

### Best Use in This System
- batch orchestration
- scheduled refreshes
- dependency management
- monitoring task state

### Not Intended For
- high-throughput data processing
- long-term storage
- dashboard serving

---

## 5.7 PostgreSQL

### Role
Operational metadata database.

### Responsibilities
- support orchestration metadata;
- support service state where required;
- optionally hold small operational reference datasets.

### Best Use in This System
- orchestration metadata backend
- small control/reference records

### Not Intended For
- core analytical serving at scale
- raw market data storage

---

## 5.8 Superset

### Role
User-facing BI and dashboard serving layer.

### Responsibilities
- render dashboards;
- expose SQL-enabled exploration;
- provide visual analytical access;
- serve user-facing pages over gold models.

### Best Use in This System
- market overview
- asset pages
- anomaly feeds
- operations and freshness views

### Not Intended For
- heavy transformation logic
- pipeline orchestration
- raw event transport

---

## 6. Recommended Data Flow

# 6.1 Historical Data Flow

```text
External historical sources
    -> ingestion jobs
    -> Bronze storage
    -> batch processing
    -> Silver datasets
    -> analytical modeling
    -> Gold marts in ClickHouse
    -> dashboards / SQL / notebooks