# README.md

# Crypto Research Workbench

An open-source self-hosted platform for collecting, normalizing, exploring, and monitoring crypto market data in a reproducible local environment.

---

## What this project is

**Crypto Research Workbench** is a local-first research platform that combines:

- historical market data ingestion,
- real-time trade monitoring,
- medallion data architecture,
- analytical marts,
- dashboards,
- reusable SQL research queries,
- and notebook-based research workflows.

The goal is to create a system that is not only technically robust, but also **actually useful after deployment** for market investigation and repeatable research.

---

## Why this project exists

Crypto market analysis often suffers from:

- fragmented data sources,
- weak reproducibility,
- disconnected historical and real-time workflows,
- dependence on third-party tools,
- and lack of a self-hosted local research environment.

This project exists to solve those problems by creating a **controllable, reusable, and extensible research workbench**.

---

## Who this project is for

### Researchers / Analysts
Use it to:
- scan the market,
- explore historical behavior,
- investigate anomalies,
- compare exchanges,
- and reuse saved workflows.

### Developers / Data Engineers
Use it to:
- study a local self-hosted data platform,
- explore batch + streaming architecture,
- contribute new sources and signals,
- or use it as a reference implementation.

### Crypto Enthusiasts
Use it to:
- view market overview pages,
- inspect assets,
- follow anomaly feeds,
- and compare exchange behavior.

### Open-Source Contributors
Use it to:
- add exchanges,
- create analytical models,
- contribute dashboards,
- improve developer experience,
- and expand research capabilities.

---

## What you can do with it

- load and query historical market data;
- monitor live trade activity;
- track unusual market behavior;
- compare exchanges;
- investigate assets through dashboards;
- run reusable research SQL;
- use notebook templates for deeper analysis;
- extend the system with new signals and sources.

---

## Core Product Features

### Historical Market Backbone
- historical OHLCV / candlestick ingestion;
- normalized storage and transformations;
- analytical marts for time-series analysis.

### Real-Time Monitoring
- live trade ingestion;
- signal generation;
- anomaly detection;
- freshness monitoring.

### Research Layer
- saved SQL queries;
- notebook templates;
- watchlists;
- reusable workflows.

### User-Facing Dashboards
- market overview;
- asset research page;
- anomaly feed;
- exchange comparison;
- data health / freshness.

### Open-Source Platform Layer
- modular code structure;
- self-hosted setup;
- documented contribution paths;
- extension guides.

---

## Architecture

### High-Level Layers
1. **Ingestion Layer**
   - historical data
   - real-time trade stream
   - metadata sources

2. **Storage Layer**
   - bronze
   - silver
   - gold
   - analytical serving layer

3. **Processing Layer**
   - Python ingestion jobs
   - Spark/PySpark jobs
   - dbt transformation models

4. **Orchestration Layer**
   - scheduled workflows
   - validations
   - reruns
   - freshness checks

5. **Serving Layer**
   - dashboards
   - SQL access
   - notebooks

6. **Research Layer**
   - saved research queries
   - watchlists
   - notebook workflows
   - investigation templates

---

## Architecture Diagram

> Add Mermaid or static image here.

### Example Mermaid Placeholder
```mermaid
flowchart LR
    A[Historical APIs / Dumps] --> B[Ingestion Layer]
    C[Live WebSocket Stream] --> D[Kafka]
    B --> E[Bronze Storage]
    D --> E
    E --> F[Silver Transformations]
    F --> G[Gold Marts]
    G --> H[ClickHouse]
    H --> I[Dashboards]
    H --> J[Saved SQL Queries]
    H --> K[Research Notebooks]
    L[Airflow] --> B
    L --> F
    L --> G