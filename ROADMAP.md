# ROADMAP.md

# Crypto Research Workbench — Project Roadmap

## 1. Project Summary

### Project Name
**Crypto Research Workbench**

### One-line Description
An open-source self-hosted platform for collecting, normalizing, exploring, monitoring, and extending crypto market intelligence in a reproducible local environment.

### Core Idea
This project is not just a data platform.  
It is a **research workbench** that combines:
- historical market data,
- real-time market monitoring,
- analytical marts,
- reusable research workflows,
- dashboards for users,
- and an extensible architecture for developers.

### Why This Project Exists
Crypto data is fragmented, research is often hard to reproduce, and most workflows depend on third-party tools.  
This project solves that by creating a **local, controllable, self-hosted analytics environment** for market research and monitoring.

---

## 2. Goals

## Primary Goal
Build a self-hosted open-source crypto research platform that is:

1. **Useful for my own research after deployment**
2. **Strong as a portfolio and resume project**
3. **Interesting for developers and contributors**
4. **Understandable and useful for users beyond engineers**

## Secondary Goals
- demonstrate end-to-end data engineering skills;
- show product thinking, not only infrastructure skills;
- create a reusable local analytics environment;
- build a project that can grow into a public open-source platform.

---

## 3. Target Users

### 3.1 Solo Researcher / Analyst
Needs:
- one place for historical and live market data;
- repeatable market research workflows;
- anomaly detection and market scanners;
- saved queries and notebooks for faster analysis.

### 3.2 Developer / Data Engineer
Needs:
- a reference architecture for local self-hosted data platforms;
- batch + streaming examples;
- modular codebase;
- clean setup and extension patterns.

### 3.3 Crypto Enthusiast / User
Needs:
- understandable dashboards;
- market overview;
- signal pages;
- anomaly feeds;
- easy ways to investigate coins and exchanges.

### 3.4 Open-Source Contributor
Needs:
- clear repository structure;
- contribution entry points;
- extension guides;
- good documentation and setup experience.

---

## 4. Problem Statement

This project addresses the following problems:

1. **Market data fragmentation**  
   Historical and live crypto data are usually scattered across multiple services and APIs.

2. **Poor reproducibility of research**  
   Many analyses are ad hoc and difficult to repeat.

3. **Weak portability**  
   Most tools depend on hosted SaaS workflows instead of self-controlled infrastructure.

4. **Gap between engineering and usability**  
   Many data projects collect data well, but fail to deliver useful interfaces and reusable research workflows.

---

## 5. Product Vision

The final system should behave like a **crypto research operating system**.

After deployment, a user should be able to:
- inspect historical market behavior;
- monitor live events and anomalies;
- compare exchanges;
- investigate specific assets;
- reuse saved research templates;
- extend the system with new sources, signals, and dashboards.

---

## 6. Product Scope

## In Scope
- historical candlestick ingestion;
- real-time trade ingestion;
- coin and exchange metadata;
- medallion architecture (bronze / silver / gold);
- analytical marts;
- dashboards and saved queries;
- research notebooks;
- orchestration;
- data quality checks;
- self-hosted deployment;
- modular developer experience.

## Out of Scope (for MVP)
- trading bot execution;
- portfolio management;
- order execution;
- advanced user account systems;
- mobile app;
- complex social features;
- on-chain analytics beyond the initial market-data scope.

---

## 7. Success Criteria

The project is successful if it achieves all of the following:

### 7.1 Personal Usefulness
After deployment, I can use it to:
- scan the market;
- investigate assets quickly;
- find anomalies;
- compare exchanges;
- reduce manual research time.

### 7.2 Technical Quality
The platform:
- runs reproducibly in a local environment;
- supports both batch and streaming;
- has tested dbt models and data quality checks;
- supports reruns without duplicating data.

### 7.3 User Value
A non-engineer can open dashboards and understand:
- what is happening in the market;
- what looks unusual;
- which assets deserve attention;
- whether the data is fresh or stale.

### 7.4 Portfolio Value
The repository clearly demonstrates:
- architecture design;
- ingestion and transformation pipelines;
- analytical modeling;
- orchestration;
- real-time processing;
- documentation quality;
- open-source readiness.

---

## 8. Guiding Principles

1. **Product before stack**  
   Every technical decision must improve usefulness, reproducibility, or extension potential.

2. **Useful immediately after deployment**  
   The platform must help with actual research, not only show infrastructure.

3. **History and real-time are separate by design**  
   Historical candles provide reliable depth; real-time trades provide live signals.

4. **Start with a narrow but strong MVP**  
   Do not overbuild.

5. **Make extensions easy**  
   New exchanges, signals, marts, and dashboards should be addable without redesigning the whole system.

6. **Favor clarity over cleverness**  
   The project should be understandable by reviewers and contributors.

7. **Everything should be reproducible**  
   Setup, ingestion, transformations, dashboards, and tests should all be documented and rerunnable.

8. **Respect proper environment thinking**  
   Even in local development, design should reflect separation between dev, test, and production-like behavior.

---

## 9. High-Level Architecture

## Core Layers
1. **Ingestion Layer**
   - historical market data
   - live trades
   - metadata APIs

2. **Storage Layer**
   - bronze
   - silver
   - gold
   - object storage + analytical database

3. **Processing Layer**
   - Python jobs
   - Spark/PySpark jobs
   - dbt transformations

4. **Orchestration Layer**
   - scheduled jobs
   - dependency management
   - validations
   - reruns

5. **Serving Layer**
   - dashboards
   - SQL access
   - research notebooks

6. **Research Layer**
   - repeatable analytical workflows
   - saved research queries
   - watchlists
   - reusable investigations

7. **Open-Source Layer**
   - docs
   - contribution model
   - CI
   - testability
   - extension standards

---

## 10. Phase-by-Phase Roadmap

# Phase 0 — Product Framing

## Objective
Define exactly what this project is, who it serves, and what problem it solves.

## Tasks
- write `PROJECT_SCOPE.md`;
- define target users and their needs;
- define MVP boundaries;
- list primary use-cases;
- list non-goals;
- define success criteria.

## Deliverables
- `PROJECT_SCOPE.md`
- `MVP_PLAN.md`
- `ROADMAP.md`
- draft README introduction

## Definition of Done
- the project has a clear purpose;
- MVP is explicitly defined;
- the final audience is clear.

---

# Phase 1 — Repository Foundations

## Objective
Create a structured repository ready for long-term development.

## Tasks
- create repository;
- define folder structure;
- add `.gitignore`, `LICENSE`, `.env.example`;
- create architecture and docs skeleton;
- add Makefile targets.

## Deliverables
- repository initialized;
- standard project structure;
- bootstrap documentation files.

## Definition of Done
- the repo is understandable before any major code is added;
- a new contributor can see where ingestion, modeling, dashboards, and docs belong.

---

# Phase 2 — Local Infrastructure

## Objective
Stand up a reproducible self-hosted local environment.

## Planned Components
- Kafka
- Spark
- MinIO
- ClickHouse
- Airflow
- PostgreSQL
- Superset

## Tasks
- create `docker-compose.yml`;
- define named volumes and network;
- configure healthchecks;
- define environment variables;
- verify local service startup;
- ensure stable port mapping;
- enforce UTC settings across services.

## Deliverables
- local stack startup command;
- working service healthchecks;
- accessible UIs for operational tools.

## Definition of Done
- platform starts reproducibly;
- all services are reachable;
- failures are visible and diagnosable;
- configuration is version-controlled except secrets.

---

# Phase 3 — Historical Data Backbone

## Objective
Build the reliable historical data foundation.

## Tasks
- ingest historical OHLCV/candle data;
- ingest coin metadata;
- land raw data into bronze storage;
- clean and normalize into silver;
- load modeled data into analytical storage;
- verify idempotent reruns.

## Core Outputs
### Bronze
- raw candles
- raw metadata

### Silver
- typed and cleaned candles
- standardized timestamps
- normalized symbols
- standardized exchange identifiers

### Gold
- `fact_candles`
- `dim_coin`
- `dim_exchange`
- `dim_time`
- initial analytical marts

## Definition of Done
- historical data for selected symbols is queryable;
- reruns do not create duplicates;
- timestamps and market fields are validated;
- historical depth is enough for research use.

---

# Phase 4 — Analytical Modeling with dbt

## Objective
Create a robust analytics layer with tested models.

## Tasks
- initialize dbt project;
- create staging models;
- create dimensions and facts;
- create analytical marts;
- add schema and business-rule tests;
- generate lineage/docs.

## Initial Models
- `stg_klines`
- `stg_coins`
- `dim_coin`
- `dim_exchange`
- `dim_time`
- `fact_candles`
- `mart_volatility`
- `mart_volume_profile`
- `mart_market_regime`

## Required Tests
- uniqueness tests;
- not-null tests;
- relationship tests;
- accepted-values tests;
- business-rule tests for OHLC consistency.

## Definition of Done
- dbt build passes;
- lineage graph exists;
- marts are usable for dashboards and notebooks;
- analytical layer is trustworthy enough for repeated research.

---

# Phase 5 — Research Layer MVP

## Objective
Make the platform personally useful for real research.

## Tasks
- create saved SQL queries;
- create notebook templates;
- create research watchlists;
- define repeatable market investigation workflows.

## Initial Saved Queries
1. top assets by unusual volume
2. assets with rising realized volatility
3. strongest movers
4. price-volume divergence scan
5. abnormal intraday range scan
6. latest active market list
7. exchange spread snapshot
8. long-wick candle finder
9. breakout candidate scan
10. market regime overview

## Initial Research Notebooks
1. `daily_market_scan.ipynb`
2. `asset_deep_dive.ipynb`
3. `cross_exchange_comparison.ipynb`

## Initial Watchlists
- major assets
- high-volatility assets
- research candidates

## Definition of Done
- the platform saves time in actual research;
- repeated questions can be answered faster than before;
- there is at least one complete workflow from idea to investigation.

---

# Phase 6 — Orchestration and Data Operations

## Objective
Automate batch data movement and validation.

## Tasks
- create orchestrated ingestion DAGs;
- add metadata refresh DAG;
- add dbt build DAG;
- add validation DAG;
- add rerun-safe scheduling patterns;
- improve logging and observability.

## Initial DAGs
- `historical_backfill`
- `daily_incremental_update`
- `coin_metadata_refresh`
- `dbt_build`
- `data_quality_validation`

## Definition of Done
- the platform can operate with minimal manual intervention;
- reruns are safe;
- task failures are visible;
- data freshness can be tracked.

---

# Phase 7 — Real-Time Streaming Layer

## Objective
Add live market monitoring with useful signals.

## Tasks
- implement WebSocket producer;
- publish live trade events;
- consume stream into storage and analytics layers;
- support checkpointing and deduplication;
- derive first real-time signals.

## First Real-Time Signals
- volume spike detection
- unusually large trades
- accelerated market activity
- exchange divergence
- data freshness lag alerts

## Real-Time Output Requirements
- event timestamps
- trade identifiers
- symbol-level streaming data
- deduplicated sink behavior
- recoverable processing state

## Definition of Done
- live data flows end-to-end;
- restart does not corrupt state;
- real-time anomalies appear in analytical outputs;
- the streaming layer adds clear research value.

---

# Phase 8 — User-Facing Dashboards

## Objective
Make the platform understandable and useful to non-engineers.

## Core Dashboard Pages
### 1. Market Overview
- broad market state
- large movers
- key volatility changes
- data freshness indicators

### 2. Asset Research Page
- price history
- volatility
- volume
- recent anomalies
- context for a selected asset

### 3. Anomaly Feed
- recent unusual events
- volume spikes
- abnormal behavior
- live signal stream

### 4. Exchange Comparison
- spread views
- normalized exchange comparisons
- symbol-level divergence

### 5. Data Health
- freshness status
- pipeline health indicators
- lag and missing data visibility

## Definition of Done
- a user can understand the system without reading raw tables;
- dashboard pages answer practical research questions;
- the platform looks like a usable product, not just a backend.

---

# Phase 9 — Open-Source Readiness

## Objective
Package the project so others can adopt and extend it.

## Tasks
- write `README.md`;
- write `ARCHITECTURE.md`;
- write `CONTRIBUTING.md`;
- add issue templates;
- add extension guides;
- add example seeds and demo flows;
- define public roadmap themes.

## Extension Guides to Include
- how to add a new exchange
- how to add a new signal
- how to add a new dbt mart
- how to add a new dashboard
- how to add a new research notebook

## Definition of Done
- a new person can clone the repo and understand what it does;
- a contributor can identify entry points;
- the project looks maintained and intentional.

---

# Phase 10 — Quality, CI, and Reliability

## Objective
Show engineering maturity and keep the project stable.

## Tasks
- add formatting and linting;
- add unit tests;
- add integration checks;
- validate dbt in CI;
- validate container builds;
- add smoke checks for startup.

## CI Scope
- Python lint/format checks
- SQL validation
- dbt compile/build
- unit tests
- Docker-related checks

## Definition of Done
- CI prevents obvious breakage;
- code quality is enforced automatically;
- the repository signals reliability to reviewers and contributors.

---

# Phase 11 — Portfolio Packaging

## Objective
Turn the project into a standout case study.

## Tasks
- prepare architecture diagram;
- prepare screenshots;
- prepare short demo scenario;
- create concise resume bullets;
- create case-study summary.

## Output Materials
- architecture diagram
- dashboard screenshots
- repository overview
- short “what problem this solves” summary
- “key design decisions” section
- resume-ready bullet points

## Definition of Done
- the project is easy to explain;
- it is usable in interviews and applications;
- the value is visible in under a few minutes of review.

---

## 11. Priority Order

## Immediate Priority
1. define scope and MVP
2. build repo structure
3. stand up local infrastructure
4. build historical backbone
5. build analytical marts
6. build first useful research workflows

## Second Priority
7. orchestration
8. dashboards
9. streaming
10. open-source packaging

## Third Priority
11. CI hardening
12. advanced signals
13. broader exchange support
14. larger contributor experience

---

## 12. MVP Definition

The project reaches MVP when all of the following are true:

- local stack starts with one command;
- historical market data is available and queryable;
- analytical marts exist and pass validation;
- at least one dashboard page is usable;
- at least ten saved research queries exist;
- at least three reusable notebooks exist;
- at least one orchestration flow exists;
- at least one live data path exists;
- at least two or three live signals exist;
- documentation is good enough for another person to run the project.

---

## 13. Definition of Done by Capability Level

### Level 1 — Infrastructure Ready
Services run reliably and are accessible.

### Level 2 — Data Backbone Ready
Historical ingestion, normalization, and storage work correctly.

### Level 3 — Research Ready
Saved queries, notebooks, and workflows save real research time.

### Level 4 — User Ready
Dashboards provide practical value to non-engineers.

### Level 5 — Portfolio Ready
Docs, structure, and visuals tell a strong story.

### Level 6 — Open-Source Ready
The system is understandable, extendable, and contribution-friendly.

---

## 14. Risks and Mitigation

### Risk 1 — Overbuilding the MVP
**Mitigation:** prioritize usefulness over architectural completeness.

### Risk 2 — Building infrastructure without user value
**Mitigation:** after each phase, ask whether this improves research speed or clarity.

### Risk 3 — Making the project useful only to the author
**Mitigation:** include dashboards, quickstart, and extension guides early.

### Risk 4 — Streaming complexity delaying the whole project
**Mitigation:** do not start with streaming; build it after historical and analytical layers are solid.

### Risk 5 — Weak packaging
**Mitigation:** treat README, diagrams, and screenshots as product deliverables, not afterthoughts.

### Risk 6 — Hard contributor onboarding
**Mitigation:** keep modules clear and write practical extension documentation.

---

## 15. Evaluation Framework for Every New Task

Before adding any major task, validate it with these questions:

1. Does it make the platform more useful?
2. Does it make research faster?
3. Does it improve reproducibility?
4. Does it make the project clearer to other people?
5. Does it strengthen the resume/portfolio value?

If the answer is “no” to most of these questions, the task is likely not a priority.

---

## 16. Resume Value

This project should ultimately demonstrate:

- product thinking;
- system design;
- data engineering;
- analytical modeling;
- orchestration;
- real-time pipelines;
- documentation quality;
- reproducibility;
- open-source readiness.

### Example Resume Positioning
**Built a self-hosted open-source crypto research workbench for historical and real-time market analysis, combining medallion data architecture, reusable analytical marts, dashboards, and streaming-based anomaly detection in a reproducible local environment.**

---

## 17. Final Intended Outcome

The final platform should be:

- technically credible,
- practically useful,
- visually understandable,
- extendable by others,
- and strong enough to stand out as a resume case.

The end result is not just a crypto data platform.  
It is a **reusable local research product**.