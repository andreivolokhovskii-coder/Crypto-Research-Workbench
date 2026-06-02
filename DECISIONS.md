# DECISIONS.md

# Crypto Research Workbench — Architecture & Product Decisions

## 1. Purpose of This Document

This document records the most important architectural and product decisions behind **Crypto Research Workbench**.

The goal of this file is to explain:

- what decisions were made,
- why they were made,
- what alternatives were considered,
- what trade-offs were accepted,
- and how these decisions support the long-term direction of the project.

This is not only a technical reference.  
It is also a **product reasoning document**.

---

## 2. Decision-Making Principles

The project follows a few core decision principles:

1. **Usefulness after deployment matters more than tool count**
2. **A strong MVP is better than a massive but unfinished system**
3. **Each architectural decision must support a real research workflow**
4. **The project must be understandable to both developers and reviewers**
5. **The platform should be extendable without redesigning the whole system**
6. **Reproducibility and rerun-safety are mandatory**
7. **The repository should look like a product, not a random set of scripts**

---

# ADR-001 — The project will be positioned as a research workbench, not just a data pipeline

## Status
Accepted

## Context
A pure “crypto data platform” framing makes the project look like an infrastructure exercise.  
That is technically interesting, but weaker in terms of:
- user-facing value,
- long-term usefulness,
- product narrative,
- open-source positioning,
- and resume storytelling.

## Decision
The project will be positioned as a:

> **Self-hosted open-source crypto research workbench**

rather than only as:
- a data warehouse,
- a streaming demo,
- or a technical pipeline showcase.

## Why
This framing better reflects the intended value:
- data ingestion,
- historical analysis,
- real-time monitoring,
- saved research queries,
- dashboards,
- notebooks,
- and extensibility.

## Alternatives Considered
### Option A — “Crypto Data Platform”
Pros:
- technically clear  
Cons:
- sounds infrastructure-only
- weaker product narrative

### Option B — “Crypto Market Analytics Platform”
Pros:
- more user-facing  
Cons:
- still broad and somewhat generic

### Option C — “Crypto Research Workbench”
Pros:
- emphasizes practical use
- feels more product-like
- naturally includes dashboards, notebooks, and workflows
- stronger for portfolio storytelling

## Consequences
The repository, README, roadmap, and dashboards should all reflect the “research workbench” identity.

---

# ADR-002 — The MVP will prioritize usefulness over breadth

## Status
Accepted

## Context
It is easy to overload this project with:
- too many exchanges,
- too many signals,
- too many dashboards,
- too much automation,
- too much UI,
- and too much infrastructure polish too early.

That creates a high risk of building a wide but incomplete system.

## Decision
The MVP will be intentionally narrow and focused on the minimum set of features that already provide value.

## MVP Priorities
- local reproducible infrastructure
- historical data backbone
- initial analytical marts
- saved research queries
- notebooks
- a small user-facing dashboard set
- a minimal real-time signal path

## What Will Not Be Prioritized in MVP
- broad exchange coverage
- advanced alerting
- extensive UI customization
- complex auth systems
- many signal families
- “platform for everything”

## Why
A smaller but truly useful MVP creates:
- faster progress,
- earlier feedback,
- higher documentation quality,
- and stronger portfolio credibility.

## Consequences
Every new feature must justify why it should exist before it enters MVP scope.

---

# ADR-003 — Historical and real-time processing will remain separate by design

## Status
Accepted

## Context
Historical and live crypto market data have different characteristics:
- historical datasets support depth and reproducible analysis;
- real-time data supports immediacy and live monitoring.

Trying to treat them as one undifferentiated pipeline often creates unnecessary complexity.

## Decision
The platform will explicitly separate:
- **historical data workflows**
- **real-time streaming workflows**

## Historical Workflows Will Focus On
- OHLCV / candle backfills
- historical normalization
- marts for research and dashboards
- batch reruns
- repeatable analytical workflows

## Real-Time Workflows Will Focus On
- live trades / tick events
- short-latency transformations
- rolling windows
- anomaly detection
- freshness and operational monitoring

## Why
This separation improves:
- clarity,
- maintainability,
- reliability,
- and conceptual cleanliness.

It also makes the README narrative much stronger:
- history for depth,
- stream for immediacy.

## Alternatives Considered
### Option A — Merge history and live into one unified ingestion flow
Pros:
- superficially simpler
Cons:
- muddled semantics
- harder debugging
- harder scheduling and validity reasoning

### Option B — Separate them explicitly
Pros:
- clearer architecture
- easier to reason about
- better product story
Cons:
- slightly more components to describe

## Consequences
The repo structure, Airflow flows, and research layer must preserve this distinction.

---

# ADR-004 — The architecture will use a medallion model: Bronze / Silver / Gold

## Status
Accepted

## Context
The platform needs:
- raw traceability,
- normalized datasets,
- and user-facing analytical outputs.

A single-layer dataset design would make the system harder to debug and extend.

## Decision
The platform will use three data layers:
- **Bronze** — raw landed data
- **Silver** — cleaned and normalized datasets
- **Gold** — analytics-ready marts and serving models

## Why
This improves:
- debuggability,
- data lineage,
- reprocessing safety,
- contributor understanding,
- and long-term maintainability.

## Layer Responsibilities
### Bronze
- preserve source-level payloads
- support reprocessing
- retain transport-level traceability

### Silver
- normalize types
- normalize timestamps
- standardize symbols and exchanges
- deduplicate records

### Gold
- expose curated facts, dimensions, marts, and serving models
- support dashboards, SQL workflows, and notebooks

## Alternatives Considered
### Option A — Raw to dashboard directly
Pros:
- fewer steps  
Cons:
- weak data discipline
- poor reusability
- hard to validate and extend

### Option B — Two-layer approach
Pros:
- somewhat simpler  
Cons:
- weaker separation between normalized and business-facing assets

### Option C — Bronze / Silver / Gold
Pros:
- clearest structure
- strongest long-term maintainability
Cons:
- adds explicit modeling effort

## Consequences
All documentation and code layout should reinforce this layering.

---

# ADR-005 — The platform will be self-hosted and local-first

## Status
Accepted

## Context
The core purpose of the project is to create a controllable, reproducible analytics environment.

A cloud-first or SaaS-dependent design would weaken:
- reproducibility,
- open-source portability,
- and the “own your research stack” narrative.

## Decision
The platform will be designed as:
- **self-hosted**
- **local-first**
- **reproducible through local infrastructure definitions**

## Why
This supports:
- platform ownership
- reproducibility
- portability
- local experimentation
- stronger open-source value

## Alternatives Considered
### Option A — Cloud-first deployment
Pros:
- closer to some production patterns  
Cons:
- harder to share and run
- less accessible
- weaker local ownership story

### Option B — Self-hosted local-first
Pros:
- reproducible
- easier to demo
- stronger open-source fit
Cons:
- local hardware limitations
- more setup burden

## Consequences
The project should optimize for:
- clear local setup,
- configuration simplicity,
- Docker-based reproducibility,
- and understandable service boundaries.

---

# ADR-006 — The platform will include both backend value and user-facing analytical value

## Status
Accepted

## Context
Many engineering projects are technically competent but fail to show user-facing usefulness.

That weakens:
- practical adoption,
- storytelling,
- and overall project impact.

## Decision
The platform will intentionally include:
- backend data engineering,
- analytical marts,
- dashboards,
- SQL workflows,
- and notebook-based research workflows.

## Why
The goal is to demonstrate not only that data can be moved and transformed, but that it can be **used**.

## Consequences
A finished MVP must include:
- at least several dashboards,
- several saved research queries,
- several notebook workflows,
- and user-facing explanations of value.

---

# ADR-007 — ClickHouse will be the primary analytical serving database

## Status
Accepted

## Context
The platform needs a database that is strong for:
- analytical queries,
- time-series workloads,
- dashboard access,
- and serving research marts.

## Decision
ClickHouse will be used as the main analytical serving layer.

## Why
It fits the project goals well because the platform needs:
- fast query performance over market datasets,
- strong analytical serving semantics,
- and a clean target for marts and dashboards.

## Alternatives Considered
### Option A — PostgreSQL as primary analytical store
Pros:
- familiar
- simpler in small setups  
Cons:
- weaker match for analytical/time-series emphasis at project scale

### Option B — DuckDB-only serving
Pros:
- very simple
- great local ergonomics  
Cons:
- weaker multi-service serving story
- less aligned with a richer self-hosted multi-component platform

### Option C — ClickHouse
Pros:
- better fit for analytical serving
- strong story for time-series and dashboard workloads
Cons:
- more setup and ecosystem considerations

## Consequences
Gold marts and dashboard-oriented models should be optimized around analytical serving in ClickHouse.

---

# ADR-008 — Object storage will be the raw and intermediate storage backbone

## Status
Accepted

## Context
The platform needs a place to store:
- raw landed data,
- normalized file-based datasets,
- and intermediate assets.

A file/object storage layer helps preserve raw inputs and enables reprocessing.

## Decision
Object storage will be the backbone for Bronze and part of Silver/intermediate storage.

## Why
It supports:
- raw data preservation,
- replayability,
- traceability,
- decoupling between ingestion and serving,
- and a clearer medallion story.

## Alternatives Considered
### Option A — Store everything directly in analytical DB
Pros:
- fewer moving pieces  
Cons:
- weaker raw traceability
- reduced reprocessing flexibility

### Option B — Use object storage for raw/intermediate layers
Pros:
- cleaner separation
- stronger replay and debugging model
Cons:
- more architectural components

## Consequences
Bronze should be file/object-first, while analytical consumption should remain gold-serving-first.

---

# ADR-009 — Spark will be included as a deliberate processing component, not because it is strictly required for small volumes

## Status
Accepted

## Context
For a relatively small MVP dataset, a simpler processing path could be enough.  
However, part of the project’s value is to demonstrate:
- scalable transformation patterns,
- batch and streaming processing,
- and strong engineering breadth.

## Decision
Spark / PySpark will be included as a first-class processing component.

## Why
Not because the MVP absolutely requires distributed compute today, but because it supports the long-term architecture and the project’s portfolio value.

It helps demonstrate:
- scalable pipeline design,
- structured streaming,
- and separation between processing and serving.

## Alternatives Considered
### Option A — Pure Python only
Pros:
- simpler
- faster to implement  
Cons:
- weaker engineering breadth
- less demonstration value for larger flows

### Option B — Python + Spark
Pros:
- stronger architecture story
- supports batch and streaming
- better extension potential
Cons:
- more setup complexity

## Consequences
Spark should earn its place through:
- meaningful transforms,
- streaming logic,
- and well-documented justification in README and architecture docs.

---

# ADR-010 — dbt will be used as the analytical modeling layer

## Status
Accepted

## Context
The project needs a standardized way to express:
- staged transformations,
- dimensional models,
- marts,
- tests,
- and model lineage.

## Decision
dbt will be used for SQL-first analytical modeling.

## Why
It creates a clear, testable, documented transformation layer and strengthens:
- maintainability,
- analytical clarity,
- and project readability.

## Alternatives Considered
### Option A — Put all SQL into ad hoc scripts
Pros:
- flexible  
Cons:
- weak structure
- hard to test and explain

### Option B — dbt-based modeling
Pros:
- structure
- testing
- docs
- lineage
Cons:
- added project setup and conventions

## Consequences
Gold-layer business logic should be increasingly centralized into dbt models and tests.

---

# ADR-011 — Airflow will orchestrate workflows, not replace processing logic

## Status
Accepted

## Context
The project needs a way to:
- schedule runs,
- define dependencies,
- support reruns,
- track status,
- and manage operational workflows.

## Decision
Airflow will be the orchestration layer.

## Why
It provides a clear, well-recognized way to manage:
- backfills,
- daily refreshes,
- validations,
- dbt runs,
- and operational coordination.

## Important Constraint
Airflow is a scheduler and orchestrator.  
It should not become the place where complex business logic lives.

## Consequences
Pipeline logic should remain in:
- ingestion modules,
- processing jobs,
- dbt models,
while Airflow manages execution order and operational visibility.

---

# ADR-012 — Superset will be the primary dashboarding layer

## Status
Accepted

## Context
The project needs a dashboard layer that can expose:
- market overview,
- asset pages,
- anomaly feeds,
- and data freshness / health views.

## Decision
Superset will be used as the main dashboarding tool.

## Why
It fits the platform goal of delivering self-hosted, BI-style access over analytical marts.

## Why This Matters
The project should show not only data preparation, but also how end users can interact with the outputs.

## Alternatives Considered
### Option A — No BI layer, notebooks only
Pros:
- simpler  
Cons:
- weak non-engineer usability
- weaker product story

### Option B — Superset
Pros:
- better user-facing access
- stronger analytics product narrative
Cons:
- adds setup effort

## Consequences
Dashboards should be treated as first-class project artifacts, not as optional extras.

---

# ADR-013 — The repo will be organized by responsibilities, not by random technology grouping

## Status
Accepted

## Context
Messy repo structure is one of the fastest ways to make a project look immature.

## Decision
The repository will be organized into clear responsibility boundaries:
- ingestion
- processing
- modeling
- orchestration
- dashboards
- notebooks
- tests
- docs

## Why
This makes the repo:
- easier to understand,
- easier to extend,
- easier to review,
- easier to document.

## Consequences
Any new module should be placed based on responsibility, not personal convenience.

---

# ADR-014 — The project will include an explicit research layer

## Status
Accepted

## Context
Without a research layer, the platform risks becoming a backend-only system.

## Decision
The project will explicitly include:
- saved SQL research queries
- notebook templates
- watchlists
- repeatable investigation flows

## Why
This is the layer that directly shortens the path from:
- market question
to
- analytical answer

It is also the layer that makes the system personally useful after deployment.

## Consequences
The MVP is not complete without usable research artifacts.

---

# ADR-015 — The system will include user-facing anomaly and market-scanning outputs early

## Status
Accepted

## Context
Signals and anomaly outputs create obvious user-facing value.  
Without them, the platform risks feeling static and generic.

## Decision
The project will prioritize a small but useful set of early scanners/signals such as:
- unusual volume
- volatility changes
- abnormal market activity
- exchange divergence
- freshness issues

## Why
These outputs help users answer:
- what changed?
- what looks unusual?
- what deserves attention?

## Consequences
The dashboard and research layers should both include anomaly-oriented outputs.

---

# ADR-016 — Environment thinking will reflect dev / test / production-like separation

## Status
Accepted

## Context
Even in local-first development, mixing experimentation, validation, and demo behavior into one uncontrolled environment creates confusion.

## Decision
The project will be designed with conceptual separation between:
- development
- validation / test
- production-like local runtime

## Why
This encourages:
- cleaner reruns,
- safer testing,
- better demo stability,
- and stronger engineering discipline.

## Consequences
Configuration, docs, and workflows should clearly distinguish:
- experimental runs,
- validation runs,
- and stable demonstration runs.

---

# ADR-017 — Reproducibility and idempotency are mandatory design goals

## Status
Accepted

## Context
A research platform that cannot be rerun or trusted after retries is not robust enough.

## Decision
The architecture will treat reproducibility and idempotency as first-class design goals.

## Why
This is essential for:
- stable backfills,
- safe retry behavior,
- trustworthy analytical outputs,
- and long-term maintainability.

## Expected Outcomes
- rerun-safe historical pipelines
- deduplicated streaming behavior
- stable partition handling
- clearly documented refresh logic

## Consequences
Any implementation that makes reruns unsafe should be treated as a design issue, not a minor inconvenience.

---

# ADR-018 — Documentation will be treated as a core engineering asset

## Status
Accepted

## Context
Open-source and portfolio projects are often evaluated first through documentation, not code execution.

## Decision
The project will maintain a structured documentation set:
- `README.md`
- `PROJECT_SCOPE.md`
- `ROADMAP.md`
- `ARCHITECTURE.md`
- `DECISIONS.md`
- future `CONTRIBUTING.md`

## Why
Documentation helps:
- reviewers understand value quickly,
- contributors onboard faster,
- the author stay aligned with original goals.

## Consequences
Docs are part of delivery, not post-project decoration.

---

# ADR-019 — The platform will aim to be useful for multiple audiences, but optimize first for the project owner

## Status
Accepted

## Context
A project intended for everyone too early often becomes useful for no one.

## Decision
The initial design will optimize first for:
- the project owner’s own research workflows

while also being structured for:
- developer understanding,
- user-facing exploration,
- and future contributor participation.

## Why
This keeps the project practical and grounded in real usage.

## Consequences
If a feature does not help the owner’s actual workflow or clearly improve external usability, it should be questioned.

---

# ADR-020 — The finished project should feel like a product, not just a demo

## Status
Accepted

## Context
A technically correct but poorly packaged project often feels unfinished.

## Decision
The project will intentionally include:
- product framing,
- user-facing dashboards,
- repeatable workflows,
- architectural clarity,
- and documentation that supports real adoption.

## Why
This increases:
- portfolio quality,
- user value,
- reviewability,
- and long-term usefulness.

## Consequences
The final output should be explainable in terms of:
- the problem it solves,
- who it helps,
- how it works,
- and why the chosen design matters.

---

## 3. Summary of Accepted Decisions

The current accepted direction is:

- position the project as a **research workbench**
- keep the MVP **focused and useful**
- separate **historical and real-time flows**
- use **Bronze / Silver / Gold**
- remain **self-hosted and local-first**
- combine **backend depth and user-facing value**
- use **ClickHouse** for analytical serving
- use **object storage** for raw/intermediate data
- include **Spark** deliberately
- use **dbt** for analytical modeling
- use **Airflow** for orchestration
- use **Superset** for dashboards
- organize the repo by **responsibility boundaries**
- include an explicit **research layer**
- prioritize **anomaly and scanner outputs**
- keep **dev / test / production-like thinking**
- treat **reproducibility and idempotency as mandatory**
- treat **documentation as core infrastructure**
- optimize first for **real personal usefulness**
- package the final project as a **real product**

---

## 4. Future Decisions to Revisit

These decisions will likely need revision after MVP:

1. Which exchanges should be supported next?
2. Which anomaly families bring the highest value?
3. Whether an API layer should be added
4. Whether alerting should become part of MVP+1
5. Whether notebook workflows should become more structured
6. Whether plugin interfaces should be formalized
7. Whether public demo mode should exist
8. Whether user-defined watchlists need their own persistence layer

---

## 5. Final Decision Statement

This project is intentionally designed as a **self-hosted, research-oriented, open-source crypto analytics product**.

The selected decisions aim to ensure that it is:

- useful in practice,
- strong as an engineering case,
- understandable as a system,
- and extensible as a public project.