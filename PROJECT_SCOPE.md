# PROJECT_SCOPE.md

# Crypto Research Workbench — Project Scope

## 1. Project Overview

### Project Name
**Crypto Research Workbench**

### Short Description
An open-source self-hosted platform for collecting, normalizing, analyzing, and monitoring crypto market data in a reproducible local environment.

### Core Positioning
This project is not only a data engineering showcase.  
It is a **crypto research workbench** designed to support:
- historical market analysis,
- live market monitoring,
- repeatable research workflows,
- reusable dashboards and notebooks,
- and an extension-friendly open-source architecture.

---

## 2. Why This Project Exists

Crypto market research is often fragmented and hard to reproduce.

Typical pain points:
- data comes from too many sources;
- historical and live workflows are disconnected;
- analysis is repeated manually;
- dashboards and research logic are often not portable;
- many useful tools are SaaS-based and not fully under user control.

This project exists to create a **local, self-hosted, reproducible research environment** that solves those problems.

---

## 3. Target Users

## 3.1 Solo Researcher / Analyst
### Needs
- a single source of market data;
- fast access to historical and live information;
- repeatable research workflows;
- reusable saved queries and notebooks;
- a way to shorten the path from question to analysis.

### Example Questions
- Which assets are showing unusual volume today?
- Which markets have become more volatile?
- Which symbols deserve deeper research right now?
- What changed in the last hour or day?

---

## 3.2 Developer / Data Engineer
### Needs
- a reference architecture for self-hosted data platforms;
- examples of medallion architecture;
- batch + streaming implementation patterns;
- modular repository structure;
- clear configuration and extension model.

### Example Questions
- How does local orchestration work in a reproducible setup?
- How are new exchanges or signals added?
- How is the data stack structured for both batch and real-time?

---

## 3.3 Crypto Enthusiast / Non-Engineer User
### Needs
- understandable dashboards;
- simple market overview pages;
- anomaly feeds;
- easy asset-specific investigation pages;
- useful signals without needing raw SQL.

### Example Questions
- What is happening in the market right now?
- Which assets are behaving unusually?
- Where are there interesting exchange differences?
- Is the data fresh and reliable?

---

## 3.4 Open-Source Contributor
### Needs
- clear project purpose;
- easy onboarding;
- contribution guidelines;
- modular extension points;
- roadmap visibility.

### Example Questions
- How do I add a new signal?
- How do I add support for a new exchange?
- Where do I contribute dashboards, notebooks, or transformations?

---

## 4. Problem Statement

The project addresses four major problems:

### 4.1 Fragmented Data
Historical and live crypto data are distributed across multiple APIs, websites, and tools.

### 4.2 Weak Reproducibility
Many analyses are done manually and cannot be repeated cleanly later.

### 4.3 Lack of Local Control
Researchers often rely on third-party platforms instead of a self-hosted local stack.

### 4.4 Gap Between Data Engineering and User Value
Many technical data projects stop at ingestion and storage, without making the outputs meaningfully useful for users and researchers.

---

## 5. Project Goals

## 5.1 Primary Goals
1. Build a platform that is **useful for my own research after deployment**
2. Create a project that is **strong for portfolio and resume positioning**
3. Make the platform **interesting and extendable for developers**
4. Make the outputs **understandable and useful for non-engineer users**

## 5.2 Secondary Goals
- demonstrate production-style data engineering thinking;
- show both batch and real-time data workflows;
- create reusable research workflows;
- build documentation and contributor onboarding from the start;
- make the project feel like a product, not just a stack.

---

## 6. Product Vision

The final system should behave like a **local crypto research operating system**.

After deployment, a user should be able to:
- inspect historical market behavior;
- monitor live market movement;
- find anomalies and market shifts;
- compare exchanges;
- investigate individual assets;
- use notebooks and saved queries as repeatable workflows;
- extend the system with new data sources, signals, marts, or dashboards.

---

## 7. In Scope

The following items are in scope for the project:

### 7.1 Market Data
- historical candle / OHLCV data;
- real-time trade or tick events;
- coin metadata;
- exchange metadata;
- normalized symbol mappings.

### 7.2 Data Platform
- self-hosted local stack;
- bronze / silver / gold architecture;
- object storage;
- analytical database;
- orchestration;
- transformation layer;
- quality checks.

### 7.3 Research Layer
- reusable SQL research queries;
- analytical marts;
- notebooks;
- watchlists;
- saved research workflows.

### 7.4 User Layer
- market overview dashboard;
- asset research dashboard;
- anomaly feed;
- exchange comparison page;
- data freshness / health page.

### 7.5 Open-Source Layer
- documentation;
- repo structure;
- contributor onboarding;
- extension guides;
- CI and quality checks.

---

## 8. Out of Scope

The following items are explicitly out of scope for the MVP:

- automated trading execution;
- order management;
- portfolio management;
- user billing or payments;
- complex authentication systems;
- mobile app;
- deep social or community features;
- full on-chain analytics platform;
- exchange execution integrations.

These may be future ideas, but they are not MVP priorities.

---

## 9. Key Use-Cases

## 9.1 Historical Market Research
A user explores historical candles, returns, volatility, and volume to investigate a hypothesis.

## 9.2 Live Monitoring
A user views real-time market behavior through anomalies, spikes, and signal feeds.

## 9.3 Exchange Comparison
A user compares price behavior or divergence across exchanges.

## 9.4 Asset Deep Dive
A user opens a single-asset page with price history, recent anomalies, volume behavior, and metadata context.

## 9.5 Repeatable Research Workflow
A researcher reuses saved queries or notebooks instead of recreating analysis manually.

## 9.6 Development and Extension
A contributor adds a new exchange, signal, transformation, or dashboard.

---

## 10. MVP Definition

The MVP should prove that the platform is not only technically valid but already useful.

### MVP Must Include
- local reproducible infrastructure;
- historical data ingestion;
- initial real-time data path;
- bronze / silver / gold structure;
- analytical marts;
- at least one orchestration workflow;
- at least ten reusable research queries;
- at least three research notebooks;
- at least three dashboards or analytical pages;
- basic anomaly detection;
- basic documentation and quickstart.

### MVP Does Not Need
- many exchanges;
- many signals;
- complex auth;
- large-scale production deployment;
- advanced UI polish beyond usability requirements.

---

## 11. Success Criteria

The project is successful if:

### 11.1 Personal Usefulness
I can use it after deployment to:
- scan the market faster,
- detect interesting behavior,
- investigate assets quicker,
- and reduce manual research time.

### 11.2 Technical Quality
The system:
- runs reproducibly,
- supports reruns without duplicating data,
- provides reliable modeling and validation,
- and separates historical and live workflows clearly.

### 11.3 User Value
A non-engineer can open dashboards and understand:
- what is changing,
- what looks unusual,
- what deserves attention,
- and whether data pipelines are healthy.

### 11.4 Portfolio Value
A reviewer can understand:
- the architecture,
- the technical depth,
- the business usefulness,
- the product thinking,
- and the project maturity quickly.

---

## 12. Non-Functional Requirements

### 12.1 Reproducibility
The stack should start locally with minimal manual setup.

### 12.2 Idempotency
Batch and streaming flows should be safe to rerun.

### 12.3 Observability
Failures, stale data, and freshness issues should be visible.

### 12.4 Documentation Quality
The project should be understandable by an external reviewer.

### 12.5 Modularity
New components should be addable without major redesign.

### 12.6 Simplicity
The MVP should favor clarity over unnecessary complexity.

---

## 13. Design Constraints

- self-hosted first;
- local-first development experience;
- no dependence on paid proprietary analytics tools for core value;
- architecture should be credible but not overbuilt for MVP;
- usefulness must be demonstrated, not assumed.

---

## 14. Open Questions

These questions should be tracked and revisited during implementation:

1. Which exchanges belong in MVP after the first source is working?
2. Which anomaly signals provide the highest value with the lowest complexity?
3. Which dashboard pages provide the best value-to-effort ratio?
4. Which research queries save the most time in real use?
5. What should remain local-only vs what can later become cloud-ready?

---

## 15. Future Expansion Ideas

Potential post-MVP directions:
- more exchange support;
- richer signal catalog;
- event/news reaction analysis;
- alerting layer;
- public demo mode;
- optional API layer;
- richer notebook templates;
- contributor plugin system.

---

## 16. Final Scope Statement

This project is a **self-hosted crypto research product**.  
Its purpose is to combine market data engineering, analytical modeling, real-time monitoring, and reusable research workflows into one reproducible local environment.

It should serve both as:
- a practical research tool,
- and a strong public engineering portfolio project.