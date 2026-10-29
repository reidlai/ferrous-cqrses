# ferrous-cqrses

[← Back to FerrousCX Root Architecture](https://github.com/reidlai/ferrous-cx)

[![Component Type: Rust CQRS-ES Core](https://img.shields.io/badge/crate-lib%20%2B%20bin-blue.svg)](https://github.com/reidlai/ferrous-cx)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-green.svg)](LICENSE)
[![DevSecOps CI: Tier 1](https://img.shields.io/badge/DevSecOps%20CI-Tier%201%20Passing-brightgreen.svg)](.github/workflows/devsecops-ci.yml)
[![Pre-Commit Enabled](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](.pre-commit-config.yaml)
[![Pattern: CQRS + Event Sourcing](https://img.shields.io/badge/pattern-CQRS%20%2B%20Event%20Sourcing-orange.svg)](#architecture-overview)

**`ferrous-cqrses`** is the core Command-Query Responsibility Segregation and Event Sourcing (CQRS-ES) submodule of the **FerrousCX** enterprise platform. Built in pure Rust, it operates in a **dual-role architecture**:

1. **Rust Library Crate (`src/lib.rs`):**  
   Exports strongly typed **Command Events** (`IssueCourtesyCreditCommand`, `CancelOrderCommand`), **Domain Events** (`CourtesyCreditIssued`, `OrderCancelled`), payload serialization contracts, and domain invariant rules.  
   - **Consumer:** Submodule **`ferrous-api`** imports `ferrous-cqrses` as a Rust crate dependency. Inside its Axum API handlers, `ferrous-api` instantiates typed Command Events from incoming requests and publishes them directly to the event bus / intake topic (`orders/intake`) managed by **`ferrous-streaming`**.
2. **Command Handler Service & Container (`src/bin/server.rs` + `Dockerfile`):**  
   A compiled, standalone Rust microservice container that subscribes to inbound Command Events from Apache Iggy:
   - Consumes command payloads sequentially partitioned by entity ID (`order_id`).
   - Rehydrates the **`OrderAggregate`** root from historical event streams.
   - Enforces all business invariants (e.g. strict **$20.00 / R$ 20,00 courtesy credit cap**, valid lifecycle state transitions, idempotency checks).
   - Produces and sends validated state snapshots as an **Aggregate / Projection** stream to **Arroyo Engine**.
   - **Arroyo** consumes these projections and materializes them into **Delta Lake** (`orders_current_state` and `orders_raw_events`) via 2PC Parquet commits.

---

## 🏛️ Architecture Overview

```mermaid
flowchart TD
    subgraph Library_Crate ["1. ferrous-cqrses (Rust Library Crate: src/lib.rs)"]
        CmdTypes["Typed Command Events<br/>• IssueCourtesyCreditCommand<br/>• CancelOrderCommand"]
        DomainEvents["Typed Domain Events<br/>• CourtesyCreditIssued<br/>• OrderCancelled"]
        AggregateModel["OrderAggregate Model<br/>• Invariant Rules & Event Folding"]
    end

    subgraph API_Tier ["2. ferrous-api (API Gateway)"]
        APIHandler["Axum API Handler<br/>(Imports ferrous-cqrses crate)"]
        APIHandler -->|1. Construct Typed Command Event| CmdTypes
    end

    subgraph Streaming_Infra ["3. ferrous-streaming (Streaming Infrastructure)"]
        IggyIntake[("Apache Iggy Broker<br/>Topic: orders/intake (WAL)<br/>(Key: order_id, fsync &lt;1ms)")]
    end

    APIHandler -->|2. Send Command Event to Event Bus| IggyIntake

    subgraph Service_Container ["4. ferrous-cqrses Container (Command Handler Service)"]
        Consumer["Iggy Consumer Group<br/>(cg-cmd-handler)"]
        Validator["Payload & Invariant Validator<br/>• Verify $20 Credit Cap<br/>• Rehydrate OrderAggregate"]
        AggProjector["Aggregate / Projection Emitter<br/>(State Image & Audit Facts)"]
    end

    IggyIntake -->|3. Consume Command Stream| Consumer
    Consumer --> Validator
    Validator --> AggProjector

    subgraph Lakehouse_Materialization ["5. Arroyo & Delta Lake Storage"]
        Arroyo["Arroyo Engine<br/>(Stream Windowing & Projection Sinks)"]
        DeltaState[("Delta Lake<br/>orders_current_state (Parquet)")]
        DeltaRaw[("Delta Lake<br/>orders_raw_events (Parquet)")]
    end

    AggProjector -->|4. Send Aggregate / Projection| Arroyo
    Arroyo -->|5. 2PC Snapshot Commit| DeltaState
    Arroyo -->|5. 2PC Append Raw Audit| DeltaRaw
```

---

## ⚡ Key Capabilities

1. **Shared Library Crate (`src/lib.rs`):**
   * Exposes canonical Rust structs for all Command Events with Serde JSON/binary serialization support.
   * Eliminates contract drift between the API gateway (`ferrous-api`) and the background command processor.
2. **Business Invariant Enforcement:**
   * Pure compiled Rust domain execution guaranteeing sub-microsecond validation.
   * Enforces business policies: max courtesy credit of $20.00 / R$ 20,00 per order (`courtesy_credit_cents <= 2000`), valid state transitions, and idempotent deduplication via `command_id` (`UUIDv7`).
3. **Sequential Partition Processing:**
   * The Command Handler subscribes to Iggy using partitions hashed on `order_id`. This guarantees strict per-order serial execution without distributed locks.
4. **Arroyo Aggregate & Projection Pipeline:**
   * State updates are formatted as structured Aggregate Projections and emitted to Arroyo.
   * Arroyo uses 2PC commits to write partitioned Parquet records into Delta Lake, creating an up-to-date queryable read model for Apache DataFusion.

---

## 📁 Repository Layout

```text
ferrous-cqrses/
├── .github/workflows/
│   └── devsecops-ci.yml        # Tier 1 Rust CI (clippy, cargo-audit, test)
├── .pre-commit-config.yaml      # Pre-commit hooks (cargo fmt, clippy, gitleaks)
├── Dockerfile                   # Multi-stage container build for Command Handler service
├── Cargo.toml                   # Dual library & binary crate definition
├── src/
│   ├── lib.rs                   # Library crate entrypoint exporting commands & events
│   ├── commands/                # Typed Command Event definitions
│   │   ├── mod.rs
│   │   ├── credit.rs            # IssueCourtesyCreditCommand
│   │   └── dispute.rs           # FlagDisputeCommand, CancelOrderCommand
│   ├── events/                  # Validated Domain Event definitions
│   │   ├── mod.rs
│   │   └── order_events.rs      # CourtesyCreditIssued, OrderStatusUpdated
│   ├── aggregate/               # Event Sourcing aggregate root
│   │   ├── mod.rs
│   │   ├── order.rs             # OrderAggregate with invariant logic ($20 cap)
│   │   └── rehydrate.rs         # Event folding engine
│   ├── projection/              # Aggregate Projection bridges for Arroyo
│   │   ├── mod.rs
│   │   └── arroyo_sink.rs       # Formats and sends state images to Arroyo
│   ├── consumer/                # Iggy TCP consumer for orders/intake
│   │   ├── mod.rs
│   │   └── handler.rs           # Command execution loop & ACK coordinator
│   └── bin/
│       └── server.rs            # Main binary entrypoint for the Docker service
└── README.md                    # Submodule technical documentation
```

---

## 🛠️ Standalone Developer Quickstart

### Prerequisites
* **Rust:** `1.82+` (`rustup update stable`)
* **Docker:** For building the Command Handler container image

### 1. Build and Run Unit Tests
```bash
# Compile library and command handler binary
cargo build --release

# Run invariant and event-folding unit tests
cargo test --all-targets --verbose
```

### 2. Build Container Image
```bash
docker build -t ferrous-cqrses:latest .
```

---

## 🛡️ DevSecOps & Quality Assurance (Tier 1 Rust)

* **Strict Linting:** `cargo clippy --all-targets -- -D warnings`
* **Supply Chain Security:** `cargo audit` and `cargo deny check`
* **Static Analysis:** `semgrep` with OWASP Top 10 rulesets
* **Pre-commit Hooks:** Enforces code formatting and secret scanning (`gitleaks`) before git commits.
