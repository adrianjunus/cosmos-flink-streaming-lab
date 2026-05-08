# cosmos-flink-streaming-lab

![Validate](https://github.com/adrianjunus/cosmos-flink-streaming-lab/actions/workflows/validate.yml/badge.svg)

A hands-on exploration of real-time streaming analytics on Azure: ingesting Cosmos DB change feed events through Apache Kafka and processing them with Apache Flink. Built as a personal learning project to understand the architecture, tradeoffs, and failure modes of production streaming systems.

- **`confluent-cloud-version/`** — the same architecture on Confluent Cloud's managed Flink and Kafka offerings, hosted on Azure. Closer to production reality.

---

## Architecture

```
   Azure                          Streaming Layer                       Outputs
   ─────                          ───────────────                       ───────

   ┌──────────────┐  change feed  ┌─────────────────┐
   │  Cosmos DB   │──────────────►│  Cosmos Source  │
   │  (NoSQL API) │  (HTTPS)      │  Connector      │
   └──────────────┘               └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │  Kafka topic    │
                                  │  cosmos.orders  │
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐         ┌──────────────────┐
                                  │  Flink SQL job  │────────►│  orders.windowed │
                                  │  (5-min tumble) │         │  per-customer    │
                                  └─────────────────┘         │  aggregations    │
                                                              └──────────────────┘
```

Cosmos DB acts as the operational data store. Its change feed emits an event for every insert and update, which a Kafka Connect connector ships into a Kafka topic. From there, a Flink SQL statement performs windowed aggregation per customer and writes results back to Kafka. The same architecture, just with different operators of the underlying infrastructure between the two implementations.

---

## Why this architecture

**Kafka as a durable event log, not a buffer.** A traditional database (Postgres, even Cosmos itself) is optimized for storing current state and answering point queries. Kafka is optimized for sequential append at high throughput, multiple independent consumers reading at their own pace, replay from arbitrary offsets, and aging out old data via retention. Trying to serve a streaming workload from a database creates contention between writers and readers, expensive replay queries, and brittle multi-consumer fan-out. I chose Kafka as it was designed for exactly this shape of workload with the necessary quality of life features in mind.

**Flink as streaming-native compute, not micro-batch.** Flink processes records one at a time through its operator graph, maintains operator state as a first-class concern, supports event-time watermarks for handling out-of-order events, and provides exactly-once semantics through coordinated checkpointing of state and Kafka offsets. I formerly used much of Spark Structured Streaming which can do similar work in micro-batch mode, but Flink's streaming-first runtime is better suited to low-latency stateful workloads. The two are competing engines, not layered ones.

**Cosmos change feed as a replayable source of truth.** The change feed retains a configurable window of every change, which means recovery from downstream errors usually means "fix the code and replay from source," not "manually reconstruct lost data." This makes retention windows — both at Cosmos and in Kafka — a first-class architectural decision rather than a cost-only knob.

The longer-form reasoning for these decisions, plus the operational concerns (failure handling, schema evolution, retention as a recovery-time budget) is in [`docs/architecture.md`](docs/architecture.md).

---

## What's in the repo

```
cosmos-flink-streaming-lab/
│
├── README.md                              ← You are here
├── LICENSE                                ← MIT
├── .gitignore
│
├── confluent-cloud-version/               ← Managed Confluent Cloud implementation
│   ├── README.md                          ← Setup walkthrough using Azure Marketplace
│   ├── connector-config/                  ← Cosmos Source V2 connector config (redacted)
│   ├── schemas/                           ← JSON Schema versions captured during evolution
│   └── flink-sql/orders_window.sql        ← Flink SQL adapted for Confluent Cloud
│
└── docs/
    └── architecture.md                    ← Architecture decisions and tradeoffs
```

---

## Getting started

Pick the implementation that matches your situation:

**Use `confluent-cloud-version/` if you want to:**
- Skip local infrastructure entirely
- See the architecture as it appears in actual production deployments
- Try the managed Cosmos Source V2 connector (Kafka-native offsets, no lease container)

Requires an Azure subscription and a Confluent Cloud account. New Confluent Cloud organizations get free trial credits that cover the lab easily.

Each subfolder has its own README with step-by-step setup. They're independent — pick one or work through both.

---

## Cost notes

**Confluent Cloud version:** New accounts get $400 of free credits, plus $600 more via Azure Marketplace promo code. A focused weekend of learning uses $5–15. The traps are leaving Flink statements running (~$0.21 per CFU-hour) or forgetting to pause connectors. Set a budget alert.

Cosmos DB free tier covers both implementations.

---

## Technologies

- **Azure Cosmos DB for NoSQL** — operational document store with change feed
- **Apache Kafka** — distributed event log
- **Kafka Connect** — connector framework (Microsoft Cosmos source V1 locally, Confluent V2 managed)
- **Apache Flink** — streaming-native compute engine
- **JSON Schema + Confluent Schema Registry** — message format and contract management
- **Docker Compose** (local version) — local orchestration
- **Confluent Cloud** (managed version) — Azure-hosted Kafka and Flink as a service

---

## What this lab demonstrates

Beyond getting a streaming pipeline working, the project covers a number of architectural concerns that show up in production streaming systems:

- **Schema evolution under compatibility constraints.** Working through real Schema Registry compatibility errors (`PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`, `TYPE_NARROWED`) under `BACKWARD` mode, and the relax-evolve-retighten pattern that production teams use for deliberate schema changes.

- **The bronze/silver/DLQ medallion pattern for streaming.** Splitting the pipeline into a permissive ingestion layer, a strictly-typed validated layer, and a dead-letter path for records that fail validation — and why the validation logic typically lives in Flink rather than at the connector.

- **Retention as a recovery-time budget.** Setting topic and change-feed retention based on a documented "time to detect, diagnose, fix, plus margin" calculation, rather than as a default to be tuned later.

- **The replay-from-source recovery model.** Why production teams don't use DLQs as a primary recovery mechanism, and instead rely on durable replayable sources (Cosmos change feed, long-retention Kafka, lakehouse mirrors) to rebuild state after errors.

- **Trade-offs between JSON Schema and Avro for streaming serialization.** When the per-record re-inference behavior of the JSON Schema converter creates compatibility headaches, and why Avro is typically preferred for high-evolution streaming workloads despite being less debuggable.

The reasoning behind each of these is in [`docs/architecture.md`](docs/architecture.md).

---

## CI/CD

The repo includes two GitHub Actions workflows:

- **`.github/workflows/validate.yml`** runs on every push and pull request. It validates JSON syntax across connector configs and schemas, checks the `docker-compose.yml` for syntax errors, lints Markdown for consistency, scans for accidentally-committed secrets, and sanity-checks the Flink SQL files. This is real CI that runs against the repo.

- **`.github/workflows/schema-compatibility-check.yml`** is an illustrative scaffold of how schema compatibility checking against a real Schema Registry would work in a production version of this architecture. It's set to manual trigger only because the repo has no live Schema Registry to validate against. The file documents how it would be wired up if connected to a real registry.

The architecture doc's "How this would be productionized" section walks through the full CI/CD picture for a production streaming pipeline — schema management, connector deploys, savepoint-based Flink job migration, infrastructure-as-code, and secret management.

---

## License

MIT — see [`LICENSE`](LICENSE).
