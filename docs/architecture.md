# Architecture Decisions and Tradeoffs

This document captures the reasoning behind the technology choices and design patterns in `cosmos-flink-streaming-lab`. The actual setup steps are in the per-implementation READMEs; this doc covers *why* the architecture looks the way it does.

---

## The core question: why these tools at all?

Anyone setting up a streaming pipeline first has to answer "why not just use a database?" Cosmos already has a change feed, you could just have downstream services subscribe to it. Why insert Kafka and Flink in the middle?

The honest answer is that databases and streaming engines optimize for different workloads, and trying to make a database serve a streaming workload — or vice versa — produces brittle, expensive systems. Three workload characteristics drive the architectural separation:

**High-throughput sequential append.** A database insert acquires row locks, updates indexes, fsyncs to a transaction log, and triggers any associated triggers. This is the right behavior for transactional workloads but it's expensive bookkeeping for an append-only event stream. Kafka's storage model is fundamentally a sequential file append per partition — no indexes, no row-level locking, no transaction semantics. A single broker can handle hundreds of thousands of writes per second per partition because the operation is structurally simpler than a database insert.

**Multiple independent consumers.** If five downstream services need to react to events, a database forces you to either (a) have all five poll the same table, creating contention with the writers, or (b) build fan-out infrastructure with notification channels and per-consumer offset tracking. Kafka treats this as a first-class concept: each consumer group has its own offset per partition, tracked by Kafka itself, and consumers don't compete with each other or with producers.

**Replay as a normal operation.** Reprocessing a week of events from a database means a `SELECT WHERE timestamp > now() - 7 days` against a table that's also accepting new writes. The replay query competes with production writes, locks pages, and blows out the buffer cache. In Kafka, replay is the standard read pattern — saying "read from offset 0" is no more expensive than "read from latest." Retention windows make this a routine recovery operation rather than a database-busting incident.

**The mental model worth carrying:** databases store *tables* (current state). Kafka stores *logs* (the history of how state evolved). These are dual representations of the same information. Modern data architectures often use both — Kafka as the durable event log, databases as queryable materialized views derived from the log.

---

## Why Flink instead of Spark Structured Streaming

Both Spark Structured Streaming and Flink can consume from Kafka, both support SQL, and both can write to a lakehouse. So why pick one over the other?

The deepest difference is the processing model:

- **Spark Structured Streaming** is micro-batch streaming built on top of Spark's batch engine. It pulls a chunk of new data every few seconds (configurable down to ~hundreds of milliseconds) and processes it as a small batch. Spark was a batch engine first and added streaming as an extension.

- **Flink** is streaming-native. Records flow through the operator graph one at a time, state is a first-class managed concept, event-time watermarks are built into the runtime, and exactly-once semantics come from coordinated checkpointing of operator state and Kafka offsets. Flink was a streaming engine first; batch is a special case (a bounded stream).

This architectural difference produces practical tradeoffs:

| Concern | Spark Structured Streaming | Flink |
|---|---|---|
| Minimum latency | Seconds | Milliseconds |
| State management | Limited, rebuilt per batch | Large, persistent, key-sharded |
| Watermark handling | Supported, less flexible | First-class, more sophisticated |
| Exactly-once | At-least-once + dedup tricks | True exactly-once via Chandy-Lamport snapshots |
| Job lifecycle | Submit-and-complete | Long-running stateful processes |
| Fit for batch ETL | Excellent | Possible but not idiomatic |
| Fit for real-time CEP | Awkward | Native |

For a Cosmos change-feed-driven pipeline with windowed aggregations, both could work. Flink wins on latency and state model; Spark wins on ecosystem maturity and batch interop. Most production systems run both — Spark for batch and ML on the lakehouse, Flink for low-latency streaming on Kafka. Picking Flink for this lab specifically was about exposing the streaming-native primitives (event-time windows, watermarks, exactly-once) directly rather than seeing them through a micro-batch abstraction.

---

## Schema management: the lessons that hurt the most

The hardest engineering work in this lab wasn't building the pipeline — it was managing the JSON Schema contract between the Cosmos source connector and Schema Registry. Several lessons emerged that are worth recording carefully.

### The connector silently dropped records on schema failures

Confluent Cloud's Cosmos Source connector, configured with `errors.tolerance: all`, will drop records that fail to serialize and continue processing rather than halting. This is the right default for production durability — one bad record shouldn't take down the pipeline — but it has a consequence that's not obvious from the UI: the connector status remains `RUNNING` while no data flows.

The diagnostic workflow that actually surfaces the problem:

1. Connector status alone is insufficient — check the **Tasks** view for individual task state
2. Errors are logged but not surfaced in the main UI — check the connector's **Logs** tab
3. The ground truth of "is data flowing" is the topic itself — check **Topics → Messages → Jump to latest**, not Flink (Flink is downstream of where the failure happens)

The architectural lesson: a "Running" badge is a status indicator about the connector process, not a guarantee about end-to-end data flow. Always verify with topic-level metrics or messages.

### JSON Schema's per-record re-inference creates compatibility battles

The JSON Schema converter in Kafka Connect re-infers a schema from each record's shape and tries to register it with Schema Registry. This is fundamentally different from Avro, which serializes against a pre-registered schema and rejects non-conforming records at the producer.

The consequence: under `BACKWARD` compatibility, every minor variation in source data shape — a missing optional field, an integer where another record had a decimal, a new field appearing — triggers a schema-registration attempt that may fail compatibility checks. This produces failure modes like:

- `TYPE_NARROWED` when a new record has `amount: 16` (inferred as integer) but the schema declared `amount: number`
- `PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL` when a new record has a field the previous schema didn't declare, even if the schema was open

These rules are correct from a strict consumer-safety perspective but they fight against the messy reality of source data from a document database. JSON Schema's compatibility semantics are notably stricter than Avro's, which makes Avro the more pragmatic choice for high-evolution streaming workloads despite being less human-readable in raw form.

### Compatibility evolution: the relax-evolve-retighten pattern

When a schema change is intentional and known-safe, the production pattern is:

1. Set compatibility mode to `NONE` temporarily (or use a more permissive mode like `FORWARD` if it accepts the change)
2. Register the new schema version
3. Replay any records that were dropped during the broken-schema window
4. Restore strict compatibility (`BACKWARD`) for future changes

In production, this would happen via a code review and deployment pipeline rather than UI clicks — schemas live in Git as `.json` or `.avsc` files, CI runs the compatibility check before merge, and Schema Registry updates are deployed via Terraform or similar tooling. The relax-evolve-retighten pattern itself is the same; the difference is process discipline.

### The deeper architectural answer: keep ingestion permissive

The cleaner production pattern, and what a redesigned version of this lab would implement, is the medallion architecture for streaming:

```
Cosmos ──► Connector ──► bronze topic (permissive) ──► Flink validation ──┬─► silver (clean)
              │             string-typed or                               └─► dlq (rejected)
              │             schema-less
              ▼
       (no schema battles)
```

The connector's job becomes "get bytes into Kafka, no questions asked." All schema enforcement, type coercion, and validation happens in Flink, where SQL is more expressive and error handling is more controllable. The bronze topic absorbs the messiness of source data variability; the silver topic becomes the contract surface for downstream consumers.

This is the architectural shift that resolves most of the schema-battle pain experienced in this lab. The connector should not be the validation layer — Flink should be.

---

## Retention as a recovery-time budget

Retention windows in streaming systems are commonly treated as a cost-only decision: more retention costs more storage, so set it to the cheapest viable value. This framing misses what retention actually does.

Retention is a deadline for incident response: "we have N days to notice a problem and fix it before recovery becomes painful." So the right way to set retention is:

```
retention >= time_to_detect + time_to_diagnose + time_to_fix + safety_margin
```

Each term forces a real engineering question:

- **Time to detect** is your monitoring story — DLQ depth alerts, lag monitoring, output validation
- **Time to diagnose** is your observability — can you trace from a bad output back to a source offset?
- **Time to fix** is your engineering process — schema-as-code with CI, automated deploys
- **Safety margin** is your hedge against the above estimates being optimistic (they always are)

For a team starting out, realistic numbers might be 1 day to detect, 2 days to diagnose, 1 day to fix, 3 days margin = 7 days minimum retention. Mature teams with strong tooling can run on 3-day retention. Teams with weaker tooling may need 30+ days.

There are also multiple retention windows in any streaming pipeline, and the real recovery budget is bounded by the *shortest* one:

- Cosmos change feed retention (default ~7 days, configurable)
- Bronze Kafka topic retention
- Silver/gold topic retention (often shorter — they're derivatives)
- DLQ retention
- Cosmos point-in-time-restore (PITR) backup window

Production architectures typically run a tiered strategy: short-retention Kafka for the hot replay window, long-retention Iceberg/lakehouse mirrors for cold replay, and Cosmos PITR as the ultimate fallback. This is partly why Confluent's Tableflow, Apache Iceberg, and similar lakehouse-on-Kafka technologies have momentum — they make long-retention "replay from Kafka" affordable enough to be the default.

---

## Recovery model: replay from source, not from DLQ

A natural assumption when a streaming pipeline rejects records is "we'll fix the bad records and replay them from the dead-letter queue." At small scale this works. At production scale it doesn't — manually fixing thousands or millions of records is unsustainable, and treating DLQ as a backup creates an unbounded storage liability.

The production answer is that **the source is the recovery mechanism**, not the DLQ:

1. Identify the affected window (logs, DLQ timestamps, output anomaly detection)
2. Fix the code or schema (PR, review, deploy)
3. Reset the connector or downstream consumer offset to before the bad window
4. Let the corrected pipeline replay through
5. Verify with reconciliation queries

DLQ is for the long tail of records that can't be recovered by replay — surgical cases, not bulk recovery. The phrase that captures the production mindset: **reprocessing is normal, DLQ is for the residue. Build for the first sentence, instrument for the second.**

This makes "is the source replayable, and within what window?" one of the first architectural questions for any streaming pipeline. If the answer is no, the first job is usually to add a durable bronze landing zone — Kafka with long retention, or a lakehouse mirror — so that "replay from source" is always a viable recovery path.

---

## Cost optimization: cut scale, not shape

A natural pressure when productionizing a streaming pipeline at a budget-conscious client is to ask "what can we cut to reduce cost?" Answering this well requires distinguishing between scale and shape:

**Scale** = capacity. Throughput, partition counts, cluster tier, parallelism, replication factor. Cutting scale is fine — the architecture stays the same, the dials are turned down, and scaling up later is straightforward.

**Shape** = capability. Retention, monitoring, alerting, DLQ topics, schema enforcement, replayable sources. Cutting shape removes entire reliability layers. The pipeline still appears to function, but the recovery and observability stories are gone.

The pattern that holds up: **cut capacity, keep capability.**

| What's safe to cut for cost | What to keep regardless |
|---|---|
| Cluster tier (Basic instead of Dedicated) | Topic retention sized to recovery budget |
| Partition counts (1–3 vs 12–30) | DLQ topics and their retention |
| Flink CFU ceiling | Schema Registry compatibility enforcement |
| Connector task count | Monitoring and alerting |
| Multi-region replication | Backup of source-of-truth (PITR) |
| Tiered storage (until volume justifies it) | Schema-as-code in version control |
| Stream lineage / governance features | Documented runbooks for failure scenarios |

Retention specifically is often miscategorized as a cost knob. At small lab scale, retention costs are negligible — pennies per month for typical volumes. The cost trap is throughput (CFU-hours, connector hours), not storage. Cutting retention to save cost is usually trading real safety for theatrical savings.

A useful framing for the cost conversation: **the architecture is the same at any scale; only the dials change.** A small-scale production-grade pipeline scales up cleanly. A small-scale pipeline that cut retention and monitoring has to be re-architected to grow.

---

## What a v2 of this lab would look like

If I were extending this lab toward a more production-realistic implementation, the priorities would be:

**1. Bronze/silver/DLQ split in Flink.** Restructure the pipeline so the connector writes to a permissive bronze topic, and a Flink job validates and splits into silver and DLQ topics. This removes most of the schema-battle pain and demonstrates the standard production pattern.

**2. Tableflow / Iceberg sink for long retention.** Add a sink that mirrors the bronze topic into Apache Iceberg on ADLS. This is the architecture pattern most companies are converging on — short-retention hot Kafka plus long-retention cheap lakehouse for the bronze layer.

**3. Stateful join with a customers stream.** Add a second Cosmos container, a second source connector, and a Flink SQL join across the two streams. This exercises the stateful-join machinery and demonstrates a realistic enrichment pattern.

**4. Avro variant.** Implement the same pipeline with Avro instead of JSON Schema, and contrast the schema-evolution experience. Avro's evolution semantics are notably cleaner than JSON Schema's, and the contrast is the strongest argument for Avro.

**5. Reconciliation tooling.** A scheduled job (Airflow or similar) that checks invariants between Cosmos source counts and Flink output aggregations, alerting on drift. This closes the loop on detection-time being part of the retention budget.

These are all productive next steps that build on what's already in the repo without rewriting the foundation.

---

## How this would be productionized

The repo as it stands is a learning artifact: configuration files, SQL, and documentation. There's no "code" to deploy in the traditional sense. But if I were extending this lab into a production system at a real company, here's the CI/CD and operational tooling I would build, layer by layer.

### What gets versioned

In production, each of these would live in Git, with its own deployment pipeline:

- **Schema definitions** — Avro `.avsc` or JSON Schema files, one per topic subject
- **Connector configurations** — JSON, with secrets templated as `${VAR}` references
- **Flink SQL or Flink jobs** — declarative SQL files for simple pipelines, or Java/Python source for jobs needing the DataStream API
- **Topic definitions** — partition counts, retention, compatibility modes — typically as Terraform
- **Infrastructure** — Confluent Cloud cluster, Cosmos DB account, Flink compute pool — Terraform or Bicep
- **Observability artifacts** — Grafana dashboard JSON, alert configs, runbooks

The repo itself becomes a polyrepo or monorepo with these as distinct concerns, each with its own deployment story.

### Schema CI/CD: the most important layer

Schemas are the contract surface between producers and consumers, and schema changes are the most common cause of pipeline breakage. Schema CI catches at PR time the same kinds of `BACKWARD` compatibility errors that this lab encountered at runtime.

**On every pull request:**

- Validate JSON Schema or Avro syntax
- Pull the currently registered version from Schema Registry
- Run `compatibility check`: would the proposed schema be compatible with the registered one under the subject's compatibility mode?
- Fail the build with a clear error if any incompatibility is found
- Post the diff and the compatibility result as a PR comment for reviewer visibility

**On merge to main:**

- Register each updated schema as a new version with Schema Registry
- Tag the Git commit with the registered version IDs for traceability

The `.github/workflows/schema-compatibility-check.yml` file in this repo is an illustrative scaffold of what this workflow looks like.

### Connector config CI/CD

**On pull request:**

- Validate JSON syntax
- Lint for common mistakes — wrong converter classes, missing required properties, topic name conventions
- Optionally dry-run against a non-prod Connect cluster

**On deploy:**

- Resolve secret references from Azure Key Vault (or equivalent)
- Submit to Connect REST API: create or update the connector
- Wait for the connector to reach `RUNNING` status — but more importantly, verify each individual task is `RUNNING`. The lab demonstrated that connector status alone is misleading; tasks can fail silently while the connector reports healthy.
- Fail the deploy if tasks don't reach a healthy state within a timeout

### Flink job CI/CD

For declarative SQL pipelines:

- Parse the SQL with Flink's SQL parser to catch syntax errors at PR time
- Optionally deploy to a staging compute pool for integration testing against synthetic data
- Validate that referenced topics exist with compatible schemas

For stateful streaming jobs, deployment requires care that stateless services don't:

- **Take a savepoint of the running job** — a consistent snapshot of all operator state plus Kafka offsets
- **Deploy the new version** — submit the new job
- **Restore the new job from the savepoint** — preserves accumulated state, resumes from the same Kafka position
- **Verify the new statement is processing records** before considering the deploy successful

This savepoint dance is the most distinctive part of streaming CI/CD versus stateless service CI/CD. Flink jobs accumulate state over time (windowed aggregations, deduplication sets, join hash tables). You can't just kill and replace them without losing that state — savepoints are the mechanism for state migration across job versions.

### Infrastructure CI/CD

Terraform with a state backend (Azure Storage Account for the state file, locked via blob lease):

- `terraform plan` runs on every PR, with the plan posted as a PR comment so reviewers see what would change
- `terraform apply` runs on merge to main, gated by environment
- Multi-environment promotion: changes flow dev → staging → prod with explicit approval gates between stages
- Cost estimation (e.g., Infracost) runs on every PR to catch unintended expensive changes
- Destructive change detection: any `terraform plan` showing resource destruction in production triggers manual review

Confluent Cloud has a mature Terraform provider; Azure Cosmos DB does too. Both are well-supported.

### Secret management

Secrets never live in Git. The pattern across all layers:

- Connector configs reference `${COSMOS_MASTER_KEY}` rather than embedding the key
- Deployment-time substitution pulls from Azure Key Vault (or AWS Secrets Manager, GitHub Secrets, HashiCorp Vault)
- Confluent Cloud's native secrets management can also store these directly, with the CLI/API substituting at connector creation time
- Rotation is automated: keys regenerate on a schedule, and connectors pick up new values via redeploy

If a secret is ever committed accidentally, the recovery is two-part: rotate the secret in the source system *first*, then remove from Git history. By the time you've removed a leaked secret from a public repo, scanners and bots may have already harvested it.

### Observability deploys with code

Often forgotten in CI/CD discussions but critical for streaming systems: the dashboards, alerts, and runbooks that operators rely on must deploy alongside the code that produces the metrics.

- Grafana dashboard JSON in Git, deployed via the Grafana API
- Alert configurations (Prometheus AlertManager, PagerDuty, Datadog) versioned and deployed
- Runbooks (Markdown in Git) linked from alert payloads so on-call engineers can find them quickly

When a new metric is added in a Flink job, the dashboard update and alert update should land in the same PR. Otherwise the metric exists but no one sees it.

### Environment promotion

The standard pattern: dev → staging → prod, with each environment having its own:

- Confluent Cloud organization or environment
- Azure Cosmos DB account
- Flink compute pool
- Schema Registry (or at minimum, separate subjects per environment)

Schema changes promote first to dev with `NONE` compatibility for fast iteration, then to staging with `BACKWARD` to mirror production rules, then to production with manual approval. A schema change that fails compatibility in staging never reaches production.

### Canary deployments for stateful jobs

The most sophisticated production pattern, worth knowing about even if rarely implemented:

- Run the new version of a Flink job in parallel with the old one
- Both consume from the same source topic
- Both write to separate output topics
- Reconciliation queries compare outputs for a soak period
- When confidence is high, cut downstream consumers over to the new output

Hard to do well — requires careful key partitioning, deduplication on the cut-over, and good reconciliation tooling. Most teams use simpler blue-green via savepoints. But canary is the gold standard for high-stakes stateful changes.

### Data contracts: the emerging pattern

Beyond schemas, the industry is moving toward formal **data contracts** between producer and consumer teams. A contract includes:

- Schema (the syntactic interface)
- Semantic meaning (what does each field represent, what are the allowed values)
- SLAs (latency, completeness, freshness commitments)
- Lifecycle commitments (deprecation policy, breaking change process)
- Ownership (who to contact, escalation path)

These live in Git alongside schemas, get registered in a contract registry, and are validated in CI. Confluent and other vendors are building tooling around this. For a streaming pipeline at any meaningful scale, this is where the industry is heading.

### What this repo demonstrates about productionization

Even though this repo doesn't deploy anything, the architecture documentation and CI workflows in it demonstrate awareness of the production patterns:

- The validation workflow (`.github/workflows/validate.yml`) catches the basic class of issues (broken JSON, accidentally committed secrets, malformed compose files) at PR time
- The illustrative schema check workflow shows the most important production-specific CI pattern
- This document captures the end-to-end thinking about how a learning artifact would mature into a production system

The honest framing: this is documentation of how I'd productionize a streaming pipeline, paired with a working learning lab that exercises the underlying technologies. The repo is intentionally not pretending to be production code.

---

## Summary of architectural decisions

| Decision | Choice | Reasoning |
|---|---|---|
| Event log technology | Kafka | Sequential append, multi-consumer, replay-friendly |
| Stream processing engine | Flink | Streaming-native, strong state, exactly-once |
| Source change capture | Cosmos change feed | Replayable, native to source |
| Serialization format | JSON Schema (lab); Avro (recommended for prod) | JSON for debuggability; Avro for evolution |
| Schema enforcement layer | Connector (lab); Flink (recommended) | Connector-level fights re-inference; Flink-level is more controllable |
| Compatibility mode | `BACKWARD` with deliberate relax-retighten cycles | Safety by default, evolution as explicit operations |
| Retention strategy | Sized to documented detect-diagnose-fix budget | Recovery time, not storage cost, is the binding constraint |
| Recovery model | Replay from source, DLQ for residue | Bulk recovery from source; DLQ as observability surface |
| Cost optimization | Cut scale, not shape | Architecture preserves through scale changes |
