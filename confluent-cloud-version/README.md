# Cosmos DB → Confluent Cloud → Flink (No Local Setup)

A fully managed version of the Cosmos → Kafka → Flink streaming lab. Everything runs in the cloud — no Docker, no local installs, just a browser and a few config screens.

---

## Architecture

```
   Azure                         Confluent Cloud (on Azure)
   ─────                         ──────────────────────────
                                                                            
   ┌──────────────┐  change      ┌─────────────────────────┐
   │  Cosmos DB   │──feed───────►│  Cosmos Source V2       │
   │  (NoSQL API) │  (HTTPS)     │  Managed Connector      │
   └──────────────┘              └────────────┬────────────┘
                                              │
                                              ▼
                                 ┌─────────────────────────┐
                                 │  Kafka topic            │
                                 │  cosmos.orders          │
                                 └────────────┬────────────┘
                                              │
                                              ▼
                                 ┌─────────────────────────┐
                                 │  Flink Compute Pool     │
                                 │  (serverless SQL)       │
                                 └────────────┬────────────┘
                                              │
                                              ▼
                                 ┌─────────────────────────┐
                                 │  Kafka topic            │
                                 │  orders.windowed_5m     │
                                 └─────────────────────────┘
```

Same shape as the local Docker version. Different operator.

---

## Prerequisites

- **A personal Azure subscription** (Pay-As-You-Go is fine, free trial works).
- **A web browser.** That's it.

You'll spin up:
- A Cosmos DB for NoSQL account (free tier — $0)
- A Confluent Cloud organization (free trial credits cover the lab)

**Important — keep these in the same Azure region.** Cross-region Cosmos ↔ Confluent traffic is slow, costs money, and burns RUs. Use East US, West US 2, or whatever's closest.

---

## Step 1 — Create the Cosmos DB account

In the Azure portal:

1. **Create a resource → Azure Cosmos DB → Azure Cosmos DB for NoSQL.**
2. **Apply Free Tier Discount:** Yes (saves you 1000 RU/s + 25 GB forever).
3. Region: pick one and remember it.
4. After provisioning, open **Data Explorer** and create:
   - Database `lab` with **Manual throughput 400 RU/s** (free tier covers this).
   - Container `orders` with partition key `/customerId`.
5. Go to **Settings → Keys** and copy:
   - URI (e.g. `https://my-cosmos-acct.documents.azure.com:443/`)
   - Primary Key

Leave this tab open — you'll need both values shortly.

**Optional (but more production-realistic):** instead of using the master key, create a Microsoft Entra ID service principal with the *Cosmos DB Built-in Data Reader* role on the account. The V2 connector supports SP auth via client secret. For first-time learning, master key is fine.

---

## Step 2 — Sign up for Confluent Cloud via Azure Marketplace

Going through the marketplace gets you the larger credit balance and unified billing.

1. Azure portal → **Marketplace → search "Apache Kafka & Apache Flink on Confluent Cloud"**.
2. **Subscribe.** Pick a plan (Pay-As-You-Go), resource group, name, region (same as Cosmos).
3. After provisioning, click into the resource → **Overview → "Open Confluent Cloud"** (single sign-on bridges you over).
4. In Confluent Cloud, **redeem the $600 promo code** if it's offered to you (Billing → Promo codes). The $400 instant credit is already applied.

You're now in the Confluent Cloud console. The mental hierarchy is:

```
Organization
└── Environment (think "subscription")
    └── Cluster (the actual Kafka cluster)
        └── Topics
        └── Connectors
        └── Schema Registry
    └── Flink Compute Pool
```

---

## Step 3 — Create a Kafka cluster

1. **Environments → default → Add cluster → Basic** (cheapest tier, fine for learning).
2. Cloud: **Azure**, Region: same as Cosmos, Availability: Single Zone.
3. Name it `lab-cluster`. Launch.

The cluster comes up in ~30 seconds. While you're here:

4. **Cluster → API Keys → Add key → Global access** (for learning only — never do this in prod). Save the key and secret somewhere safe; you'll need them for the connector.
5. **Cluster → Topics → Create topic**: name `cosmos.orders`, partitions 1, defaults for the rest. *(The connector can auto-create this, but creating it manually makes the data flow more visible.)*

---

## Step 4 — Create the Cosmos Source V2 connector

This is the equivalent of submitting that JSON config to Kafka Connect in the Docker version, but with a UI form.

1. **Connectors → Add connector → search "Azure Cosmos DB Source V2".**
2. **Topic selection:** select `cosmos.orders`.
3. **Authentication:**
   - Kafka credentials: select the API key/secret you just created.
   - Cosmos auth type: `MasterKey` (or `ServicePrincipal` if you set that up).
   - Cosmos endpoint: paste your URI from Step 1.
   - Cosmos master key: paste your primary key.
4. **Configuration:**
   - Database name: `lab`
   - Containers config (JSON): `[{"database": "lab", "container": "orders"}]`
   - Topic-container mapping: `cosmos.orders#orders`
   - Change feed start from: `Beginning` (so you can ingest the test data you've already added)
   - Output format: `JSON`
5. **Sizing:** 1 task is fine.
6. **Review and Launch.**

Wait for the connector status to flip to **Running**. If it goes to **Failed**, the Errors tab shows what's wrong — usually wrong key, wrong endpoint, or networking (Cosmos firewall blocking Confluent Cloud's IPs).

> **Cosmos firewall gotcha:** if your Cosmos account has "Selected networks" enabled under Networking, Confluent Cloud can't reach it. Either set it to "All networks" (fine for a personal lab account) or add Confluent Cloud's egress IPs to the allow-list (printed in the connector error message).

---

## Step 5 — Insert test data and verify the flow

In Cosmos Data Explorer (`orders` container → Items → New Item), add a few:

```json
{ "id": "order-001", "customerId": "cust-A", "amount": 49.99,  "items": 2, "ts": "2026-04-29T10:15:00Z" }
```
```json
{ "id": "order-002", "customerId": "cust-B", "amount": 120.50, "items": 5, "ts": "2026-04-29T10:16:30Z" }
```
```json
{ "id": "order-003", "customerId": "cust-A", "amount": 15.00,  "items": 1, "ts": "2026-04-29T10:17:45Z" }
```

In Confluent Cloud → **Topics → cosmos.orders → Messages**, you should see the events land within a couple of seconds. Each message has Cosmos metadata fields (`_rid`, `_etag`, `_ts`, `_lsn`) appended — same as the local version. That's the change feed payload format.

---

## Step 6 — Create a Flink Compute Pool

Flink Compute Pools are billed per CFU-minute when queries are running. For learning, the smallest pool is plenty.

1. **Environment → Flink → Compute pools → Create.**
2. Region: same as Cosmos and Kafka.
3. Max CFUs: **5** (default minimum, scales to zero when idle).
4. Name: `lab-flink-pool`. Create.

---

## Step 7 — Run Flink SQL in the Workspace

1. **Flink → Workspaces → Create workspace** → pick `lab-flink-pool`.
2. The workspace is a SQL editor. Your Kafka topics already appear as Flink tables (this is the magic — no `CREATE TABLE` boilerplate for sources).

Run this to verify:

```sql
SHOW TABLES;
-- You should see: cosmos.orders
```

```sql
DESCRIBE `cosmos.orders`;
-- Confluent infers the schema from the first messages.
```

```sql
SELECT * FROM `cosmos.orders` LIMIT 10;
-- Click "Run" — you'll see a streaming result that updates as new events arrive.
```

Now create the windowed aggregation:

```sql
-- Sink table for the windowed output. Confluent Cloud will create
-- the underlying Kafka topic automatically.
CREATE TABLE orders_windowed_5m (
  customerId    STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  order_count   BIGINT,
  total_amount  DOUBLE
);
```

```sql
-- The streaming job: 5-minute tumbling window per customer.
-- Note: 'ts' here is the field from your Cosmos document. If Flink
-- inferred it as STRING, you'll need to CAST it. The DESCRIBE output
-- tells you what types Confluent inferred.
INSERT INTO orders_windowed_5m
SELECT
  customerId,
  window_start,
  window_end,
  COUNT(*)    AS order_count,
  SUM(amount) AS total_amount
FROM TABLE(
  TUMBLE(
    TABLE `cosmos.orders`,
    DESCRIPTOR(`$rowtime`),       -- Confluent's built-in event-time column
    INTERVAL '5' MINUTES))
GROUP BY customerId, window_start, window_end;
```

A few things worth calling out:

- **`$rowtime` is a Confluent Cloud convenience.** Every table has it; it's based on the Kafka record timestamp. It saves you defining a watermark explicitly. (If you want event-time from your `ts` field instead, you can declare it — just like the Docker version.)
- **The `INSERT INTO` runs as a long-running statement.** Confluent calls these "statements" rather than "jobs" but they're the same concept — they keep running until you stop them.
- **You can see the live DAG and metrics** under Flink → Statements → click your statement.

To watch the output:

```sql
SELECT * FROM orders_windowed_5m;
```

Insert a few more orders in Cosmos (different `customerId`, different `ts` values across multiple windows). Watch the windows close and aggregates flow through.

---

## Step 8 — Stop the meter when you're done

This part matters — Confluent Cloud will happily keep your connectors and statements running and burn through credits.

In rough order of "biggest cost":

1. **Stop the Flink statement** (Flink → Statements → Stop). Resumable later from the same point.
2. **Pause or delete the Cosmos source connector** (Connectors → cosmos-source → Pause). Pausing keeps the offset; deleting forces a re-snapshot next time.
3. **Optionally delete the Kafka cluster** (cheapest tier is essentially free at idle, so usually fine to leave).
4. **Delete the Flink Compute Pool** if you're done for the week — pools at zero CFU still have a small base cost.

To wipe the slate clean: delete the whole Confluent environment and the Cosmos `lab` database.

---

## What you've actually learned

| Concept                           | Where you saw it                                              |
|-----------------------------------|---------------------------------------------------------------|
| Change feed → Kafka topic         | Cosmos source connector V2                                    |
| Schema-on-read for stream events  | Flink auto-inferred schema in the workspace                   |
| Event-time windowing              | `TUMBLE` with `$rowtime` descriptor                           |
| Watermarks                        | Implicit in `$rowtime`; explicit if you bring your own        |
| Stateful aggregation              | `COUNT/SUM` over windowed group                               |
| Streaming SQL semantics           | `INSERT INTO ... SELECT` runs forever, not "complete-and-exit"|
| Kafka as durable buffer           | Pause Flink, restart, watch it resume from offset             |
| Connector ↔ Kafka ↔ Flink topology| The whole architecture, end to end                            |

This is exactly the architecture you'll see at most companies running real-time analytics on Cosmos data. The Docker version teaches you the same thing — Confluent Cloud just hides the operations.

---

## Next things to try (Confluent-Cloud-specific)

1. **Flink Workspace versions.** Save your SQL as a "model" in the workspace; modify it; run a v2 alongside v1. This is how you'd do blue/green deploys with savepoints in production.
2. **Tableflow.** Confluent Cloud can mirror any Kafka topic as an Iceberg table in your own ADLS Gen2. Try it on `cosmos.orders` — gets you a queryable historical archive without writing any sink connector.
3. **Schema Registry.** Promote one of your topics to use Avro instead of JSON. Watch what happens when you change a field type without proper schema evolution.
4. **A second source.** Add a `customers` container in Cosmos with a second connector instance. Join `cosmos.orders` with `cosmos.customers` in Flink SQL.
5. **Vector search.** Confluent Cloud's Flink integrates with Cosmos DB as a vector store backend for RAG/semantic search. If you want to extend into the AI side, this is where it lives.

---

## When to graduate back to self-hosted

Confluent Cloud is the right call when you want to ship something. But it's worth understanding when other companies pick the alternative:

| Reason                                 | What they pick instead                  |
|----------------------------------------|-----------------------------------------|
| Strict data residency / private VNet   | Self-hosted Flink on AKS                |
| High volume — connector + Flink costs  | Self-hosted, often Aiven for hybrid     |
| Already have Kafka somewhere else      | Self-hosted Flink against existing Kafka|
| Need DataStream API / CEP / custom UDFs| Self-hosted (Workspace is SQL-focused)  |
| Regulated industry, no SaaS allowed    | Confluent Platform on-prem, or AKS      |

For learning the architecture, none of those apply. Confluent Cloud is the path.
