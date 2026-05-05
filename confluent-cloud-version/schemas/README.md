# Schema Versions

This folder is where the JSON Schema versions captured during the lab's evolution should live.

## Expected files

When you've pulled your schemas from Confluent Cloud Schema Registry, they should land here as:

```
cosmos.orders-value.v1.json    ← Initial inferred schema (probably from placeholder doc)
cosmos.orders-value.v2.json    ← First evolution attempt
cosmos.orders-value.v3.json    ← The "amount as number" fix
cosmos.orders-value.v4.json    ← Final working schema (with items, ts, etc.)
```

The version history is more interesting than just the final schema — it tells the story of the debugging journey through `BACKWARD` compatibility constraints.

## How to capture them

In Confluent Cloud:

1. **Environment → Schema Registry → Schemas**
2. Click into `cosmos.orders-value`
3. Use the version selector to view each version
4. Copy the JSON definition for each version
5. Save here with the naming convention above

Schemas don't contain secrets, so they're safe to commit publicly.
