# Architecture Brief: Decree 13-Compliant Ride-Hailing CDC Lakehouse

## 1. Problem statement

A Vietnamese ride-hailing company streams production Oracle changes into a lakehouse for analytics. The scale is 100 million trips per year, with peak write bursts around 30,000 writes/sec from booking, GPS, pricing, payment, and driver state tables. Driver and rider PII such as phone number, citizen ID, and precise GPS is in scope for Decree 13/2023/ND-CP, so raw identifiers must not be readable by analysts by default. The analytics SLA is strict: operational dashboards must refresh within 60 seconds of source commit, while ad-hoc p95 queries over hot tables should stay below 1 second. Late events are normal because phones and regional networks disconnect. The hard part is not only ingestion volume; it is making late CDC, PII controls, lineage, auditability, and fast query layout work together without creating a brittle one-off pipeline.

## 2. Architecture diagram

```text
Oracle OLTP
  |
  | Debezium CDC: before/after/op/source_ts/lsn/table
  v
Kafka topics: trips, riders, drivers, payments, gps_points
  |
  | stream job: schema validation, deterministic tokenization, raw access deny-by-default
  v
Bronze Delta: cdc_raw_tokenized
  - partition: ingest_date, source_table
  - columns: pii tokens, encrypted PII pointer, op, lsn, source_ts, ingest_ts
  - CDF enabled, append-only audit trail
  |
  | MERGE WHEN MATCHED AND src.source_ts > tgt.source_ts
  | SCD2 for rider/driver dimensions, quarantine invalid schema/drift rows
  v
Silver Delta: trips_current, trips_scd2, drivers_scd2, gps_5s_snapshots
  - partition: event_date, city_id
  - Z-ORDER/cluster: tenant_id, trip_id, driver_token
  - row/column policies via catalog
  |
  | incremental aggregates every 1 min
  v
Gold Delta/Iceberg: city_ops_1m, tenant_cost_5m, fraud_features_daily
  - BI path: DuckDB/Trino/Spark reads tokenized views only
  - PII break-glass path: approved service reads encrypted pointer, writes pii_access_audit

Control plane:
  Catalog + data contracts + OpenLineage -> table/column lineage
  Maintenance jobs -> compact, cluster, vacuum/checkpoint, orphan sweep
  Alerting -> lag, stale CDC drops, schema drift, PII reads, file count
```

## 3. Key decisions and rejected alternatives

### Decision 1: Delta Lake for CDC-heavy Bronze/Silver

I chose **Delta Lake** for Bronze and Silver because the workload depends on high-volume `MERGE`, Change Data Feed, time travel, and operational compaction. CDC tables need deterministic replay from a source offset, and Delta CDF gives downstream Gold jobs a bounded incremental input instead of full-table rescans.

I rejected **plain Parquet folders** because they cannot protect table state during concurrent writes, cannot express row-level deletes/upserts safely, and make late CDC correction a pile of custom file rewrites. I rejected **Iceberg as the first CDC table format** because Iceberg is excellent for catalog interoperability and hidden partitioning, but Delta has the smoother local path here for CDF and frequent `MERGE` in the lab stack.

### Decision 2: Catalog as the governance control plane

I chose a **catalog-first layout**: every table is registered with owner, schema contract, retention, PII classification, and read policy. Analysts query catalog views, not storage paths. The catalog is also the place to attach column tags such as `pii.token`, `pii.encrypted_pointer`, `geo.precise`, and `public.aggregate`.

I rejected **path-based access to object storage** because a user with the bucket path can bypass column policies and read data outside approved views. I rejected **governance only in dashboard code** because notebooks, ad-hoc SQL, backfills, and model training would still become uncontrolled side doors.

### Decision 3: Tokenize PII at Bronze landing

I chose **deterministic HMAC tokenization before Bronze commit** for phone, citizen ID, rider ID, driver ID, and device ID. Analysts can join and group by stable tokens without seeing raw values. Raw PII is stored only as an encrypted pointer to a separate vault-like store, and any break-glass read writes to `pii_access_audit` with requester, reason, ticket, columns, row count, and timestamp.

I rejected **masking only in Silver** because raw PII would already be durable in Bronze and readable by anyone with low-level access. I rejected **one-way random tokenization** because fraud and support workflows need stable joins across events. I rejected **reversible encryption as the default analyst column** because it turns every query engine into a potential PII exposure point.

### Decision 4: Partition by event date and city; cluster for hot predicates

I chose **event_date + city_id partitioning** for Silver trip tables, then clustering/Z-ORDER on `tenant_id`, `trip_id`, and `driver_token`. Date and city bound most dashboard scans; clustering handles point lookups and tenant dashboards without exploding the partition count.

I rejected **partitioning by tenant_id** because a large number of tenants creates tiny files and expensive metadata. I rejected **partitioning by driver_id/token** because high-cardinality partitions make maintenance worse than the queries they help. I rejected **ingest_date only** because late events would be stored far from their business date, making incident review and BI filters scan the wrong files.

### Decision 5: Late CDC uses source timestamp plus source LSN

I chose a merge rule of **accept only newer source state**: `MERGE WHEN MATCHED AND src.source_ts > tgt.source_ts`, with source LSN as a tie-breaker. Late events are not dropped blindly; they are applied if they represent a newer business state and quarantined if they are older than the current record. SCD2 dimensions preserve history with `valid_from`, `valid_to`, and `is_current`.

I rejected **last writer wins by ingest time** because network-delayed mobile events would overwrite newer truth. I rejected **batch dedup by primary key only** because CDC order matters across updates and deletes. I rejected **trusting Kafka order globally** because partitioning and retries can reorder events across topics.

### Decision 6: One-minute Gold aggregates for SLA, not raw-table dashboards

I chose **incremental Gold tables refreshed every minute** from Delta CDF. Dashboard reads should hit compact aggregate tables such as `city_ops_1m` and `tenant_cost_5m`, not raw trip or GPS tables. This makes the 60-second dashboard SLA realistic and protects the hot Silver tables from BI fan-out.

I rejected **direct dashboards on Bronze/Silver CDC** because BI concurrency will fight ingestion and maintenance. I rejected **daily batch Gold only** because it misses the operational SLA. I rejected **materializing every possible metric** because cost and governance drift become unmanageable; only operational SLO metrics are precomputed.

### Decision 7: Maintenance is a product SLO

I chose scheduled **compaction, clustering, checkpointing, vacuum, snapshot expiry, and orphan sweeps** with before/after metrics. File count, metadata:data ratio, small-file rate, and skipped-file ratio are tracked like product latency. Jobs run more frequently on hot 7-day tables and less frequently on 90-day warm partitions.

I rejected **manual maintenance after dashboards slow down** because the first symptom becomes an incident. I rejected **vacuum-only cleanup** because Day 18 showed that uncommitted orphans and metadata-only expiry can leave real bytes behind. I rejected **over-optimizing cold partitions** because compute spend would exceed the query benefit.

## 4. Failure modes

### Failure mode A: Schema drift from Oracle breaks ingestion

Detection: schema contract validation catches new, missing, or type-changed columns before Bronze commit. The stream emits a `schema_drift` alert with table, source LSN, and offending fields.

Rollback: pause only the affected table topic, keep offsets, and route rows to `bronze_quarantine`. If the change is additive and approved, evolve schema explicitly and replay from the stored offset. If it is breaking, restore the table to the pre-change version and ask the source owner for a contract migration.

Day 18 concept: schema enforcement plus opt-in evolution.

### Failure mode B: Late mobile events overwrite newer trip state

Detection: Silver merge metrics report `stale_updates_rejected` by table/city. A spike means a network region or producer is replaying old messages.

Rollback: because the merge condition rejects older `source_ts`, the default rollback is no-op for current state. For a bad merge rule deployment, time travel restores the last good Silver version, then CDF replays accepted events with the corrected predicate.

Day 18 concept: time travel and replayable transaction logs.

### Failure mode C: Unauthorized PII access path appears

Detection: catalog policy scans and object storage access logs are compared daily. Any read of encrypted PII pointer columns without a matching `pii_access_audit` ticket triggers a security page.

Rollback: revoke the view or principal, rotate tokenization/encryption keys if needed, and restore affected derived tables from versions before the unauthorized job wrote outputs. Rebuild only allowed tokenized views.

Day 18 concept: provenance and governance through the catalog/control plane.

### Failure mode D: Small files cause BI p95 to exceed 1 second

Detection: table metrics show files per partition, average file size, and skipped-file ratio. Dashboard p95 crossing 1 second with rising file count points to layout rather than business logic.

Rollback: pause non-critical Gold refreshes, run compaction and clustering on the hot partitions, then resume. If a maintenance job produced wrong output, restore the previous Delta version and rerun with dry-run metrics.

Day 18 concept: OPTIMIZE/Z-ORDER and maintenance jobs.

## 5. Cost back-of-envelope

Assumptions: 100 million trips/year is about 274,000 trips/day, but the CDC stream also includes GPS, driver state, pricing, payment, and app events. I size raw CDC at **1 TB/day before compression** during normal operations, with 2 TB/day peak capacity. Parquet/ZSTD typically cuts structured CDC to about **0.25 TB/day**.

Storage:

- Bronze tokenized CDC: 0.25 TB/day x 30 days = 7.5 TB hot.
- Silver current/SCD2/GPS snapshots: about 1.5x Bronze = 11.25 TB hot/warm.
- Gold aggregates/features: about 1 TB.
- One-year audit and compacted aggregates: about 6 TB.
- Total managed lakehouse storage: about 25.75 TB.

Using rough object-storage prices:

- Hot Standard: 15 TB x $23/TB-month = **$345/month**.
- Warm infrequent tier: 10 TB x $12/TB-month = **$120/month**.
- Cold audit/archive: 6 TB x $4/TB-month = **$24/month**.
- Storage subtotal: **about $489/month**, before request and replication charges.

Compute:

- Streaming CDC jobs: 4 small workers x $0.20/hour x 24 x 30 = **$576/month**.
- Gold aggregate jobs: 2 workers x $0.20/hour x 24 x 30 = **$288/month**.
- Maintenance: 8 worker-hours/day x $0.40 x 30 = **$96/month**.
- Ad-hoc/BI warehouse reserve: **$1,000/month**.

Estimated total: **about $2,450/month**. I would budget **$4,000/month** after request costs, retries, observability, and peak buffers. The key FinOps risk is not raw bytes; it is runaway small files, over-clustering cold data, and dashboards scanning Silver instead of Gold.

## 6. One-week MVP

The first shippable slice is one source table, `trips`, from synthetic Debezium-style CDC into Bronze, Silver, and Gold.

Day 1: define schema contract, PII tags, and tokenization function for rider/driver/phone fields.

Day 2: build Bronze append with deterministic tokens, encrypted-pointer placeholder, source LSN, source timestamp, and quarantine for invalid rows.

Day 3: implement Silver `trips_current` merge with `src.source_ts > tgt.source_ts`, stale-event metrics, and SCD2 history for status changes.

Day 4: build `city_ops_1m` Gold aggregate from CDF-style increments: completed trips, cancellations, p50 pickup latency, gross merchandise value.

Day 5: add catalog views, PII access audit table, and one break-glass mock flow.

Day 6: add compaction/clustering job with before/after file metrics.

Day 7: run a replay drill: inject late events, schema drift, and a bad update; prove quarantine, stale rejection, and time-travel rollback work.

The MVP is successful if a clean run shows tokenized Bronze, correct current state after late CDC, a dashboard aggregate refreshed from increments, and an auditable PII read path.
