# Veloparc: interview preparation notes

Talking points on the Veloparc data quality platform (fictitious European parcel carrier): scalability, ACID,
idempotency, data quality, and how *Designing Data-Intensive Applications* frames scalability. Each section points to
the files in this repository that back the claim, and is honest about what does not scale or is not done yet.

Pipeline in one line: Python generator → Kafka (Aiven) → Snowflake (Snowpipe Streaming) and files → internal stage →
bronze → dbt (silver, gold, DQ), all run from GitHub Actions.

---

## 1. Scalability

### 1.1 Architecture: what is already designed to scale

**a) Volume is a parameter of the generator**
- `generator/config/simulation.yaml`: `scale: 1.0` multiplies customers, shipments, opportunities and leads. The
  reference volume is ~140k shipments and ~1M scan events in 2 months.
- `docs/architecture/implementation_plan.md` (lines ~107 and ~351) already plans the question "what about 30 million
  records per day?".
- *Pitch:* "The generator scales by configuration, so scale was part of the plan from day one."

**b) Kafka: partitions and keys**
- `generator/config/topics.yaml`: 2 partitions per topic (Aiven free-tier limit). Keys are business entities:
  `trackId`, `invoiceNumber`, `messageId`, `Id`.
- *Pitch:* keying by entity keeps per-parcel and per-invoice ordering. Scaling out means adding partitions;
  parallelism follows. The trade-off is that ordering only holds within a partition, which is why the key matters.

**c) Snowflake sink connector (Snowpipe Streaming)**
- `connect/snowflake_sink.json`: `tasks.max: 2` matches the partition count (extra tasks would sit idle).
- `buffer.flush.time` and `snowflake.streaming.max.client.lag` tune the latency vs cost trade-off.
- Snowpipe Streaming is serverless: no warehouse to size for ingestion.

**d) Append-only bronze that processes only new rows**
- `infra/snowflake/migrations/R__bronze_connector_tasks.sql`: an `APPEND_ONLY` stream plus a task triggered by
  `SYSTEM$STREAM_HAS_DATA`. Each run processes only what arrived since the previous one.
- The backfill is loaded in bulk with COPY from the stage (ADR 0003): one path for bulk loads, one for streaming,
  both writing to the same bronze layout.

**e) One warehouse per workload**
- `infra/snowflake/migrations/V1.1.0__warehouses.sql`: `WH_LOAD`, `WH_TRANSFORM`, `WH_ANALYTICS`, all XS with a 60 s
  auto-suspend.
- *Pitch:* each workload scales on its own. Scale **up** (bigger warehouse) for heavy transformations, scale **out**
  (multi-cluster) on `WH_ANALYTICS` for many concurrent users. dbt never competes with dashboards for compute.

**f) Clustering deferred, with the criterion written down**
- `docs/architecture/implementation_plan.md` (lines ~218-220): no clustering keys yet; add
  `CLUSTER BY (TO_DATE(_KAFKA_TS))` on shipments and tracking events if volumes grow.
- *Pitch:* tables are small today (largest ~140 MB of gzip JSON) and automatic clustering is billed. I know when it
  pays off and which key I would use.

### 1.2 dbt: what is already designed to scale

**a) Incremental silver with a watermark on `_inserted_at`**
- `dbt/veloparc/macros/staging.sql`: macro `new_rows_only()`.
- The strongest point to defend is the choice of column: `_inserted_at` only grows per table, unlike `_source_ts` or
  `_kafka_ts`. With a business timestamp, a late event (rule R5) would fall below the watermark and be lost.

**b) Incremental strategy chosen per model**
- `merge` for entities with a unique key, e.g. `models/silver/tracking_event.sql`.
- `delete+insert` for header/lines, e.g. `models/silver/invoice_line.sql`: a new invoice version replaces all its
  lines, including lines that no longer exist. A `merge` would leave those orphan lines behind.

**c) In-batch dedup**
- `qualify row_number() over (partition by <key> order by {{ latest_first() }}) = 1` keeps one row per key.

**d) Hash surrogate keys**
- `macros/surrogate_key.sql` (MD5): no sequences, the same key on a full refresh, safe in parallel processing, and no
  Hub package needed.

**e) Schema evolution**
- `dbt_project.yml`: `on_schema_change: append_new_columns`. A new source column does not break the incremental
  models.

### 1.3 What does not scale yet (and how to fix it)

**① Bronze dedup views scan the whole table on every run (the most important one)**
- `infra/snowflake/migrations/R__bronze_views.sql`: `ROW_NUMBER()` and `FIRST_VALUE()` with
  `PARTITION BY _KAFKA_TOPIC, _EVENT_ID` over the entire `RAW_*` table.
- The `stg_` models read these views, and `new_rows_only()` is only applied later, in silver. Because `_inserted_at`
  is not in the `PARTITION BY`, Snowflake cannot push the filter below the window function. Silver is incremental on
  write, but the read grows with the full history: O(N) per run.
- *Fix:* dedup inside the incremental model against a lookback window (e.g. `_inserted_at > max - 3 days`), flag
  duplicates at bronze load time, or use Dynamic Tables.

**② Gold is fully rebuilt (`materialized: table`) on every run**
- `dbt_project.yml`; `models/gold/fct_parcel_event.sql` computes `row_number()` over the whole history for
  `is_duplicate`.
- *Fix:* incremental facts partitioned by `event_date_key`, reprocessing only the dates that received new rows. This
  requires keeping `is_duplicate` local to the partition or recomputing it only for the affected `track_id`s.

**③ DQ rules also reread everything**
- `models/dq/*` are tables; rules like `dq_rule_r05.sql` use `union` and `not exists` over all events.
- *Fix:* incremental rules per day with a lookback for late data, so `dq_scorecard_daily` is truly daily.

**④ `merge` without `incremental_predicates` scans the whole target**
- On tables with hundreds of millions of rows, add `incremental_predicates` (e.g.
  `DBT_INTERNAL_DEST.event_time_utc >= dateadd(day, -7, current_date)`) together with date clustering so Snowflake
  can prune micro-partitions.

**⑤ Ingestion limits**
- 2 partitions and `tasks.max = 2` cap consumer parallelism (a free-tier cost decision, not a design one).
- Payloads are `VARIANT`, parsed field by field in `stg_`. At high volume the pre-extracted `BK_*` columns help, and
  the connector's schematization is an alternative.

### 1.4 Short script

1. **Design:** decoupled layers, append-only bronze, serverless ingestion, one warehouse per workload.
2. **Deliberate decisions:** watermark on `_inserted_at`, `merge` vs `delete+insert` per model, hash keys,
   clustering deferred with a written criterion.
3. **Known limits:** dedup views, gold and DQ in full rebuild.
4. **Next step for 30M records/day:** incremental dedup with lookback, date-partitioned facts with
   `incremental_predicates`, date clustering, more Kafka partitions, Dynamic Tables as an alternative to tasks.

---

## 2. ACID

Summary: **in Snowflake every statement is ACID; the pipeline as a whole is not one transaction.** Across layers the
project relies on idempotency, dedup and data quality tests, and consistency between layers is eventual.

### A: Atomicity
**Definition:** a transaction happens entirely or not at all. If something fails midway, everything is rolled back
and no partial state remains.

- **Best example: explicit transaction on the reference tables.** `generator/src/veloparc_gen/bronze/load.py`
  (`load_reference`): `BEGIN` → `DELETE FROM REF.<table>` → `COPY INTO` → `COMMIT`, `ROLLBACK` on error. Without the
  transaction, a COPY failure would leave the table empty. With it, readers see the old or the new version, never a
  half-loaded table.
- **dbt:** a `merge` is a single statement, so it is atomic. `delete+insert` is two statements that dbt-snowflake runs
  inside `begin … commit`, so an invoice is never left without lines between the delete and the insert.
- **Gold and DQ** (`materialized: table`) use `create or replace table … as select`: an atomic swap.
- **Connector bronze path:** the task's `INSERT … FROM <stream>` only advances the stream offset when the insert
  commits. On failure the rows stay in the stream for the next run: no loss, no duplication.

*Honest weak point:* in `load.py` (`_copy_and_audit`), the bronze `COPY INTO` and the `INSERT` into `LOAD_AUDIT` run
in autocommit as separate transactions. If the audit insert fails, the data is loaded but unaudited. Fix: wrap both
in `BEGIN/COMMIT`, as the reference tables already do.

### C: Consistency
**Definition:** a transaction takes the database from one valid state to another, respecting integrity rules (keys,
constraints, business rules).

- **Snowflake does not enforce PRIMARY KEY, UNIQUE or FOREIGN KEY** (it accepts the declaration but only enforces
  `NOT NULL`). Consistency is therefore owned by the pipeline:
  - dbt uniqueness and integrity tests (`tests/generic/unique_combination.sql`, `tests/generic/no_violations.sql`);
  - business rules R1–R16 as `dq_rule_*` models and tests with `meta.rule_id`;
  - unknown members `-1` in gold, so every fact has a valid dimension (referential integrity by construction);
  - dedup in `stg_` (`macros/staging.sql`, `not_duplicate()`), so silver has one row per key.
- **Schema-on-read:** bronze accepts payloads as received (`RAW_PAYLOAD VARIANT`) and JSON Schema rules are checked
  in dbt (ADR 0004). Design choice: never lose data at the door, measure quality afterwards.

*Pitch:* "In a cloud warehouse, the engine only guarantees the C of ACID for types and `NOT NULL`. Business integrity
is guaranteed by tests, and I measure those violations instead of rejecting the data."

### I: Isolation
**Definition:** concurrent transactions do not interfere with each other; each sees the data as if it ran alone,
according to the isolation level.

- Snowflake uses MVCC over immutable micro-partitions with **READ COMMITTED** isolation: each statement sees a
  snapshot of the data committed when it starts.
- *In practice:* while dbt rebuilds `fct_parcel_event`, a dashboard on `WH_ANALYTICS` keeps reading the full previous
  version, never a half-built one. Separate warehouses isolate compute; MVCC isolates data.
- **Concurrent writes to the same table:** DML such as `UPDATE`, `DELETE` and `MERGE` on one table is serialized by
  table locks. That is why bronze is **append-only**: both loaders (COPY and the stream task) only insert.
- **Streams:** inside a transaction a stream is repeatable-read: reading it twice in the same transaction returns the
  same rows.

### D: Durability
**Definition:** once committed, data is not lost, even after a failure, crash or restart.

- After commit, data sits in Snowflake's replicated storage (Snowflake's own cloud account, `eu-west-3`).
- **Time Travel** (1 day by default; the project does not change `DATA_RETENTION_TIME_IN_DAYS`) and **Fail-safe**
  (7 days for permanent tables) recover data after human error.
- **Upstream durability:** bronze is append-only and keeps the original payload, so silver and gold can be rebuilt
  with `--full-refresh`. Files stay in `@LANDING`, Kafka retains messages, and the generator is reproducible (seed in
  `simulation.yaml`).

### ACID does not cross systems

There is no distributed transaction across Kafka → connector → Snowflake → dbt. The project compensates with:

| Problem | How the project handles it |
| --- | --- |
| Kafka is at-least-once (`docs/architecture/ingestion_kafka_s3_bronze.md`) and the producer may resend | `_IS_TECH_DUP` and `_IS_RESEND` flags in `R__bronze_views.sql`, applied by the `stg_` models |
| Rerunning the file load would duplicate data | COPY keeps load metadata and skips files already loaded, so the load is idempotent |
| A `dbt build` that fails midway leaves silver updated and gold stale | Each model is atomic and the next run converges (eventual consistency across layers). The `_inserted_at` watermark and hash keys make reruns safe |

*Closing line:* "I rely on Snowflake's ACID guarantees per statement, with an explicit transaction wherever a delete
is followed by a load. Across systems I trade atomicity for idempotency and dedup, and I measure business consistency
with tests, because Snowflake does not enforce PKs or FKs."

---

## 3. Idempotency

**Definition:** running the same operation once or many times, with the same input, leaves the system in the same
state. It matters in data pipelines because failures and retries are normal: if a rerun duplicates or loses data,
every failure becomes an incident.

It complements ACID: **inside Snowflake, ACID makes each statement safe; across systems, idempotency makes retries
safe.**

### 3.1 Infrastructure (DDL and deploy)

| Where | Mechanism |
| --- | --- |
| `infra/snowflake/bootstrap/000_bootstrap.sql` | "Safe to re-run": everything is `IF NOT EXISTS`, except the last statement, which issues a new token on purpose |
| `infra/snowflake/migrations/V*__` | schemachange records applied versions in a history table; each runs once |
| `infra/snowflake/migrations/R__*` | Repeatable scripts use `CREATE OR REPLACE`: rerunning reaches the same state |
| `.github/workflows/kafka-setup.yml` | Creates or aligns topics, ACL and schemas on Aiven: compares with the desired state instead of creating blindly |

**Good detail:** in `R__bronze_connector_tasks.sql` the **task** uses `CREATE OR REPLACE` but the **stream** uses
`CREATE STREAM IF NOT EXISTS`. Recreating the stream would reset its offset and replay every row (with
`SHOW_INITIAL_ROWS`), duplicating bronze. Not every object should be replaced: whatever holds state must be preserved.

**Deliberate exception:** `connector-deploy` rotates the key pair on every run (`connect/README.md`). The connector
configuration is idempotent; the credential is not, by security design.

### 3.2 Generator (reproducible data)

- `generator/README.md`: "The same seed and configuration always produce byte-identical files."
- Every random stream comes from `SeedSequence([seed, key])` (`world.py`), and even `ce_id` and `traceparent` come
  from that seeded stream (`sink/envelope.py`, `new_ids`).
- *Why it matters:* the anomaly answer key (`anomaly_manifest.jsonl`) and the detection evaluation
  (`dq_detection_eval`) are only comparable across runs because the input is deterministic.
- **Producer retries:** the generator simulates resends with the **same `ce_id`**, like a real producer that resends
  without knowing whether the first attempt arrived. This is what bronze has to handle.

### 3.3 Bronze (loading)

**Files (COPY):**
- `generator/src/veloparc_gen/bronze/load.py` and `.github/workflows/bronze-load.yml`: COPY keeps load metadata (file
  name and checksum) and **skips files already loaded**. Rerunning the workflow duplicates nothing.
- **Reference tables:** full replace with `BEGIN` → `DELETE` → `COPY … FORCE = TRUE` → `COMMIT`. Idempotent because it
  replaces everything (the result is always "the file's content"), atomic because of the transaction.
- **Manifests:** append that skips already-loaded files.

**Kafka (stream + task):** the stream offset only advances when the `INSERT` commits. If the task fails, the next run
processes the same rows: no loss, no duplication on this hop.

**Eventually idempotent dedup:** Kafka is at-least-once, so idempotency is not in the bronze write (append-only, it
accepts duplicates) but in the **read**. `R__bronze_views.sql` flags:
- `_IS_TECH_DUP`: same topic + partition + offset + `ce_id` (the same message loaded twice);
- `_IS_RESEND`: same topic + `ce_id` at another offset (a producer retry).

The `stg_` models filter both (`macros/staging.sql`). *Pitch:* "Bronze keeps everything that arrived, as an audit
trail. Idempotency is applied at the bronze → silver boundary, using an idempotency key (`ce_id`) the producer sends
in a header."

### 3.4 dbt (transformation)

- **Upsert by business key:** `merge` with `unique_key` (`models/silver/tracking_event.sql`) updates if the key exists
  and inserts otherwise; processing the same event twice gives the same result. `delete+insert`
  (`models/silver/invoice_line.sql`) replaces an invoice's full set of lines.
- **Deterministic ordering in dedup:** `latest_first()` (`macros/staging.sql`) orders by
  `_source_ts desc, _kafka_ts desc, _kafka_offset desc`, with the offset as tie-breaker. Without a tie-breaker,
  `row_number()` could pick different rows on each run when there are ties, and the output would change without the
  input changing. Same care in `fct_parcel_event.sql`: `order by e.event_time_utc, e.event_id`. **This one tends to
  impress interviewers.**
- **Hash surrogate keys** (`macros/surrogate_key.sql`): with sequences, each full refresh would issue new IDs and break
  external references. With an MD5 of the business key, the same input always gives the same key.
- **Gold and DQ as tables:** rebuilt from silver on each run, so idempotent by definition (same input, same output).
  The simplest case; its cost shows up at scale (section 1.3).
- **`--full-refresh` as a safety net:** because bronze keeps everything and keys are deterministic, silver can be
  rebuilt from scratch to the same result (`implementation_plan.md`).

### 3.5 Real risks to know about

**① Race on the `_inserted_at` watermark** (`macros/staging.sql`, `new_rows_only()`)

`_inserted_at` is `DEFAULT CURRENT_TIMESTAMP()`, evaluated when the statement **starts**, not when it commits. A
possible sequence:
1. A long COPY starts at 10:00 and writes rows with `_inserted_at = 10:00`.
2. Meanwhile the stream task inserts other rows at 10:02 and commits.
3. dbt runs at 10:03 and records `max(_inserted_at) = 10:02`.
4. The COPY commits at 10:04 with rows stamped 10:00, below the watermark: **they never reach silver**.

*Fix:* a lookback window (`> max - interval '1 hour'`), safe because `merge` is idempotent and reprocessing does not
duplicate; or serialize loading and dbt in the orchestrator. Here the workflows are manual and never overlap, but with
continuous loading the risk is real.

**② COPY load metadata expires after 64 days.** After that, an old file with an unknown modification date may be loaded
again. The bronze dedup (`_IS_TECH_DUP`) covers it: defense in depth.

**③ COPY and `LOAD_AUDIT` are not in the same transaction** (see ACID). On a retry after an audit failure, COPY skips
the files (correct) but the audit records zero loaded rows, which is misleading.

*Closing line:* "I designed for at-least-once delivery with an exactly-once effect: every layer can be rerun safely.
Infrastructure is declarative (`IF NOT EXISTS`, `OR REPLACE`, migration history), loading skips processed files,
bronze accepts duplicates and flags each one by the `ce_id` idempotency key, and dbt upserts by business key with
deterministic ordering and hash keys. The source data is reproducible from the seed, which makes quality evaluation
comparable across runs."

---

## 4. Data quality

### The core idea

**"Having quality tests is not enough. I measured whether the tests work."** The generator injects known anomalies and
writes an answer key, and dbt computes precision and recall for each rule against it.

Flow: **contract at the source → bronze accepts everything → rules in dbt (one row per violation) → scorecard per
dimension → evaluation against the answer key**.

### 4.1 Contracts: prevention before the data arrives

- **8 JSON Schemas** in `contracts/schemas/`, the source of truth for what each system sends.
- **Examples validated in CI:** `contracts/validate_examples.py` runs on every PR (`make validate-contracts`) and checks
  schemas plus rules R1, R2, R6–R9, R12, R14 and R15. A schema change that breaks a coherent example fails CI.
- **Schema registry on Aiven** with `BACKWARD` compatibility (`generator/README.md`): a producer cannot publish a
  version that breaks consumers.

### 4.2 Ingestion: lose nothing and know what arrived

- **DLQ:** messages the connector cannot process go to `vlp.dlq.s3-sink.v1` (`connect/snowflake_sink.json`) instead of
  blocking the pipeline or vanishing.
- **Load audit:** `BRONZE.LOAD_AUDIT` records, per topic and run, files loaded, partially loaded and failed, rows
  parsed vs loaded, and the first error with its file name.
- **Bronze × source reconciliation** (`dbt/veloparc/tests/bronze/`):
  - `assert_bronze_file_rows_match_generator.sql`: loaded rows match what the generator produced;
  - `assert_bronze_dedup_matches_generator_records.sql`: after removing technical duplicates and resends, **exactly**
    one record per generated `ce_id` remains, so the dedup itself is tested;
  - `warn_gl_batches_incomplete.sql`: incomplete ledger (GL) batches, comparing rows received with the header's
    `_SOURCE_TXN_COUNT`. This detects **data that never arrived**.

### 4.3 Business rules: one per quality dimension

Each rule is a `dq_rule_*.sql` model returning one row per violation, with a `no_violations` test and
`meta: {rule_id, dimension, owner}` (`models/dq/_dq.yml`). Examples:

| Dimension | Rule | What it detects | How |
| --- | --- | --- | --- |
| **Validity** | SCHEMA (`dq_rule_schema.sql`) | status `DELIVERD`, 12-digit GLN, credit note without a reference invoice | contract enum, pattern and if/then, checked on silver |
| **Validity** | R15 | invalid GS1 check digit in GLN and SSCC | project macros in `macros/gs1.sql`, unit-tested in `tests/assert_gs1_macros.sql` |
| **Uniqueness** | R4 (`dq_rule_r04.sql`) | repeated scan (same trackId + status + GLN) | `group by … having count(*) > 1` |
| **Consistency** | R3 | `CANCELLED` after `PICKUP`, `DELIVERED` without `IN_DELIVERY` | state machine of the scan sequence |
| **Consistency** | R2 | parcel invoiced without pickup, delivered without invoice, unknown customer | referential integrity across sources |
| **Consistency** | R14 | tariffs with overlapping validity, weight bands with gaps | interval overlap |
| **Timeliness** | R5 (`dq_rule_r05.sql`) | scan recorded more than 15 min after the event **and a whole country without scans on a day** | the second part detects **missing data** (a gateway outage), which is hard to do |
| **Accuracy** | R8 (`dq_rule_r08.sql`) | invoiced price differs from the tariff valid on the shipping date, fuel surcharge miscalculated | **recomputes** the price from tariff and weight |
| **Accuracy** | R13 | ledger revenue differs from invoiced revenue per country and month (0.5% tolerance) | Finance × Billing reconciliation |
| **Completeness** | R1 | message with more units than TrackIDs | count per message |

**Strong examples for the interview:** R5 (detects absence of data), R8 (recomputes the expected value) and R13 (an
accounting reconciliation between two systems).

### 4.4 Design decisions

- **Flag, don't drop** (ADR 0004, decision 6): bronze rejects nothing. Schema-breaking records reach silver flagged.
  Nothing is lost silently and quality becomes **measurable**.
- **Severity `warn`** (`_dq.yml`): rules measure without stopping the pipeline. A 1% invoice error should not block
  the operations dashboard. In production, raise severity on critical rules.
- **Rule catalog** (`seeds/dq_rule_catalog.csv`): each rule has a dimension, a **business owner** (Finance, Network
  operations…) and a **target** (`threshold_pct`, e.g. 99.9%). Quality gets an owner and an SLA.
- **Lineage for root cause:** the `dq_violation()` macro (`macros/dq.sql`) standardizes every violation with business
  key, country, date and bronze lineage: `_event_id`, `_source_txn_id`, `_file_name`, `_kafka_offset`. Every issue
  traces back to its **file, offset and batch**.
- **Gold protection:** unknown member `-1` for missing customers, monitored by
  `tests/dq/warn_file_shipments_without_customer.sql`. The fact does not disappear from reports, and the problem stays
  visible.
- **Normalize and record:** a lead with country `"Spain"`, `"España"` or `"Spian"` is normalized in silver, but the
  source is flagged (`LEAD_COUNTRY_ISO2`). Fixing without recording would hide the problem from the CRM owner.

### 4.5 Consumption: `dq_issue` and the scorecard

- **`dq_issue`** unions the 18 rules into one table, with a hashed `issue_id`, dimension, severity and owner.
- **`dq_scorecard_daily`** computes, per data product, source, country, dimension and day,
  `1 - records with an issue / records evaluated`, and compares it with the catalog target. It answers "can I trust
  Portugal's billing today?".

### 4.6 Evaluating the detectors

`models/dq/dq_detection_eval.sql` compares `dq_issue` with **two references**:
1. **Injected anomalies** (`anomaly_manifest.jsonl`): what was planted;
2. **The generator's own checker**, which also counts **natural** violations of the simulation (a genuinely late
   scan, for example).

Both are needed: a rule that finds natural violations looks imprecise against the injection key even when it is
right.

**First run results** (`docs/data-quality/detection_evaluation.md`):
- R1 and R14: precision and recall of **1.000**. SCHEMA, R4 and LEAD_COUNTRY_ISO2: precision 1.0, recall 0.97–0.99.
- R3 and R5: "low" precision against injection (0.485 and 0.329) but **1.0 against the checker**. The detections were
  real, just not planted.

**What the evaluation revealed** (the best interview material):
1. **One defect counted three times:** a 12-digit GLN triggered SCHEMA, R6 and R15 (1,020 events each). The cause was
   the flag-don't-drop decision: every later rule saw the malformed value. *Fix:* R6 and R15 ignore GLNs that are not
   13 digits; that case belongs to SCHEMA alone. *Lesson:* once validation leaves the load step, each rule must decide
   what to do with values the contract already rejects, or the scorecard overstates the damage.
2. **The evaluation window cut real detections:** October invoices are issued in November, so R8 showed 0 detections
   in the evaluation vs 254 in the build. *Fix:* exclude by origin (connector rows), not by date.
3. **Recall below 100% is documented, not hidden:** R2 (0.842), R9 and R13 remain open items, with hypotheses for the
   cause.

### 4.7 Limits and next steps

- The **second run** after the fixes is not recorded in the document yet. Worth running and filling in before the
  interview.
- No **statistical volume and freshness detection** yet (e.g. `elementary`, planned in `implementation_plan.md`), and
  no **alerts** to owners.
- `store_failures` and scorecard history across runs are planned, not implemented.
- DQ models are fully rebuilt (the scalability point in section 1.3).

*Closing line:* "I treated quality as a product. Contracts prevent at the source, bronze never drops, each business
rule is a model with a dimension, an owner and a target, and every violation points to its source file and offset.
Above all, I measured the detectors: the generator injects anomalies with an answer key and I compute precision and
recall per rule. That is how I found a single defect being counted three times and inflating the scorecard."

---

## 5. Scalability through the lens of *Designing Data-Intensive Applications*

### What the book proposes

In chapter 1 of *Designing Data-Intensive Applications* (Kleppmann), scalability is the ability to cope with
increased load. The book asks you first to **describe load**, then to **describe performance**:

**1. Load parameters:** the numbers that describe what the system receives, e.g. requests per second, read/write
ratio, active users, cache hit rate. The classic example is Twitter's timeline fan-out: what matters is not only the
volume of tweets but the distribution of followers per user.

**2. Performance measures:**
- **Throughput:** records processed per second, or the total time a job takes on a dataset of a given size. The book
  says this is the relevant measure for **batch** systems.
- **Response time / latency:** relevant for **online** systems. The book insists it is a **distribution**, not a
  number: use **percentiles** (p50, p95, p99, p999) rather than the mean, because the mean hides the tail.
- **Tail latency amplification:** when an operation depends on several calls, the slowest one sets the total time.
- **SLOs/SLAs:** targets expressed as percentiles, e.g. "p99 below X".

**3. Ways to scale:** up (bigger machine) or out (distribute, shared-nothing), elastic (automatic) or manual.

### The best fit for this project

**Throughput as the primary measure, with data freshness measured in percentiles as the secondary one.**

- **Throughput**, because the core of the project is **batch analytics**: dbt on Snowflake, COPY backfill, DQ rules.
  Nobody expects millisecond responses. The relevant question is "does `dbt build` still fit its window when volume
  grows 10×?" In the book's terms: the total time of a job on a dataset of known size.
- **Freshness in percentiles**, because there is a streaming part (Kafka → Snowpipe Streaming → task → silver). Its
  equivalent of response time is **end-to-end data latency**: how long a scan event takes to become queryable. It is
  measured as a distribution, as the book recommends: "p99 of events available in under N minutes".
- Online request response time barely applies. The exception would be a Streamlit dashboard with many users, where
  scaling out `WH_ANALYTICS` (multi-cluster) comes in.

### Mapping each concept to the project

**Load parameters already defined:**
- volume: ~3,000 shipments per working day and ~1M events in 2 months, multiplied by `scale` in
  `generator/config/simulation.yaml`;
- **peak:** Black Friday ×2.5, weekends at 20%. The system must be sized for the peak, not the average;
- **skew:** `volume_pareto_shape: 1.3`, a few customers carry most of the volume. This is the direct parallel with the
  Twitter example. Because the Kafka key is `trackId` (one parcel), load spreads well across partitions. If the key
  were the customer, a large customer would become a **hot partition** (hot spot, a theme of the book's partitioning
  chapter).

**Throughput, measured with what Snowflake already provides:**
- time per dbt model and for the whole `dbt build` (`QUERY_HISTORY` / dbt Projects on Snowflake run history);
- COPY rows per second (`LOAD_AUDIT` has `STARTED_AT`, `FINISHED_AT` and `ROWS_LOADED`);
- a complement not in the book but important in the cloud: **credits per million records**. In Snowflake, scaling is
  almost always a time vs cost trade-off.

**Freshness, with columns already in bronze:**
- `_KAFKA_TS` (broker time) → `_LOAD_TS` (connector push) → `_INSERTED_AT` (task insert into `RAW_*`);
- the differences between them show **where** the delay is: in the connector (`buffer.flush.time`,
  `max.client.lag`) or in the task.

Example query to show in the interview:

```sql
-- End-to-end latency (broker -> bronze) of scan events, as percentiles, per day
select
    _kafka_ts::date as day,
    count(*) as events,
    approx_percentile(datediff(second, _kafka_ts, _load_ts), 0.50)     as p50_connector_s,
    approx_percentile(datediff(second, _kafka_ts, _inserted_at), 0.50) as p50_total_s,
    approx_percentile(datediff(second, _kafka_ts, _inserted_at), 0.95) as p95_total_s,
    approx_percentile(datediff(second, _kafka_ts, _inserted_at), 0.99) as p99_total_s
from VLP_DP.BRONZE.RAW_TRACKING_EVENTS
where _file_name is null          -- connector path only
group by 1
order by 1;
```

**Caveat:** the project does **not yet measure** any of this systematically: there is no throughput or latency table
or dashboard. A `dq_freshness` model with these percentiles would be a quick next step.

**Scaling up and out in the project:**
- **up:** grow `WH_TRANSFORM` from XS to M or L to shorten a heavy dbt run;
- **out:** more Kafka partitions with a matching `tasks.max`, and multi-cluster on `WH_ANALYTICS` for read
  concurrency;
- **elastic:** Snowpipe Streaming is serverless and warehouses auto-suspend and auto-resume. Choosing the size is the
  manual part.

A point the book reinforces, tied to section 1.3: **more hardware does not fix an algorithm that is O(N) per run**.
The dedup views and the gold full rebuild grow with the whole history. A bigger warehouse only postpones the problem;
the real fix is to make the work proportional to the **delta** (incremental with lookback).

*Closing line:* "Following Kleppmann, I start with load: daily volume, a Black Friday peak of ×2.5 and Pareto skew
across customers, which led me to key Kafka by `trackId` to avoid hot partitions. Because the core is batch analytics,
my primary metric is throughput: does the build fit its window when volume grows 10×? For the streaming part I
measure freshness as a distribution, with p95 and p99 of the broker-to-bronze delay, never the mean. And I know that a
bigger warehouse does not fix a cost that grows with history: for that, processing has to be incremental."
