# Veloparc data generator

Synthetic data for the 8 sources of **Veloparc**, a fictitious European parcel carrier. The generator builds a
coherent world in memory, then injects anomalies with an answer key, and only at the end writes the files the
S3 Sink connector would leave in the landing zone. Phase 2 of
[`docs/architecture/implementation_plan.md`](../docs/architecture/implementation_plan.md).

## Run it

From the repository root (requires [uv](https://docs.astral.sh/uv/)):

```bash
make setup
make generate        # 2-month backfill, 4 countries, replaces out/ and state/offsets.json
```

`make generate` runs:

```bash
uv run veloparc-gen backfill --from 2026-09-01 --to 2026-10-31 --countries PT,ES,DE,PL --out out/ --reset
```

Other options: `--seed`, `--scale 0.1` (a tenth of the volume), `--no-anomalies`, `--state-dir`, `--config-dir`.
The reference run takes about 4 minutes on a 4-core laptop. It produces ~141k shipments, ~1.18M scans and ~5.5k
files (210 MB). The same seed and configuration always produce byte-identical files.

## Output

```text
out/
├── landing/kafka/<topic>/year=YYYY/month=MM/day=dd/hour=HH/<topic>+<partition>+<startOffset>.json.gz
│                                         (daily topics, settlements and GL, have no hour= folder)
├── reference/<TABLE>/<TABLE>.csv         FX_RATE_DAILY, POSTAL_ZONE, COUNTRY_ALIAS (dbt seeds)
└── _generator/runs/<run_id>/
    ├── run_manifest.json                 seed, parameters, counts, sink stats, schema gate, rule check
    ├── anomaly_manifest.jsonl            answer key: one line per injected anomaly
    └── rule_violations.jsonl             every R1-R16 violation found by the local rule checker
state/offsets.json                        next offset per topic and partition
```

Each landing line is one message, as written by the S3 Sink with HoistField + InsertField + headers:

```json
{"payload": {...}, "_kafka_topic": "vlp.ops.tracking-event.v1", "_kafka_partition": 0, "_kafka_offset": 42,
 "_kafka_timestamp": 1788280029000, "_kafka_headers": {"ce_id": "...", "vlp_op": "I", "vlp_batch_id": "...", ...}}
```

The folder is the broker arrival time (LogAppendTime, UTC), not the event time: late scans land in the hour
they arrived.

## How to trace what went wrong

Each anomaly in `anomaly_manifest.jsonl` has:

- `anomaly_id`, `anomaly_type`, `rule`, `dq_dimension`, `source`, `business_key`, `country`, `event_date` and
  `description`: the answer key loaded in Phase 9.
- `details`: the field that was changed, with its original and injected values.
- `related_rules`: rules expected to fire as a consequence. For example, a `DELIVERD` scan is quarantined, so
  silver also sees an incomplete sequence (R3) and a settlement count that no longer matches (R11).
- `landing`: topic, partition, offsets and file of every affected message.

`run_manifest.json → rules` reconciles the two sides for each rule: the violations found by the rule checker,
the anomalies injected for that rule, and the anomalies listed as related. `unexplained` (violations with no
anomaly behind them) and `undetected` (injected anomalies the rule did not catch) must both be empty.
Otherwise the CLI exits with an error.

## What each configuration file controls

| File | Controls |
| --- | --- |
| [`config/simulation.yaml`](config/simulation.yaml) | Global `seed`, default window and `scale`. Countries: currency, VAT rate, time zone, Faker locale, fictitious GS1 prefix, postal format, volume share. Network size (hubs, depots, ParcelShops, zones, partners). Customers and segments, products, tariff bands and prices, CRM volumes, shipment volume (weekends, peaks, parcels per shipment, weights), delivery mix and scan delays, invoicing and credit notes, FX random walk, settlement costs, GL timing. Sink behaviour: file size, arrival delay, and at-least-once and DLQ rates. |
| [`config/anomalies.yaml`](config/anomalies.yaml) | Master switch, then one entry per anomaly with its rule, DQ dimension, source, eligible population, `rate` (exactly `round(rate x eligible)` are injected) or `count`, and whether it must fail the JSON Schema (`schema_anomaly`). Entries run in file order; an object touched by one anomaly is not eligible for the next ones. |
| [`config/topics.yaml`](config/topics.yaml) | The 9 topics: name, `transport: kafka \| file` (live path from Phase 3), partitions used by the murmur2 partitioner, landing granularity (hour/day), key, schema, producer and entity (headers). |

## How it works

| Step | Module | Notes |
| --- | --- | --- |
| GS1 utilities | `gs1.py` | Check digits, GLN/SSCC, SSCC ↔ EPC URN, VAT numbers with the country prefix |
| 1 Master data | `sources/master_data.py` | Depots with GLN, one partner per zone (R16), customers (10% with a second version) |
| 2 Tariffs | `sources/tariffs.py` | Customer × product × zone, contiguous bands with increasing price (R14), some price changes |
| 3 CRM | `sources/crm.py` | Closed Won opportunities linked to customers, 25% of leads converted, `Country` in ISO2 |
| 4 Shipments | `sources/shipments.py` | Weekday/weekend volume, 1–5 parcels, one TrackID per parcel (R1) |
| 5 Tracking | `sources/tracking.py` | R3 state machine, depots in the consignee zone (R6), seconds-to-minutes record delay (R5) |
| 6 Invoices | `sources/invoices.py` | Monthly per customer, band price + FUEL (R7, R8), negative credit notes (R9), FX of the issue date (R10) |
| 7 Settlements | `sources/settlements.py` | From DELIVERED scans per zone and month (R11), cost per stop (R12) |
| 8 General ledger | `sources/gl.py` | Per company and month, equal to the invoices in EUR (R13), computed before anomalies |
| 9 Reference | `sources/reference.py` | FX_RATE_DAILY, POSTAL_ZONE (zone = first digit of the postal code), COUNTRY_ALIAS |
| Anomalies | `anomalies/` | One injector per row of the anomalies table in `contracts/README.md` |
| Schema gate | `validate.py` | Clean records must pass their schema; schema anomalies must fail it; rule anomalies must pass it |
| Rule check | `rules.py` | R1–R16 and `LEAD_COUNTRY_ISO2` in Python, over the records that pass their schema (what silver sees) |
| Sink | `sink/` | murmur2 partitioner, offsets in `state/`, LogAppendTime, envelope with 13 headers, rotation per hour/day or `flush_size`; on `kafka` topics only: file redelivery, producer retries (same `ce_id`) and truncated copies sent to `vlp.dlq.s3-sink.v1` |

Scope notes: shipments are domestic only (GL account 700200 is always 0). Only `backfill` mode exists so far;
`stream` mode arrives with Phase 3. The Black Friday peak is configured, but only applies when the window
includes 2026-11-27.
