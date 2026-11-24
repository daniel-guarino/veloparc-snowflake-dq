# Detection evaluation: are the DQ rules finding the injected anomalies?

The generator writes an answer key next to the data:

- `anomaly_manifest.jsonl` lists every anomaly it injected.
- `rule_violations.jsonl` lists what its own R1–R16 checker finds in the generated data. That includes *natural*
  violations: the simulation produces some of these by itself, such as a late scan.

Both files are loaded into `DQ.ANOMALY_GROUND_TRUTH` and `DQ.GENERATOR_RULE_VIOLATIONS` (ADR 0004). The model
`DQ.dq_detection_eval` compares them with `DQ.dq_issue`, which lists what the dbt rules flagged. It does this per rule,
for the latest backfill run.

| Column | Meaning |
| --- | --- |
| `injected`, `injected_found`, `injected_missed` | Injected anomalies, how many a detection matches, and how many it misses (false negatives) |
| `detected_not_injected` | Detections that match no injected anomaly. These are either natural violations or false positives. |
| `precision_vs_injected`, `recall_vs_injected` | Share of detections that were injected; share of injected anomalies that were detected |
| `checker_*`, `*_vs_checker` | The same comparison against the generator's checker, which counts natural violations too |

A detection matches an expected key when the two keys are equal. It also matches when the detection is a line of
that key, for example `INV#3` for `INV`. Business keys follow the answer-key formats (`contracts/README.md`).

Two references are needed. Against the injected anomalies alone, a rule that also finds natural violations looks
imprecise even when it is right. The checker is the arbiter for those.

## First run (2026-11-24, dbt run 45470828924)

| Rule | Detected | Injected | Found | Missed | Not injected | Precision | Recall | Checker | Checker found |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SCHEMA | 3035 | 3082 | 3035 | 47 | 0 | 1.000 | 0.985 | 0 | 0 |
| R1 | 1415 | 1415 | 1415 | 0 | 0 | 1.000 | 1.000 | 1415 | 1415 |
| R2 | 123 | 146 | 123 | 23 | 0 | 1.000 | 0.842 | 146 | 99 |
| R3 | 3976 | 1927 | 1927 | 0 | 2049 | 0.485 | 1.000 | 3976 | 3976 |
| R4 | 2561 | 2594 | 2561 | 33 | 0 | 1.000 | 0.987 | 2594 | 2561 |
| R5 | 8926 | 2976 | 2937 | 39 | 5989 | 0.329 | 0.987 | 8964 | 8926 |
| R6 | 1020 | 0 | 0 | 0 | 1020 | 0.000 | – | 0 | 0 |
| R7 | 0 | 0 | 0 | 0 | 0 | – | – | 0 | 0 |
| R8 | 0 | 0 | 0 | 0 | 0 | – | – | 254 | 0 |
| R9 | 1 | 2 | 0 | 2 | 1 | 0.000 | 0.000 | 256 | 0 |
| R10 | 0 | 0 | 0 | 0 | 0 | – | – | 0 | 0 |
| R11 | 40 | 3 | 3 | 0 | 37 | 0.075 | 1.000 | 55 | 40 |
| R12 | 0 | 0 | 0 | 0 | 0 | – | – | 0 | 0 |
| R13 | 1 | 0 | 0 | 0 | 1 | 0.000 | – | 2 | 1 |
| R14 | 17 | 17 | 17 | 0 | 0 | 1.000 | 1.000 | 17 | 17 |
| R15 | 1020 | 0 | 0 | 0 | 1020 | 0.000 | – | 0 | 0 |
| R16 | 0 | 0 | 0 | 0 | 0 | – | – | 0 | 0 |
| LEAD_COUNTRY_ISO2 | 29 | 30 | 29 | 1 | 0 | 1.000 | 0.967 | 30 | 29 |

## What the first run showed

### 1. One anomaly counted three times (R6, R15)

R6 and R15 each flagged the same **1,020 tracking events**. Nothing was injected for either rule, and the
generator's checker found none.

The cause is the anomaly `gln_12_digits`, which truncates a HUB scan's `bizLocation.gln` to 12 digits. That GLN:

- breaks the contract pattern, so **SCHEMA** catches it, which is the correct rule;
- is not a depot GLN, so **R6** also caught it;
- has no valid GS1 check digit, so **R15** also caught it.

The generator's checker only receives payloads that pass their schema. It assumes, as the original plan did, that a
schema-invalid record never reaches silver. ADR 0004 (decision 6) changed that: rows that break a schema reach silver
*flagged, not dropped*. Every rule after the schema check therefore sees the malformed value as well.

**Lesson:** once validation moves from a quarantine at load to tests on silver, each business rule must decide what
it does with values the contract already rejects. Otherwise a single defect inflates three dimensions (Validity,
Consistency, Validity again) and the scorecard overstates the damage.

**Fix:** R6 and R15 only consider scan GLNs of 13 characters. A malformed GLN belongs to SCHEMA alone.

### 2. The evaluation window cut real detections (R8, R9, R11, SCHEMA, R2, R4, R5)

The first version compared only detections with an event date up to the end of the backfill window (2026-10-31).
The aim was to leave out the connector data of 2026-11-01, which has no answer key. Business dates run past the
window, though:

- invoices for October are **issued in November**. R8 showed 0 detections in the evaluation against 254 in the
  build, and R9 showed 1 against 258;
- late scans of parcels shipped at the end of October fall in November;
- partner settlements and the ledger are dated by period.

**Fix:** the comparison now leaves out *connector rows* (no file name in their bronze lineage), not late dates.
Detections without lineage, from group rules, are kept.

### 3. Low precision against the injected anomalies can be correct (R3, R5, R11)

R3, R5 and R11 detect far more than was injected. For R3 and R5, the checker reports exactly the same records:
`detected_not_in_checker` = 0 and precision against the checker is 1.0.

- R5: recording delays over 15 minutes also happen naturally in the simulation.
- R3: some sequence breaks are side effects of other anomalies, such as a duplicated or dropped scan.
- R11: a partner's delivered count no longer matches the scans once scans are duplicated or dropped upstream.

These detections are true, and the owner should still see them. They are just not the anomaly that was planted.

### 4. Still open after the fix

These are checked again on the second run. A rule with recall below 100% is documented, not hidden
(implementation plan, Phase 9):

- **SCHEMA (47 missed), R4 (33), R5 (39), LEAD_COUNTRY_ISO2 (1):** probably the date filter of finding 2.
- **R2:** 23 injected anomalies missed, and 24 detections the checker does not report. This points to a key-format
  difference between some R2 sub-checks and the answer key.
- **R9:** 2 injected, 0 found, while the checker reports 256.
- **R13:** the checker reports 2 country-months; the rule finds 1.

## Second run (after the fix)

To be filled after the fix is deployed.
