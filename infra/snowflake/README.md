# Snowflake foundation (Phase 3b)

Everything in Snowflake is created by versioned SQL, except a short bootstrap that a human runs once.

## One-time setup (maintainer)

1. **Trial account:** Snowflake trial, **Enterprise** edition, cloud **AWS**, region **Europe (Paris) `eu-west-3`**.
2. **Bootstrap:** in Snowsight (Projects → Workspaces → My Workspace → + Add new → SQL File), paste
   [`bootstrap/000_bootstrap.sql`](bootstrap/000_bootstrap.sql), select the role ACCOUNTADMIN and run the
   statements **one at a time** (cursor on each, Ctrl+Enter): *Run All* in Workspaces did not run the whole script. It creates the resource monitor, the `VLP_DEPLOYER` role, the `VLP_ADMIN` database, the
   `WH_DEPLOY` warehouse and the service user `SVC_VELOPARC_CI`. Its **last statement** returns a
   `token_secret`, which is shown only once.
3. **GitHub secrets** (Settings → Environments → `dev` → Environment secrets):
   - `SNOWFLAKE_PAT`: the `token_secret` from step 2.
   - `SNOWFLAKE_ACCOUNT`: the account identifier (Snowsight → your name, bottom left → *Account* → *Copy account
     identifier*). Both `ORG.ACCOUNT` and `ORG-ACCOUNT` work.
4. **Run** *Actions → snowflake-deploy*.

The token expires after 90 days. To rotate it, see the comment at the end of the bootstrap script.

## What the migrations create

| Script | Objects |
| --- | --- |
| `V1.0.0__database_and_schemas.sql` | `VLP_DP` with `BRONZE`, `SILVER`, `GOLD`, `DQ`, `REF` |
| `V1.1.0__warehouses.sql` | `WH_LOAD`, `WH_TRANSFORM`, `WH_ANALYTICS` (XS, 60 s auto-suspend) |
| `V1.2.0__landing_stages.sql` | File format `FF_LANDING_JSONL`; internal stages `BRONZE.LANDING` and `BRONZE.GENERATOR` (ADR 0003) |
| `V1.3.0__roles_and_connector_user.sql` | Roles `VLP_LOADER`, `VLP_TRANSFORMER`, `VLP_READER`, `VLP_CONNECTOR`; service user `SVC_AIVEN_CONNECTOR` (its key pair is set in Phase 3c) |
| `V1.4.0__bronze_raw_tables.sql` | The 8 bronze `RAW_*` tables (25 metadata columns, `RAW_PAYLOAD`, `BK_*`) and their grants. **Generated** by `veloparc-gen bronze-sql` from `generator/src/veloparc_gen/bronze/spec.py`; a test fails if it is stale (ADR 0004) |
| `V1.5.0__bronze_load_audit.sql` | `BRONZE.LOAD_AUDIT`: one row per topic per run of the file loader (`bronze-load` workflow) |
| `R__bronze_connector_tasks.sql` | **Generated**, repeatable. Change tracking on the `KAFKA_*` tables, an append-only stream per table (created once, `SHOW_INITIAL_ROWS`) and a triggered task on `WH_LOAD` that inserts the new connector rows into the `RAW_*` table |
| `V1.6.0__ref_and_generator_manifests.sql` | `REF.FX_RATE_DAILY`, `REF.POSTAL_ZONE`, `REF.COUNTRY_ALIAS`; `DQ.GENERATOR_RUNS`, `DQ.ANOMALY_GROUND_TRUTH`, `DQ.GENERATOR_RULE_VIOLATIONS`; file formats for the generator stage |
| `V1.7.0__dbt_project_schema.sql` | Schema `VLP_DP.DBT` for the dbt project object `VELOPARC`, deployed and executed by the `dbt` workflow |
| `R__bronze_views.sql` | **Generated**, repeatable. `V_RAW_*` (every row plus `_IS_TECH_DUP` and `_IS_RESEND`) and `V_GL_BATCH_COMPLETENESS`: the sources dbt reads |

The migrations are applied with [schemachange](https://github.com/Snowflake-Labs/schemachange); the history is in
`VLP_ADMIN.SCHEMACHANGE.CHANGE_HISTORY`. Add a change as a new `V<x.y.z>__<description>.sql` file, never by
editing an applied one.

Before applying a bronze migration, run *Actions → bronze-preview* (read-only): it runs the loaders' SELECTs on
a sample of the landing files and connector tables and shows, per column, how many values come out non-NULL.

Loading bronze: *Actions → bronze-load* runs `COPY INTO` for every topic from `@BRONZE.LANDING` (files already
loaded are skipped, so it can be re-run), replaces the `REF` tables and appends new generator manifests to `DQ` and ends with the status of both paths; `veloparc-gen bronze-status` shows
the same status read-only. The connector path needs no workflow: its triggered tasks run when the streams have rows.
