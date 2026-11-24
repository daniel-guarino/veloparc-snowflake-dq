# Snowflake foundation (Phase 3b)

Everything in Snowflake is created by versioned SQL, except a short bootstrap that a human runs once.

## One-time setup (maintainer)

1. **Trial account:** Snowflake trial, **Enterprise** edition, cloud **AWS**, region **Europe (Paris) `eu-west-3`**.
2. **Bootstrap:** in Snowsight, open a SQL worksheet and run [`bootstrap/000_bootstrap.sql`](bootstrap/000_bootstrap.sql)
   with *Run All*. It creates the resource monitor, the `VLP_DEPLOYER` role, the `VLP_ADMIN` database, the
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

The migrations are applied with [schemachange](https://github.com/Snowflake-Labs/schemachange); the history is in
`VLP_ADMIN.SCHEMACHANGE.CHANGE_HISTORY`. Add a change as a new `V<x.y.z>__<description>.sql` file, never by
editing an applied one.
