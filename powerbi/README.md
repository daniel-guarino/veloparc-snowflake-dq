# Veloparc Power BI report

A Power BI report on the Sales & Finance data products of Veloparc (a fictitious European parcel carrier), connected
directly to Snowflake. It reads the gold star schema (`VLP_DP.GOLD`) and the data quality layer (`VLP_DP.DQ`) built by
dbt, and it is versioned as a **Power BI Project** (`.pbip`): the semantic model in TMDL and the report in PBIR, all
plain text, so every change shows up as a readable diff.

```
powerbi/
├── Veloparc.pbip                    open this file in Power BI Desktop
├── Veloparc.SemanticModel/          semantic model (TMDL): tables, measures, relationships, Snowflake parameters
├── Veloparc.Report/                 report (PBIR): 7 pages + 1 drill-through page, theme registered as a resource
├── theme/veloparc.json              report theme (the copy Desktop loads must stay identical; see "Theme")
└── tools/validate_pbip.py           offline checks (make validate-powerbi; also a pre-commit hook)
```

## Prerequisites

- **Power BI Desktop** (Windows, 64-bit), a recent release: PBIR is the default report format and the Snowflake
  connector uses its ADBC implementation (`Implementation = "2.0"`).
- A **Snowflake user** that can use the role `VLP_READER` and the warehouse `WH_ANALYTICS`. `VLP_READER` has `SELECT`
  on the gold and DQ models and `USAGE` on the warehouse (`infra/snowflake/migrations/V1.3.0__roles_and_connector_user.sql`
  and the `+grants` in `dbt/veloparc/dbt_project.yml`).
- The dbt project has run at least once (`dbt` workflow), so the tables exist.

## Open and refresh

1. Clone the repository and open `powerbi/Veloparc.pbip` in Power BI Desktop. There is no `cache.abf` in git, so the
   report opens with the full model definition but no data.
2. **Fill the parameters**: *Home > Transform data > Edit parameters*.

   | Parameter | Value |
   | --- | --- |
   | `SnowflakeServer` | your account host, `<account>.snowflakecomputing.com` |
   | `Warehouse` | `WH_ANALYTICS` |
   | `Role` | `VLP_READER` |
   | `Database` | `VLP_DP` |

3. **Refresh** (*Home > Refresh*). Desktop asks for credentials the first time; choose the Snowflake authentication
   your user has (key pair, Microsoft Entra ID or user name and password; Snowflake is retiring single-factor
   passwords). Credentials are stored by Power BI Desktop on your machine, never in the project files.
4. Save. Desktop keeps the PBIP format (TMDL + PBIR).

> **Before committing**: Desktop writes the parameter value into
> `Veloparc.SemanticModel/definition/expressions.tmdl`. Put the placeholder back
> (`git checkout -- powerbi/Veloparc.SemanticModel/definition/expressions.tmdl`) or keep that file out of the commit.
> The `powerbi-validate` pre-commit hook and `make lint` in CI fail if any `*.snowflakecomputing.com` host other than
> `<account>` is found in `powerbi/`. Local caches and settings (`.pbi/`, `cache.abf`, `localSettings.json`) are
> git-ignored by `powerbi/.gitignore`.

## Pages

Every page opens with a kicker, a **headline sentence computed by a DAX measure** (folder *Headlines*) and one line
saying what the page shows, then a row of KPIs and two or three visuals. Month and country slicers sit top right
(`dim_month`, `dim_country`).

| Page | Question it answers | Main sources |
| --- | --- | --- |
| Executive overview | How much did we invoice, how many parcels did we ship, what did partners cost and what margin is left, by country and month? | `fct_invoice_line`, `fct_parcel`, `fct_partner_cost` |
| Revenue & billing | Do invoices and the general ledger agree within 0.5% per issuing country and billed month (R13)? Which segments and products bring revenue? | `fct_invoice_line`, `fct_gl_revenue` |
| Parcel operations | How many parcels were shipped and delivered, how fast (pickup to delivery, target 92% within 24 h), and where are scans late or duplicated, by depot and zone? | `fct_parcel`, `fct_parcel_event` |
| Partner costs | What do last-mile partners cost per stop and per parcel, by partner and zone? | `fct_partner_cost` |
| Sales pipeline | How big is the open pipeline, what is won, and how many leads convert, by stage, source and country? | `fct_opportunity`, `fct_lead` |
| Data quality scorecard | Which data product x dimension cells are green, amber or red, how did pct_valid move day by day, and which rules produce the issues? Right-click a rule > *Drill through* > *Issue detail*. | `dq_scorecard_daily`, `dq_issue` |
| Detection evaluation | Do the dbt rules find the anomalies the generator injected (recall), and are their detections real (precision against the generator's own checker)? | `dq_detection_eval` |
| Issue detail (hidden, drill-through) | Which records broke a rule, and where do they come from in bronze: landing file, Kafka offset, event id, source transaction id? | `dq_issue` |

## Semantic model

**Storage mode**: Import. Every table is read through `fnVeloparcTable` (in `expressions.tmdl`): native connector
`Snowflake.Databases(SnowflakeServer, Warehouse, [Role = Role, Implementation = "2.0"])`, navigation to
`Database > schema > table`, column names lower-cased (Snowflake returns upper case), only the modelled columns kept and
typed. These steps fold into one `SELECT` per table.

**Tables** (column names, grain and keys as in `dbt/veloparc/models/gold/_gold.yml` and `models/dq/_dq.yml`):

- Gold: `dim_date`, `dim_customer` (SCD2), `dim_depot`, `dim_partner_zone`, `dim_tariff`, `fct_invoice_line`,
  `fct_gl_revenue`, `fct_parcel`, `fct_parcel_event`, `fct_partner_cost`, `fct_opportunity`, `fct_lead`.
- DQ: `dq_scorecard_daily`, `dq_issue`, `dq_detection_eval`.
- Derived in Power Query: `dim_month`, `dim_country` (see deviations).
- `_Measures`: 73 measures in display folders (Revenue, Parcels, Partners, Sales, Data quality, Detection, Headlines),
  each with an English description and a format string.

**Relationships**: many-to-one, single direction, as in `docs/data-model/gold_star_schema.dbml`. Surrogate keys,
date keys and lineage columns are hidden. SCD2 is respected because the facts already carry the `customer_sk` of the
version valid at their business date; the model joins on that key and never looks up `is_current`.
`dim_date` is marked as the date table (`full_date`), and auto date/time is off.

**Deviations from the star schema**, all forced by Power BI and accepted for this report:

1. `dim_month` (derived from `dim_date`): the monthly facts `fct_partner_cost` and `fct_gl_revenue` join on `period`,
   which is not unique in `dim_date`. `dim_date[period] -> dim_month[period]` lets one month slicer filter daily and
   monthly facts alike.
2. `dim_country` (derived from the country codes of the facts and DQ tables): gold has no country dimension, and the
   overview compares countries across facts. Joined to `billing_country`, `consignee_country` and the `country_code`
   columns of `fct_gl_revenue`, `fct_opportunity`, `fct_lead`, `dq_scorecard_daily` and `dq_issue`.
3. **Inactive relationships** (Power BI allows one active filter path between two tables):
   `fct_invoice_line -> fct_parcel`, `fct_parcel_event -> fct_parcel`, `fct_lead -> fct_opportunity`,
   `dim_tariff -> dim_customer`, `dim_partner_zone -> dim_depot` and `dim_partner_zone -> dim_country`. The partner
   measures activate the last one with `USERELATIONSHIP`, so partner costs follow the country slicer through the
   partner's country.
4. The unknown date member (`date_key -1`, 1900-01-01) is filtered out of `dim_date` so it can be marked as a date
   table (a date table must be contiguous). Facts with an unknown date appear under *(Blank)*.
5. `dq_rule_catalog` is not imported: `VLP_READER` has no grant on dbt seeds. `dq_detection_eval` (one row per rule,
   with dimension and owner) plays the rule dimension, and `dq_issue[rule_id]` joins it.

**Measure notes**:

- *Revenue EUR* is by invoice issue date. R13 uses the billed month, so *Revenue EUR (billing period)* re-filters
  `fct_invoice_line[billing_period]` with the selected months before comparing with *GL revenue EUR*.
- *Open issues* counts the rows of `dq_issue`: every row is a current violation (the table has no status column).
- *pct_valid* and *DQ status* recompute the scorecard rule at any grain, weighting by records evaluated:
  green at or above the threshold, amber within 1 point, red below. Status is shown as color **and** glyph (● ◆ ▲).
- *Precision vs injected* counts natural violations (for example late R5 scans) against the rule. *Precision vs
  checker* compares with the generator's own checker, which also sees natural violations, and is the fairer precision.

## Theme

`theme/veloparc.json` is the "editorial / storytelling" direction: paper background `#FBFBF8`, ink `#1C1F26`,
deep teal accent `#0D5C63`, status colors green `#2C7A4B`, amber `#9A6400`, red `#B3261E` (each also carries a glyph),
Georgia for headlines and KPI values, Segoe UI for text. Desktop loads the copy in
`Veloparc.Report/StaticResources/RegisteredResources/`, whose file name carries a short suffix because Desktop caches
themes by name. To change the theme: edit `theme/veloparc.json`, give it a new suffix in `name`, copy it to
`RegisteredResources/` under that name and update the two references in `Veloparc.Report/definition/report.json`. The
validator checks that both copies are identical.

## Validation

`make validate-powerbi` (or `python powerbi/tools/validate_pbip.py`) runs offline and checks that:

- every JSON file parses, and the TMDL tables, columns and measures parse;
- every imported column exists in the dbt model it comes from (yml first, then model SQL and macros, e.g. the lineage
  columns of `dq_issue` from the `dq_violation` macro);
- every DAX reference and every field used by a visual or filter exists in the model, and no measure uses a table that
  is not imported;
- the active relationships leave one filter path between any two tables;
- no Snowflake account identifier is committed, and the theme copies match.

When the files were written they were also checked against Microsoft's JSON schemas (`microsoft/json-schemas`, theme
schema 2.159) and with Microsoft's `powerbi-report-author validate` (roles, formatting objects and enum values).

**Still to check in Power BI Desktop** (cannot be done without it): that the project opens without errors, the
parameters and credentials work, the refresh succeeds (types, row counts), the DAX measures return the expected values,
the date table marking is accepted, the visuals render with the theme, the slicers filter as described, and the
drill-through and Back button work.

## Screenshots

Put screenshots of each page in `docs/screenshots/powerbi/` (for example `01-executive-overview.png` ...
`07-detection-evaluation.png`). They must show only the fictitious Veloparc data and no account host.

## References

- Power BI Desktop project report folder (PBIR): https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-report
- Power BI Desktop project semantic model folder: https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-dataset
- Tabular Model Definition Language (TMDL): https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview
- Power Query Snowflake connector: https://learn.microsoft.com/en-us/power-query/connectors/snowflake
- Fabric report definition (PBIR file layout): https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/report-definition
- PBIR and item JSON schemas: https://github.com/microsoft/json-schemas/tree/main/fabric
- Report theme JSON schema: https://github.com/microsoft/powerbi-desktop-samples/tree/main/Report%20Theme%20JSON%20Schema
- PBIR authoring reference and validator (`@microsoft/powerbi-report-authoring-cli`): https://github.com/microsoft/skills-for-fabric
