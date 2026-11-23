# ADR 0001 · CI/CD pipeline: PR checks, merge on approval, deploy from main

- Status: accepted (Phase 1); dbt and Snowflake stages are filled in from Phase 5 onwards
- Date: 2026-11-23

## Context

The repository is public, has a single maintainer and must show an auditable path from a change to `main`.
Changes go through branches and pull requests (Conventional Commits). The maintainer wants the approval of a PR
to be the only manual step: once approved, a pipeline verifies the change again and carries it to `main`.
Code changes must be tested before they reach `main`, including the dbt models that arrive in Phase 7.

GitHub constraints that shape the design:

- A PR author cannot approve their own PR. PRs opened by the maintainer therefore need a second approval signal.
- A merge done with the workflow `GITHUB_TOKEN` does not trigger `push` workflows, so the deploy has to be started
  explicitly (`workflow_dispatch`).

## Decision

Three workflows, with `ci.yml` as a reusable workflow so every stage runs exactly the same checks.

| Workflow | Trigger | What it does |
| --- | --- | --- |
| `ci.yml` | every pull request; `workflow_call` | `make lint` (ruff + all pre-commit hooks: schemas against the draft 2020-12 metaschema, examples against their schemas, workflow files, detect-secrets), `make test` (pytest + coverage), `make validate-contracts` |
| `merge-on-approval.yml` | PR approved by OWNER/MEMBER/COLLABORATOR, or label `ready-to-merge` added by the repository owner | 1) gate: base is `main`, PR comes from this repository; 2) re-runs `ci.yml` on the PR **merged with the current main**; 3) merges with a merge commit (keeps the Conventional Commits history) only if the head SHA is still the approved one, deletes the branch; 4) dispatches `deploy.yml` |
| `deploy.yml` | `workflow_dispatch` (from the merge workflow) or `push` to `main` (manual merge in the UI) | Re-verifies `main`. Later stages are added per phase (below) |

Repository settings the maintainer applies once (Settings → Branches → rule for `main`):

- Require a pull request before merging; required approvals: 0 (the label is the approval signal for own PRs).
- Require status check `checks` (job of `ci.yml`) to pass; require branches to be up to date.
- Block force pushes and deletions. Settings → Actions → General → Workflow permissions: read and write is **not**
  needed; the workflows request the permissions they use.

### Testing dbt changes (Phase 7 onwards)

dbt changes are tested in CI with dbt's own components, against an isolated schema per PR:

1. `sqlfluff lint` (snowflake dialect, dbt templater) in pre-commit, so `make lint` covers SQL.
2. `dbt parse` and `dbt-project-evaluator` (package) for modelling hygiene: undocumented or untested models,
   direct source references in marts, fan-outs.
3. **Slim CI:** `dbt build --select state:modified+ --defer --state <prod manifest>` into `CI_PR_<number>`
   in Snowflake. This runs only the changed models and their children, with
   - generic and singular tests (including the R1–R16 rule tests with `meta: {rule_id, dimension, owner}`),
   - dbt unit tests (`unit_tests:` in YAML) for transformation logic, with mocked inputs,
   - model contracts (`contract: {enforced: true}`) on gold, so a breaking column change fails the build.
4. The production `manifest.json` is published as an artifact by `deploy.yml`, and is the `--state` for the next PR.
5. A cleanup job drops `CI_PR_<number>` when the PR closes.

`deploy.yml` grows accordingly: `schemachange` for `infra/snowflake/` (Phase 5–6), then
`dbt build --target prod` (Phase 7–9), then the DQ report (Phase 9). Snowflake credentials live in a GitHub
`production` environment (key-pair auth, secrets never in the repository).

### SonarQube

Not adopted now. Reasons:

- The checks it would add for Python already run here, faster and inside pre-commit: ruff covers style, bugs,
  complexity and the bandit security rules (`S`); detect-secrets covers secrets; pytest-cov covers coverage.
- Its SQL analysis does not understand dbt's Jinja templating or the Snowflake dialect, which is where most of the
  code in this project will be. sqlfluff and dbt-project-evaluator cover that ground.
- A self-hosted SonarQube server is an extra service to run and secure, for a single-maintainer repository.

Reconsider in Phase 10 with **SonarQube Cloud** (free for public repositories, no server). It would add a
quality-gate badge and trend dashboards for the portfolio, at the cost of one more secret (`SONAR_TOKEN`). A
lighter alternative for security scanning is GitHub CodeQL, which is native and free for public repositories.

## Consequences

- One manual action per change: approve (or label) the PR. Everything after that is reproducible and logged.
- Commits pushed after an approval are never merged unverified (`--match-head-commit`).
- Fork PRs get a read-only token and cannot trigger the merge.
- Until branch protection is configured in the UI, direct pushes to `main` are still technically possible.
