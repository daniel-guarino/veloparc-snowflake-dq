# ADR 0002 · Managed Kafka Connect on Aiven, and every cloud run in GitHub Actions

- Status: accepted
- Date: 2026-11-23
- Supersedes: "self-hosted Kafka Connect (Docker on WSL)" in the implementation plan, Phase 3

## Context

The plan put Kafka Connect in Docker on the maintainer's WSL, because the Aiven free tier has no managed
connectors. Two things changed:

- The Aiven service moved to the **trial plan**, which includes managed Kafka Connect. No credit card is on the
  account: when the trial ends the service is powered off, never billed.
- The maintainer does not want anything running on their computer: no local `.env`, no Docker, no CLI runs.

Claude cloud sessions are not a place to run against the services either: their outbound traffic goes through an
HTTPS proxy, and Kafka's binary protocol (SASL_SSL on a custom port) does not go through it. Secrets stored in a
cloud session's environment would only help HTTPS APIs.

## Decision

1. **Kafka Connect is the managed one on Aiven**, enabled with "Set up Kafka Connect", running the Aiven S3 sink
   connector. Its configuration is versioned in `connect/` (no secrets: AWS credentials are entered in the
   connector configuration, preferably as an assumed IAM role).
2. **The topology stays within the free-tier limits** (5 topics × 2 partitions on Kafka; master data, tariffs,
   settlements and GL as files). If the trial ends and the service drops to the free plan, the topics keep
   working; only Connect is lost, and the fallback is self-hosted Connect.
3. **Every run that touches a cloud service happens in GitHub Actions:** the generator (`generate.yml` for the
   backfill, `stream.yml` for the next hour), `kafka-setup.yml`, Terraform, schemachange, dbt. Workflows are
   started by hand (`workflow_dispatch`), by cron or by the deploy pipeline. Generator runs are therefore always
   tied to a commit, a seed and the inputs of the run, and are logged.
4. **Credentials live only in the secrets of the GitHub Environment `dev`:** Aiven bootstrap, SASL user and
   password, CA certificate and Schema Registry; Snowflake key pair; AWS through OIDC (no stored access keys).
   Workflows that use them run only on `main` or by manual dispatch, never on pull requests from forks.
5. **Claude sessions write and test code** (unit tests with a mocked producer) and can trigger the workflows and
   read their logs, but never hold credentials.

## Consequences

- Nothing to install or keep running on the maintainer's machine, including during the demo.
- Runners are ephemeral: generator state (`offsets.json`, parcels in transit) is kept in
  `s3://<bucket>/_generator/state/` (Phase 4); until the bucket exists, runs publish their output as workflow
  artifacts.
- The trial has an end date. Snowflake (Phase 5) also runs on a 30-day trial: open it when the Kafka and S3 path
  works, so both trials overlap as much as possible.
- GitHub Actions minutes are free for public repositories on standard runners. The full backfill takes about
  6 minutes.
