"""Command-line interface: ``veloparc-gen``."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Annotated

import typer

from veloparc_gen import __version__
from veloparc_gen.config import DEFAULT_CONFIG_DIR, DEFAULT_SCHEMA_DIR, load_config

app = typer.Typer(help="Veloparc synthetic data generator (fictitious company).", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Veloparc synthetic data generator (fictitious company)."""


@app.command()
def version() -> None:
    """Print the generator version."""
    typer.echo(__version__)


def _date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


@app.command()
def backfill(
    start: Annotated[str, typer.Option("--from", help="First day (YYYY-MM-DD); default from simulation.yaml")] = "",
    end: Annotated[str, typer.Option("--to", help="Last day (YYYY-MM-DD); default from simulation.yaml")] = "",
    countries: Annotated[str, typer.Option(help="Comma-separated country codes, e.g. PT,ES,DE,PL")] = "",
    out: Annotated[Path, typer.Option(help="Output directory (landing, reference, manifests)")] = Path("out"),
    state_dir: Annotated[Path, typer.Option(help="Where offsets.json is kept between runs")] = Path("state"),
    seed: Annotated[int | None, typer.Option(help="Override the seed in simulation.yaml")] = None,
    scale: Annotated[float | None, typer.Option(help="Volume multiplier (1.0 = reference volume)")] = None,
    anomalies: Annotated[bool, typer.Option("--anomalies/--no-anomalies", help="Inject anomalies")] = True,
    reset: Annotated[bool, typer.Option(help="Replace existing output and offsets")] = False,
    config_dir: Annotated[Path, typer.Option(help="Directory with the 3 YAML files")] = DEFAULT_CONFIG_DIR,
    schema_dir: Annotated[Path, typer.Option(help="Directory with the JSON Schemas")] = DEFAULT_SCHEMA_DIR,
) -> None:
    """Generate the history in one go and write it in the S3 Sink landing layout."""
    from veloparc_gen.run import OutputExists
    from veloparc_gen.run import backfill as run_backfill
    from veloparc_gen.validate import ValidationFailed

    config = load_config(
        config_dir,
        schema_dir,
        start=_date(start) if start else None,
        end=_date(end) if end else None,
        countries=[c.strip().upper() for c in countries.split(",") if c.strip()] or None,
        seed=seed,
        scale=scale,
        anomalies_enabled=anomalies,
    )
    try:
        result = run_backfill(config, out, state_dir, reset=reset, log=typer.echo)
    except (OutputExists, ValidationFailed) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    manifest = result.run_manifest
    rules = manifest["rules"]
    typer.echo(f"run {result.run_id}")
    typer.echo(f"  records:   {sum(manifest['records'].values())}  files: {manifest['sink']['files']}")
    typer.echo(f"  anomalies: {sum(s['manifest_entries'] for s in manifest['anomalies'].values())}")
    typer.echo(f"  manifests: {result.run_dir}")
    if rules["unexplained"]:
        typer.secho(f"  rules violated without an injected anomaly: {rules['unexplained']}", fg=typer.colors.RED)
        raise typer.Exit(code=2)


@app.command()
def publish(
    start: Annotated[str, typer.Option("--from", help="First day (YYYY-MM-DD)")],
    end: Annotated[str, typer.Option("--to", help="Last day (YYYY-MM-DD)")],
    countries: Annotated[str, typer.Option(help="Comma-separated country codes")] = "",
    out: Annotated[Path, typer.Option(help="Output for file-based topics and manifests")] = Path("out"),
    state_dir: Annotated[Path, typer.Option(help="Offsets of the file-based topics")] = Path("state"),
    seed: Annotated[int | None, typer.Option(help="Override the seed")] = None,
    scale: Annotated[float | None, typer.Option(help="Volume multiplier")] = None,
    anomalies: Annotated[bool, typer.Option("--anomalies/--no-anomalies")] = True,
    dry_run: Annotated[bool, typer.Option(help="Do not connect to Kafka: simulate the producer")] = False,
    reset: Annotated[bool, typer.Option(help="Replace existing output and offsets")] = False,
) -> None:
    """Publish what arrives in the window: kafka topics to Aiven, file topics as landing files."""
    from veloparc_gen.run import OutputExists, PublishFailed
    from veloparc_gen.run import publish as run_publish
    from veloparc_gen.sink.kafka import DryRunProducer
    from veloparc_gen.validate import ValidationFailed

    config = load_config(
        start=_date(start),
        end=_date(end),
        countries=[c.strip().upper() for c in countries.split(",") if c.strip()] or None,
        seed=seed,
        scale=scale,
        anomalies_enabled=anomalies,
    )
    producer = DryRunProducer({t.name: t.partitions for t in config.topics.values()}) if dry_run else None
    try:
        result = run_publish(config, out, state_dir, producer=producer, reset=reset, log=typer.echo)
    except (OutputExists, ValidationFailed, PublishFailed) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    manifest = result.run_manifest
    for stats in manifest["kafka"]["topics"].values():
        typer.echo(f"  {stats['name']}: {stats['delivered']}/{stats['messages']} delivered")
    typer.echo(f"run {result.run_id}{' (dry run)' if dry_run else ''}: manifests in {result.run_dir}")
    if manifest["rules"]["unexplained"]:
        typer.secho(f"  rules violated without an injected anomaly: {manifest['rules']['unexplained']}", fg="red")
        raise typer.Exit(code=2)


@app.command("kafka-setup")
def kafka_setup_cmd() -> None:
    """Create/align the Kafka topics, the producer ACL and the JSON Schemas on Aiven (idempotent)."""
    from veloparc_gen import kafka_setup
    from veloparc_gen.aiven import AivenError
    from veloparc_gen.kafka_conn import MissingSettings

    try:
        kafka_setup.run(load_config(), log=typer.echo)
    except (AivenError, MissingSettings, RuntimeError) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo("OK")


@app.command("kafka-check")
def kafka_check() -> None:
    """Connect to Aiven Kafka and the Schema Registry with the credentials in the environment."""
    import json

    from veloparc_gen.kafka_conn import MissingSettings, check

    try:
        result = check()
    except MissingSettings as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2))
    failed = [name for name, part in result.items() if not part["ok"]]
    if failed:
        typer.secho(f"FAILED: {', '.join(failed)}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    kafka = result["kafka"]
    typer.echo(
        f"OK: {kafka['brokers']} broker(s), {len(kafka['topics'])} topic(s), "
        f"{len(result['schema_registry']['subjects'])} schema subject(s)"
    )


@app.command("snowflake-check")
def snowflake_check() -> None:
    """Connect to Snowflake as the CI service user and show region, role and Veloparc objects."""
    import json

    from veloparc_gen.snowflake_conn import MissingSnowflakeSettings, check

    try:
        result = check()
    except MissingSnowflakeSettings as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2))
    if result["region"] and "EU_WEST_3" not in str(result["region"]).upper():
        typer.secho(f"WARNING: account region {result['region']} is not AWS eu-west-3 (Paris)", fg="yellow")
    typer.echo("OK")


@app.command("snowflake-inspect")
def snowflake_inspect() -> None:
    """Read-only: connector tables, landing and generator stages, UDF packages (input for the bronze layer)."""
    import json

    from veloparc_gen import snowflake_inspect as inspect_mod
    from veloparc_gen.snowflake_conn import MissingSnowflakeSettings

    try:
        result = inspect_mod.run()
    except MissingSnowflakeSettings as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2, default=str))
    if result["errors"]:
        typer.secho(f'{len(result["errors"])} statement(s) failed, see "errors"', fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    typer.echo("OK")


@app.command("connector-deploy")
def connector_deploy() -> None:
    """Rotate the connector key pair and create/update the Aiven Snowflake sink connector."""
    from veloparc_gen import connector
    from veloparc_gen.aiven import AivenError
    from veloparc_gen.snowflake_conn import MissingSnowflakeSettings

    try:
        connector.run(log=typer.echo)
    except (connector.ConnectorFailed, AivenError, MissingSnowflakeSettings) as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo("OK")


@app.command("stage-upload")
def stage_upload(
    out: Annotated[Path, typer.Option(help="Generator output folder")] = Path("out"),
    state_dir: Annotated[Path, typer.Option(help="Folder with offsets.json")] = Path("state"),
) -> None:
    """Upload landing files, run manifests, reference tables and offsets to the Snowflake stages."""
    import json

    from veloparc_gen import stage
    from veloparc_gen.snowflake_conn import MissingSnowflakeSettings, connect

    try:
        with connect() as conn, conn.cursor() as cur:
            result = stage.upload_output(cur, out, log=typer.echo)
            result["state"] = {"offsets.json": int(stage.push_state(cur, state_dir))}
    except MissingSnowflakeSettings as exc:
        typer.secho(str(exc), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(json.dumps(result, indent=2))


@app.command("stage-pull-state")
def stage_pull_state(
    state_dir: Annotated[Path, typer.Option(help="Where to put offsets.json")] = Path("state"),
) -> None:
    """Download the generator offsets saved by the previous run (if any)."""
    from veloparc_gen import stage
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        found = stage.pull_state(cur, state_dir)
    typer.echo("offsets.json restored from @GENERATOR/state" if found else "no saved offsets: starting from 0")


if __name__ == "__main__":
    app()
