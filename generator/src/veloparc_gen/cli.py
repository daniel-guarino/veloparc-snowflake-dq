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


if __name__ == "__main__":
    app()
