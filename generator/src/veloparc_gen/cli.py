"""Command-line interface: ``veloparc-gen``."""

import typer

from veloparc_gen import __version__

app = typer.Typer(help="Veloparc synthetic data generator.", no_args_is_help=True)


@app.callback()
def main() -> None:
    """Veloparc synthetic data generator (fictitious company)."""


@app.command()
def version() -> None:
    """Print the generator version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
