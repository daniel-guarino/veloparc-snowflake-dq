from typer.testing import CliRunner

from veloparc_gen import __version__
from veloparc_gen.cli import app


def test_version_command():
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__
