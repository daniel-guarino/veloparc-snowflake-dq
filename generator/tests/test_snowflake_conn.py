import pytest

from veloparc_gen.snowflake_conn import MissingSnowflakeSettings, connection_args


def test_defaults_and_pat_as_password():
    args = connection_args({"SNOWFLAKE_ACCOUNT": "myorg-myaccount", "SNOWFLAKE_PAT": "token-value"})
    assert args == {
        "account": "myorg-myaccount",
        "user": "SVC_VELOPARC_CI",
        "password": "token-value",  # pragma: allowlist secret
        "role": "VLP_DEPLOYER",
        "warehouse": "WH_DEPLOY",
    }


@pytest.mark.parametrize(
    "pasted",
    ["myorg.myaccount", "https://myorg-myaccount.snowflakecomputing.com", " myorg-myaccount "],
)
def test_account_identifier_formats(pasted):
    args = connection_args({"SNOWFLAKE_ACCOUNT": pasted, "SNOWFLAKE_PAT": "t"})
    assert args["account"] == "myorg-myaccount"


def test_missing_values_are_named():
    with pytest.raises(MissingSnowflakeSettings, match="SNOWFLAKE_PAT"):
        connection_args({"SNOWFLAKE_ACCOUNT": "a-b"})
