"""Snowflake connection for GitHub Actions: service user + programmatic access token (PAT), from the environment.

SNOWFLAKE_ACCOUNT (org-account identifier), SNOWFLAKE_USER (default SVC_VELOPARC_CI), SNOWFLAKE_PAT (secret),
SNOWFLAKE_ROLE (default VLP_DEPLOYER), SNOWFLAKE_WAREHOUSE (default WH_DEPLOY). Values are never printed.
"""

from __future__ import annotations

import os


class MissingSnowflakeSettings(RuntimeError):
    pass


def connection_args(env: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(os.environ if env is None else env)
    # Accepts "org-account", "org.account" (Snowsight's copy format) or the account URL.
    account = env.get("SNOWFLAKE_ACCOUNT", "").strip().removeprefix("https://").rstrip("/")
    account = account.removesuffix(".snowflakecomputing.com").replace(".", "-")
    token = env.get("SNOWFLAKE_PAT", "").strip()
    missing = [name for name, value in (("SNOWFLAKE_ACCOUNT", account), ("SNOWFLAKE_PAT", token)) if not value]
    if missing:
        raise MissingSnowflakeSettings(f"missing: {', '.join(missing)} (GitHub Environment dev)")
    return {
        "account": account,
        "user": env.get("SNOWFLAKE_USER", "").strip() or "SVC_VELOPARC_CI",
        "password": token,  # a PAT is accepted in place of a password
        "role": env.get("SNOWFLAKE_ROLE", "").strip() or "VLP_DEPLOYER",
        "warehouse": env.get("SNOWFLAKE_WAREHOUSE", "").strip() or "WH_DEPLOY",
    }


def connect(env: dict[str, str] | None = None):
    import snowflake.connector

    # UTC sessions: CURRENT_TIMESTAMP() defaults (_INSERTED_AT, _LOADED_AT) and displayed timestamps in UTC.
    return snowflake.connector.connect(
        **connection_args(env), application="veloparc-gen", login_timeout=30, session_parameters={"TIMEZONE": "UTC"}
    )


def check() -> dict:
    """Who and where we are, and which Veloparc objects already exist (no secret values)."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT CURRENT_REGION(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_VERSION()")
        region, role, warehouse, version = cur.fetchone()
        cur.execute("SHOW DATABASES LIKE 'VLP%'")
        databases = sorted(row[1] for row in cur.fetchall())
        stages: list[str] = []
        if "VLP_DP" in databases:
            cur.execute("SHOW STAGES IN SCHEMA VLP_DP.BRONZE")
            stages = sorted(row[1] for row in cur.fetchall())
    return {
        "region": region,
        "role": role,
        "warehouse": warehouse,
        "version": version,
        "databases": databases,
        "bronze_stages": stages,
    }
