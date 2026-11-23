import pytest

from veloparc_gen.kafka_conn import MissingSettings, kafka_config, normalize_pem

BODY = "MIIB" + "A" * 120 + "=="
PEM = f"-----BEGIN CERTIFICATE-----\n{BODY[:64]}\n{BODY[64:]}\n-----END CERTIFICATE-----\n"


@pytest.mark.parametrize(
    "pasted",
    [PEM, PEM.replace("\n", " "), PEM.replace("\n", "\\n"), "  " + PEM.replace("\n", "\r\n") + "  "],
)
def test_normalize_pem_survives_lost_newlines(pasted):
    assert normalize_pem(pasted) == PEM


def test_kafka_config_writes_private_ca(tmp_path):
    env = {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka.example.test:12345 ",
        "KAFKA_SASL_USERNAME": "vlp-generator",
        "KAFKA_SASL_PASSWORD": "not-a-real-password",  # pragma: allowlist secret
        "KAFKA_SSL_CA_PEM": PEM.replace("\n", " "),
    }
    cfg = kafka_config(env, ca_dir=tmp_path)
    assert cfg["bootstrap.servers"] == "kafka.example.test:12345"
    assert cfg["security.protocol"] == "SASL_SSL"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-256"
    assert (tmp_path / "ca.pem").read_text() == PEM
    assert oct((tmp_path / "ca.pem").stat().st_mode & 0o777) == "0o600"


def test_missing_settings_are_named_without_values():
    with pytest.raises(MissingSettings, match="KAFKA_SASL_PASSWORD"):
        kafka_config({"KAFKA_BOOTSTRAP_SERVERS": "x:1"})


URI_ESCAPED = "https://avnadmin:s3cr%40t@sr.example.test:443"  # pragma: allowlist secret
URI_WITH_USER = "https://avnadmin:x@sr.example.test:443"  # pragma: allowlist secret
BASE = "https://sr.example.test:443"


@pytest.mark.parametrize(
    ("pasted", "auth_secret", "base", "auth"),
    [
        (BASE, "u:p", BASE, "u:p"),
        ("  sr.example.test:443/ \n", "u:p", BASE, "u:p"),
        ('"https://sr.example.test:443"', "u:p", BASE, "u:p"),
        (URI_ESCAPED, "", BASE, "avnadmin:s3cr@t"),
        (URI_WITH_USER, "u:p", BASE, "u:p"),
    ],
)
def test_schema_registry_endpoint_accepts_console_formats(pasted, auth_secret, base, auth):
    from veloparc_gen.kafka_conn import schema_registry_endpoint

    env = {"SCHEMA_REGISTRY_URL": pasted, "SCHEMA_REGISTRY_BASIC_AUTH": auth_secret}
    assert schema_registry_endpoint(env) == (base, auth)


def test_describe_url_hides_values():
    from veloparc_gen.kafka_conn import describe_url

    shape = describe_url("https://avnadmin:topsecret@sr.example.test:443")  # pragma: allowlist secret
    assert "topsecret" not in shape and "example" not in shape
    assert "credentials_in_url=yes" in shape
