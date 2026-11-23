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
