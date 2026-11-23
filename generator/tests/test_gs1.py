import numpy as np
import pytest

from veloparc_gen import gs1


def test_check_digit_matches_contract_examples():
    assert gs1.is_valid("5601234000016")  # depot GLN in contracts/examples
    assert gs1.is_valid("356012340000000017")  # SSCC in contracts/examples
    assert gs1.is_valid("356012340000000024")
    assert not gs1.is_valid("5601234000017")


def test_make_gln_and_sscc():
    assert gs1.make_gln("5601234", 1) == "5601234000016"
    assert gs1.make_sscc(3, "5601234", 1) == "356012340000000017"
    assert gs1.make_sscc(3, "5601234", 2) == "356012340000000024"
    with pytest.raises(ValueError):
        gs1.make_gln("5601234", 100_000)


def test_sscc_epc_urn_round_trip():
    urn = gs1.sscc_to_epc_urn("356012340000000017")
    assert urn == "urn:epc:id:sscc:5601234.3000000001"  # tracking_event example
    assert gs1.epc_urn_to_sscc(urn) == "356012340000000017"
    rng = np.random.default_rng(1)
    for serial in rng.integers(0, 10**9, size=50):
        sscc = gs1.make_sscc(int(serial % 10), "8401234", int(serial))
        assert gs1.epc_urn_to_sscc(gs1.sscc_to_epc_urn(sscc)) == sscc


def test_invalid_inputs_rejected():
    with pytest.raises(ValueError):
        gs1.sscc_to_epc_urn("356012340000000018")
    with pytest.raises(ValueError):
        gs1.epc_urn_to_sscc("urn:epc:id:sscc:5601234.30000001")
    with pytest.raises(ValueError):
        gs1.check_digit("12a")


@pytest.mark.parametrize("country", ["PT", "ES", "DE", "PL"])
def test_vat_number_has_country_prefix_and_matches_schema(country):
    import re

    rng = np.random.default_rng(7)
    for _ in range(20):
        vat = gs1.vat_number(country, rng)
        assert gs1.vat_prefix_ok(vat, country)
        assert re.fullmatch(r"[A-Z]{2}[0-9A-Z]{8,12}", vat)
