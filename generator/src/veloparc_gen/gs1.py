"""GS1 identifiers (GLN, SSCC, EPC URN) and VAT numbers. They underpin rule R15 and the GS1_SSCC_FROM_URN UDF."""

from __future__ import annotations

import re

import numpy as np

_SSCC_URN = re.compile(r"^urn:epc:id:sscc:(?P<prefix>[0-9]{6,12})\.(?P<serial>[0-9]{5,11})$")


def check_digit(body: str) -> int:
    """GS1 mod-10 check digit: weights 3,1,3,... from the rightmost digit of the body."""
    if not body.isdigit():
        raise ValueError(f"GS1 body must be digits: {body!r}")
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10


def is_valid(code: str) -> bool:
    """True when the last digit of a GS1 key (GLN, SSCC, GTIN) is the right check digit."""
    return len(code) >= 2 and code.isdigit() and check_digit(code[:-1]) == int(code[-1])


def make_gln(company_prefix: str, location_ref: int) -> str:
    """13-digit GLN = company prefix + location reference (zero-padded) + check digit."""
    ref_len = 12 - len(company_prefix)
    if ref_len < 1 or location_ref >= 10**ref_len:
        raise ValueError(f"location_ref {location_ref} does not fit prefix {company_prefix}")
    body = f"{company_prefix}{location_ref:0{ref_len}d}"
    return body + str(check_digit(body))


def make_sscc(extension_digit: int, company_prefix: str, serial_ref: int) -> str:
    """18-digit SSCC = extension digit + company prefix + serial reference + check digit."""
    serial_len = 16 - len(company_prefix)
    if not 0 <= extension_digit <= 9 or serial_ref >= 10**serial_len:
        raise ValueError("extension digit or serial reference out of range")
    body = f"{extension_digit}{company_prefix}{serial_ref:0{serial_len}d}"
    return body + str(check_digit(body))


def sscc_to_epc_urn(sscc: str, company_prefix_length: int = 7) -> str:
    """SSCC -> pure identity URN ``urn:epc:id:sscc:<prefix>.<extension><serial>`` (no check digit)."""
    if len(sscc) != 18 or not is_valid(sscc):
        raise ValueError(f"invalid SSCC: {sscc}")
    extension, prefix = sscc[0], sscc[1 : 1 + company_prefix_length]
    serial = sscc[1 + company_prefix_length : 17]
    return f"urn:epc:id:sscc:{prefix}.{extension}{serial}"


def epc_urn_to_sscc(urn: str) -> str:
    """Inverse of sscc_to_epc_urn: rebuilds the 18-digit SSCC with its check digit."""
    match = _SSCC_URN.match(urn)
    if not match or len(match["prefix"]) + len(match["serial"]) != 17:
        raise ValueError(f"invalid SSCC EPC URN: {urn}")
    body = match["serial"][0] + match["prefix"] + match["serial"][1:]
    return body + str(check_digit(body))


# Fictitious VAT number shapes per country, without the country prefix (# = digit, @ = letter).
_VAT_SHAPES = {"PT": "#########", "ES": "@#######@", "DE": "#########", "PL": "##########"}
_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def vat_number(country_code: str, rng: np.random.Generator) -> str:
    """Random VAT number whose prefix is the country code (R15). Not a real, checksum-valid number."""
    shape = _VAT_SHAPES.get(country_code, "##########")
    chars = [
        str(rng.integers(10)) if ch == "#" else _LETTERS[rng.integers(len(_LETTERS))] if ch == "@" else ch
        for ch in shape
    ]
    return country_code + "".join(chars)


def vat_prefix_ok(vat: str, country_code: str) -> bool:
    return vat[:2] == country_code
