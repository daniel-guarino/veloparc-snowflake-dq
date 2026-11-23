"""Builds the coherent world in dependency order (contracts/README.md, "Ordem de geração")."""

from __future__ import annotations

from veloparc_gen.config import Config
from veloparc_gen.sources import crm, gl, invoices, master_data, reference, settlements, shipments, tariffs, tracking
from veloparc_gen.world import World


def build_world(config: Config) -> World:
    world = World(config)
    reference.build_fx(world)  # needed by tariffs (PLN prices) and invoices (R10)
    master_data.build(world)  # 1. depots, partners + zones, customers
    tariffs.build(world)  # 2. tariffs
    crm.build(world)  # 3. opportunities and leads
    shipment_records = shipments.build(world)  # 4. VeloShip shipments
    tracking.build(world, shipment_records)  # 5. tracking events
    invoices.build(world)  # 6. invoices and credit notes
    settlements.build(world)  # 7. partner settlements
    gl.build(world)  # 8. general ledger extract
    reference.build_tables(world)  # 9. POSTAL_ZONE, COUNTRY_ALIAS (FX_RATE_DAILY done above)
    return world


def check_world_rules(world: World, validator=None) -> list:
    """Runs R1-R16 on what silver would see: live records that pass their schema."""
    from veloparc_gen import rules
    from veloparc_gen.validate import SchemaValidator

    validator = validator or SchemaValidator(world)
    payloads: dict[str, list[dict]] = {}
    for source, records in world.records.items():
        for record in records:
            if record.dropped or record.schema_invalid:
                continue
            payloads.setdefault(source, []).append(record.payload(world))
    postal = {(c, digit): zone for c, digit, zone in world.postal_zones}
    return rules.check(
        payloads,
        fx=world.fx,
        postal_zones=postal,
        max_delay_minutes=world.sim.tracking.max_normal_delay_minutes,
    )
