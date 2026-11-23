"""Step 1 · master data: depots (GLN), partners and their zones (R16), customers with versions."""

from __future__ import annotations

from datetime import datetime, timedelta

from veloparc_gen import gs1
from veloparc_gen.world import Batch, Customer, Depot, Partner, Record, World, at_utc, money, utc_z

SOURCE = "master_data"
_PARTNER_SUFFIX = {"PT": "Entregas", "ES": "Reparto", "DE": "Kurierdienst", "PL": "Kurier"}
_SEGMENT_CHANGE = {"B2C_SHIPPER": "MARKETPLACE", "MARKETPLACE": "B2B_SHIPPER", "B2B_SHIPPER": "B2C_SHIPPER"}


def build(world: World) -> None:
    sim = world.sim
    if sim.network.depots_per_country < sim.network.zones_per_country:
        raise ValueError("network.depots_per_country must be >= zones_per_country: every zone needs a depot")
    for country in world.countries:
        _depots(world, country)
    for country in world.countries:
        _partners(world, country)
    for country in world.countries:
        _customers(world, country)
    for country in world.countries:
        for digit in range(10):
            world.postal_zones.append((country, str(digit), f"{country}-{world.zone_of_digit(digit)}"))
    _records(world)


def _depots(world: World, country: str) -> None:
    net = world.sim.network
    rng = world.rng("depots", country)
    fake = world.faker(country, "depots")
    prefix = world.sim.countries[country].gs1_company_prefix
    zones = world.zones(country)
    plan = (
        [("HUB", zones[int(rng.integers(len(zones)))]) for _ in range(net.hubs_per_country)]
        + [("DEPOT", zones[i % len(zones)]) for i in range(net.depots_per_country)]
        + [
            ("LOCKER" if rng.random() < net.locker_share else "PARCELSHOP", zones[i % len(zones)])
            for i in range(net.parcelshops_per_country)
        ]
    )
    scanner_no = 0
    for ref, (kind, zone) in enumerate(plan, start=1):
        city = fake.city()[:40]
        name = {
            "HUB": f"Veloparc Hub {city}",
            "DEPOT": f"Veloparc Depot {city}",
            "PARCELSHOP": f"ParcelShop {fake.last_name()} {city}",
            "LOCKER": f"Veloparc Locker {city} {ref:03d}",
        }[kind][:60]
        n_scanners = {"HUB": 6, "DEPOT": 4}.get(kind, 1)
        scanners = [f"SCN-{country}-{scanner_no + i + 1:04d}" for i in range(n_scanners)]
        scanner_no += n_scanners
        gln = gs1.make_gln(prefix, ref)
        world.depots[gln] = Depot(
            gln=gln,
            name=name,
            type=kind,
            country=country,
            city=city,
            postal_code=world.postal_code(country, zone, rng),
            zone=zone,
            scanners=scanners,
        )


def _partners(world: World, country: str) -> None:
    rng = world.rng("partners", country)
    fake = world.faker(country, "partners")
    zones = list(world.zones(country))
    rng.shuffle(zones)
    n_partners = min(world.sim.network.partners_per_country, len(zones))
    # Every zone gets exactly one partner (R16); the first partners take the remaining zones.
    groups = [[z] for z in zones[:n_partners]]
    for i, zone in enumerate(zones[n_partners:]):
        groups[i % n_partners].append(zone)
    currency = world.currency(country)
    cost = world.sim.settlements.cost_per_stop[currency]
    for zones_of_partner in groups:
        number = len(world.partners) + 1
        partner_id = f"P{number:03d}"
        base = world.depots_in(country, zones_of_partner[0], ("DEPOT",))
        partner = Partner(
            partner_id=partner_id,
            name=f"{fake.last_name()} {_PARTNER_SUFFIX.get(country, 'Logistics')}"[:80],
            country=country,
            zones=sorted(zones_of_partner),
            base_depot_gln=base[int(rng.integers(len(base)))].gln,
            cost_per_stop=money(rng.uniform(cost.min, cost.max)),
            parcels_per_stop=float(
                rng.uniform(world.sim.settlements.parcels_per_stop.min, world.sim.settlements.parcels_per_stop.max)
            ),
        )
        world.partners[partner_id] = partner
        for zone in partner.zones:
            world.zone_partner[zone] = partner_id


def _customers(world: World, country: str) -> None:
    sim = world.sim
    rng = world.rng("customers", country)
    fake = world.faker(country, "customers")
    cfg = sim.customers
    n = max(1, round(cfg.count * sim.scale * sim.countries[country].share))
    prefix = sim.countries[country].contact_id_prefix
    segments, seg_p = list(cfg.segments), list(cfg.segments.values())
    products, prod_p = list(sim.products.mix), list(sim.products.mix.values())
    depots = world.depots_in(country, types=("DEPOT",))
    weights = rng.pareto(cfg.volume_pareto_shape, size=n) + 1.0
    weights = weights / weights.sum() * sim.countries[country].share
    group_no = len({c.group_id for c in world.customers.values()})
    previous_group = None
    window_days = world.window_days
    for i in range(n):
        contact_id = world.unique_id(
            "contact",
            lambda: (
                prefix
                + "".join(str(d) for d in rng.integers(0, 10, size=9 - len(prefix)))
                + "ABCDEFGHJK"[rng.integers(10)]
            ),
        )
        if previous_group and rng.random() < 0.2:
            group_id = previous_group  # same corporate group as the previous customer
        else:
            group_no += 1
            group_id = f"GRP-{group_no:06d}"
        previous_group = group_id
        primary = products[rng.choice(len(products), p=prod_p)]
        enabled = [primary]
        if rng.random() < sim.products.second_product_share:
            enabled.append(next(p for p in products if p != primary))
        first_seen = _ts(world, cfg.first_seen, rng, span_days=150)
        if first_seen >= world.window_start_utc:
            first_seen = world.window_start_utc - timedelta(days=1)
        customer = Customer(
            contact_id=contact_id,
            group_id=group_id,
            name=fake.company()[:80],
            country=country,
            segment=segments[rng.choice(len(segments), p=seg_p)],
            vat=gs1.vat_number(country, rng),
            products=enabled,
            home_depot_gln=depots[int(rng.integers(len(depots)))].gln,
            volume_weight=float(weights[i]),
            first_seen=first_seen,
        )
        customer.versions.append(_customer_payload(customer, customer.name, customer.segment, first_seen))
        if rng.random() < cfg.two_versions_share:
            day = window_days[int(rng.integers(len(window_days)))]
            updated = _ts(world, day, rng, span_days=1)
            if rng.random() < 0.5:
                customer.versions.append(
                    _customer_payload(customer, customer.name, _SEGMENT_CHANGE[customer.segment], updated)
                )
            else:
                new_name = (customer.name.split(",")[0] + " Group")[:80]
                customer.versions.append(_customer_payload(customer, new_name, customer.segment, updated))
        world.customers[contact_id] = customer


def _ts(world: World, start, rng, span_days: int):
    return at_utc(start) + timedelta(seconds=int(rng.integers(0, span_days * 86400)))


def _customer_payload(customer: Customer, name: str, segment: str, updated) -> dict:
    return {
        "entity": "customer",
        "contactId": customer.contact_id,
        "groupCustomerId": customer.group_id,
        "name": name,
        "countryCode": customer.country,
        "segment": segment,
        "vatNumber": customer.vat,
        "updatedAt": utc_z(updated),
    }


def _records(world: World) -> None:
    """Depots, partners and first customer versions form the initial load (op R); later versions are updates."""
    rng = world.rng("master_records")
    initial: list[tuple[str, str, dict]] = []
    for d in world.depots.values():
        payload = {
            "entity": "depot",
            "gln": d.gln,
            "name": d.name,
            "type": d.type,
            "countryCode": d.country,
            "city": d.city,
            "postalCode": d.postal_code,
        }
        initial.append((f"depot:{d.gln}", d.country, payload))
    for p in world.partners.values():
        payload = {
            "entity": "partner",
            "partnerId": p.partner_id,
            "name": p.name,
            "countryCode": p.country,
            "zones": p.zones,
            "baseDepotGln": p.base_depot_gln,
        }
        initial.append((f"partner:{p.partner_id}", p.country, payload))
    for c in world.customers.values():
        initial.append((f"customer:{c.contact_id}", c.country, c.versions[0]))

    batch_id = world.uuid4(rng)
    for seq, (key, country, payload) in enumerate(initial, start=1):
        world.add(
            Record(
                source=SOURCE,
                key=key,
                source_ts=world.initial_load_at,
                arrival=world.initial_load_at + timedelta(milliseconds=seq),
                country=country,
                business_key=key,
                op="R",
                data=payload,
                batch=Batch(batch_id, seq, len(initial)),
            )
        )
    for c in world.customers.values():
        for version in c.versions[1:]:
            updated = datetime.fromisoformat(version["updatedAt"])
            world.add(
                Record(
                    source=SOURCE,
                    key=f"customer:{c.contact_id}",
                    source_ts=updated,
                    arrival=world.arrival_after(updated, rng),
                    country=c.country,
                    business_key=f"customer:{c.contact_id}",
                    op="U",
                    data=version,
                    batch=Batch(world.uuid4(rng), 1, 1),
                )
            )
