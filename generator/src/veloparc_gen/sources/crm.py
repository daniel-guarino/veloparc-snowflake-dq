"""Step 3 · CRM: opportunities ("Closed Won" linked to existing customers) and leads (25% converted)."""

from __future__ import annotations

from datetime import timedelta

from veloparc_gen.world import ALNUM_MIXED, Batch, Record, World, at_utc, days, salesforce_ts

SOURCE = "crm_object"
_OPEN_STAGES = ["Prospecting", "Qualification", "Proposal", "Negotiation"]
_LEAD_STATUSES = ["Open", "Working", "Qualified", "Unqualified"]
_LEAD_SOURCES = ["Web", "Referral", "Event", "Partner", "Outbound"]
_DEALS = ["national contract", "e-commerce volume deal", "B2B distribution", "peak season pilot", "returns service"]


def build(world: World) -> None:
    sim = world.sim
    cfg = sim.crm
    rng = world.rng("crm")
    share = sum(c.share for c in sim.countries.values())
    countries = world.countries
    country_p = [sim.countries[c].share / share for c in countries]
    n_opps = max(1, round(cfg.opportunities * sim.scale * share))
    n_leads = max(1, round(cfg.leads * sim.scale * share))
    history = days(cfg.history_start, sim.window.end)
    fakers = {c: world.faker(c, "crm") for c in countries}

    def sf_id(prefix: str) -> str:
        return world.unique_id("sfid", lambda: prefix + world.random_string(rng, ALNUM_MIXED, 15))

    def random_ts(span: list) -> object:
        return at_utc(span[int(rng.integers(len(span)))], float(rng.uniform(8, 19)))

    customers = list(world.customers.values())
    n_won = min(round(n_opps * cfg.closed_won_share), len(customers))
    won_customers = [customers[i] for i in rng.choice(len(customers), size=n_won, replace=False)]

    opportunities: list[tuple[dict, object, str]] = []
    for i in range(n_opps):
        if i < n_won:
            customer = won_customers[i]
            country, company = customer.country, customer.versions[0]["name"]
            closed = customer.first_seen - timedelta(days=int(rng.integers(3, 45)), hours=int(rng.integers(0, 8)))
            stage, close_date, modified, contact = "Closed Won", closed.date(), closed, customer.contact_id
        else:
            country = countries[rng.choice(len(countries), p=country_p)]
            company = fakers[country].company()
            contact = None
            if rng.random() < 0.3:
                stage = "Closed Lost"
                modified = random_ts(history)
                close_date = modified.date()
            else:
                stage = _OPEN_STAGES[int(rng.integers(len(_OPEN_STAGES)))]
                modified = random_ts(history)
                close_date = sim.window.end + timedelta(days=int(rng.integers(15, 120)))
        currency = world.currency(country)
        amount = round(float(rng.lognormal(10.3, 0.8)), 2)
        payload = {
            "attributes": {"type": "Opportunity"},
            "Id": sf_id("006"),
            "AccountId": sf_id("001"),
            "VLP_ContactID__c": contact,
            "Name": f"{company} - {_DEALS[int(rng.integers(len(_DEALS)))]} {close_date.year}"[:120],
            "StageName": stage,
            "Amount": amount if stage == "Closed Won" or rng.random() < 0.7 else None,
            "CurrencyIsoCode": currency,
            "CloseDate": close_date.isoformat(),
            "Country__c": country,
            "Expected_Parcels_Month__c": int(rng.integers(50, 5000)),
            "LastModifiedDate": salesforce_ts(modified),
        }
        opportunities.append((payload, modified, country))

    leads: list[tuple[dict, object, str]] = []
    n_converted = min(round(n_leads * cfg.converted_lead_share), len(opportunities))
    converted_opps = [opportunities[i] for i in rng.choice(len(opportunities), size=n_converted, replace=False)]
    for i in range(n_leads):
        if i < n_converted:
            opp, opp_modified, country = converted_opps[i]
            created = opp_modified - timedelta(days=int(rng.integers(15, 120)), hours=int(rng.integers(0, 10)))
            status, company, converted_id = "Converted", opp["Name"].split(" - ")[0], opp["Id"]
        else:
            country = countries[rng.choice(len(countries), p=country_p)]
            created = random_ts(history)
            status, company, converted_id = (
                _LEAD_STATUSES[int(rng.integers(len(_LEAD_STATUSES)))],
                fakers[country].company(),
                None,
            )
        payload = {
            "attributes": {"type": "Lead"},
            "Id": sf_id("00Q"),
            "Company": company[:120],
            "Country": country,  # clean contract: ISO 3166-1 alpha-2 (anything else is a Validity anomaly)
            "LeadSource": _LEAD_SOURCES[int(rng.integers(len(_LEAD_SOURCES)))],
            "Status": status,
            "CreatedDate": salesforce_ts(created),
        }
        if converted_id:
            payload["ConvertedOpportunityId"] = converted_id
        leads.append((payload, created, country))

    _records(world, opportunities + leads)


def _records(world: World, objects: list[tuple[dict, object, str]]) -> None:
    rng = world.rng("crm_records")
    for payload, ts, country in sorted(objects, key=lambda o: (o[1], o[0]["Id"])):
        before_window = ts < world.window_start_utc
        world.add(
            Record(
                source=SOURCE,
                key=payload["Id"],
                source_ts=ts,
                arrival=world.arrival_after(ts, rng),
                country=country,
                business_key=payload["Id"],
                op="R" if before_window else "I",
                data=payload,
                batch=Batch(world.uuid4(rng), 1, 1),
            )
        )
