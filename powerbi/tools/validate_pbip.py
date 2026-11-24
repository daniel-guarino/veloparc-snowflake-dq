"""Offline checks for the Power BI project in powerbi/ (no Power BI Desktop needed).

1. Every JSON file parses (report, theme, item files).
2. TMDL: model.tmdl refs match the table files; tables, columns and measures parse.
3. Every column imported from Snowflake exists in the dbt model it comes from (_gold.yml / _dq.yml, or the model SQL
   and macros for columns the yml does not list, e.g. dq_issue's lineage columns from the dq_violation macro).
4. Every DAX reference (table[column], [measure]) and every report field (visual.json, page filters) exists in the
   model, and no measure references a table that is not imported.
5. Relationships point to existing columns and the active ones leave a single filter path between any two tables.
6. No Snowflake account identifier is committed (only the <account> placeholder), and the theme copy that Desktop
   loads is identical to powerbi/theme/veloparc.json.

Usage: python powerbi/tools/validate_pbip.py  (exit code 1 on any error)
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # powerbi/
REPO = ROOT.parent
SM = ROOT / "Veloparc.SemanticModel" / "definition"
REPORT = ROOT / "Veloparc.Report"
DBT = REPO / "dbt" / "veloparc"
DERIVED = {"dim_month", "dim_country", "_Measures"}  # built in Power Query, not read from one dbt model

errors: list[str] = []
notes: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


# ---------------------------------------------------------------- 1. JSON
def check_json() -> int:
    n = 0
    for p in sorted(ROOT.rglob("*")):
        if p.is_file() and (p.suffix in {".json", ".pbir", ".pbism", ".pbip"} or p.name == ".platform"):
            try:
                json.loads(p.read_text(encoding="utf-8"))
                n += 1
            except json.JSONDecodeError as e:
                err(f"JSON: {p.relative_to(REPO)}: {e}")
    return n


# ---------------------------------------------------------------- 2. TMDL
def unquote(name: str) -> str:
    name = name.strip()
    return name[1:-1].replace("''", "'") if name.startswith("'") and name.endswith("'") else name


def parse_tmdl_tables() -> dict:
    tables = {}
    for f in sorted((SM / "tables").glob("*.tmdl")):
        lines = f.read_text(encoding="utf-8").split("\n")
        tname, cols, measures, partition = None, {}, {}, []
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith(" "):
                err(f"TMDL: {f.name}:{i + 1}: indentation must use tabs")
            if line.startswith("table "):
                tname = unquote(line[6:])
            elif line.startswith("\tcolumn "):
                cname = unquote(line[8:])
                props = {}
                i += 1
                while i < len(lines) and lines[i].startswith("\t\t"):
                    m = re.match(r"\t\t(\w+)(?::\s*(.*))?$", lines[i])
                    if m:
                        props[m.group(1)] = (m.group(2) or "true").strip()
                    i += 1
                cols[cname] = props
                continue
            elif line.startswith("\tmeasure "):
                m = re.match(r"\tmeasure ('(?:[^']|'')+'|\S+) =(.*)$", line)
                if not m:
                    err(f"TMDL: {f.name}:{i + 1}: cannot parse measure")
                    i += 1
                    continue
                mname, dax = unquote(m.group(1)), m.group(2).strip()
                props = {}
                i += 1
                body = []
                while i < len(lines) and lines[i].startswith("\t\t\t"):
                    body.append(lines[i][3:])
                    i += 1
                while i < len(lines) and lines[i].startswith("\t\t") and not lines[i].startswith("\t\t\t"):
                    pm = re.match(r"\t\t(\w+)(?::\s*(.*))?$", lines[i])
                    if pm:
                        props[pm.group(1)] = (pm.group(2) or "true").strip()
                    i += 1
                measures[mname] = {"dax": dax or "\n".join(body), "props": props}
                if (
                    "formatString" not in props
                    and not mname.startswith("Headline")
                    and not re.search(r"(status|color|label|cell)$", mname, re.I)
                ):
                    err(f"TMDL: measure [{mname}] has no formatString")
                continue
            elif line.startswith("\tpartition "):
                i += 1
                while i < len(lines) and (lines[i].startswith("\t\t") or lines[i] == ""):
                    partition.append(lines[i])
                    i += 1
                continue
            i += 1
        if tname is None:
            err(f"TMDL: {f.name}: no table declaration")
            continue
        if f.stem != tname:
            err(f"TMDL: {f.name}: file name does not match table {tname}")
        tables[tname] = {"cols": cols, "measures": measures, "partition": "\n".join(partition)}
    model_text = (SM / "model.tmdl").read_text(encoding="utf-8")
    refs = {unquote(x) for x in re.findall(r"^ref table (.+)$", model_text, re.M)}
    if refs != set(tables):
        err(f"TMDL: model.tmdl refs {sorted(refs ^ set(tables))} do not match the table files")
    for need in ("database.tmdl", "model.tmdl", "expressions.tmdl", "relationships.tmdl"):
        if not (SM / need).exists():
            err(f"TMDL: missing {need}")
    if not (SM / "database.tmdl").read_text(encoding="utf-8").startswith("database "):
        err("TMDL: database.tmdl must start with a named database declaration")
    return tables


# ---------------------------------------------------------------- 3. columns vs dbt
def dbt_columns() -> dict:
    """model -> set of column names, from the yml files (simple line parser, no PyYAML needed)."""
    out = defaultdict(set)
    for yml in (DBT / "models" / "gold" / "_gold.yml", DBT / "models" / "dq" / "_dq.yml"):
        model, model_indent, section = None, None, None
        for line in yml.read_text(encoding="utf-8").split("\n"):
            top = re.match(r"^(\w+):\s*$", line)
            if top:
                section, model_indent = top.group(1), None
                continue
            if section != "models":
                continue
            m = re.match(r"^(\s*)- name: (\w+)\s*$", line)
            if not m:
                continue
            indent = len(m.group(1))
            if model_indent is None or indent == model_indent:
                model_indent, model = indent, m.group(2)
                out[model]
            elif indent > model_indent and model:
                out[model].add(m.group(2))
    return out


def sql_columns(model: str) -> set:
    """Output column aliases of a model's SQL (and of the dq_violation macro for dq models)."""
    paths = list((DBT / "models").rglob(f"{model}.sql"))
    text = "\n".join(p.read_text(encoding="utf-8") for p in paths)
    if model.startswith("dq_"):
        text += "\n" + (DBT / "macros" / "dq.sql").read_text(encoding="utf-8")
    cols = set(re.findall(r"\bas\s+(\w+)", text, re.I))
    cols |= set(re.findall(r"\b[a-z]\.(\w+)\s*,?\s*$", text, re.M))  # bare "c.dimension," style selects
    return {c.lower() for c in cols}


def check_imports(tables: dict) -> int:
    yml = dbt_columns()
    n = 0
    for tname, t in tables.items():
        part = t["partition"]
        for schema, table, typelist in re.findall(r'fnVeloparcTable\("(\w+)", "(\w+)", \{(.*)\}\)', part):
            model = table.lower()
            names = re.findall(r'\{"(\w+)", ', typelist)
            if model not in yml:
                err(f"columns: {tname}: {schema}.{table} is not a model in _gold.yml/_dq.yml")
                continue
            sql = sql_columns(model)
            for c in names:
                n += 1
                if c in yml[model]:
                    continue
                if c in sql:
                    notes.append(f"{model}.{c}: not listed in the yml, found in the model SQL/macro")
                    continue
                err(f"columns: {tname}: {model}.{c} not found in dbt (yml or SQL)")
            if tname not in DERIVED:
                declared = {p.get("sourceColumn") for p in t["cols"].values()}
                if set(names) != declared:
                    err(f"columns: {tname}: imported {sorted(set(names) ^ declared)} differ from the TMDL columns")
        for _schema, table, c in re.findall(r'\{"(GOLD|DQ)", "(\w+)", "(\w+)"\}', part):
            n += 1
            model = table.lower()
            if c not in yml.get(model, set()) and c not in sql_columns(model):
                err(f"columns: {tname}: {model}.{c} not found in dbt")
    return n


# ---------------------------------------------------------------- 4. DAX and report fields
def strip_strings(dax: str) -> str:
    return re.sub(r'"(?:[^"]|"")*"', '""', dax)


def check_dax(tables: dict) -> int:
    allm = {m for t in tables.values() for m in t["measures"]}
    n = 0
    for t in tables.values():
        for mname, m in t["measures"].items():
            dax = strip_strings(m["dax"])
            for tbl, colname in re.findall(r"('(?:[^']|'')+'|\b[A-Za-z_]\w*)\[([^\]]+)\]", dax):
                n += 1
                tbl = unquote(tbl)
                if tbl not in tables:
                    err(f"DAX: [{mname}] references table {tbl}, which is not imported")
                elif colname not in tables[tbl]["cols"]:
                    err(f"DAX: [{mname}] references {tbl}[{colname}], which does not exist")
            for ref in re.findall(r"(?<![\w'\]])\[([^\]]+)\]", dax):
                n += 1
                if ref not in allm:
                    err(f"DAX: [{mname}] references measure [{ref}], which does not exist")
    return n


def walk_fields(obj, found):
    if isinstance(obj, dict):
        for kind in ("Column", "Measure"):
            v = obj.get(kind)
            if isinstance(v, dict) and "Property" in v:
                ent = v.get("Expression", {}).get("SourceRef", {}).get("Entity")
                found.append((kind, ent, v["Property"]))
        for v in obj.values():
            walk_fields(v, found)
    elif isinstance(obj, list):
        for v in obj:
            walk_fields(v, found)


def check_report(tables: dict) -> int:
    n = 0
    for p in sorted((REPORT / "definition").rglob("*.json")):
        found = []
        walk_fields(json.loads(p.read_text(encoding="utf-8")), found)
        for kind, ent, prop in found:
            n += 1
            where = p.relative_to(ROOT)
            if ent not in tables:
                err(f"report: {where}: table {ent} is not in the model")
            elif kind == "Column" and prop not in tables[ent]["cols"]:
                err(f"report: {where}: column {ent}[{prop}] is not in the model")
            elif kind == "Measure" and prop not in tables[ent]["measures"]:
                err(f"report: {where}: measure {ent}[{prop}] is not in the model")
    return n


# ---------------------------------------------------------------- 5. relationships
def check_relationships(tables: dict) -> int:
    text = (SM / "relationships.tmdl").read_text(encoding="utf-8")
    rels = []
    for block in text.split("relationship ")[1:]:
        fc = re.search(r"fromColumn: (.+)", block).group(1).strip()
        tc = re.search(r"toColumn: (.+)", block).group(1).strip()
        active = "isActive: false" not in block
        (ft, fcol), (tt, tcol) = [tuple(unquote(x) for x in s.split(".", 1)) for s in (fc, tc)]
        for t, c in ((ft, fcol), (tt, tcol)):
            if t not in tables or c not in tables[t]["cols"]:
                err(f"relationship {fc} -> {tc}: {t}[{c}] does not exist")
        rels.append((ft, tt, active))
    # filters flow from the one side (to) to the many side (from); count active paths between every pair
    graph = defaultdict(list)
    for ft, tt, active in rels:
        if active:
            graph[tt].append(ft)

    def paths(src, dst, seen):
        if src == dst:
            return 1
        return sum(paths(n, dst, seen | {n}) for n in graph[src] if n not in seen)

    for a in tables:
        for b in tables:
            if a != b and paths(a, b, {a}) > 1:
                err(f"relationships: more than one active filter path from {a} to {b} (ambiguous model)")
    return len(rels)


# ---------------------------------------------------------------- 6. secrets and theme copy
def check_secrets_and_theme() -> None:
    pat = re.compile(r"([A-Za-z0-9_<>.-]+)\.snowflakecomputing\.com")
    for p in ROOT.rglob("*"):
        if p.is_file() and p.suffix in {".tmdl", ".json", ".md", ".pbir", ".pbism", ".pbip", ".py"}:
            for host in pat.findall(p.read_text(encoding="utf-8", errors="ignore")):
                if host not in {"<account>"}:
                    err(f"secrets: {p.relative_to(REPO)} contains a Snowflake host '{host}.snowflakecomputing.com'")
    canonical = (ROOT / "theme" / "veloparc.json").read_text(encoding="utf-8")
    name = json.loads(canonical)["name"]
    loaded = REPORT / "StaticResources" / "RegisteredResources" / name
    if not loaded.exists() or loaded.read_text(encoding="utf-8") != canonical:
        err(f"theme: {loaded.relative_to(REPO)} must be an identical copy of powerbi/theme/veloparc.json")
    report = json.loads((REPORT / "definition" / "report.json").read_text(encoding="utf-8"))
    if report["themeCollection"].get("customTheme", {}).get("name") != name:
        err("theme: report.json customTheme does not reference the theme file")


def main() -> int:
    n_json = check_json()
    tables = parse_tmdl_tables()
    n_cols = check_imports(tables)
    n_dax = check_dax(tables)
    n_fields = check_report(tables)
    n_rels = check_relationships(tables)
    check_secrets_and_theme()
    n_meas = sum(len(t["measures"]) for t in tables.values())
    print(f"JSON files parsed: {n_json}")
    print(f"TMDL tables: {len(tables)}, measures: {n_meas}, relationships: {n_rels}")
    print(f"imported columns checked against dbt: {n_cols}")
    print(f"DAX references checked: {n_dax}; report field references checked: {n_fields}")
    for note in sorted(set(notes)):
        print(f"note: {note}")
    for e in errors:
        print(f"ERROR {e}")
    print("OK" if not errors else f"{len(errors)} error(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
