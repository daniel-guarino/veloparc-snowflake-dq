"""Upload generator output to the Snowflake internal stages (ADR 0003), from GitHub Actions.

    out/landing/kafka/...                    -> @VLP_DP.BRONZE.LANDING/landing/kafka/...  (same layout)
    out/_generator/runs/<run_id>/...         -> @VLP_DP.BRONZE.GENERATOR/runs/<run_id>/...
    out/reference/<TABLE>/<TABLE>.csv        -> @VLP_DP.BRONZE.GENERATOR/reference/<TABLE>/
    state/offsets.json                       <-> @VLP_DP.BRONZE.GENERATOR/state/offsets.json

One PUT per local folder (wildcard), files uploaded as they are (already gzip for the landing), OVERWRITE=TRUE:
the same seed produces the same bytes, so re-running a generation replaces files with identical ones.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

LANDING = "VLP_DP.BRONZE.LANDING"
GENERATOR = "VLP_DP.BRONZE.GENERATOR"


def _quote(text: str) -> str:
    return text.replace("'", "''")


def put_folder(cursor, folder: Path, stage: str, stage_path: str, parallel: int = 8) -> dict[str, int]:
    """PUT every file of one local folder to @stage/stage_path/. Returns counts per status."""
    stage_path = stage_path.strip("/")
    target = f"@{stage}/{stage_path}/" if stage_path else f"@{stage}/"
    sql = (
        f"PUT 'file://{_quote(str(folder.resolve()))}/*' '{_quote(target)}' "
        f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL={parallel}"
    )
    cursor.execute(sql)
    counts: dict[str, int] = defaultdict(int)
    for row in cursor.fetchall() or []:
        counts[str(row[6]) if len(row) > 6 else "UPLOADED"] += 1  # PUT result: status in the 7th column
    return dict(counts)


def upload_tree(cursor, root: Path, stage: str, prefix: str = "", log=print) -> dict[str, int]:
    """Uploads every file under root, keeping the relative folders below `prefix` on the stage."""
    folders = sorted({p.parent for p in root.rglob("*") if p.is_file()})
    totals: dict[str, int] = defaultdict(int)
    for n, folder in enumerate(folders, start=1):
        relative = folder.relative_to(root).as_posix()
        stage_path = "/".join(part for part in (prefix.strip("/"), "" if relative == "." else relative) if part)
        for status, count in put_folder(cursor, folder, stage, stage_path).items():
            totals[status] += count
        if n % 200 == 0 or n == len(folders):
            log(f"  {stage}: {n}/{len(folders)} folders, {sum(totals.values())} files")
    return dict(totals)


def upload_output(cursor, out_dir: Path, log=print) -> dict[str, dict[str, int]]:
    result = {}
    if (out_dir / "landing").exists():
        log("landing files -> @LANDING")
        result["landing"] = upload_tree(cursor, out_dir / "landing", LANDING, "landing", log)
    if (out_dir / "_generator" / "runs").exists():
        log("run manifests -> @GENERATOR/runs")
        result["runs"] = upload_tree(cursor, out_dir / "_generator" / "runs", GENERATOR, "runs", log)
    if (out_dir / "reference").exists():
        log("reference tables -> @GENERATOR/reference")
        result["reference"] = upload_tree(cursor, out_dir / "reference", GENERATOR, "reference", log)
    return result


def push_state(cursor, state_dir: Path) -> bool:
    offsets = state_dir / "offsets.json"
    if not offsets.exists():
        return False
    put_folder(cursor, state_dir, GENERATOR, "state")
    return True


def pull_state(cursor, state_dir: Path) -> bool:
    """Downloads state/offsets.json from the stage if a previous run stored it."""
    state_dir.mkdir(parents=True, exist_ok=True)
    cursor.execute(f"LIST @{GENERATOR}/state/offsets.json")
    if not cursor.fetchall():
        return False
    cursor.execute(f"GET '@{GENERATOR}/state/offsets.json' 'file://{_quote(str(state_dir.resolve()))}/'")
    return (state_dir / "offsets.json").exists()
