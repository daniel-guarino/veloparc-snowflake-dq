"""Stage upload: one PUT per folder, same layout on the stage, state round trip (fake cursor)."""

from veloparc_gen import stage


class FakeCursor:
    def __init__(self, listed=()):
        self.sql = []
        self.listed = list(listed)
        self._last = []

    def execute(self, sql):
        self.sql.append(sql)
        if sql.startswith("PUT"):
            self._last = [("f", "f", 1, 1, "GZIP", "GZIP", "UPLOADED", "")]
        elif sql.startswith("LIST"):
            self._last = self.listed
        else:
            self._last = []

    def fetchall(self):
        return self._last


def _touch(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def test_upload_output_keeps_the_landing_layout(tmp_path):
    out = tmp_path / "out"
    hour = "landing/kafka/vlp.ops.tracking-event.v1/year=2026/month=09/day=01/hour=16"
    _touch(out / hour / "vlp.ops.tracking-event.v1+0+0000000000.json.gz")
    _touch(out / hour / "vlp.ops.tracking-event.v1+1+0000000000.json.gz")
    _touch(out / "landing/kafka/vlp.finance.gl-extract.v1/year=2026/month=10/day=15/x.json.gz")
    _touch(out / "_generator/runs/backfill-x/run_manifest.json")
    _touch(out / "reference/FX_RATE_DAILY/FX_RATE_DAILY.csv")
    cur = FakeCursor()
    result = stage.upload_output(cur, out, log=lambda *_: None)
    puts = [s for s in cur.sql if s.startswith("PUT")]
    assert len(puts) == 4  # one per folder, not per file
    assert any(f"'@VLP_DP.BRONZE.LANDING/{hour}/'" in s for s in puts)
    assert any("'@VLP_DP.BRONZE.GENERATOR/runs/backfill-x/'" in s for s in puts)
    assert any("'@VLP_DP.BRONZE.GENERATOR/reference/FX_RATE_DAILY/'" in s for s in puts)
    assert all("AUTO_COMPRESS=FALSE" in s and "OVERWRITE=TRUE" in s for s in puts)
    assert result["landing"] == {"UPLOADED": 2}


def test_state_push_and_pull(tmp_path):
    state = tmp_path / "state"
    cur = FakeCursor()
    assert stage.push_state(cur, state) is False  # nothing to push yet
    _touch(state / "offsets.json")
    assert stage.push_state(cur, state) is True
    assert "'@VLP_DP.BRONZE.GENERATOR/state/'" in cur.sql[-1]
    assert stage.pull_state(FakeCursor(listed=[]), tmp_path / "empty") is False
    cur = FakeCursor(listed=[("state/offsets.json", 10, "md5", "date")])
    stage.pull_state(cur, state)
    assert cur.sql[-1].startswith("GET '@VLP_DP.BRONZE.GENERATOR/state/offsets.json'")


def test_publish_keeps_existing_offsets(tmp_path):
    from veloparc_gen.run import OutputExists, prepare

    state = tmp_path / "state"
    _touch(state / "offsets.json")
    prepare(tmp_path / "out", state, reset=False, keep_state=True)  # publish: fine
    assert (state / "offsets.json").exists()
    try:
        prepare(tmp_path / "out", state, reset=False)  # backfill: must refuse
    except OutputExists:
        pass
    else:
        raise AssertionError("backfill should refuse existing offsets without --reset")
