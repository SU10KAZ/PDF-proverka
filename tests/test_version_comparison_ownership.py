"""A second portal must not interrupt comparisons merely by reading them."""
import json

import pytest

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs, paths, recovery, series, work_queue


@pytest.mark.parametrize("executor", ["worker", "process"])
@pytest.mark.parametrize("state", ["queued", "running", "cancelling"])
@pytest.mark.parametrize("live", [True, False])
def test_reading_foreign_series_respects_process_identity(tmp_path, monkeypatch, executor, state, live):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", executor)
    monkeypatch.setattr(work_queue, "heartbeat", lambda: {"online": True, "active": 1})
    monkeypatch.setattr(work_queue, "cancel_run", lambda _: None)
    owner = recovery.identity() if live else {"pid": 99999999, "start": "0", "boot": "old"}
    run = {"run_id": "run-foreign", "state": state, "owner": "other-portal", "owner_process": owner,
           "items": [{"kind": "compare", "status": "running"}], "progress": {"total": 1, "done": 0}}
    data = {"series_id": "series-foreign", "state": state, "owner": "other-portal", "owner_process": owner,
            "pairs": [{"run_id": run["run_id"], "state": state}]}
    run_path = jobs._run_path("obj", run["run_id"])
    series_path = paths.series_path("obj", data["series_id"])
    atomic_write_json(run_path, run)
    atomic_write_json(series_path, data)

    result = series.get("obj", data["series_id"])
    expected = state if live else ("cancelled" if state == "cancelling" else "interrupted")
    assert result["state"] == expected
    assert result["pairs"][0]["run_state"] == expected
    if live:
        # GET must preserve both the persisted state and in-flight sheet status.
        assert json.loads(run_path.read_text()) == run
        assert json.loads(series_path.read_text()) == data
