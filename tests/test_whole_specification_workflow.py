"""One sequential launch, distinct evidence routes and one MD; offline executors only."""
import json
import time
from pathlib import Path

import pytest

from backend.app.services.version_comparison import paths, specification_export as export
from backend.app.services.version_comparison_ai import executor, jobs, sheet_match, stage_queue as queue
from backend.app.services.version_comparison_ai import whole_specifications as whole
from backend.app.services.version_comparison_ai.sheet_changes import series_summary


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    # Механика этапа (повторы, квота, остановка) проверяется на разметке каждой версии целиком;
    # цепочка с наследованием — test_inherit_sends_second_version_without_a_full_call.
    monkeypatch.setenv("VC_WS_INHERIT", "0")
    versions = {vid: {"version_id": vid, "sha256": char * 64, "file_name": vid + ".pdf", "user_label": label}
                for vid, char, label in (("va", "a", "V1"), ("vb", "b", "V2"))}
    write(paths.catalog_path("obj"), {"projects": [{"project_id": "proj", "code": "TEST", "versions": list(versions)}],
                                     "versions": versions, "object_id": "obj"})
    run = {"object_id": "obj", "run_id": "run", "project_id": "proj", "state": "done",
           "a": versions["va"] | {"label": "V1"}, "b": versions["vb"] | {"label": "V2"},
           "items": [{"spid": "plan", "title": "План со встроенной спецификацией", "kind": "compare", "status": "done",
                      "a_page": 1, "b_page": 1, "result_dir": "plan", "result_status": "different"},
                     {"spid": "spec", "title": "Спецификация", "kind": "compare", "status": "done", "spec": {},
                      "a_page": 2, "b_page": 3, "result_dir": "spec", "result_status": "different"}]}
    write(paths.run_dir("obj", "run") / "run.json", run)
    write(paths.series_path("obj", "series"), {"object_id": "obj", "series_id": "series", "project_id": "proj", "state": "done",
        "pairs": [{"a": "va", "b": "vb", "run_id": "run", "run_state": "done", "a_label": "V1", "b_label": "V2"}]})
    for vid, page in (("va", 2), ("vb", 3)):
        write(paths.profile_dir("obj", versions[vid]["sha256"]) / "pages.json", {"pages": [
            {"page": 1, "kind": "spec", "stamp": {"title": "План со встроенной спецификацией"}},
            {"page": page, "kind": "spec", "stamp": {"title": "Спецификация"}}]})
    def prepare(odir, codes, output):
        output.mkdir(parents=True)
        sides = []
        for vid, page, qty in (("va", 2, "10"), ("vb", 3, "12")):
            text = (f"# Document: {vid}.pdf\n## Page {page} — Sheet 1 — Спецификация\n"
                    f"### BLOCK #1 [TEXT]: blk_{vid[-1] * 8}\n"
                    "| Поз. | Наименование | Марка | Код | Поставщик | Ед. изм. | Количество | Масса | Примечание |\n"
                    "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
                    f"| 1 | Кабель | AB-1 | К1 | Завод | м | {qty} | | |\n")
            (output / (vid + ".md")).write_text(text)
            sides.append({"path": vid + ".md", "version_id": vid, "label": versions[vid]["user_label"],
                          "pdf_sha256": versions[vid]["sha256"], "pages": [page]})
        write(output / "manifest.json", {"schema": "whole-specification-input/v1",
            "pairs": [{"id": "va--vb", "old": sides[0], "new": sides[1]}]})
    monkeypatch.setattr(whole.specification_prepare, "prepare", prepare)
    fake = executor.FakeDescriber(lambda task: {"sections": [], "parents": [], "continuations": [], "notes": []},
                                  model="gpt-6-luna")
    executor.set_override(fake)
    yield run, fake
    executor.set_override(None)


def finish_spec():
    for _ in range(500):
        st = whole.status("obj", "series")
        if st["state"] not in ("running", "pausing"):
            return st
        time.sleep(.01)
    pytest.fail("spec stage did not finish")


def test_full_spec_stage_builds_publishes_and_reuses_without_model_or_pdf(corpus, monkeypatch):
    _, fake = corpus
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result
    assert len(fake.calls) == 2 and whole.ready("obj", "series")
    cmp = export.context("obj", "run")["reports"][0]["comparison"]
    assert cmp["counts"]["changed"] == 1
    monkeypatch.setattr(executor, "get", lambda *args: pytest.fail("published data must not probe Codex"))
    assert whole.start("obj", "series", consent=True)["reused"]
    assert len(fake.calls) == 2
    md = series_summary.build("obj", "series")["text"]
    assert md.count("Количество: 10 м → 12 м") == 1
    d = whole.detail("obj", "series")                       # блок «Полные спецификации» страницы проекта
    assert d["state"] == "done" and len(d["pairs"]) == 1
    row = d["pairs"][0]
    assert row["method"] == "whole" and row["reports"] and row["counts"]["changed"] == 1 and not row["issue"]
    assert row["covered"] == row["spec_pages"]


def test_inherit_sends_second_version_without_a_full_call(corpus, monkeypatch):
    """Цепочка: вторая версия отличается только количеством — разметка первой переносится без модели."""
    _, fake = corpus
    monkeypatch.setenv("VC_WS_INHERIT", "1")
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result
    assert len(fake.calls) == 1 and whole.ready("obj", "series")
    assert export.context("obj", "run")["reports"][0]["comparison"]["counts"]["changed"] == 1
    assert series_summary.build("obj", "series")["text"].count("Количество: 10 м → 12 м") == 1


def test_detail_shows_excluded_version_as_sheet_ai_with_short_reason(corpus):
    run, _ = corpus
    export.exclude_versions("obj", {run["b"]["sha256"]: [f"blk_{'a' * 32}: Parent row has no exact name cell: {n}"
                                                        for n in (10, 54)]}, "2026-10-20")
    row = whole.detail("obj", "series")["pairs"][0]
    assert row["method"] == "sheet_ai"
    assert row["reasons"] == ["B: двухуровневая СО: у строки-родителя нет наименования (строк: 2)"]


def test_sheet_jobs_and_estimation_exclude_specifications_even_when_explicit(corpus, monkeypatch):
    run, _ = corpus
    items, skipped = jobs._sc_items(run, None)
    assert [it["spid"] for it in items] == ["plan"]
    assert [it["spid"] for it in skipped] == ["spec"]
    assert jobs._sc_items(run, ["spec"])[0] == []
    mixed = run | {"items": [run["items"][0] | {"b_page": 3}]}
    assert jobs._sc_items(mixed, None)[0] == [], "an incorrect cross-kind pair cannot send a spec page"
    monkeypatch.setattr(jobs.vc_jobs, "get_run", lambda *args: run)
    monkeypatch.setattr(executor, "get", lambda *args: pytest.fail("spec sheet must not reach any executor"))
    with pytest.raises(jobs.JobError, match="нет листов"):
        jobs.create_job("obj", "run", spids=["spec"], method="sheet_changes", consent=True)


def test_old_queued_spec_job_is_skipped_before_packet_or_session(corpus, monkeypatch, tmp_path):
    job = {"job_id": "old", "sheets": [{"spid": "spec", "ai_status": "queued", "chunks": []}], "progress": {}}
    write(jobs.job_dir("obj", "run", "old") / "job.json", job)
    from backend.app.services.version_comparison_ai.sheet_changes import run as sheet_run
    monkeypatch.setattr(sheet_run, "run_sheet", lambda *a, **k: pytest.fail("old spec job must be suppressed"))
    jobs._run_sheet_changes("obj", "run", job, job["sheets"][0], None, None, {}, tmp_path / "packet")
    saved = json.loads((jobs.job_dir("obj", "run", "old") / "job.json").read_text())
    assert saved["sheets"][0]["ai_status"] == "engine_only"
    assert not (tmp_path / "packet").exists()


def test_luna_questions_exclude_specs_but_keep_drawings_with_tables(corpus):
    run, _ = corpus
    versions = [run["a"], run["b"]]
    rows = [{"cells": {"va": {"state": "matched", "page": page},
                       "vb": {"state": "unmatched", "candidates": [{"page": 1}, {"page": 3}]}}}
            for page in (1, 2)]
    qs = sheet_match.questions("obj", {"project_id": "proj", "code": "TEST"}, versions, {"rows": rows}, {})
    assert len(qs) == 1 and qs[0]["a"] == 1 and qs[0]["cands"] == [1]


def test_reverse_question_cannot_send_a_spec_candidate(corpus, monkeypatch):
    _, fake = corpus
    monkeypatch.setattr(sheet_match, "_labeled", lambda *args: pytest.fail("spec must not be rasterized"))
    result = sheet_match.ask("obj", "TEST", {"sha": "a" * 64, "page": 2, "label": "V1"},
                            {"sha": "b" * 64, "label": "V2"}, [3], True, fake, None, {})
    assert result["cached"] and not fake.calls


@pytest.fixture
def staged(corpus, monkeypatch):
    states = {key: "todo" for key in queue.STAGES}
    started, paused = [], []
    monkeypatch.setattr(queue, "GRACE_S", 0)
    monkeypatch.setattr(queue, "_stages", lambda *a: {key: {"state": value} for key, value in states.items()})
    def start(oid, item):
        started.append(item["stage"])
        states[item["stage"]] = "running"
    monkeypatch.setattr(queue, "_start_stage", start)
    monkeypatch.setattr(queue, "_pause_stage", lambda oid, item: paused.append(item["stage"]))
    return states, started, paused


def test_one_luna_launch_runs_all_five_stages_strictly_in_order(staged):
    states, started, _ = staged
    result = queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert [it["stage"] for it in result["pipeline"]["items"]] == list(queue.PIPELINE)
    assert started == ["luna"]
    for stage in queue.PIPELINE:
        queue.dispatch("obj")
        assert started[-1] == stage
        assert not whole.summary_gate("obj", "series")["ready"]
        states[stage] = "done"
        queue.dispatch("obj")
    assert started == list(queue.PIPELINE)
    assert queue.luna_status("obj")["state"] == "done"
    assert whole.summary_gate("obj", "series")["ready"]


@pytest.mark.parametrize("stage,state", [("luna", "failed"), ("ai", "partial"), ("specifications", "failed")])
def test_pipeline_failure_never_starts_next_stage_or_publishes_md(staged, monkeypatch, stage, state):
    states, started, _ = staged
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {"job": {"sheet"}})
    if stage == "ai":
        def start(oid, item):           # доразбор неполных листов снова заканчивается «частично»
            started.append(item["stage"])
            states[item["stage"]] = "partial" if item["stage"] == "ai" and started.count("ai") > 1 else "running"
        monkeypatch.setattr(queue, "_start_stage", start)
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE:
        states[key] = state if key == stage else "done"
        queue.dispatch("obj")
        if key == stage:
            break
    if stage == "ai":
        queue.dispatch("obj")                                    # первый «частично» с неполными — один доразбор
        queue.dispatch("obj")
        assert started.count("ai") == 2
    before = list(started)
    queue.dispatch("obj")
    assert started == before and queue.status("obj")["state"] == "failed"
    assert not whole.summary_gate("obj", "series")["ready"]
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert started[-1] == stage and len(queue.status("obj")["items"]) == 5


def test_partial_only_by_engine_notes_continues_pipeline(staged, monkeypatch):
    """Модель разобрала все записи, «частично» — от пометок движка (дубль листа, нет текстового слоя)."""
    states, started, _ = staged
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {})
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "partial" if key == "ai" else "done"
        queue.dispatch("obj")
    assert started[-1] == "specifications" and started.count("ai") == 1
    ai = next(it for it in queue.status("obj")["items"] if it["stage"] == "ai")
    assert ai["state"] == "done" and ai["result"] == "partial"


def test_incomplete_sheets_fixed_by_retry_continue_pipeline(staged, monkeypatch):
    states, started, _ = staged
    bad = {"job": {"sheet"}}
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: bad)
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "partial" if key == "ai" else "done"
        queue.dispatch("obj")
    queue.dispatch("obj")
    assert started.count("ai") == 2 and queue.status("obj")["state"] == "running"
    bad.clear()
    states["ai"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "specifications"


def test_sheets_incomplete_only_by_429_wait_and_retry_instead_of_stopping_queue(staged, monkeypatch):
    """Листы неполны только из-за отказа провайдера: очередь не останавливается — пауза и повтор до 3 раз."""
    states, started, _ = staged
    monkeypatch.setattr(queue, "TRANSIENT_WAIT_S", 0)
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {"job": {"sheet"}})
    monkeypatch.setattr(queue.series_ai, "incomplete_transient", lambda *a: True)
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "partial" if key == "ai" else "done"
        queue.dispatch("obj")
    queue.dispatch("obj")                                    # обычный доразбор
    for _ in range(queue.TRANSIENT_ROUNDS):
        states["ai"] = "partial"
        queue.dispatch("obj")                                # неполно из-за 429 → ожидание
        it = next(i for i in queue.status("obj")["items"] if i["stage"] == "ai")
        assert it["state"] == "running" and it.get("retry_at") and "429" in it["note"]
        queue.dispatch("obj")                                # пауза прошла → повтор
        queue.dispatch("obj")                                # перезапуск этапа
    assert started.count("ai") == 2 + queue.TRANSIENT_ROUNDS
    states["ai"] = "partial"
    queue.dispatch("obj")                                    # повторы исчерпаны — только теперь ошибка
    assert queue.status("obj")["state"] == "failed"


def test_pause_blocks_later_stages_and_resume_keeps_scope(staged):
    states, started, paused = staged
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    queue.pause_luna("obj")
    states["luna"] = "paused"
    queue.dispatch("obj")
    assert started == ["luna"] and paused == ["luna"]
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    assert started == ["luna", "luna"]


def test_empty_spec_scope_finishes_without_model(corpus, monkeypatch):
    run, fake = corpus
    run["items"] = run["items"][:1]
    write(paths.run_dir("obj", "run") / "run.json", run)
    for side in ("a", "b"):
        write(paths.profile_dir("obj", run[side]["sha256"]) / "pages.json", {"pages": [
            {"page": 1, "kind": "drawing", "stamp": {"title": "План"}}]})
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("no specifications, no calls"))
    assert whole.start("obj", "series", consent=True)["state"] == "done"


def test_incomplete_extraction_stops_publication(corpus):
    """Неполное извлечение не публикуется (и не выдаётся за «изменений нет»): версии исключаются,
    переход описывает листовой ИИ."""
    executor.set_override(executor.FakeDescriber(lambda t: {"bad": "answer"}, model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    assert not (paths.run_dir("obj", "run") / export.FILENAME).exists()
    assert len(export.excluded("obj")) == 2 and not export.context("obj", "run")["whole_spec"]


def test_executor_failure_still_fails_the_stage(corpus):
    """Сбой исполнителя (не форма СО) версию не исключает — этап падает, как раньше."""
    executor.set_override(executor.FakeDescriber(
        lambda t: executor.DescribeResult(False, error_kind="TIMEOUT", error="timeout"), model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "failed" and not export.excluded("obj")


def test_transient_429_is_retried_then_succeeds_and_quota_is_not(corpus, monkeypatch):
    """«429 Too Many Requests» (TRANSIENT) — повтор после паузы; квота — без повторов, этап падает как раньше."""
    monkeypatch.setenv("VC_WS_RETRY_DELAYS", "0,0,0")
    monkeypatch.setenv("VC_WS_PARALLEL", "2")
    answer = {"sections": [], "parents": [], "continuations": [], "notes": []}
    calls = []

    def flaky(task):
        calls.append(task.marker)
        if sum(m == task.marker for m in calls) <= 2:
            return executor.DescribeResult(False, error_kind="TRANSIENT", error="временный отказ провайдера")
        return answer
    executor.set_override(executor.FakeDescriber(flaky, model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    assert len(calls) == 6, "две версии × (2 отказа + успех)"



def test_transient_after_retries_waits_and_repeats_stage_instead_of_failing(corpus, monkeypatch):
    """429 дольше пауз обращения (ОВ3, 21.10.2026): этап не падает, а после паузы повторяет только неполученное."""
    monkeypatch.setenv("VC_WS_RETRY_DELAYS", "0")
    monkeypatch.setenv("VC_WS_TRANSIENT_WAIT_S", "0")
    answer = {"sections": [], "parents": [], "continuations": [], "notes": []}
    calls = []

    def flaky(task):
        calls.append(task.marker)
        if len(calls) <= 3:                                   # первый прогон этапа — отказы
            return executor.DescribeResult(False, error_kind="TRANSIENT", error="429 Too Many Requests")
        return answer
    executor.set_override(executor.FakeDescriber(flaky, model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result.get("reason")
    assert not export.excluded("obj")


def test_form_failures_stop_sending_remaining_versions(corpus, monkeypatch):
    """Форма СО не проходит проверку у нескольких версий подряд — остальные версии модели не отправляются,
    все уходят листовому ИИ; при повторном запуске непрошедшие версии заново не отправляются."""
    run, _ = corpus
    monkeypatch.setenv("VC_WS_BREAK_AFTER", "1")
    monkeypatch.setenv("VC_WS_PARALLEL", "1")
    bad = {"sections": [{"line": 1}], "parents": [], "continuations": [], "notes": []}
    fake = executor.FakeDescriber(lambda t: bad, model="gpt-6-luna")
    executor.set_override(fake)
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result.get("reason")
    assert set(result["excluded_versions"]) == {run["a"]["sha256"], run["b"]["sha256"]}
    assert len({t.marker for t in fake.calls}) == 1                     # вторая версия не отправлялась
    assert any("форма СО проекта" in r for r in export.excluded("obj")[run["b"]["sha256"]]["reasons"]) or \
        any("форма СО проекта" in r for r in export.excluded("obj")[run["a"]["sha256"]]["reasons"])


def test_quota_is_not_retried(corpus, monkeypatch):
    monkeypatch.setenv("VC_WS_RETRY_DELAYS", "0,0,0")
    quota = []
    executor.set_override(executor.FakeDescriber(
        lambda t: quota.append(1) or executor.DescribeResult(False, error_kind="QUOTA", error="квота"), model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] in ("failed", "paused")
    assert len(quota) <= 2, "квота: по одному обращению на версию, без повторов"


def test_ws_settings_from_file_and_env(tmp_path, monkeypatch):
    from backend.app.services.version_comparison_ai import settings as ai
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path))
    monkeypatch.delenv("VC_WS_PARALLEL", raising=False)
    monkeypatch.delenv("VC_WS_RETRY_DELAYS", raising=False)
    assert ai.ws_parallel() == ai.WS_DEFAULT_PARALLEL and ai.ws_retry_delays() == ai.WS_DEFAULT_RETRY_DELAYS
    write(ai.switch_file(), {"whole_specifications": {"parallel": 6, "retry_delays_s": [5, 30]}})
    assert ai.ws_parallel() == 6 and ai.ws_retry_delays() == (5, 30)
    monkeypatch.setenv("VC_WS_PARALLEL", "99")
    assert ai.ws_parallel() == 50


def test_rematching_same_versions_rebinds_saved_spec_without_another_call(corpus, monkeypatch):
    run, fake = corpus
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    write(paths.run_dir("obj", "newrun") / "run.json", run | {"run_id": "newrun"})
    ser = json.loads(paths.series_path("obj", "series").read_text())
    ser["pairs"][0]["run_id"] = "newrun"
    write(paths.series_path("obj", "series"), ser)
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("same PDFs must reuse their specification"))
    assert whole.start("obj", "series", consent=True)["reused"]
    assert len(fake.calls) == 2 and export.context("obj", "newrun")["reports"]


def test_final_stage_writes_one_md_from_drawings_and_full_specifications(corpus, monkeypatch):
    run, _ = corpus
    whole.start("obj", "series", consent=True)
    assert finish_spec()["state"] == "done"
    ser = json.loads(paths.series_path("obj", "series").read_text())
    monkeypatch.setattr(queue.vc_series, "get", lambda *a: ser)
    monkeypatch.setattr(queue.vc_jobs, "get_run", lambda *a: run)
    monkeypatch.setattr(queue, "_stages", lambda *a: {"series": {"state": "done", "series_id": "series"},
                                                    "summary": {"state": "todo"}})
    monkeypatch.setattr(queue.series_ai, "status", lambda *a: {"state": "done"})
    monkeypatch.setattr(queue.series_ai, "_load", lambda *a: {"pairs": [
        {"index": 0, "run_id": "run", "job_id": "draw", "a_label": "V1", "b_label": "V2"}]})
    monkeypatch.setattr(jobs, "get_job", lambda *a: {"job_id": "draw", "a": run["a"], "b": run["b"],
        "sheets": [it | {"ai_status": "done", "item_kind": "compare"} for it in run["items"]]})
    monkeypatch.setattr(series_summary.view, "load_result", lambda o, r, spid: {
        "job_id": "draw", "changes": [{"object": "Стена" if spid == "plan" else "ДУБЛЬ",
                                      "summary": "100 → 120 мм" if spid == "plan" else "ПОЛИСТОВАЯ СПЕЦИФИКАЦИЯ"}]})
    queue._start_stage("obj", {"project_id": "proj", "stage": "summary", "pipeline": True})
    text = (paths.series_path("obj", "series").parent / "summary.md").read_text()
    assert text.count("100 → 120 мм") == 1 and text.count("Количество: 10 м → 12 м") == 1
    assert "ДУБЛЬ" not in text and "ПОЛИСТОВАЯ" not in text


def test_user_pause_stops_submitting_further_full_versions(corpus, tmp_path):
    import threading
    from backend.app.services.version_comparison import specification_run as runner
    whole.specification_prepare.prepare(None, None, tmp_path / "input")
    pause = threading.Event()
    def answer(task):
        pause.set()
        return {"sections": [], "parents": [], "continuations": [], "notes": []}
    fake = executor.FakeDescriber(answer, model="gpt-6-luna")
    result = runner.run(runner.prepare(tmp_path / "input/manifest.json", structure=True), tmp_path / "out",
                        execute=True, parallel=1, executor=fake, pause=pause)
    assert result["paused"] and len(fake.calls) == 1 and not result["extractions_complete"]


def test_other_dispatcher_cannot_start_the_same_stage(staged):
    _, started, _ = staged
    with queue._dispatch_owner("obj") as owned:
        assert owned
        queue.start_luna("obj", project_ids=["proj"], consent=True)
        assert started == []
    queue.dispatch("obj")
    assert started == ["luna"]


def test_match_reader_preserves_live_worker_and_detects_stopped_worker(corpus, monkeypatch):
    write(sheet_match.job_path("obj"), {"state": "running", "owner": "another-worker", "owner_process": {"pid": 999}})
    monkeypatch.setattr(sheet_match.recovery, "owner_alive", lambda owner: True)
    assert sheet_match.status("obj")["state"] == "running"
    with pytest.raises(sheet_match.SheetMatchError, match="уже идёт"):
        sheet_match.start("obj", consent=True)
    monkeypatch.setattr(sheet_match.recovery, "owner_alive", lambda owner: False)
    assert sheet_match.status("obj")["state"] == "interrupted"


def test_existing_luna_api_starts_pipeline_and_md_waits_for_its_completion(staged, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.app.api.routers import version_comparison_ai as api
    monkeypatch.setenv("VERSION_COMPARISON_ENABLED", "1")
    states, started, _ = staged
    app = FastAPI()
    app.include_router(api.router)
    base = "/api/version-comparison/objects/obj"
    with TestClient(app) as client:
        assert client.post(base + "/ai-sheet-match", json={"consent": False, "project_ids": ["proj"]}).status_code == 400
        response = client.post(base + "/ai-sheet-match", json={"consent": True, "project_ids": ["proj"]})
        assert response.status_code == 200 and started == ["luna"]
        assert response.json()["pipeline"]["progress"]["total"] == 5
        assert client.get(base + "/series/series/ai-sheet-changes/summary.md").status_code == 409
        assert client.get(base + "/series/series/whole-specifications/status").json()["pipeline"]["ready"] is False


def _profile(sha, pages, words=None, inventory=None):
    folder = paths.profile_dir("obj", sha)
    write(folder / "pages.json", {"pages": pages})
    for page, ws in (words or {}).items():
        (folder / "words").mkdir(parents=True, exist_ok=True)
        import gzip
        (folder / "words" / f"p{page:04d}.json.gz").write_bytes(gzip.compress(json.dumps(ws).encode()))
    if inventory is not None:
        write(folder / "spec_so3.json", {"pages": inventory, "items": [], "parents": []})


def _stamp_page(page, title, kind="spec"):
    return {"page": page, "kind": kind, "stamp": {"title": title, "zone": [500, 700, 600, 800]}}


def test_drawing_with_truncated_stamp_title_is_not_a_specification(corpus):
    """Профиль хранит нижнюю строку названия: «ЩУАХП» от «Монтажная схема ЩУАХП» — чертёж, а не СО."""
    run, _ = corpus
    stamp = lambda *texts: [[510 + 10 * i, 750, 515 + 10 * i, 760, t] for i, t in enumerate(texts)]
    for sha, so_page in ((run["a"]["sha256"], 2), (run["b"]["sha256"], 3)):
        _profile(sha, [_stamp_page(1, "ЩУАХП"), _stamp_page(so_page, "изделий и материалов")],
                 words={1: stamp("Монтажная", "схема", "ЩУАХП"),
                        so_page: stamp("Спецификация", "оборудования,", "изделий", "и", "материалов")})
    export._drawing_stamp.cache_clear()
    ctx = export.context("obj", "run")
    assert ctx["kinds"]["a"][1] == "drawing" and ctx["spec_pages"] == {"a": {2}, "b": {3}}
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan"]


def test_transition_without_text_spec_goes_to_sheet_ai_not_full_spec(corpus, monkeypatch):
    """Версия, где СО только в кривых (перечень пуст): листы СО перехода описывает листовой ИИ."""
    run, _ = corpus
    _profile(run["a"]["sha256"], [_stamp_page(2, "Спецификация")], inventory=[{"page": 2, "parsed": True}])
    _profile(run["b"]["sha256"], [_stamp_page(3, "", kind="title")], inventory=[])
    import fitz
    for side in ("a", "b"):
        pdf = fitz.open()
        pdf.new_page()
        (paths.object_dir("obj") / "sources").mkdir(parents=True, exist_ok=True)
        pdf.save(paths.object_dir("obj") / "sources" / f"{run[side]['sha256']}.pdf")
    export._scope.cache_clear()
    ctx = export.context("obj", "run")
    assert not ctx["whole_spec"] and ctx["spec_pages"] == {"a": set(), "b": set()}
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan", "spec"]
    monkeypatch.setattr(executor, "get", lambda *a: pytest.fail("no text spec — no full-spec call"))
    assert whole.start("obj", "series", consent=True)["state"] == "done" and whole.ready("obj", "series")


def test_skip_marker_is_refused_for_transition_with_text_spec(corpus):
    with pytest.raises(ValueError, match="пропуск допустим только без текстовой СО"):
        export.publish_series("obj", "series", {"schema": export.SERIES_SCHEMA,
                                                "transitions": [{"run_id": "run", "skipped": "no_text_spec"}]})


def test_sheets_outside_ai_jobs_are_described_once_more_then_stop(staged, monkeypatch):
    states, started, _ = staged
    left = [3]
    monkeypatch.setattr(queue.series_ai, "missing", lambda *a: left[0])
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {})
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "done"
        queue.dispatch("obj")
    queue.dispatch("obj")
    assert started.count("ai") == 2
    states["ai"] = "done"
    queue.dispatch("obj")
    item = next(it for it in queue.status("obj")["items"] if it["stage"] == "ai")
    assert item["state"] == "failed" and "3" in item["reason"]
    left[0] = 0
    queue.start_luna("obj", project_ids=["proj"], consent=True)        # «Продолжить» — снова дозапуск
    states["ai"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "specifications"


def test_numbered_heading_in_name_cell_of_grid_row_is_prepared_as_section_text():
    """Раздел «1.Электрооборудование» в строке сетки (графа «Наименование», прочие пусты) — текст раздела;
    строка родителя без номера раздела остаётся строкой таблицы."""
    import fitz
    from backend.app.services.version_comparison import specification_prepare as prep
    edges = [50, 110, 480, 650, 750, 880, 935, 990, 1060, 1180]
    doc = fitz.open()
    page = doc.new_page(width=1240, height=800)
    for x in edges:
        page.draw_line((x, 50), (x, 170))
    for y in (50, 90, 110, 130, 150, 170):
        page.draw_line((edges[0], y), (edges[-1], y))
    words = []

    def word(x, y, text):
        words.append([x, y - 4, x + 6 * len(text), y + 4, text])
    for x, text in zip(edges, ("Поз.", "Наименование", "Тип,марка", "Код", "Поставщик", "Ед.", "Кол.", "Масса",
                               "Примечание")):
        word(x + 3, 70, text)
    word(115, 100, "1.Электрооборудование")
    word(55, 120, "1.1"), word(115, 120, "Щит"), word(885, 120, "шт."), word(940, 120, "1")
    word(115, 140, "Кабель")
    word(55, 160, "1.2"), word(485, 160, "ВВГ"), word(885, 160, "м"), word(940, 160, "10")
    rows, coverage = prep.page_table(page, words, {"zones": []})
    assert rows[2] == "1.Электрооборудование"
    assert rows[3].startswith("| 1.1 | Щит |") and rows[4].startswith("|  | Кабель |")
    assert coverage["table_words"] == len(words) - 9


def test_non_so_table_form_routes_version_to_sheet_ai_instead_of_failing(corpus):
    """Перечень элементов (7 граф) вместо формы СО: версия не годится для полного сравнения,
    переход уходит листовому ИИ — этап и очередь не падают."""
    import fitz
    from backend.app.services.version_comparison import specification_prepare as prep
    run, _ = corpus
    edges = [50, 110, 200, 480, 650, 750, 880, 1000]
    for side in ("a", "b"):
        sha = run[side]["sha256"]
        doc = fitz.open()
        page = doc.new_page(width=1240, height=800)
        for x in edges:
            page.draw_line((x, 50), (x, 130))
        for y in (50, 90, 110, 130):
            page.draw_line((edges[0], y), (edges[-1], y))
        words = [[x + 3, 66, x + 40, 74, t] for x, t in zip(edges, (
            "Поз.", "Поз.обозн.", "Наименование", "Марка", "Кол.", "Ед.изм.", "Примечание"))]
        words += [[55, 96, 70, 104, "1"], [205, 96, 260, 104, "Контроллер"], [655, 96, 662, 104, "1"]]
        (paths.object_dir("obj") / "sources").mkdir(parents=True, exist_ok=True)
        doc.save(paths.object_dir("obj") / "sources" / f"{sha}.pdf")
        _profile(sha, [{"page": 1, "kind": "spec", "words_count": len(words), "stamp": {"title": "Перечень"}}],
                 words={1: words}, inventory=[{"page": 1, "parsed": True}])
    scope = prep.version_scope(paths.object_dir("obj"), run["a"]["sha256"])
    assert not scope["ok"] and scope["reason"].startswith("стр. 1:")
    export._scope.cache_clear()
    assert not export.context("obj", "run")["whole_spec"]
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan", "spec"]


def test_stale_prepared_input_is_prepared_again_and_kept_aside(corpus):
    """Вход подготовлен, когда версия была негодной (переходов с ней нет), а теперь она годна: этап готовит вход
    заново и не падает «другим порядком версий»; прежний вход сохраняется рядом."""
    import hashlib
    snapshots = [("va", "a" * 64), ("vb", "b" * 64)]
    signature = hashlib.sha256(json.dumps(snapshots, sort_keys=True).encode()).hexdigest()
    stale = paths.object_dir("obj") / "whole_specifications" / "proj" / signature / "input"
    write(stale / "manifest.json", {"schema": "whole-specification-input/v1", "pairs": []})
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done", result
    assert export.context("obj", "run")["reports"] and whole.ready("obj", "series")
    assert len(json.loads((stale / "manifest.json").read_text())["pairs"]) == 1
    assert [p.name for p in stale.parent.glob("input.old-*")]


def test_rejected_extraction_excludes_version_and_stage_finishes(corpus):
    """Ответ модели по версии не прошёл строгую проверку (форма СО не укладывается в схему):
    версия исключается, её переход — листовому ИИ, этап завершается, а не роняет очередь."""
    run, _ = corpus
    bad = {"sections": [{"line": 1}], "parents": [], "continuations": [], "notes": []}
    good = {"sections": [], "parents": [], "continuations": [], "notes": []}
    executor.set_override(executor.FakeDescriber(lambda t: bad if "vb.pdf" in t.prompt else good, model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done" and result["excluded_versions"] == [run["b"]["sha256"]], result
    assert run["b"]["sha256"] in export.excluded("obj")
    assert not export.context("obj", "run")["whole_spec"]
    assert [it["spid"] for it in jobs._sc_items(run, None)[0]] == ["plan", "spec"]


def test_excluded_version_is_rechecked_by_current_code_on_next_start_without_model(corpus, monkeypatch):
    """Исключение — итог проверки прошлым кодом: следующий запуск этапа возвращает версию и перепроверяет её
    сохранённый ответ (повторы исчерпаны — модель не вызывается); годная — снова в полном сравнении."""
    run, _ = corpus
    from backend.app.services.version_comparison import specification_portions as portions
    from backend.app.services.version_comparison import specification_structure as structure
    note_on_row = {"sections": [], "parents": [], "continuations": [], "notes": [{"line": 6}]}
    good = {"sections": [], "parents": [], "continuations": [], "notes": []}
    # «Прошлый код»: правил программы и автоисправления нет — «примечание» на строке таблицы отвергается.
    real_normalize = structure.normalize
    monkeypatch.setattr(structure, "normalize", lambda src, payload: (payload, []))
    monkeypatch.setattr(portions, "AUTOFIX_MAX", 0)
    executor.set_override(executor.FakeDescriber(lambda t: note_on_row if "vb.pdf" in t.prompt else good,
                                                 model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    assert finish_spec()["excluded_versions"] == [run["b"]["sha256"]]
    # «Текущий код» исправляет это правилом программы; модель больше не вызывается.
    monkeypatch.setattr(structure, "normalize", real_normalize)
    executor.set_override(executor.FakeDescriber(lambda t: pytest.fail("повторы исчерпаны — модель не вызывается"),
                                                 model="gpt-6-luna"))
    whole.start("obj", "series", consent=True)
    result = finish_spec()
    assert result["state"] == "done" and not result.get("excluded_versions"), result
    assert run["b"]["sha256"] not in export.excluded("obj") and export.context("obj", "run")["whole_spec"]


def test_excluded_versions_send_project_back_to_sheet_ai_before_summary(staged, monkeypatch):
    states, started, _ = staged
    left = [0]
    monkeypatch.setattr(queue.series_ai, "missing", lambda *a: left[0])
    monkeypatch.setattr(queue.series_ai, "incomplete", lambda *a: {})
    queue.start_luna("obj", project_ids=["proj"], consent=True)
    for key in queue.PIPELINE[:3]:
        states[key] = "done"
        queue.dispatch("obj")
    assert started[-1] == "specifications"
    left[0] = 4                                  # полное сравнение исключило версию — её листы СО вне заданий
    states["specifications"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "ai" and started.count("ai") == 2
    left[0] = 0
    states["ai"] = "done"
    queue.dispatch("obj")
    assert started[-1] == "summary"


def test_so_page_of_suitable_version_is_spec_even_if_geometry_says_drawing(corpus, monkeypatch):
    """133/23-ГК-АК, СО1 стр. 267–268: шапка формы СО, штамп не чертёжный, а профиль — «drawing».
    Для годной версии страница перечня СО — СО (иначе публикация отказывает «область включает чертёж»)."""
    run, _ = corpus
    _profile(run["a"]["sha256"], [_stamp_page(2, "", kind="drawing")])
    _profile(run["b"]["sha256"], [_stamp_page(3, "Спецификация")])
    pages = {run["a"]["sha256"]: [2], run["b"]["sha256"]: [3]}
    monkeypatch.setattr(export, "_scope", lambda root, obj, sha: {"ok": True, "pages": pages[sha], "reason": None})
    ctx = export.context("obj", "run")
    assert ctx["kinds"]["a"][2] == "spec" and ctx["spec_pages"] == {"a": {2}, "b": {3}}
