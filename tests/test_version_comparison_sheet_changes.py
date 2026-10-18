"""«Изменения в листе» (способ sheet_changes): сборщик кандидатов, пакет, проверка ответа, лист, API.

План — docs/version_comparison_sheet_changes_plan.md. Модель не вызывается: исполнитель — подменный.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
from pathlib import Path

import fitz
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.version_comparison import engine, paths, profile
from backend.app.services.version_comparison_ai import executor as ex
from backend.app.services.version_comparison_ai import jobs, settings
from backend.app.services.version_comparison_ai.sheet_changes import collect, pack
from backend.app.services.version_comparison_ai.sheet_changes import validate as val
from backend.app.services.version_comparison_ai.sheet_changes import view
from tests import version_comparison_fixtures as fx

OBJ, PID, RUN, SPID = "obj-sc", "p1", "run1", "r1"
ROWS_A = [("Лк-1", "Люк 2190х520", "2"), ("Лк-2", "Люк 2190х620", "1"), ("Лк-3", "Люк 300х300", "4")]
# В B строки пересобраны: на месте Лк-1 теперь Лк-9 — позиционная пара движка неверна.
ROWS_B = [("Лк-9", "Люк 2190х1880", "1"), ("Лк-2", "Люк 2190х620", "1"), ("Лк-3", "Люк 300х300", "3")]


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("CPU_POOL_WORKERS", "1")
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    for name in ("VC_SC_MODEL", "VC_SC_EFFORT", "VC_SC_CODEX_BIN", "VC_SC_CONTEXT_ROUNDS"):
        monkeypatch.delenv(name, raising=False)
    yield tmp_path
    ex.set_override(None)


def _page(doc: fitz.Document, rows: list[tuple[str, str, str]], label: str) -> None:
    page = doc.new_page(width=fx.W, height=fx.H)
    page.insert_font(fontname="F1", fontbuffer=fx._cyr_font())
    fx._frame_and_stamp(page, {"sheet_no": "3", "title": "План 1 этажа", "designation": "ТЕСТ-01-АР"})
    sh = page.new_shape()
    x0, xs, top, rh = 500.0, [500.0, 560.0, 720.0, 770.0], 100.0, 25.0
    ys = [top, top + 18] + [top + 18 + rh * (k + 1) for k in range(len(rows))]
    for y in ys:
        sh.draw_line((x0, y), (xs[-1], y))
    for x in xs:
        sh.draw_line((x, top), (x, ys[-1]))
    # стены плана вокруг метки: «полосы» без заголовка не должны стать строками таблицы
    sh.draw_rect(fitz.Rect(80, 250, 260, 360))
    sh.draw_line((80, 320), (260, 320))
    sh.finish(color=(0, 0, 0), width=0.8)
    sh.commit()
    for x, word in zip(xs, ("Поз.", "Наименование", "Кол.")):
        page.insert_text((x + 3, top + 13), word, fontname="F1", fontsize=8)
    for k, (mark, name, qty) in enumerate(rows):
        y = top + 18 + rh * k + 16
        page.insert_text((xs[0] + 3, y), mark, fontname="F1", fontsize=8)
        page.insert_text((xs[1] + 3, y), name, fontname="F1", fontsize=8)
        page.insert_text((xs[2] + 3, y), qty, fontname="F1", fontsize=8)
    page.insert_text((100, 300), "К.5", fontname="F1", fontsize=9)
    page.insert_text((140, 300), label, fontname="F1", fontsize=9)


def _source(p: Path) -> tuple[str, dict]:
    sha = hashlib.sha256(p.read_bytes()).hexdigest()
    dst = paths.source_pdf(OBJ, sha)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.copyfile(p, dst)
        os.chmod(dst, 0o444)
    return sha, profile.build(OBJ, sha)


def make_run(tmp: Path) -> dict:
    srcs = []
    for name, rows, label in (("a", ROWS_A, "Лк-1"), ("b", ROWS_B, "Лк-9")):
        doc = fitz.open()
        _page(doc, rows, label)
        f = tmp / "src" / f"{name}.pdf"
        f.parent.mkdir(parents=True, exist_ok=True)
        doc.save(str(f))
        srcs.append(_source(f))
    (sa, pa), (sb, pb) = srcs
    out = tmp / "res" / SPID
    task = {"sheet_pair_id": SPID, "out_dir": str(out), "reasons": [], "params": {"dpi": 200, "tol_pt": 0.5},
            "a": {"version_id": "va", "sha": sa, "page": 1, "pdf": str(paths.source_pdf(OBJ, sa)),
                  "profile_dir": str(paths.profile_dir(OBJ, sa)), "profile": pa["pages"][0]},
            "b": {"version_id": "vb", "sha": sb, "page": 1, "pdf": str(paths.source_pdf(OBJ, sb)),
                  "profile_dir": str(paths.profile_dir(OBJ, sb)), "profile": pb["pages"][0]}}
    summary = engine.compute_sheet_pair(task)
    items = [{"spid": SPID, "row_id": SPID, "title": "л.3 · План 1 этажа", "kind": "compare", "a_page": 1, "b_page": 1,
              "confidence": "high", "reasons": [], "status": "done", "result_dir": str(out),
              "result_status": summary["status"]}]
    cat = {"schema": "version_comparison.catalog/v1", "object_id": OBJ,
           "projects": [{"project_id": PID, "title": "Тест", "code": "ТЕСТ-01-АР", "versions": ["va", "vb"]}],
           "versions": {}}
    paths.catalog_path(OBJ).write_text(json.dumps(cat, ensure_ascii=False), encoding="utf-8")
    run = {"schema": "version_comparison.run/v1", "run_id": RUN, "object_id": OBJ, "project_id": PID,
           "a": {"version_id": "va", "label": "(1) Изм.1", "file_name": "ТЕСТ (1).pdf", "sha256": sa},
           "b": {"version_id": "vb", "label": "(2) Изм.2", "file_name": "ТЕСТ (2).pdf", "sha256": sb},
           "scope": "all", "params": {"dpi": 200, "tol_pt": 0.5, "algo": "test"}, "state": "done", "owner": "x",
           "items": items, "progress": {"total": 1, "done": 1, "failed": 0}}
    d = paths.run_dir(OBJ, RUN)
    d.mkdir(parents=True, exist_ok=True)
    (d / "run.json").write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
    return run


def _table(col):
    return next(c for c in col["candidates"] if c["kind"] == "table")


def _place(col):
    return next(c for c in col["candidates"] if c["kind"] == "place" and "Лк-1" in c["labels_a"])


# ------------------------------------------------------------------ сборщик

def test_table_rows_matched_by_key_not_position(root):
    make_run(root)
    t = _table(collect.collect(OBJ, RUN, SPID))
    assert [r["key"] for r in t["rows_a"]] == ["Лк-1", "Лк-2", "Лк-3"]
    assert [r["key"] for r in t["rows_b"]] == ["Лк-9", "Лк-2", "Лк-3"]
    assert sorted(p["key"] for p in t["pairs"]) == ["Лк-2", "Лк-3"]
    assert t["unpaired_a"] == ["Лк-1"] and t["unpaired_b"] == ["Лк-9"]
    assert "Люк 2190х520" in t["rows_a"][0]["text"] and "Кол." in t["header"]


def test_plan_label_is_place_with_anchor_not_table_row(root):
    make_run(root)
    col = collect.collect(OBJ, RUN, SPID)
    pl = _place(col)
    assert pl["labels_b"] == ["Лк-9"]
    assert any(a.startswith("К.5") for a in pl["anchors"])
    assert pl["id"] in _table(col)["links"]                       # одна марка на плане и в таблице — связь


def test_every_pointer_gets_exactly_one_fate(root):
    make_run(root)
    col = collect.collect(OBJ, RUN, SPID)
    assert set(col["ledger"]) == set(col["pointers"])
    assert all(e["status"] in ("candidate", "attached", "excluded", "linked") for e in col["ledger"].values())
    for e in col["ledger"].values():
        if e["status"] == "excluded":
            assert e["reason"] in collect.REASONS


def test_role_rules_keep_markup_and_tags_only_for_current_izm():
    tag = {"type": "text", "kind": "added", "after": "15.1", "before": ""}
    assert collect._role(tag, 15) == ("excluded", "revision_tag")
    assert collect._role(tag, 14) == ("strong", None)          # не номер текущего изменения — не отметка
    assert collect._role({"type": "annotation", "kind": "added"}, 15) == ("strong", None)
    assert collect._role({"type": "region", "kind": "style"}, 15) == ("weak", "line_style")
    assert collect._role({"type": "region", "kind": "shifted"}, 15) == ("weak", "object_shift")
    assert collect._role({"type": "text", "kind": "moved"}, 15) == ("weak", "label_move")


# ------------------------------------------------------------------ пакет

def test_packet_has_fence_no_paths_and_provenance(root, tmp_path):
    make_run(root)
    p = pack.build(OBJ, RUN, SPID, tmp_path / "pk")
    prompt = (tmp_path / "pk" / "prompt.md").read_text(encoding="utf-8")
    assert "<<<ДАННЫЕ" in prompt and str(tmp_path) not in prompt and "/srv/" not in prompt
    assert "Лк-1" in prompt and "Лк-9" in prompt and "Предварительно по обозначению" in prompt
    man = json.loads((tmp_path / "pk" / "manifest.json").read_text(encoding="utf-8"))
    assert all(k.startswith(("PDF ", "профили", "результат движка")) for k in man["provenance"]["inputs"])
    assert p["images"][0]["kind"] == "overview" and p["budget"]["images"] == len(p["images"])
    assert p["method"]["name"] == "sheet_changes"


def test_packet_built_in_process_pool_matches_inline(root, tmp_path, monkeypatch):
    """Подготовка в отдельном процессе (spawn) даёт тот же пакет; корень хранилища уходит явно."""
    import multiprocessing
    from concurrent.futures import ProcessPoolExecutor
    from backend.app.services.common import cpu_pool
    make_run(root)
    inline = pack.build(OBJ, RUN, SPID, tmp_path / "inline")
    pool = ProcessPoolExecutor(1, mp_context=multiprocessing.get_context("spawn"))
    monkeypatch.setattr(cpu_pool, "get_executor", lambda: pool)
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "нет-такого"))   # ребёнок берёт корень из аргумента
    vc = tmp_path / "vc"
    monkeypatch.setattr(paths, "root", lambda: vc)
    try:
        pooled = pack.build_pooled(OBJ, RUN, SPID, tmp_path / "pooled")
    finally:
        pool.shutdown()
    assert pooled["packet_id"] == inline["packet_id"]
    assert (tmp_path / "pooled" / "prompt.md").read_text(encoding="utf-8") == \
        (tmp_path / "inline" / "prompt.md").read_text(encoding="utf-8")


def test_packet_pool_broken_falls_back_to_thread(root, tmp_path, monkeypatch):
    from concurrent.futures import BrokenExecutor
    from backend.app.services.common import cpu_pool

    class Broken:
        def submit(self, *a, **kw):
            raise BrokenExecutor("пул упал")
    disabled = []
    monkeypatch.setattr(cpu_pool, "get_executor", lambda: Broken())
    monkeypatch.setattr(cpu_pool, "disable_pool", lambda reason: disabled.append(reason))
    make_run(root)
    p = pack.build_pooled(OBJ, RUN, SPID, tmp_path / "pk")
    assert p["packet_id"] and (tmp_path / "pk" / "packet.json").exists() and disabled


# ------------------------------------------------------------------ проверка ответа

def _answer(p, **kw):
    base = {"packet_id": p["packet_id"], "part_id": "P1", "changes": [], "unresolved": [], "excluded": [],
            "context_requests": []}
    base.update(kw)
    return base


def _change(cids, fields, summary="У К.5 люк Лк-1 заменён на Лк-9.", kind="replacement", images=(), other=None):
    return {"object": "Люк у К.5", "place": "у К.5", "kind": kind, "summary": summary,
            "fields": [dict(name=n, before=b, after=a, unit=u, source=s, certainty="confirmed") for n, b, a, u, s in fields],
            "members": [], "mapping": "one_to_one", "candidate_ids": cids, "image_ids": list(images),
            "other_representation": other or {"what": "", "value_a": "", "value_b": "", "image_id": ""}, "limitation": ""}


def test_validate_confirmation_service_strip_and_coverage(root, tmp_path):
    make_run(root)
    p = pack.build(OBJ, RUN, SPID, tmp_path / "pk")
    t, pl = _table(p), next(c for c in p["candidates"] if c["kind"] == "place" and "Лк-1" in c["labels_a"])
    img = next(i["id"] for i in p["images"] if pl["id"] in i["candidates"] and i["kind"] != "overview")
    a = _answer(p, changes=[
        _change([pl["id"], t["id"]], [("марка", "Лк-1", "Лк-9", "", "text"), ("размер", "2190х520", "2190х1880", "мм", "text")],
                summary="У К.5 люк Лк-1 (2190х520) заменён на Лк-9 (2190х1880). Место обведено облаком изменений."),
        _change([pl["id"]], [("отметка", "", "+0,150", "", "image")], summary="Добавлена отметка +0,150.", images=[img]),
        _change([t["id"]], [("толщина", "45-170", "45-115", "мм", "text")], kind="text_correction",
                summary="Исправлен текст."),
    ])
    chk = val.check(p, a)
    assert chk["ok"]
    ch1, ch2 = chk["changes"][0], chk["changes"][1]
    assert "облак" not in ch1["summary"] and "Лк-9" in ch1["summary"]          # вырезана фраза, не пункт
    assert all(f["confirmation"] == "text" for f in ch1["fields"])
    assert ch2["fields"][0]["confirmation"] == "image"                        # с изображения — не отклонено
    assert len(chk["changes"]) == 2 and any("по другому представлению" in u["question"] for u in chk["unresolved"])
    assert chk["coverage"]["not_reviewed"] == [c["id"] for c in p["candidates"] if c["id"] not in (t["id"], pl["id"])]
    assert val.state_of(chk, p) == ("partial" if chk["coverage"]["not_reviewed"] else "needs_clarification")
    sides = {e["side"] + ":" + e["what"] for e in ch1["evidence"]}
    assert any("Лк-1" in s and s.startswith("A") for s in sides) and any("Лк-9" in s and s.startswith("B") for s in sides)
    assert not any("Лк-2" in s for s in sides)                                 # чужая строка с тем же размером не доказательство


# ------------------------------------------------------------------ лист и API

def _full_answer(task, extra_requests=None):
    p = json.loads((Path(task.image_paths[0]).parent.parent / "packet.json").read_text(encoding="utf-8"))
    t, pl = _table(p), next(c for c in p["candidates"] if c["kind"] == "place" and "Лк-1" in c["labels_a"])
    rest = [c["id"] for c in p["candidates"] if c["id"] not in (t["id"], pl["id"])]
    return _answer(p, changes=[_change([pl["id"], t["id"]], [("марка", "Лк-1", "Лк-9", "", "text")])],
                   excluded=[{"candidate_ids": rest, "reason": "formatting", "note": ""}] if rest else [],
                   context_requests=extra_requests or [])


def _wait(job_id, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        j = jobs.get_job(OBJ, RUN, job_id)
        if j["state"] not in ("queued", "running", "cancelling"):
            return j
        time.sleep(0.1)
    raise AssertionError("задание не завершилось")


def test_run_sheet_with_one_context_round_and_launch_cap(root):
    make_run(root)
    calls = []

    def responder(task):
        calls.append(task)
        return _full_answer(task, [{"kind": "mark", "value": "Лк-9", "side": "B", "candidate_ids": [], "reason": "x"}])
    ex.set_override(ex.FakeDescriber(responder))
    j = _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert len(calls) == 2                                            # основное + один дозапрос, не больше
    assert "ДОПОЛНИТЕЛЬНЫЙ КОНТЕКСТ" in calls[1].prompt
    res = view.load_result(OBJ, RUN, SPID)
    assert res["state"] == "done" and res["diagnostics"]["launches"] == 2 and j["method"] == "sheet_changes"
    assert res["changes"][0]["summary"].startswith("У К.5")


def test_open_and_switch_never_call_model_and_states(root):
    make_run(root)
    fake = ex.FakeDescriber(lambda task: _full_answer(task))
    ex.set_override(fake)
    assert view.panel(OBJ, RUN, SPID)["state"] == "not_processed"            # не «изменений нет»
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    n = len(fake.calls)
    app = FastAPI()
    from backend.app.api.routers import version_comparison_ai as router
    app.include_router(router.router)
    c = TestClient(app)
    base = f"/api/version-comparison/objects/{OBJ}/runs/{RUN}/sheets/{SPID}"
    pv = c.get(base + "/changes").json()
    assert pv["state"] == "done" and pv["changes"][0]["object"] == "Люк у К.5"
    f = c.get(base + "/changes/focus", params={"item": "CH1"}).json()
    assert f["available"] and f["boxes"]
    assert c.get(base + "/changes.md").status_code == 200
    assert len(fake.calls) == n                                          # GET не обращаются к исполнителю


def test_fake_answers_never_cached_real_answers_reused(root):
    make_run(root)

    class Real(ex.FakeDescriber):
        executor_id = "codex_exec_test"
    real = Real(lambda task: _full_answer(task))
    ex.set_override(real)
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert len(real.calls) == 1                                          # второй раз — из кэша
    fake = ex.FakeDescriber(lambda task: _full_answer(task))
    ex.set_override(fake)
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert len(fake.calls) == 1                                          # чужой кэш подменному не отдаётся


def _gapped_answer(task):
    """Ответ без решения по части записей: строка таблицы и прочие записи не отнесены никуда."""
    full = _full_answer(task)
    t = _table(json.loads((Path(task.image_paths[0]).parent.parent / "packet.json").read_text(encoding="utf-8")))
    ch = dict(full["changes"][0], candidate_ids=[i for i in full["changes"][0]["candidate_ids"] if i != t["id"]])
    return dict(full, changes=[ch], excluded=[])


class _Real(ex.FakeDescriber):
    executor_id = "codex_exec_test"


def test_unreviewed_records_get_one_coverage_round(root):
    make_run(root)
    calls = []

    def responder(task):
        calls.append(task)
        return _gapped_answer(task) if len(calls) == 1 else _full_answer(task)
    ex.set_override(_Real(responder))
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    res = view.load_result(OBJ, RUN, SPID)
    assert len(calls) == 2 and "ДОРАЗБОР" in calls[1].prompt
    assert "Не разобраны записи: O" in calls[1].prompt
    assert res["state"] == "done" and not res["coverage"]["not_reviewed"]
    assert [c["purpose"] for c in res["diagnostics"]["calls"]] == ["primary", "coverage"]


def test_answer_with_unreviewed_records_is_not_cached(root):
    make_run(root)
    real = _Real(_gapped_answer)
    ex.set_override(real)
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert len(real.calls) == 2                                          # основное + доразбор, лучше не стало
    assert view.load_result(OBJ, RUN, SPID)["state"] == "partial"
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert len(real.calls) == 4                                          # неполный ответ из кэша не берётся


def test_series_incomplete_sheets_are_retried_past_cache(root):
    """Неполные листы серии — ошибка или записи без решения; «частично» по пометкам движка — не неполнота."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    make_run(root)
    _one_pair_series("qi", "pA")
    real = _Real(_gapped_answer)
    ex.set_override(real)
    ja = series_ai.start(OBJ, "qi", consent=True)["pairs"][0]["job_id"]
    assert _wait(ja)["state"] == "partial"
    assert series_ai.incomplete(OBJ, "qi") == {ja: {SPID}}
    with pytest.raises(jobs.JobError):
        series_ai.resume(OBJ, "qi")                                      # обычный «Досчитать» тут ни при чём
    real.responder = _full_answer
    series_ai.retry_incomplete(OBJ, "qi")
    assert _wait(ja)["state"] == "done" and series_ai.incomplete(OBJ, "qi") == {}
    assert len(real.calls) == 3
    with pytest.raises(jobs.JobError, match="неполных листов нет"):
        series_ai.retry_incomplete(OBJ, "qi")

    def engine_notes_only(j):
        j["state"] = "partial"
        j["sheets"][0].update(ai_status="partial", coverage={"not_reviewed": []})
        return j
    jobs._update(OBJ, RUN, ja, engine_notes_only)
    assert series_ai.status(OBJ, "qi")["state"] == "partial" and series_ai.incomplete(OBJ, "qi") == {}


def test_old_method_results_untouched(root):
    make_run(root)
    ex.set_override(ex.FakeDescriber(lambda task: _full_answer(task)))
    _wait(jobs.create_job(OBJ, RUN, spids=[SPID], consent=True, method="sheet_changes")["job_id"])
    assert not list(paths.run_dir(OBJ, RUN).glob("ai/*/sheets/*/result.json"))   # файлов прежнего способа не создано


# ------------------------------------------------------------------ исполнитель

def test_executor_for_sheet_changes_sol_high_normal_tier(root, tmp_path):
    settings.switch_file().parent.mkdir(parents=True, exist_ok=True)
    binary = tmp_path / "codex"
    binary.write_text("#!/bin/sh\n")
    settings.switch_file().write_text(json.dumps({"enabled": True, "sheet_changes": {"codex_bin": str(binary)}}),
                                      encoding="utf-8")
    d = ex.get("sheet_changes")
    assert (d.model, d.effort, d.binary) == ("gpt-6.1-sol", "high", str(binary))
    cmd = d.command(str(binary), tmp_path, tmp_path / "o.json", tmp_path / "s.json", [])
    assert "--model=gpt-6.1-sol" in cmd and "model_reasoning_effort=high" in cmd and "--ignore-user-config" in cmd
    assert not any("service_tier" in c for c in cmd)
    assert ex.get("records").model == "gpt-6-astra"                     # прежний способ не меняется
    binary.unlink()
    with pytest.raises(Exception):
        d.resolve_binary()                                                # пропал CLI — отказ, не подмена


def test_sheets_run_in_parallel_failed_sheet_gets_three_launches(root, monkeypatch):
    """Листы идут одновременно (не больше VC_SC_PARALLEL); упавший лист — до 3 обращений, затем «ошибка»."""
    run = make_run(root)
    extra = [dict(run["items"][0], spid=f"{SPID}x{k}", row_id=f"{SPID}x{k}") for k in range(1, 5)]
    run["items"] += extra
    (paths.run_dir(OBJ, RUN) / "run.json").write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("VC_SC_PARALLEL", "3")
    bad = f"{SPID}x2"
    lock, live, seen = threading.Lock(), [0], {"max": 0, "bad": 0}

    def responder(task):
        with lock:
            live[0] += 1
            seen["max"] = max(seen["max"], live[0])
        time.sleep(0.3)
        with lock:
            live[0] -= 1
        if f"/{bad}/" in task.raw_path:
            with lock:
                seen["bad"] += 1
            return None                                               # ответ не по схеме — повтор
        return _full_answer(task)
    ex.set_override(ex.FakeDescriber(responder))
    spids = [SPID] + [it["spid"] for it in extra]
    j = _wait(jobs.create_job(OBJ, RUN, spids=spids, consent=True, method="sheet_changes")["job_id"])
    st = {s["spid"]: s["ai_status"] for s in j["sheets"]}
    assert st[bad] == "failed" and seen["bad"] == 3                   # три обращения и пометка «ошибка»
    assert all(v == "done" for k, v in st.items() if k != bad)        # остальные доведены
    assert 2 <= seen["max"] <= 3                                      # одновременно, но не больше трёх
    assert j["usage"]["calls"] == 4 + 3 and j["state"] == "partial"


def test_sheets_with_only_service_zone_changes_are_not_sent(root):
    """Лист, где движок нашёл отличия только в служебных зонах, в модель не уходит — ни в оценке, ни в задании."""
    run = make_run(root)
    run["items"].append(dict(run["items"][0], spid=f"{SPID}z", row_id=f"{SPID}z", result_status="same_zones_changed"))
    (paths.run_dir(OBJ, RUN) / "run.json").write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
    fake = ex.FakeDescriber(lambda task: _full_answer(task))
    ex.set_override(fake)
    est = jobs.estimate_sheet_changes(OBJ, RUN)
    assert [s["spid"] for s in est["sheets"]] == [SPID] and [s["spid"] for s in est["skipped"]] == [f"{SPID}z"]
    j = _wait(jobs.create_job(OBJ, RUN, spids=[SPID, f"{SPID}z"], consent=True, method="sheet_changes")["job_id"])
    assert [s["spid"] for s in j["sheets"]] == [SPID] and len(fake.calls) == 1
    with pytest.raises(jobs.JobError):
        jobs.create_job(OBJ, RUN, spids=[f"{SPID}z"], consent=True, method="sheet_changes")


def test_series_describes_every_transition_with_changes(root):
    """«Описать изменения ИИ» по серии: задание на каждый посчитанный переход с изменениями содержания,
    переход без них и недосчитанный — пропуск с причиной; ход и итог собираются из заданий."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    run = make_run(root)
    run2 = dict(run, run_id="run2", items=[dict(run["items"][0], result_status="same_zones_changed")])
    d2 = paths.run_dir(OBJ, "run2")
    d2.mkdir(parents=True, exist_ok=True)
    (d2 / "run.json").write_text(json.dumps(run2, ensure_ascii=False), encoding="utf-8")
    sp = paths.series_path(OBJ, "ser1")
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps({"schema": "version_comparison.series/v1", "series_id": "ser1", "object_id": OBJ,
                              "project_id": PID, "state": "done", "owner": "x", "created_at": "2026-10-17T00:00:00+00:00",
                              "pairs": [{"a": "va", "b": "vb", "a_label": "(1)", "b_label": "(2)", "run_id": RUN, "state": "done"},
                                        {"a": "vb", "b": "vc", "a_label": "(2)", "b_label": "(3)", "run_id": "run2", "state": "done"},
                                        {"a": "vc", "b": "vd", "a_label": "(3)", "b_label": "(4)", "run_id": None, "state": "queued"}]},
                             ensure_ascii=False), encoding="utf-8")
    fake = ex.FakeDescriber(lambda task: _full_answer(task))
    ex.set_override(fake)
    est = series_ai.estimate(OBJ, "ser1")
    assert [p["sheets"] for p in est["pairs"]] == [1, 0, 0] and est["calls_max"] == 3
    assert "изменений содержания нет" in est["pairs"][1]["reason"] and "не досчитано" in est["pairs"][2]["reason"]
    with pytest.raises(jobs.JobError):
        series_ai.start(OBJ, "ser1", consent=False)
    st = series_ai.start(OBJ, "ser1", consent=True)
    job_id = st["pairs"][0]["job_id"]
    assert job_id and st["pairs"][1]["state"] == "skipped" and st["pairs"][2]["state"] == "skipped"
    _wait(job_id)
    st = series_ai.status(OBJ, "ser1")
    assert st["state"] == "done" and st["total"]["sheets_done"] == 1 and st["total"]["changes"] == 1
    assert st["total"]["calls"] == 1 and len(fake.calls) == 1 and st["wall_s"] is not None
    with pytest.raises(jobs.JobError):
        series_ai.resume(OBJ, "ser1")                                  # досчитывать нечего


def test_added_and_removed_sheets_get_one_call_what_is_on_the_sheet(root):
    """Лист только в одной версии: одно обращение «что на листе» (без пары и без «было → стало»);
    в сводке переходов — отдельным списком с кратким описанием."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai, single
    run = make_run(root)
    run["items"] += [{"spid": "n1", "row_id": "n1", "title": "Новый лист", "kind": "only_b", "a_page": None,
                      "b_page": 1, "status": "done", "result_status": "added"},
                     {"spid": "o1", "row_id": "o1", "title": "Старый лист", "kind": "only_a", "a_page": 1,
                      "b_page": None, "status": "done", "result_status": "removed"}]
    (paths.run_dir(OBJ, RUN) / "run.json").write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
    sp = paths.series_path(OBJ, "ser2")
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps({"schema": "version_comparison.series/v1", "series_id": "ser2", "object_id": OBJ,
                              "project_id": PID, "state": "done", "owner": "x", "created_at": "2026-10-17T00:00:00+00:00",
                              "pairs": [{"a": "va", "b": "vb", "a_label": "(1)", "b_label": "(2)", "run_id": RUN,
                                         "state": "done"}]}, ensure_ascii=False), encoding="utf-8")
    seen = []

    def responder(task):
        seen.append(task)
        if task.schema is single.SINGLE_SCHEMA:
            assert "Ответ — только JSON" in task.prompt and len(task.image_paths) == 1
            return {"sheet_kind": "спецификация", "summary": "Спецификация люков.",
                    "objects": [{"name": "Люк", "detail": "Лк-9"}], "marks": ["Лк-9", "Лк-9"], "limitation": ""}
        return _full_answer(task)
    ex.set_override(ex.FakeDescriber(responder))
    est = series_ai.estimate(OBJ, "ser2")
    assert est["pairs"][0]["sheets"] == 3 and est["pairs"][0]["added"] == 1 and est["pairs"][0]["removed"] == 1
    st = series_ai.start(OBJ, "ser2", consent=True)
    _wait(st["pairs"][0]["job_id"])
    st = series_ai.status(OBJ, "ser2")
    row = st["pairs"][0]
    assert st["state"] == "done" and st["total"]["calls"] == 3 and st["total"]["singles"] == 2
    by = {x["spid"]: x for x in row["singles"]}
    assert by["n1"]["kind"] == "added" and by["n1"]["side"] == "B" and by["o1"]["kind"] == "removed"
    assert by["n1"]["description"]["summary"] == "Спецификация люков." and by["n1"]["description"]["marks"] == ["Лк-9"]
    assert row["changes"] == 1 and row["sha_b"]                       # пара листов описана как прежде


    # Сводка всех переходов одним MD: переход → лист (страницы A/B) → «объект — суть»; без «Неясно».
    from backend.app.services.version_comparison_ai.sheet_changes import series_summary
    md = series_summary.build(OBJ, "ser2")
    text = md["text"]
    assert md["filename"].endswith(".md") and "## 1. " in text and "(1) → (2)" in text
    assert "- **Люк у К.5** — У К.5 люк Лк-1 заменён на Лк-9." in text and "стр. PDF" in text
    assert "### Лист добавлен: Новый лист (B стр. PDF 1)" in text and "Спецификация люков." in text
    assert "### Лист удалён: Старый лист (A стр. PDF 1)" in text and "Неясно" not in text.split("## 1. ")[1]
    with pytest.raises(jobs.JobError):
        series_summary.build(OBJ, "ser-нет")


# ------------------------------------------------------------------ очередь проектов «Описать изменения ИИ»

def _one_pair_series(sid: str, pid: str) -> None:
    sp = paths.series_path(OBJ, sid)
    sp.parent.mkdir(parents=True, exist_ok=True)
    sp.write_text(json.dumps({"schema": "version_comparison.series/v1", "series_id": sid, "object_id": OBJ,
                              "project_id": pid, "state": "done", "owner": "x", "created_at": "2026-10-18T00:00:00+00:00",
                              "pairs": [{"a": "va", "b": "vb", "a_label": "(1)", "b_label": "(2)", "run_id": RUN,
                                         "state": "done"}]}, ensure_ascii=False), encoding="utf-8")


def _gated(gate: threading.Event) -> ex.FakeDescriber:
    def responder(task):
        assert gate.wait(30)
        return _full_answer(task)
    return ex.FakeDescriber(responder)


def test_series_queue_runs_projects_one_after_another(root):
    """Пока идёт описание одного проекта, следующие встают в очередь по порядку и начинаются сами;
    из очереди можно убрать, повторно поставить тот же проект нельзя."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    make_run(root)
    for sid, pid in (("qa", "pA"), ("qb", "pB"), ("qc", "pC")):
        _one_pair_series(sid, pid)
    gate = threading.Event()
    ex.set_override(_gated(gate))
    a = series_ai.start(OBJ, "qa", consent=True)
    assert a["state"] == "running" and a["queue"] == {"current": True, "waiting": 0}
    b = series_ai.start(OBJ, "qb", consent=True)
    assert b["state"] == "queued" and b["queue"]["position"] == 1 and b["pairs"] == []      # заданий ещё нет
    assert [(x["code"], x["phase"]) for x in b["queue"]["ahead"]] == [("pA", "running")]
    c = series_ai.start(OBJ, "qc", consent=True)
    assert c["queue"]["position"] == 2 and [x["code"] for x in c["queue"]["ahead"]] == ["pA", "pB"]
    with pytest.raises(jobs.JobError, match="уже в очереди"):
        series_ai.start(OBJ, "qb", consent=True)
    assert series_ai.estimate(OBJ, "qa")["queue"] == {"busy": True, "current": "pA", "waiting": 2}
    q = series_ai.queue_status()
    assert q["current"]["series_id"] == "qa" and [e["series_id"] for e in q["waiting"]] == ["qb", "qc"]
    series_ai.dispatch()                                                  # первый ещё идёт — очередь стоит
    assert series_ai.status(OBJ, "qb")["state"] == "queued"
    assert series_ai.cancel(OBJ, "qc")["state"] == "none"                 # убран из очереди
    gate.set()
    _wait(a["pairs"][0]["job_id"])
    series_ai.dispatch()
    b = series_ai.status(OBJ, "qb")
    assert b["state"] in ("running", "done") and b["pairs"][0]["job_id"]
    _wait(b["pairs"][0]["job_id"])
    series_ai.dispatch()
    assert series_ai.queue_status() == {"current": None, "waiting": []}
    assert series_ai.status(OBJ, "qb")["state"] == "done" and series_ai.status(OBJ, "qc")["state"] == "none"


def test_series_queue_paused_head_holds_until_cancel_and_resume_waits_its_turn(root):
    """Пауза текущего (лимит подписки) держит очередь; «Отменить» отпускает её — начинается следующий.
    «Досчитать», пока идёт другой проект, тоже встаёт в очередь."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    make_run(root)
    _one_pair_series("qa", "pA")
    _one_pair_series("qb", "pB")
    gate = threading.Event()
    ex.set_override(_gated(gate))
    ja = series_ai.start(OBJ, "qa", consent=True)["pairs"][0]["job_id"]
    series_ai.start(OBJ, "qb", consent=True)
    gate.set()
    _wait(ja)
    jobs._update(OBJ, RUN, ja, lambda j: j | {"state": "paused_quota"})
    series_ai.dispatch()
    assert series_ai.status(OBJ, "qb")["state"] == "queued"
    assert series_ai.queue_status()["current"]["phase"] == "paused"
    gate.clear()
    series_ai.cancel(OBJ, "qa")                                           # пропустить приостановленный
    assert jobs.get_job(OBJ, RUN, ja)["state"] == "cancelled"
    jb = series_ai.status(OBJ, "qb")["pairs"][0]["job_id"]
    assert jb and series_ai.queue_status()["current"]["series_id"] == "qb"
    r = series_ai.resume(OBJ, "qa")
    assert r["state"] == "queued" and r["queue"]["action"] == "resume"
    gate.set()
    _wait(jb)
    series_ai.dispatch()
    assert series_ai.queue_status()["current"]["series_id"] == "qa"
    assert _wait(ja)["state"] == "done"
    series_ai.dispatch()
    assert series_ai.queue_status() == {"current": None, "waiting": []}


def test_series_queue_resumes_head_interrupted_by_restart(root):
    """Перезапуск сервера посреди описания: текущий проект очереди досчитывается сам, без «Досчитать»."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    make_run(root)
    _one_pair_series("qa", "pA")
    ex.set_override(ex.FakeDescriber(lambda task: _full_answer(task)))
    ja = series_ai.start(OBJ, "qa", consent=True)["pairs"][0]["job_id"]
    _wait(ja)

    def crashed(j):
        j.update(state="running", owner="прежний-процесс")
        for s in j["sheets"]:
            s["ai_status"] = "running"
        return j
    jobs._update(OBJ, RUN, ja, crashed)
    assert series_ai.queue_status()["current"]["phase"] == "interrupted"
    series_ai.dispatch()
    assert _wait(ja)["state"] == "done"


def test_series_queue_launch_failure_is_noted_and_next_starts(root):
    """Проект, который из очереди не запустился (серия удалена), получает заметку; очередь идёт дальше."""
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    make_run(root)
    for sid, pid in (("qa", "pA"), ("qb", "pB"), ("qc", "pC")):
        _one_pair_series(sid, pid)
    gate = threading.Event()
    ex.set_override(_gated(gate))
    ja = series_ai.start(OBJ, "qa", consent=True)["pairs"][0]["job_id"]
    series_ai.start(OBJ, "qb", consent=True)
    series_ai.start(OBJ, "qc", consent=True)
    shutil.rmtree(paths.series_path(OBJ, "qb").parent)
    gate.set()
    _wait(ja)
    series_ai.dispatch()
    q = series_ai.queue_status()
    assert q["current"]["series_id"] == "qc" and q["waiting"] == []
    st = series_ai._qload()["notes"]
    assert "серия не найдена" in st[f"{OBJ}/qb"]["reason"]
    _wait(series_ai.status(OBJ, "qc")["pairs"][0]["job_id"])


def test_series_user_pause_finishes_started_sheet_releases_queue_and_resumes(root, monkeypatch):
    """Пауза пользователя: начатый лист доводится (ответ не теряется), следующий не начинается, очередь проектов
    отпускается; «Досчитать» описывает только оставшийся лист."""
    import shutil
    from backend.app.services.version_comparison_ai.sheet_changes import series_ai
    monkeypatch.setenv("VC_SC_PARALLEL", "1")
    run = make_run(root)
    second = dict(run["items"][0], spid=SPID + "b", row_id=SPID + "b", title="л.4 · План 2 этажа")
    shutil.copytree(run["items"][0]["result_dir"], root / "res" / second["spid"])
    second["result_dir"] = str(root / "res" / second["spid"])
    run["items"].append(second)
    run["progress"] = {"total": 2, "done": 2, "failed": 0}
    (paths.run_dir(OBJ, RUN) / "run.json").write_text(json.dumps(run, ensure_ascii=False), encoding="utf-8")
    _one_pair_series("qa", "pA")
    gate = threading.Event()
    fake = _gated(gate)
    ex.set_override(fake)
    job_id = series_ai.start(OBJ, "qa", consent=True)["pairs"][0]["job_id"]
    end = time.time() + 30
    while not fake.calls and time.time() < end:
        time.sleep(0.05)
    assert fake.calls, "первый лист отправлен"
    assert series_ai.pause(OBJ, "qa")["state"] == "pausing"
    gate.set()
    end = time.time() + 60
    while jobs.get_job(OBJ, RUN, job_id)["state"] in jobs.ACTIVE and time.time() < end:
        time.sleep(0.1)
    j = jobs.get_job(OBJ, RUN, job_id)
    assert j["state"] == "paused_user"
    assert sorted(s["ai_status"] for s in j["sheets"]) == ["done", "queued"]
    calls = len(fake.calls)
    st = series_ai.status(OBJ, "qa")
    assert st["state"] == "paused"
    assert series_ai.queue_status()["current"] is None, "пауза отпускает очередь — следующий проект может идти"
    series_ai.resume(OBJ, "qa")
    series_ai.dispatch()
    end = time.time() + 60
    while jobs.get_job(OBJ, RUN, job_id)["state"] not in ("done", "partial") and time.time() < end:
        time.sleep(0.1)
    j = jobs.get_job(OBJ, RUN, job_id)
    assert j["state"] in ("done", "partial") and all(s["ai_status"] != "queued" for s in j["sheets"])
    assert calls == 1 and len(fake.calls) == 2, "по одному обращению на лист: доведённый до паузы не повторяется"
