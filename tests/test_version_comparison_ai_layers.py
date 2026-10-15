"""Слои поверх ответов ИИ-описаний: проверка предупреждений и сводка листа.

Модель не вызывается: исполнитель сводки — ``FakeDescriber``, слова страниц — подставные.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from backend.app.services.version_comparison_ai import executor as ex
from backend.app.services.version_comparison_ai import jobs, navigation, summary, verify, view
from tests.test_version_comparison_ai import OBJ, describe_all, make_run, root, run_job, sheet  # noqa: F401


# ------------------------------------------------------------------ проверка предупреждений

class FakePages:
    """Слова страниц: {(сторона, страница): [[x0, y0, x1, y1, текст], …]}."""

    def __init__(self, words: dict, spec: dict | None = None):
        self._w, self._spec = words, spec or {"items": []}

    def words(self, side, page):
        return self._w.get((side, page), [])

    def spec(self, side):
        return self._spec


PKT = {"a": {"page": 1}, "b": {"page": 1}, "coordinates": {"transform_b_to_a": [[1, 0, 0], [0, 1, 0]]}}


def _rec(rid, box, kind="changed"):
    return {"id": rid, "type": "text", "kind": kind, "bbox_a": box if kind != "added" else None,
            "bbox_b": box if kind != "removed" else None, "before": "", "after": "", "flags": []}


def test_mark_confirmed_only_when_nearest_of_its_kind():
    words = [[90, 85, 115, 95, "QF3.24"], [140, 85, 165, 95, "QF3.25"]]
    pages = FakePages({("A", 1): words, ("B", 1): words})
    res = verify.check_mark(pages, PKT, [_rec("T1", [100, 100, 110, 110])], "QF3.24")
    assert res["status"] == "context_confirmed"
    assert res["sides"]["A"]["context"]["note"].startswith("добавлено при проверке")
    # Чужая метка того же вида ближе — привязка не подтверждена, хотя «QF3.24» рядом.
    res = verify.check_mark(pages, PKT, [_rec("T1", [100, 100, 110, 110])], "QF3.25")
    assert res["status"] == "context_near"


def test_mark_tie_between_neighbours_is_not_confirmed():
    words = [[60, 85, 85, 95, "QF3.23"], [125, 85, 150, 95, "QF3.24"]]
    pages = FakePages({("B", 1): words})
    res = verify.check_mark(pages, PKT, [_rec("T1", [100, 100, 110, 110], kind="added")], "QF3.24")
    assert res["status"] == "context_near" and "A" not in res["sides"]


def test_mark_found_only_elsewhere_on_page_is_image_only():
    pages = FakePages({("A", 1): [[900, 900, 930, 910, "QF3.24"]], ("B", 1): [[900, 900, 930, 910, "QF3.24"]]})
    res = verify.check_mark(pages, PKT, [_rec("T1", [100, 100, 110, 110])], "QF3.24")
    assert res["status"] == "image_only"


def test_count_uses_unique_places_not_fragments():
    recs = [_rec(f"T{k}", [10 * k, 0, 10 * k + 5, 5], kind="added") for k in range(1, 4)]
    assert verify.check_count(recs, 3)["status"] == "count_confirmed"
    recs.append(_rec("T9", [10, 0, 15, 5], kind="added"))          # тот же фрагмент дважды
    res = verify.check_count(recs, 4)
    assert res["status"] == "count_unconfirmed" and res["unique_places"] == 3


def test_suspect_tokens_split_id_refs_marks_and_counts():
    packet = {"records": [{"id": "G528", "type": "region", "before": "", "after": "", "label": "", "fields": {},
                           "context_ids": []},
                          {"id": "T1", "type": "text", "before": "", "after": "Ir=OFF", "label": "", "fields": {},
                           "context_ids": []}], "contexts": [], "xrefs": []}
    item = {"summary": "В области G528 у QF3.24 добавлено «Ir=OFF» в 17 местах.", "evidence_ids": ["T1"],
            "context_ids": [], "values": []}
    kinds = {t["token"]: t["kind"] for t in verify.suspect_tokens(packet, item)}
    assert kinds == {"G528": "id_reference", "QF3.24": "mark", "17": "number"}


def test_program_warning_lifted_only_with_basis_model_warning_kept():
    words = [[90, 85, 115, 95, "QF3.24"], [150, 85, 175, 95, "QF3.25"]]
    pages = FakePages({("A", 1): words, ("B", 1): words})
    rec = dict(_rec("T1", [100, 100, 110, 110]), before="10А", after="16А", label="", fields={}, context_ids=[])
    packet = dict(PKT, records=[rec], contexts=[], xrefs=[])
    item = {"item_id": "K1.I2", "category": "needs_check", "original_category": "content", "flags": ["unsupported_token"],
            "summary": "У автомата QF3.24 номинал изменён с 10А на 16А.", "evidence_ids": ["T1"], "context_ids": [],
            "values": [{"evidence_id": "T1", "before": "10А", "after": "16А", "unit": ""}], "cross_check": False}
    v = verify.verify_item(pages, None, packet, item, [], None)
    assert v["class"] == "resolved" and v["final_category"] == "content" and v["set_by"] == "program"
    # Тот же пункт, но неоднозначность отметил сам ИИ: основание прикладывается, категория остаётся.
    ai_item = dict(item, flags=[], original_category=None, summary="Номинал изменён с 10А на 16А.")
    v = verify.verify_item(pages, None, packet, ai_item, [], None)
    assert v["final_category"] == "needs_check" and v["set_by"] == "ai"


def test_unconfirmed_mark_keeps_main_conclusion_in_needs_check():
    words = [[60, 85, 85, 95, "QF3.23"], [125, 85, 150, 95, "QF3.24"]]
    pages = FakePages({("A", 1): words, ("B", 1): words})
    rec = dict(_rec("T1", [100, 100, 110, 110]), before="10А", after="16А", label="", fields={}, context_ids=[])
    packet = dict(PKT, records=[rec], contexts=[], xrefs=[])
    item = {"item_id": "K1.I2", "category": "needs_check", "original_category": "content", "flags": ["unsupported_token"],
            "summary": "У автомата QF3.24 номинал изменён.", "evidence_ids": ["T1"], "context_ids": [], "values": [],
            "cross_check": False}
    v = verify.verify_item(pages, None, packet, item, [], None)
    assert v["final_category"] == "needs_check" and v["field_doubts"] == ["QF3.24"] and v["main_conclusion_affected"]
    assert v["class"] == "image_only"


# ------------------------------------------------------------------ сводка

def _item(iid, cat="content", pairs=(("10А", "16А"),), lim="", cls=""):
    return {"item_id": iid, "category": cat, "check_class": cls, "summary": f"Пункт {iid}: значение изменено.",
            "location": "", "limitation": lim, "values": [{"before": b, "after": a, "unit": ""} for b, a in pairs],
            "evidence_ids": [f"E{iid}"], "verification": None}


def test_units_merge_only_identical_changes():
    items = [_item("A"), _item("B"), _item("C", lim="только часть листа"), _item("D", pairs=(("10А", "25А"),))]
    us = summary.units(items)
    assert [u["item_ids"] for u in us] == [["A", "B"], ["C"], ["D"]]


@pytest.mark.parametrize("brief, problem", [
    ("Номинал изменён везде.", "недопустимые слова"),
    ("Номинал изменён, расчёт выполнен правильно.", "недопустимые слова"),
    ("Номинал изменён из-за роста нагрузки.", "недопустимые слова"),
    ("Номинал изменён с «10А» на «25А».", "пары «10А» → «25А»"),
    ("Номинал изменён на 32А.", "числа не из пунктов"),
])
def test_group_text_rules(brief, problem):
    g = {"group_id": "S1", "category": "content", "title": "Номиналы", "brief": brief, "unit_ids": ["U1"]}
    assert any(problem in p for p in summary.check_group(g, [_item("A"), _item("B")]))


def test_group_text_allowed_wording():
    g = {"group_id": "S1", "category": "content", "title": "Номиналы", "brief":
         "У 2 автоматов номинал изменён с «10А» на «16А»; соответствующие позиции не менялись.", "unit_ids": ["U1"]}
    assert summary.check_group(g, [_item("A"), _item("B")]) == []


def test_assemble_rejects_bad_groups_and_keeps_every_item_once():
    items = [_item("A"), _item("B", pairs=(("1", "2"),)), _item("C", cat="needs_check", lim="мелко", cls="unreadable")]
    us = summary.units(items)
    ans = {"sheet": "Лист", "groups": [
        {"group_id": "S1", "category": "content", "title": "Смешано", "brief": "Изменения.", "unit_ids": ["U1", "U3"]},
        {"group_id": "S2", "category": "content", "title": "Значения", "brief": "Значение изменено.", "unit_ids": ["U2"]},
        {"group_id": "S3", "category": "content", "title": "Повтор", "brief": "Значение изменено.", "unit_ids": ["U2"]},
    ]}
    out = summary.assemble(us, items, ans, "Лист")
    assert {r["group_id"] for r in out["rejected"]} == {"S1", "S3"}
    assert out["coverage"]["ok"] and sorted(out["mapping"]) == ["A", "B", "C"]
    c = next(g for g in out["groups"] if "C" in g["item_ids"])
    assert c["brief_source"] == "program" and c["limitations"] == [{"item_id": "C", "text": "мелко"}] and c["uncertain"]


def test_assemble_wrong_sheet_or_schema_falls_back_to_program():
    items = [_item("A")]
    us = summary.units(items)
    out = summary.assemble(us, items, {"sheet": "Другой", "groups": []}, "Лист")
    assert out["coverage"]["ok"] and out["groups"][0]["brief_source"] == "program" and out["rejected"]


# ------------------------------------------------------------------ слой на сохранённом задании

def test_summary_layer_cached_separately_and_answers_untouched(root):
    make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    job = run_job(ex.FakeDescriber(describe_all))
    sdir = jobs.sheet_dir(OBJ, "run1", job["job_id"], "r1")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in sdir.rglob("*") if p.is_file()}
    res = json.loads((sdir / "result.json").read_text(encoding="utf-8"))
    label = f"{res.get('title')} — лист {res['b'].get('sheet_no') or res['a'].get('sheet_no')}"

    def grouping(task):
        assert not task.image_paths and "<<<ДАННЫЕ-" in task.prompt
        body = task.prompt.rsplit("<<<ДАННЫЕ-", 1)[1].split(">>>", 1)[1].rsplit("<<<КОНЕЦ-", 1)[0]
        data = json.loads(body)
        return {"sheet": label, "groups": [{"group_id": "S1", "category": u["category"], "title": "Группа",
                                            "brief": "Изменено.", "unit_ids": [u["unit_id"]]} for u in data["units"]]}
    fake = ex.FakeDescriber(grouping)
    s1 = summary.build(sdir, OBJ, describer=fake, allow_call=True)
    s2 = summary.build(sdir, OBJ, describer=fake, allow_call=True)
    assert len(fake.calls) == 1 and s1["key"] == s2["key"] and s1["mode"] == "editorial" and s1["coverage"]["ok"]
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in sdir.rglob("*") if p.is_file() and "layers" not in p.parts}
    assert after == before                                   # ответы, проверка и результат листа не тронуты
    v = view.job_view(OBJ, "run1", job["job_id"])
    sv = v["sheets"][0]["summary"]
    assert sv["mode"] == "editorial" and sum(len(g["members"]) for g in sv["groups"]) == len(res["items"])


def test_detailed_report_survives_broken_layer(root, monkeypatch):
    make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    job = run_job(ex.FakeDescriber(describe_all))
    monkeypatch.setattr(summary, "build", lambda *a, **k: (_ for _ in ()).throw(OSError("диск недоступен")))
    monkeypatch.setattr(summary, "latest", lambda *a, **k: None)
    s = view.job_view(OBJ, "run1", job["job_id"])["sheets"][0]
    assert s["summary"] is None and any("подробный отчёт" in e for e in s["layer_errors"])
    assert sum(len(g["entries"]) for g in s["groups"]) > 0


def test_page_focus_picks_sheet_by_page_and_crop_names_are_whitelisted(root):
    make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    f = navigation.page_focus(OBJ, "run1", "B", 1, [("строка", [10, 10, 60, 20])])
    assert f["available"] and f["spid"] == "r1" and f["boxes"][0]["px"][2] > f["boxes"][0]["px"][0]
    assert not navigation.page_focus(OBJ, "run1", "B", 99, [("x", [0, 0, 1, 1])])["available"]
    job = run_job(ex.FakeDescriber(describe_all))
    sdir = jobs.sheet_dir(OBJ, "run1", job["job_id"], "r1")
    assert verify.crop_file(sdir, "../result.json") is None and verify.crop_file(sdir, "K1_I1_A.png") is None


# ------------------------------------------------------------------ выпуск: переключатель, готовые листы, перенос

from backend.app.services.version_comparison import paths as vc_paths  # noqa: E402
from backend.app.services.version_comparison_ai import importer, settings as ai_settings  # noqa: E402


def test_switch_file_enables_module_env_overrides(root, monkeypatch):
    monkeypatch.delenv("VERSION_COMPARISON_AI_ENABLED", raising=False)
    assert not ai_settings.enabled()
    f = ai_settings.switch_file()
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text('{"enabled": true}', encoding="utf-8")
    assert ai_settings.enabled() and "файл" in ai_settings.enabled_source()
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "0")
    assert not ai_settings.enabled()
    monkeypatch.delenv("VERSION_COMPARISON_AI_ENABLED")
    f.write_text('{"enabled": "yes"}', encoding="utf-8")       # только явное true
    assert not ai_settings.enabled()


def _as_real(job_id):
    """Задание подменного исполнителя, помеченное как реальное (для проверки повторного использования)."""
    jp = jobs._job_path(OBJ, "run1", job_id)
    j = json.loads(jp.read_text(encoding="utf-8"))
    j["executor"] = {"executor": "codex_exec", "model": "m", "effort": "medium"}
    jp.write_text(json.dumps(j, ensure_ascii=False), encoding="utf-8")
    for c in jobs.job_dir(OBJ, "run1", job_id).rglob("call.json"):
        d = json.loads(c.read_text(encoding="utf-8"))
        d["executor"] = j["executor"]
        c.write_text(json.dumps(d), encoding="utf-8")


def test_fake_results_are_never_reused(root):
    make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    run_job(ex.FakeDescriber(describe_all))
    assert jobs.find_reusable(OBJ, "run1", "r1") is None


def test_new_job_takes_done_sheet_without_calls_and_estimate_shows_it(root):
    make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    j1 = run_job(ex.FakeDescriber(describe_all))
    _as_real(j1["job_id"])
    est = jobs.estimate(OBJ, "run1")
    row = next(s for s in est["sheets"] if s["spid"] == "r1")
    assert row["reusable_from"] == j1["job_id"] and est["chunks_estimate"] == 0
    fake = ex.FakeDescriber(describe_all)
    j2 = run_job(fake)
    assert not fake.calls and j2["state"] == "done"
    s = next(x for x in j2["sheets"] if x["spid"] == "r1")
    assert s["reused_from"]["job_id"] == j1["job_id"]
    assert (jobs.sheet_dir(OBJ, "run1", j2["job_id"], "r1") / "result.json").exists()


def test_reuse_refused_when_engine_result_changed(root):
    run = make_run(root, [sheet()], [sheet(values={"pp": "Рр=324.26кВт"})])
    j1 = run_job(ex.FakeDescriber(describe_all))
    _as_real(j1["job_id"])
    rf = vc_paths.Path(run["items"][0]["result_dir"]) / "result.json"
    rf.write_text(rf.read_text(encoding="utf-8") + " ", encoding="utf-8")
    src = jobs.sheet_dir(OBJ, "run1", j1["job_id"], "r1")
    ok, why = jobs.check_reusable(OBJ, "run1", "r1", src, jobs.get_job(OBJ, "run1", j1["job_id"]))
    assert not ok and "движка" in why
    est = jobs.estimate(OBJ, "run1")
    assert est["chunks_estimate"] >= 1 and not est["sheets"][0]["reusable_from"]


def test_import_makes_done_job_without_running_and_scope_is_explicit(root):
    make_run(root, [sheet(), sheet()], [sheet(values={"pp": "Рр=324.26кВт"}), sheet(values={"pp": "Рр=1кВт"})])
    j1 = run_job(ex.FakeDescriber(describe_all), spids=["r1"])
    _as_real(j1["job_id"])

    class Boom(ex.FakeDescriber):
        def run(self, task, cancel):
            raise AssertionError("перенос не должен обращаться к модели")
    ex.set_override(Boom())
    src = jobs.job_dir(OBJ, "run1", j1["job_id"])
    job = importer.import_sheets(OBJ, "run1", [(src, ["r1"])], note="тест")
    assert job["state"] == "done" and job["scope"]["kind"] == "imported" and job["usage"]["calls"] == 0
    assert jobs.get_job(OBJ, "run1", job["job_id"])["state"] == "done"
    v = view.job_view(OBJ, "run1", job["job_id"])
    assert v["scope_text"].startswith("ИИ описал 1 лист(ов)") and "не описывал" in v["scope_text"]
    assert v["sheets"][0]["reused_text"]
    with pytest.raises(jobs.JobError):                          # фальшивое не переносится
        importer.import_sheets(OBJ, "run1", [(src, ["r2"])], note="тест")
