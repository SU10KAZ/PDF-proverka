"""Редакционный отбор для заказчика: пакет из всех фактов, проверка связки «объект → было → стало», лимит обращений."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from backend.app.services.version_comparison_ai.md_report import client, client_edit as ce, disciplines

EM = disciplines.by_key("EM")       # факты тестов — силовое электрооборудование

T = {"from_version": "V5", "to_version": "V6", "change_scope": "Широкая переработка", "changes": [],
     "completeness": {"complete": True}, "main_changes": "x", "construction_impact": "", "from_date": "2025-08-04",
     "to_date": "2025-10-01"}


def _ai(fid, obj, fields, tr="V5→V6", **kw):
    return {"fact_id": fid, "transition": tr, "kind": "ai_change", "sheet": "л.6", "confirmation_code": "text",
            "confirmation": "ИИ-описание листа", "text": f"{obj} — изменено",
            "details": {"object": obj, "change_kind": "параметр", "mapping": "one_to_one",
                        "fields": [dict(name=n, before=b, after=a, certainty=c, unit="") for n, b, a, c in fields]}} | kw


def _spec(fid, kind, body, tr="V5→V6"):
    return {"fact_id": fid, "transition": tr, "kind": "spec_entry", "sheet": "л.10",
            "text": f"Старая: —; новая: л.10 (стр. PDF 50). {body}", "details": {"change_kind": kind}}


FACTS = [
    _ai("Ф0001", "УЭРМ обогрева QFD1", [("Полюсность ВН-63 40 А", "1P", "3P", "confirmed"),
                                         ("Кабель", "ППГнг(А)-HF 3х6, 37 м", "ППГнг(А)-HF 5х6, 37 м", "confirmed")]),
    _ai("Ф0002", "Шкаф ШУН/В", [("Марка", "ШУН/В-1,5-03-УК6-R3", "ШУН/В-1,5-03-ПЧ/УК6-R3", "unclear")]),
    _ai("Ф0009", "QF9 другой переход", [("Ток", "10 А", "16 А", "confirmed")], tr="V6→V7"),
    _spec("Ф0003", "added", "Добавлено: не было → Поз. 1; Наименование Кабель нагревательный; Марка Freezstop-30; "
                            "раздел 3 этаж кв.20; Количество 11 м"),
    _spec("Ф0004", "added", "Добавлено: не было → Поз. 1; Наименование Кабель нагревательный; Марка Freezstop-30; "
                            "раздел 3 этаж кв.21; Количество 27 м"),
]


def _answer(points, **kw):
    return {"transition": "V5→V6", "lead": "Пересмотрены защита и обогрев.", "points": points,
            "impact": "При выполнении по V5 потребуется увязать полюсность аппаратов.", "impact_fact_ids": ["Ф0001"],
            "questions": []} | kw


def _pt(obj, text, ids, values, status="confirmed"):
    return {"object": obj, "text": text, "fact_ids": ids, "values": values, "status": status}


def test_packet_groups_spec_and_hides_other_transitions():
    text, groups, support = ce.packet(FACTS, "V5→V6")
    assert "Ф0009" not in text and "Ф0001" in text and "1P → 3P" in text
    (code, ids), = groups.items()
    assert ids == ["Ф0003", "Ф0004"] and "позиций: 2" in support[code] and "всего 38 м" in support[code]
    assert "(неясно)" in text


def test_check_accepts_chain_and_rejects_wrong_direction_object_and_number():
    _, groups, support = ce.packet(FACTS, "V5→V6")
    code = next(iter(groups))
    good = _pt("УЭРМ обогрева QFD1", "ВН-63 40 А: 1 → 3 полюса; кабель ППГнг(А)-HF 3×6 → 5×6, 37 м", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    reversed_ = _pt("Ветвь УЭРМ QFD1", "кабель 5×6 → 3×6", ["Ф0001"],
                    [{"parameter": "кабель", "before": "5х6", "after": "3х6", "fact_id": "Ф0001"}])
    foreign_obj = _pt("УЭРМ QFD7", "кабель 3×6 → 5×6", ["Ф0001"],
                      [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    group = _pt("Кабель нагревательный Freezstop-30", "добавлен в 2 квартирах, всего 38 м", [code],
                [{"parameter": "количество", "before": "нет", "after": "38 м", "fact_id": code}])
    unclear = _pt("Шкаф ШУН/В", "марка ШУН/В-1,5-03-УК6-R3 → ШУН/В-1,5-03-ПЧ/УК6-R3", ["Ф0002"], [])
    chk = ce.check(_answer([good, reversed_, foreign_obj, group, unclear]), T, FACTS, groups, support)
    kept = [k["point"]["object"] for k in chk["kept"]]
    assert kept == ["УЭРМ обогрева QFD1", "Кабель нагревательный Freezstop-30"]
    why = {d["point"]["object"]: " ".join(d["problems"]) for d in chk["dropped"]}
    assert "направление" in why["Ветвь УЭРМ QFD1"]
    assert "QFD7" in why["УЭРМ QFD7"] and "подтверждённый" in why["Шкаф ШУН/В"]
    assert not chk["hard"] or "не прошли проверку" in chk["hard"][0]


def test_check_hard_problems():
    _, groups, support = ce.packet(FACTS, "V5→V6")
    p = _pt("QF9", "10 → 16 А", ["Ф0009"], [])
    chk = ce.check(_answer([p], lead="Ф0001 изменено из-за ошибки проектировщика. Второе предложение."),
                   T, FACTS, groups, support)
    hard = " ".join(chk["hard"])
    assert "одно предложение" in hard and "ID" in hard and "причина" in hard and not chk["kept"]


class _Fake:
    def __init__(self, answers):
        self.answers, self.calls = list(answers), 0

    def describe_runtime(self):
        return {"executor": "fake", "model": "fake"}

    def run(self, task, tok):
        self.calls += 1
        a = self.answers.pop(0)
        return SimpleNamespace(ok=True, parsed=a, error_kind=None, error=None, duration_ms=10,
                               usage={"input_tokens": len(task.prompt) // 3})


def _rdir(tmp_path, transitions):
    rdir = tmp_path / "r"
    rdir.mkdir()
    (rdir / "report.json").write_text(json.dumps({"project_code": "К", "transitions": transitions}), encoding="utf-8")
    (rdir / "facts.json").write_text(json.dumps({"facts": FACTS}), encoding="utf-8")
    (rdir / "job.json").write_text(json.dumps({"source": {"sha256": "abc"}}), encoding="utf-8")
    return rdir


def test_run_budget_reuse_and_client_view(tmp_path):
    rdir = _rdir(tmp_path, [T])
    good = _pt("УЭРМ обогрева QFD1", "кабель ППГнг(А)-HF 3×6 → 5×6", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    bad = _answer([good], lead="")                       # жёсткая ошибка → повтор из дополнительного лимита
    fake = _Fake([bad, _answer([good], questions=[{"text": "Уточнить исполнение шкафа.", "fact_ids": ["Ф0002"]}])])
    out = ce.run(rdir, fake, log=lambda *_: None, wait_s=0)
    assert fake.calls == 2 and out["calls"] == 2 and out["transitions"]["V5→V6"]["accepted"]
    # Повторный запуск по тому же снимку — без обращений.
    again = ce.run(rdir, _Fake([]), log=lambda *_: None, wait_s=0)
    assert again["transitions"]["V5→V6"] == {"reused": True} and again["calls"] == 0
    ed = ce.load(rdir)
    view = client.build(json.loads((rdir / "report.json").read_text()), {"facts": FACTS}, {}, ed)
    row = view["rows"][0]
    assert row["lead"] == "Пересмотрены защита и обогрев." and row["points"][0]["object"] == "УЭРМ обогрева QFD1"
    assert row["questions"] == ["Уточнить исполнение шкафа."] and view["source"] == "editorial"
    # Изменение фактов — другой снимок: сохранённый отбор не применяется к новым данным.
    (rdir / "facts.json").write_text(json.dumps({"facts": FACTS[:2]}), encoding="utf-8")
    assert ce.load(rdir) is None


class _Flaky(_Fake):
    """Ответы по переходу: None — сбой исполнителя (TRANSIENT), иначе ответ; учёт одновременных обращений."""

    def __init__(self, by_label):
        super().__init__([])
        import threading
        self.by_label, self.lock, self.active, self.peak = {k: list(v) for k, v in by_label.items()}, threading.Lock(), 0, 0

    def run(self, task, tok):
        import time
        with self.lock:
            self.calls += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            a = self.by_label[task.chunk_id].pop(0)
        time.sleep(0.05)
        with self.lock:
            self.active -= 1
        if a is None:
            return SimpleNamespace(ok=False, parsed=None, error_kind="TRANSIENT", error="обрыв", duration_ms=5, usage={})
        if a == "QUOTA":
            return SimpleNamespace(ok=False, parsed=None, error_kind="QUOTA", error="квота", duration_ms=5, usage={})
        return SimpleNamespace(ok=True, parsed=a, error_kind=None, error=None, duration_ms=5, usage={})


def test_run_parallel_retries_failed_transition_up_to_three_times(tmp_path):
    rdir = _two(tmp_path)
    good = _pt("УЭРМ обогрева QFD1", "кабель 3×6 → 5×6", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    other = _pt("QF7.1", "10 → 16 А", ["Ф0100"], [{"parameter": "ток", "before": "10А", "after": "16А", "fact_id": "Ф0100"}])
    ans67 = dict(_answer([other]), transition="V6→V7", impact="", impact_fact_ids=[])
    # V5→V6: сбой, непригодный ответ, сбой — и только третий повтор удачный; V6→V7 — сразу.
    fake = _Flaky({"V5→V6": [None, _answer([good], lead=""), None, _answer([good])], "V6→V7": [ans67]})
    units = {}
    out = ce.run(rdir, fake, log=lambda *_: None, wait_s=0, on_unit=lambda l, **f: units.setdefault(l, []).append(f))
    assert out["transitions"]["V5→V6"]["accepted"] and out["transitions"]["V5→V6"]["attempts"] == 4
    assert out["transitions"]["V6→V7"]["accepted"] and fake.calls == 5 and fake.peak == 2      # оба сразу
    assert units["V5→V6"][-1]["state"] == "done" and ce.status(rdir)["ready"]


def test_run_marks_failed_only_after_retries_and_keeps_other_transitions(tmp_path):
    rdir = _two(tmp_path)
    other = _pt("QF7.1", "10 → 16 А", ["Ф0100"], [{"parameter": "ток", "before": "10А", "after": "16А", "fact_id": "Ф0100"}])
    fake = _Flaky({"V5→V6": [None] * 4, "V6→V7": [dict(_answer([other]), transition="V6→V7", impact="",
                                                       impact_fact_ids=[])]})
    units = {}
    out = ce.run(rdir, fake, log=lambda *_: None, wait_s=0, on_unit=lambda l, **f: units.setdefault(l, []).append(f))
    r = out["transitions"]["V5→V6"]
    assert not r["accepted"] and r["attempts"] == 4 and "TRANSIENT" in r["error"]       # 1 + 3 повтора
    assert units["V5→V6"][-1]["state"] == "failed" and out["transitions"]["V6→V7"]["accepted"]
    assert ce.status(rdir)["missing"] == ["V5→V6"] and not out["stopped"]


def test_run_quota_stops_without_retries(tmp_path):
    rdir = _rdir(tmp_path, [T])
    fake = _Flaky({"V5→V6": ["QUOTA"]})
    out = ce.run(rdir, fake, log=lambda *_: None, wait_s=0)
    assert out["stopped"] == "QUOTA" and fake.calls == 1


def test_profiles_mark_important_by_own_discipline_only():
    shun = FACTS[1] | {"details": FACTS[1]["details"] | {"fields": [dict(name="Марка", before="ШУН/В-1", after="ЩУВ-3",
                                                                          certainty="confirmed", unit="")]}}
    assert ce.important(shun, EM) == "оборудование или марка"
    assert ce.important(shun, disciplines.by_key("OV")) is None          # электрические марки — не для ОВ
    assert ce.important(shun) is None                                    # без профиля важное не подгоняется
    vent = _ai("Ф0300", "Установка П1", [("Расход", "2000 м3/ч", "3500 м3/ч", "confirmed")])
    assert ce.important(vent, disciplines.by_key("OV")) and not ce.important(vent, EM)
    text, _, _ = ce.packet([vent], "V5→V6", disciplines.by_key("OV"))
    assert "★" in text
    prompt = ce.prompt_for(T, text, ce.prompt_text(), "s", disciplines.by_key("OV"))
    assert "Отопление, вентиляция" in prompt and "силов" not in prompt.lower()
    neutral = ce.prompt_for(T, text, ce.prompt_text(), "s", disciplines.resolve("К-ХЗ"))
    assert "не определён" in neutral and "{{" not in neutral


def test_composite_values_unclear_fields_and_cut_words():
    f = _ai("Ф0010", "Автоматические выключатели ВА-332А", [("Маршрут", "", "Лоток-110м", "unclear")])
    f["details"]["members"] = [{"object": "QF1.2", "fields": [dict(name="н", before="63А", after="50А", certainty="confirmed")]},
                               {"object": "QF1.3", "fields": [dict(name="н", before="20А", after="63А", certainty="confirmed")]}]
    f["text"] = "Номиналы изменены; плита TEHSTRONG FIRESTOP H EI150"
    facts = FACTS + [f]
    _, groups, support = ce.packet(facts, "V5→V6")
    ok = _pt("QF1.2/QF1.3 ВА-332А", "63/20 А → 50/63 А; плита TEHSTRONG FIRESTO… заменена", ["Ф0010"],
             [{"parameter": "номиналы", "before": "63А / 20А", "after": "50А / 63А", "fact_id": "Ф0010"}])
    swapped = _pt("QF1.2/QF1.3", "63/20 А → 63/50 А", ["Ф0010"],
                  [{"parameter": "номиналы", "before": "63А / 20А", "after": "63А / 50А", "fact_id": "Ф0010"}])
    route = _pt("QF1.2", "маршрут Лоток-110м", ["Ф0010"], [])
    chk = ce.check(_answer([ok, swapped, route]), T, facts, groups, support)
    assert [k["point"]["object"] for k in chk["kept"]] == ["QF1.2/QF1.3 ВА-332А"]
    assert "FIRESTOP заменена" in chk["kept"][0]["point"]["text"]
    why = {d["point"]["object"]: " ".join(d["problems"]) for d in chk["dropped"]}
    assert "направление" in why["QF1.2/QF1.3"] and "подтверждённый" in why["QF1.2"]
    assert ce.repair_cut("ток TA1.1…TA1.6 и марка ABCD…", "нет")[1] is True
    assert ce.repair_cut("ток TA1.1…TA1.6", "нет") == ("ток TA1.1…TA1.6", False)


# ------------------------------------------------------------------ клиентский слой: ключи, покрытие, правка

T2 = dict(T, from_version="V6", to_version="V7")


def _two(tmp_path):
    other = _ai("Ф0100", "QF7.1", [("Ток", "10А", "16А", "confirmed")], tr="V6→V7")
    rdir = _rdir(tmp_path, [T, T2])
    (rdir / "facts.json").write_text(json.dumps({"facts": FACTS + [other]}), encoding="utf-8")
    return rdir


def test_data_key_is_per_transition(tmp_path):
    rdir = _two(tmp_path)
    facts = json.loads((rdir / "facts.json").read_text())["facts"]
    k56, k67 = ce.data_key(facts, T), ce.data_key(facts, T2)
    facts[-1]["text"] = "QF7.1 — изменено иначе"        # меняются данные только V6→V7
    assert ce.data_key(facts, T) == k56 and ce.data_key(facts, T2) != k67


def test_coverage_decisions_and_repeat_merge():
    rep = _ai("Ф0011", "УЭРМ другого этажа", [("Кабель", "ППГнг(А)-HF 3х6, 37 м", "ППГнг(А)-HF 5х6, 37 м", "confirmed")],
              sheet="л.7")
    facts = FACTS + [rep, _ai("Ф0012", "Шкаф ЩУВ", [("Марка", "ЩУВ-3-03-R3", "ЩУВ-3-03-ПЧ-R3", "confirmed")])]
    kept = [{"point": {"object": "УЭРМ обогрева QFD1", "text": "кабель 3×6 → 5×6"}, "fact_ids": ["Ф0001"]}]
    cov = ce.coverage(facts, "V5→V6", kept, None, EM)
    assert cov["facts"]["Ф0001"]["status"] == "included"
    assert cov["facts"]["Ф0011"]["status"] == "merged"           # тот же «было → стало» на другом листе
    assert "Ф0012" in cov["unresolved"]
    cov = ce.coverage(facts, "V5→V6", kept, {"Ф0012": {"status": "excluded", "reason": "проверка"}}, EM)
    assert "Ф0012" not in cov["unresolved"] and cov["facts"]["Ф0012"]["reason"] == "проверка"


def test_save_edit_rejects_repeats_and_stray_impact(tmp_path):
    rdir = _rdir(tmp_path, [T])
    good = _pt("УЭРМ обогрева QFD1", "кабель ППГнг(А)-HF 3×6 → 5×6; распределение уточнить", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}], "needs_clarification")
    dup_q = [{"text": "Уточнить УЭРМ QFD1.", "fact_ids": ["Ф0001"]}]
    dec = {"Ф0002": {"status": "excluded", "reason": "значение неясно"}}
    with pytest.raises(ValueError) as e:
        ce.save_edit(rdir, "V5→V6", _answer([good], questions=dup_q, impact="Уточнить кабели.",
                                            impact_fact_ids=["Ф0002"]), dec, editor="тест")
    msg = str(e.value)
    assert "повторяет оговорку пункта" in msg and "последствие повторяет оговорку" in msg and "вне принятых пунктов" in msg
    rec = ce.save_edit(rdir, "V5→V6", _answer([good], impact="При закупке учесть кабель 5×6.",
                                              impact_fact_ids=["Ф0001"]), dec, editor="тест")
    assert rec["coverage"]["unresolved"] == [] and ce.status(rdir)["ready"]
    assert ce.load(rdir)["V5→V6"]["source"] == "edits"


def test_run_only_missing_and_reuse_by_key(tmp_path):
    rdir = _two(tmp_path)
    good = _pt("УЭРМ обогрева QFD1", "кабель 3×6 → 5×6", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    other = _pt("QF7.1", "10 → 16 А", ["Ф0100"], [{"parameter": "ток", "before": "10А", "after": "16А", "fact_id": "Ф0100"}])
    fake = _Fake([_answer([good]), dict(_answer([other]), transition="V6→V7", impact="", impact_fact_ids=[])])
    ce.run(rdir, fake, log=lambda *_: None, wait_s=0, parallel=1)
    assert fake.calls == 2 and ce.status(rdir)["ready"]
    facts = json.loads((rdir / "facts.json").read_text())
    facts["facts"][-1]["text"] = "QF7.1 — изменено иначе"
    (rdir / "facts.json").write_text(json.dumps(facts), encoding="utf-8")
    st = ce.status(rdir)
    assert st["missing"] == ["V6→V7"] and ce.estimate(rdir)["calls"] == 1      # V5→V6 не пересчитывается


def test_pdf_continuation_repeats_transition(tmp_path):
    pytest.importorskip("reportlab")
    pytest.importorskip("fitz")
    from backend.app.services.version_comparison_ai.md_report import pdf
    pts = [{"object": f"Объект {k}", "text": "кабель ППГнг(А)-HF 3×6 → 5×6, длина 37 м; " * 3} for k in range(40)]
    row = {"label": "V5→V6", "dates": ["04.08.2025", "01.10.2025"], "scale": "Широкая переработка",
           "lead": "Пересмотрены нагрузки.", "points": pts, "questions": [], "impact": ""}
    view = {"title": "Изменения проектных решений", "object": "О", "code": "К-1", "section": "Р", "versions": "V5–V6",
            "period": "04.08.2025–01.10.2025", "rows": [row]}
    out = pdf.render_client(view, tmp_path / "c.pdf")
    chk = pdf.check_client(out, view)
    assert chk["pages"] > 1 and chk["ok"], chk["problems"]
    import fitz
    doc = fitz.open(str(out))
    assert "продолжение" in doc[1].get_text() and "04.08.2025" in doc[1].get_text()


def test_runner_client_prepare_consent_and_no_deadlock(tmp_path, monkeypatch):
    import shutil
    import time as _t
    from backend.app.services.version_comparison_ai import executor as ex
    from backend.app.services.version_comparison_ai.md_report import runner
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "vc"))
    monkeypatch.setenv("VERSION_COMPARISON_AI_ENABLED", "1")
    rdir = runner.report_dir("obj1", "mr1")
    rdir.parent.mkdir(parents=True)
    shutil.copytree(_rdir(tmp_path, [T]), rdir)
    (rdir / "report.pdf").write_bytes(b"%PDF-1.4")
    (rdir / "job.json").write_text(json.dumps({"report_id": "mr1", "state": "done", "source": {"sha256": "abc"}}),
                                   encoding="utf-8")
    good = _pt("УЭРМ обогрева QFD1", "кабель 3×6 → 5×6", ["Ф0001"],
               [{"parameter": "кабель", "before": "3х6", "after": "5х6", "fact_id": "Ф0001"}])
    fake = _Fake([_answer([good])])
    ex.set_override(fake)
    try:
        st = runner.client_state("obj1", "mr1")
        assert st["available"] and not st["ready"] and st["estimate"]["calls"] == 1
        with pytest.raises(runner.ReportError):
            runner.client_pdf("obj1", "mr1")                       # не подготовлено — PDF не отдаётся
        with pytest.raises(runner.ReportError):
            runner.client_prepare("obj1", "mr1", consent=False)
        runner.client_prepare("obj1", "mr1", consent=True)          # раньше здесь была взаимная блокировка
        for _ in range(100):
            if runner.client_state("obj1", "mr1")["job"].get("state") not in runner.CLIENT_ACTIVE:
                break
            _t.sleep(0.05)
        st = runner.client_state("obj1", "mr1")
        assert st["ready"] and fake.calls == 1 and st["job"]["usage"]["calls"] == 1
        runner.client_prepare("obj1", "mr1", consent=True)          # готово — новых обращений нет
        assert fake.calls == 1
    finally:
        ex.set_override(None)


def test_coverage_does_not_merge_by_equal_calc_values_or_same_sheet():
    a = _ai("Ф0201", "Линия QFD3", [("Расчётные значения линии", "Ру=3,8 кВт", "Ру=8,37 кВт", "confirmed"),
                                    ("Кабель к ЯУР", "ППГнг(А)-HF 3х6, 13м", "ППГнг(А)-HF 5х6, 27м", "confirmed")])
    b = _ai("Ф0202", "Линия Q4", [("Расчётные значения линии", "Ру=5,0 кВт", "Ру=8,37 кВт", "confirmed"),
                                  ("Кабель к ЩМо", "ППГнг(А)-HF 5х6, 27м", "ППГнг(А)-HF 5х6, 13м", "confirmed")])
    kept = [{"point": {"object": "УЭРМ, ветвь QFD3", "text": "кабель 3×6 → 5×6"}, "fact_ids": ["Ф0201"]}]
    cov = ce.coverage([a, b], "V5→V6", kept, None, EM)
    assert cov["facts"]["Ф0202"]["status"] == "unresolved"     # другая линия того же листа — не «то же значение»
