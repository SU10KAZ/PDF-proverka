"""Задания этапов (аналитик, проверяющий, составитель), программная проверка ответов и сборка report_v1.

Модель не является единственным контролёром: идентификаторы, покрытие блоков, диапазоны строк,
вердикты по каждому утверждению и порядок переходов проверяет программа. Подписи A/B, даты, лист
и страницы PDF в отчёт подставляются из разбора, а не из ответа модели.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .. import prompt as old_prompt
from .. import settings as ai_settings
from . import parse, schemas, settings

PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
PROMPT_NAMES = ("rules", "common", "analyst", "verifier", "composer")
_PLACEHOLDER = re.compile(r"\{\{([A-Z_]+)\}\}")
RELATED_PER_TOKEN = 5
RELATED_MAX_TOKENS = 40
RELATED_MAX_CHARS = 40000
EXCERPT_PAD = 2
WORD_LIMIT = 1000


def prompt_text(name: str) -> str:
    return (PROMPTS_DIR / f"md_report_{name}.md").read_text(encoding="utf-8")


def prompt_digest() -> str:
    return ai_settings.digest({"version": settings.PROMPT_VERSION} | {n: prompt_text(n) for n in PROMPT_NAMES})


def _fill(template: str, values: dict[str, str]) -> str:
    """Один проход подстановки: вставленные данные повторно не разбираются (``{{…}}`` в MD — просто текст)."""
    missing = [m for m in _PLACEHOLDER.findall(template) if m not in values]
    if missing:
        raise ValueError(f"нет значения для {', '.join(sorted(set(missing)))}")
    return _PLACEHOLDER.sub(lambda m: values[m.group(1)], template)


def render(stage: str, values: dict[str, str], seed: str, texts: dict[str, str] | None = None) -> str:
    """Общая инструкция + задание этапа; метки блока данных подбираются так, чтобы их не было в данных.

    ``texts`` — тексты промптов, записанные в отчёт при создании (идущий отчёт не видит правок кода)."""
    t = texts or {n: prompt_text(n) for n in PROMPT_NAMES}
    body = "\n".join(v for v in values.values())
    begin, end = old_prompt.boundary({"packet_id": seed}, body)
    common = _fill(t["common"], {"BEGIN": begin, "END": end, "TRUSTED_ANALYSIS_RULES": t["rules"].strip()})
    return common.rstrip() + "\n\n" + _fill(t[stage], values | {"BEGIN": begin, "END": end})


def _clean(text: str | None, limit: int = 160) -> str:
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", str(text or "")).strip()
    return text[:limit]


def source_metadata(src: dict, manifest: dict) -> str:
    return json.dumps({"file": _clean(src.get("filename")), "sha256": src.get("sha256"), "bytes": src.get("bytes"),
                       "lines": manifest["lines_total"], "project_code": _clean(manifest.get("project_code"), 80),
                       "transitions": len(manifest["transitions"]),
                       "basis": "только MD-сводка; исходные PDF не предоставлены"}, ensure_ascii=False)


def numbered(lines: list[str], ranges: list[list[int]], context: list[list[int]] | None = None) -> str:
    out = []
    for s, e in sorted(context or []):
        out += [f"[{i}] (контекст) {lines[i - 1]}" for i in range(s, e + 1)]
    for s, e in ranges:
        out += [f"[{i}] {lines[i - 1]}" for i in range(s, e + 1)]
    return "\n".join(out)


def _tr_by_id(manifest: dict) -> dict[str, dict]:
    return {t["transition_id"]: t for t in manifest["transitions"]}


def _tr_line(t: dict, extra: str = "") -> str:
    note = []
    if t.get("explicit_no_content_changes"):
        note.append("в сводке явная отметка «изменений содержания нет»")
    if t.get("source_incomplete"):
        note.append(f"в сводке есть неописанные места: {len(t['source_incomplete'])}")
    return (f"- {t['transition_id']}: {t['label']} (A: {_clean(t['a_label'])}; B: {_clean(t['b_label'])}), "
            f"строки {t['line_start']}–{t['line_end']}, листов в сводке {len(t['sheets'])}"
            + (f"; {extra}" if extra else "") + (f"; {'; '.join(note)}" if note else ""))


# ------------------------------------------------------------------ аналитик

def group_ranges(plan: dict, group: dict) -> list[list[int]]:
    by = {b["block_id"]: b for b in plan["blocks"]}
    return [r for bid in group["block_ids"] for r in by[bid]["ranges"]]


def analyst_prompt(run_id: str, src: dict, manifest: dict, plan: dict, group: dict, lines: list[str],
                   texts: dict[str, str] | None = None) -> str:
    by = {b["block_id"]: b for b in plan["blocks"]}
    trs = _tr_by_id(manifest)
    assigned = []
    for tid in group["transition_ids"]:
        parts = [by[b] for b in group["block_ids"] if by[b]["transition_id"] == tid and by[b]["kind"] == "part"]
        extra = ", ".join(f"часть {p['part']} из {p['parts']}" for p in parts)
        assigned.append(_tr_line(trs[tid], extra))
    manifest_lines, text_parts = [], []
    for bid in group["block_ids"]:
        b = by[bid]
        rng = ", ".join(f"{s}–{e}" for s, e in b["ranges"])
        kind = {"transition": "переход целиком", "part": f"часть {b.get('part')} из {b.get('parts')}",
                "unknown": "НЕРАСПОЗНАННЫЙ раздел (не переход A → B): опиши в input_problems"}[b["kind"]]
        manifest_lines.append(f"- {bid}: {b['transition_id'] or b.get('section_id')}, {kind}, строки {rng}, "
                              f"{b['chars']} символов")
        text_parts.append(f"=== Блок {bid} ===\n" + numbered(lines, b["ranges"], b.get("context")))
    return render("analyst", {
        "RUN_ID": run_id, "GROUP_ID": group["group_id"], "SOURCE_METADATA": source_metadata(src, manifest),
        "ASSIGNED_TRANSITIONS": "\n".join(assigned) or "- нет (только нераспознанные разделы)",
        "ASSIGNED_BLOCK_MANIFEST": "\n".join(manifest_lines),
        "SOURCE_LIMITATIONS": numbered(lines, [[1, manifest["header"]["line_end"]]]) if manifest["header"]["line_end"] else "—",
        "ASSIGNED_SOURCE_TEXT": "\n\n".join(text_parts)}, f"{run_id}:A:{group['group_id']}", texts)


def _in(ranges: list[list[int]], a: int, b: int) -> bool:
    return any(s <= a and b <= e for s, e in ranges)


def check_analyst(ans: dict, run_id: str, group: dict, plan: dict, manifest: dict) -> dict[str, Any]:
    hard, soft = [], []
    if ans.get("run_id") != run_id or ans.get("group_id") != group["group_id"]:
        hard.append("run_id или group_id не совпадают с заданием")
    want = set(group["transition_ids"])
    got = [t["transition_id"] for t in ans.get("transitions") or []]
    if set(got) != want or len(got) != len(set(got)):
        hard.append(f"переходы в ответе {sorted(got)} не совпадают с назначенными {sorted(want)}")
    if set(ans.get("processed_block_ids") or []) != set(group["block_ids"]) or ans.get("unprocessed_block_ids") \
            or ans.get("partial"):
        hard.append(f"обработаны не все назначенные блоки: {sorted(ans.get('processed_block_ids') or [])} из "
                    f"{group['block_ids']}; не обработаны {ans.get('unprocessed_block_ids')}")
    allowed = group_ranges(plan, group) + ([[1, manifest["header"]["line_end"]]] if manifest["header"]["line_end"] else [])
    trs = _tr_by_id(manifest)
    bad_refs = 0
    total_refs = 0
    for t in ans.get("transitions") or []:
        mt = trs.get(t["transition_id"])
        refs = [r for f in t.get("facts") or [] for r in f.get("evidence") or []]
        refs += [r for n in (t.get("explicit_conditions") or []) + (t.get("contradictions") or [])
                 for r in n.get("evidence") or []]
        for r in refs:
            total_refs += 1
            if not (r["md_from"] <= r["md_to"] and _in(allowed, r["md_from"], r["md_to"])):
                bad_refs += 1
        for f in t.get("facts") or []:
            if not f.get("evidence"):
                hard.append(f"{t['transition_id']}/{f.get('fact_id')}: факт без строк источника")
        if mt and mt.get("explicit_no_content_changes") and not mt["sheets"]:
            if t.get("content_status") != "no_content_changes" or t.get("facts"):
                hard.append(f"{t['transition_id']}: в сводке явная отметка «изменений содержания нет», "
                            "а в ответе есть изменения")
    if bad_refs:
        hard.append(f"ссылок на строки вне назначенного текста: {bad_refs} из {total_refs}")
    if ans.get("input_problems"):
        soft.append("аналитик сообщил о проблемах входа: " + "; ".join(ans["input_problems"])[:500])
    return {"ok": not hard, "hard": hard, "soft": soft}


# ------------------------------------------------------------------ проверяющий

def claims_for_unit(results: dict[str, dict]) -> list[dict]:
    """Утверждения на проверку: факты, оценки переходов, условия и противоречия всех групп единицы."""
    out = []
    for gid, ans in results.items():
        for t in ans.get("transitions") or []:
            tid = t["transition_id"]
            out.append({"claim_id": f"{gid}.{tid}.A", "transition_id": tid, "kind": "оценка перехода",
                        "text": f"масштаб «{t['change_scope']}», значимость «{t['significance']}», "
                                f"основание: {t['assessment_basis']}; последствия: {t['construction_impact']}",
                        "evidence": []})
            for f in t.get("facts") or []:
                text = f"{f['object']}: {f['before']} → {f['after']}"
                if f.get("possible_consequence"):
                    text += f"; последствие: {f['possible_consequence']}"
                if f.get("uncertainty"):
                    text += f"; неясность: {f['uncertainty']}"
                out.append({"claim_id": f"{gid}.{tid}.{f['fact_id']}", "transition_id": tid,
                            "kind": "факт" + (" (кандидат в серьёзные)" if f.get("serious_candidate") else ""),
                            "text": text, "evidence": f.get("evidence") or [],
                            "related_objects": f.get("related_objects") or [], "serious": bool(f.get("serious_candidate"))})
            for k, n in enumerate(t.get("explicit_conditions") or [], 1):
                out.append({"claim_id": f"{gid}.{tid}.K{k}", "transition_id": tid, "kind": "условие выдачи",
                            "text": n["text"], "evidence": n.get("evidence") or []})
            for k, n in enumerate(t.get("contradictions") or [], 1):
                out.append({"claim_id": f"{gid}.{tid}.X{k}", "transition_id": tid, "kind": "противоречие",
                            "text": n["text"], "evidence": n.get("evidence") or []})
    return out


_TOKEN = re.compile(r"[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё0-9.\-/]*\d[A-Za-zА-Яа-яЁё0-9.\-/]*")


def related_tokens(claims: list[dict]) -> list[str]:
    seen: list[str] = []
    for c in claims:
        for obj in c.get("related_objects") or []:
            for tok in _TOKEN.findall(obj or ""):
                tok = tok.strip(".-/")
                if len(tok) >= 3 and tok not in seen:
                    seen.append(tok)
    return seen[:RELATED_MAX_TOKENS]


def _excerpt_ranges(claims: list[dict], manifest: dict, lines_total: int) -> list[list[int]]:
    marks: set[int] = set()
    for c in claims:
        for r in c.get("evidence") or []:
            a, b = max(1, r["md_from"] - EXCERPT_PAD), min(lines_total, r["md_to"] + EXCERPT_PAD)
            marks.update(range(a, b + 1))
            loc = parse.locate(manifest, r["md_from"])
            if loc["sheet"]:
                marks.add(loc["sheet"]["line_start"])
            if loc["transition"]:
                marks.add(loc["transition"]["line_start"])
    out: list[list[int]] = []
    for n in sorted(marks):
        if out and out[-1][1] + 1 == n:
            out[-1][1] = n
        else:
            out.append([n, n])
    return out


def verifier_prompt(run_id: str, src: dict, manifest: dict, plan: dict, unit: dict, results: dict[str, dict],
                    claims: list[dict], lines: list[str],
                    texts: dict[str, str] | None = None) -> tuple[str, list[list[int]]]:
    """Задание проверяющего и диапазоны строк, которые ему показаны (ссылки допускаются только на них)."""
    trs = _tr_by_id(manifest)
    by = {b["block_id"]: b for b in plan["blocks"]}
    unit_ranges = [[trs[t]["line_start"], trs[t]["line_end"]] for t in unit["transition_ids"]]
    groups = {g["group_id"]: g for g in plan["groups"]}
    for gid in unit["group_ids"]:
        for bid in groups[gid]["block_ids"]:
            if by[bid]["kind"] == "unknown":
                unit_ranges += by[bid]["ranges"]
    full = sum(sum(len(lines[i - 1]) + 1 for i in range(s, e + 1)) for s, e in unit_ranges) <= settings.verify_full_chars()
    shown = unit_ranges if full else _excerpt_ranges(claims, manifest, len(lines))
    mode = ("Полный текст переходов этой единицы." if full else
            "Выдержки: строки доказательств аналитиков с соседними строками и заголовками листов "
            "(полный текст единицы больше предела одного задания).")
    unit_lines = {i for s, e in unit_ranges for i in range(s, e + 1)}
    rel_ranges: list[list[int]] = []
    rel_text, size = [], 0
    for tok in related_tokens(claims):
        hits = [i for i, ln in enumerate(lines, 1) if tok in ln and i not in unit_lines][:RELATED_PER_TOKEN]
        for i in hits:
            loc = parse.locate(manifest, i)
            where = loc["transition"]["label"] if loc["transition"] else "заголовок"
            head = f" ({_clean(loc['sheet']['heading'], 120)})" if loc["sheet"] else ""
            row = f"[{i}] «{tok}» — {where}{head}: {lines[i - 1]}"
            if size + len(row) > RELATED_MAX_CHARS:
                break
            rel_text.append(row)
            rel_ranges.append([i, i])
            size += len(row)
    unit_manifest = [_tr_line(trs[t]) for t in unit["transition_ids"]]
    for gid in unit["group_ids"]:
        g = groups[gid]
        unit_manifest.append(f"- группа {gid}: блоки " + ", ".join(
            f"{b} ({', '.join(f'{s}–{e}' for s, e in by[b]['ranges'])}"
            + (f"; часть {by[b]['part']} из {by[b]['parts']} перехода {trs[by[b]['transition_id']]['label']}"
               if by[b]["kind"] == "part" else "") + ")" for b in g["block_ids"]))
    claim_lines = [f"- {c['claim_id']} [{c['transition_id']}, {c['kind']}]: {c['text']}"
                   + (f" (строки {', '.join(str(r['md_from']) + '–' + str(r['md_to']) for r in c['evidence'])})"
                      if c.get("evidence") else "") for c in claims]
    analyst_json = json.dumps({gid: {"transitions": ans.get("transitions"), "input_problems": ans.get("input_problems")}
                               for gid, ans in results.items()}, ensure_ascii=False)
    split = sorted({by[b]["transition_id"] for gid in unit["group_ids"] for b in groups[gid]["block_ids"]
                    if by[b]["kind"] == "part"})
    whole = set()
    for t in split:
        mine = [b for gid in unit["group_ids"] for b in groups[gid]["block_ids"] if by[b]["transition_id"] == t]
        if len(mine) == by[mine[0]].get("parts"):
            whole.add(t)
    notes = []
    if whole:
        notes.append("Переход, разбитый на части, представлен несколькими частичными результатами аналитиков — "
                     "сведи их в один итог перехода.")
    if set(split) - whole:
        notes.append("В этой единице только часть перехода " + ", ".join(trs[t]["label"] for t in sorted(set(split) - whole))
                     + ": дай итог по переданной части; части сведёт составитель.")
    prompt = render("verifier", {
        "UNIT_NOTE": " ".join(notes) or "Все переходы единицы переданы целиком.",
        "RUN_ID": run_id, "UNIT_ID": unit["unit_id"], "SOURCE_METADATA": source_metadata(src, manifest),
        "UNIT_MANIFEST": "\n".join(unit_manifest), "ANALYST_RESULTS": analyst_json,
        "CLAIMS_TO_VERIFY": "\n".join(claim_lines), "SOURCE_MODE": mode,
        "SOURCE_TEXT": "\n".join(numbered(lines, [r]) for r in shown),
        "RELATED_TEXT": "\n".join(rel_text) or "— связанных мест в других переходах не найдено"},
        f"{run_id}:V:{unit['unit_id']}", texts)
    allowed = shown + rel_ranges
    if manifest["header"]["line_end"]:
        allowed = allowed + [[1, manifest["header"]["line_end"]]]
    return prompt, allowed


def check_verifier(ans: dict, run_id: str, unit: dict, claims: list[dict], shown: list[list[int]]) -> dict[str, Any]:
    hard, soft = [], []
    if ans.get("run_id") != run_id or ans.get("unit_id") != unit["unit_id"]:
        hard.append("run_id или unit_id не совпадают с заданием")
    want = {c["claim_id"] for c in claims}
    got = [c["claim_id"] for c in ans.get("claims") or []]
    missing = sorted(want - set(got))
    if missing:
        hard.append(f"нет вердикта по утверждениям: {', '.join(missing[:30])}" + (" …" if len(missing) > 30 else ""))
    extra = sorted(set(got) - want)
    if extra:
        soft.append(f"вердикты по неизвестным утверждениям отброшены: {', '.join(extra[:20])}")
    tids = [t["transition_id"] for t in ans.get("transitions") or []]
    if set(tids) != set(unit["transition_ids"]) or len(tids) != len(set(tids)):
        hard.append(f"итоги переходов {sorted(tids)} не совпадают с единицей {unit['transition_ids']}")
    refs = [r for c in ans.get("claims") or [] for r in c.get("evidence") or []]
    refs += [r for t in ans.get("transitions") or [] for r in t.get("evidence") or []]
    refs += [r for s in ans.get("significant_candidates") or [] for r in s.get("evidence") or []]
    bad = [r for r in refs if not (r["md_from"] <= r["md_to"] and _in(shown, r["md_from"], r["md_to"]))]
    if refs and len(bad) > max(2, 0.2 * len(refs)):
        hard.append(f"ссылок на строки, не показанные в задании: {len(bad)} из {len(refs)}")
    elif bad:
        soft.append(f"ссылки на непоказанные строки отброшены: {len(bad)}")
    return {"ok": not hard, "hard": hard, "soft": soft, "bad_refs": bad}


def clean_verifier(ans: dict, claims: list[dict], shown: list[list[int]]) -> dict:
    """Отбросить чужие вердикты, ссылки вне показанного и опору кандидатов на отклонённые утверждения."""
    want = {c["claim_id"]: c for c in claims}
    ok_ref = lambda r: r["md_from"] <= r["md_to"] and _in(shown, r["md_from"], r["md_to"])  # noqa: E731
    out = json.loads(json.dumps(ans))
    out["claims"] = [c | {"evidence": [r for r in c["evidence"] if ok_ref(r)],
                          "transition_id": want[c["claim_id"]]["transition_id"], "kind": want[c["claim_id"]]["kind"]}
                     for c in out["claims"] if c["claim_id"] in want]
    verdict = {c["claim_id"]: c["verdict"] for c in out["claims"]}
    for t in out["transitions"]:
        t["evidence"] = [r for r in t["evidence"] if ok_ref(r)]
        t["key_claim_ids"] = [x for x in t["key_claim_ids"] if verdict.get(x) not in (None, "rejected")]
    cands = []
    for s in out["significant_candidates"]:
        s["claim_ids"] = [x for x in s["claim_ids"] if verdict.get(x) not in (None, "rejected")]
        s["evidence"] = [r for r in s["evidence"] if ok_ref(r)]
        if s["claim_ids"]:
            cands.append(s)
    out["significant_candidates"] = cands
    return out


# ------------------------------------------------------------------ составитель

def verified_parts(verified: dict[str, dict]) -> dict[str, list[dict]]:
    """Проверенные итоги по переходам; у перехода, проверенного по частям, их несколько."""
    out: dict[str, list[dict]] = {}
    for uid, v in verified.items():
        for t in v["transitions"]:
            out.setdefault(t["transition_id"], []).append(t | {"unit_id": uid})
    return out


def final_assessment(tid: str, composed: dict, parts: dict[str, list[dict]]) -> tuple[str, str, str]:
    """(масштаб, значимость, примечание). Один проверенный итог — он и есть оценка. Несколько частей —
    сведение составителя, но не ниже любой проверенной части; иначе берётся наибольшая оценка частей."""
    ps = parts.get(tid) or []
    if not ps:
        return composed["change_scope"], composed["significance"], "нет проверенного итога"
    if len(ps) == 1:
        return ps[0]["change_scope"], ps[0]["significance"], ""
    sc_part = max(ps, key=lambda p: schemas.SCOPE_RANK[p["change_scope"]])["change_scope"]
    sg_part = max(ps, key=lambda p: schemas.SIGNIFICANCE_RANK[p["significance"]])["significance"]
    sc, sg, note = composed["change_scope"], composed["significance"], ""
    if schemas.SCOPE_RANK[sc] < schemas.SCOPE_RANK[sc_part]:
        sc, note = sc_part, f"масштаб поднят до оценки части ({sc_part})"
    if schemas.SIGNIFICANCE_RANK[sg] < schemas.SIGNIFICANCE_RANK[sg_part]:
        sg, note = sg_part, (note + "; " if note else "") + f"значимость поднята до оценки части ({sg_part})"
    return sc, sg, note


def coverage_text(manifest: dict, verified: dict[str, dict]) -> str:
    rows = []
    vt = {t["transition_id"]: t for v in verified.values() for t in v["transitions"]}
    for t in manifest["transitions"]:
        v = vt.get(t["transition_id"])
        rows.append(_tr_line(t, f"проверено: {'да' if v else 'НЕТ'}"))
    return "\n".join(rows)


def composer_prompt(run_id: str, src: dict, manifest: dict, verified: dict[str, dict], retry_note: str = "",
                    texts: dict[str, str] | None = None) -> str:
    data = {"transitions": [], "claims": [], "significant_candidates": [], "unresolved_questions": []}
    parts = verified_parts(verified)
    for uid, v in verified.items():
        data["transitions"] += [t | {"unit_id": uid, "partial_of_split_transition": len(parts[t["transition_id"]]) > 1}
                                for t in v["transitions"]]
        data["claims"] += [{k: c[k] for k in ("claim_id", "transition_id", "kind", "verdict", "verified_wording",
                                               "evidence", "linked_claim_ids")}
                           for c in v["claims"] if c["verdict"] != "rejected"]
        data["significant_candidates"] += v["significant_candidates"]
        data["unresolved_questions"] += v["unresolved_questions"]
    order = {t["transition_id"]: k for k, t in enumerate(manifest["transitions"])}
    data["transitions"].sort(key=lambda t: order.get(t["transition_id"], 999))
    return render("composer", {
        "RUN_ID": run_id, "SOURCE_METADATA": source_metadata(src, manifest),
        "VERIFIED_COVERAGE": coverage_text(manifest, verified),
        "VERIFIED_RESULTS": json.dumps(data, ensure_ascii=False),
        "RETRY_NOTE": retry_note}, f"{run_id}:C", texts)


# Обозначения версий, листов и страниц — не числовые утверждения («V12», «л.9», «лист 9», «стр. PDF 14»).
_SKIP_NUM = re.compile(r"(?:\bV|\bл\.\s*|\bлист(?:а|е|у|ом|ы|ов|ах)?\s*№?\s*|\bP|стр\.\s*(?:PDF\s*)?|"
                       r"страниц[аеыу]?\s*(?:PDF\s*)?|СО\s+л\.\s*)\d+", re.IGNORECASE)
_NUM = re.compile(r"\d+(?:[.,]\d+)?")


def numbers(text: str) -> set[str]:
    text = _SKIP_NUM.sub(" ", text or "")
    return {n.replace(",", ".") for n in _NUM.findall(text)}


def _lines_text(lines: list[str], refs: list[dict]) -> str:
    out = []
    for r in refs:
        a, b = max(1, r["md_from"]), min(len(lines), r["md_to"])
        out += lines[a - 1:b]
    return "\n".join(out)


def check_composer(ans: dict, run_id: str, manifest: dict, verified: dict[str, dict],
                   lines: list[str]) -> dict[str, Any]:
    hard, soft = [], []
    if ans.get("run_id") != run_id:
        hard.append("run_id не совпадает с заданием")
    want = [t["transition_id"] for t in manifest["transitions"]]
    got = [t["transition_id"] for t in ans.get("transitions") or []]
    if sorted(got) != sorted(want):
        hard.append(f"в отчёте должны быть ровно переходы {want}; получены {got}")
    claims = {c["claim_id"]: c for v in verified.values() for c in v["claims"]}
    sig = ans.get("significant_changes") or []
    if len(sig) > 5:
        hard.append(f"серьёзных изменений {len(sig)} — допускается не больше 5")
    for i, s in enumerate(sig):
        bad = [x for x in s.get("claim_ids") or [] if x not in claims or claims[x]["verdict"] == "rejected"]
        if bad:
            hard.append(f"серьёзное изменение {i + 1} опирается на неизвестные или отклонённые утверждения: {bad}")
        if not s.get("claim_ids"):
            hard.append(f"серьёзное изменение {i + 1} без проверенных утверждений (claim_ids)")
        if not s.get("sources"):
            hard.append(f"серьёзное изменение {i + 1} без источника")
    allrefs = [r for r in ans.get("summary_sources") or []] + [r for t in ans.get("transitions") or []
                                                                for r in t.get("sources") or []]
    allrefs += [r for s in sig for r in s.get("sources") or []]
    out_of_file = [r for r in allrefs if not (1 <= r["md_from"] <= r["md_to"] <= manifest["lines_total"])]
    if out_of_file:
        hard.append(f"ссылки за пределами файла: {len(out_of_file)}")
    # Числа: каждое число текста должно встречаться в процитированных строках или проверенных формулировках.
    unconfirmed = []
    for i, s in enumerate(sig):
        pool = _lines_text(lines, s.get("sources") or []) + "\n" + "\n".join(
            claims[x]["verified_wording"] for x in s.get("claim_ids") or [] if x in claims)
        miss = (numbers(s.get("before_after", "")) | numbers(s.get("consequence", "")) | numbers(s.get("title", ""))) \
            - numbers(pool)
        if miss:
            unconfirmed.append({"where": f"серьёзное изменение {i + 1} «{s.get('title', '')[:60]}»",
                                "numbers": sorted(miss)})
    vt = verified_parts(verified)
    for t in ans.get("transitions") or []:
        tid = t["transition_id"]
        pool = _lines_text(lines, t.get("sources") or []) + "\n" + json.dumps(vt.get(tid) or [], ensure_ascii=False) \
            + "\n" + "\n".join(c["verified_wording"] for c in claims.values() if c.get("transition_id") == tid)
        miss = (numbers(t.get("main_changes", "")) | numbers(t.get("construction_impact", ""))) - numbers(pool)
        if miss:
            unconfirmed.append({"where": f"строка таблицы {tid}", "numbers": sorted(miss)})
    pool_all = "\n".join(c["verified_wording"] for c in claims.values()) + "\n" + _lines_text(
        lines, ans.get("summary_sources") or [])
    miss = set().union(*[numbers(x) for x in ans.get("summary") or []] or [set()]) - numbers(pool_all) \
        - {str(len(want))}
    if miss:
        unconfirmed.append({"where": "общий вывод", "numbers": sorted(miss)})
    # Исправимое: повод повторить обращение; если не исправлено — отчёт выпускается с предупреждением.
    fixable = []
    if unconfirmed:
        fixable.append("числа не найдены ни в процитированных строках, ни в проверенных формулировках: "
                       + "; ".join(f"{u['where']}: {', '.join(u['numbers'])}" for u in unconfirmed))
    leaked = internal_ids(ans, manifest, lines)
    if leaked:
        fixable.append("служебные идентификаторы в тексте для человека: " + ", ".join(leaked)
                       + " — называй переходы версиями (например, «V10→V11»)")
    words = word_count(ans)
    if words > WORD_LIMIT:
        fixable.append(f"текст длиннее предела: {words} слов при ориентире 900")
    parts = verified_parts(verified)
    for t in ans.get("transitions") or []:
        sc, sg, note = final_assessment(t["transition_id"], t, parts)
        if (sc, sg) != (t["change_scope"], t["significance"]):
            soft.append(f"{t['transition_id']}: оценка составителя ({t['change_scope']} / {t['significance']}) "
                        f"заменена проверенной ({sc} / {sg}){'; ' + note if note else ''}")
    return {"ok": not hard and not fixable, "hard": hard, "fixable": fixable, "soft": soft,
            "unconfirmed_numbers": unconfirmed, "words": words}


_CLAIM_ID = re.compile(r"\bG\d{2}\.T\d{2}(?:\.[A-Z]+\d*)?\b")


def _user_texts(ans: dict) -> list[str]:
    parts = list(ans.get("summary") or []) + list(ans.get("limitations") or [])
    for t in ans.get("transitions") or []:
        parts += [t.get("main_changes", ""), t.get("construction_impact", "")]
    for s in ans.get("significant_changes") or []:
        parts += [s.get("title", ""), s.get("before_after", ""), s.get("consequence", ""), s.get("version_range", "")]
    return parts


def _id_pattern(manifest: dict, lines: list[str]) -> re.Pattern | None:
    """Обозначения переходов программы (T01 …), которых нет в тексте источника, — служебные."""
    source = "\n".join(lines)
    ids = [t["transition_id"] for t in manifest["transitions"] if not re.search(rf"\b{t['transition_id']}\b", source)]
    return re.compile(r"\b(" + "|".join(ids) + r")\b") if ids else None


def internal_ids(ans: dict, manifest: dict, lines: list[str]) -> list[str]:
    pat = _id_pattern(manifest, lines)
    found: set[str] = set()
    for text in _user_texts(ans):
        found.update(_CLAIM_ID.findall(text))
        if pat:
            found.update(pat.findall(text))
    return sorted(found)


def humanize(text: str, manifest: dict, pat: re.Pattern | None) -> str:
    """Подстраховка после проверки: служебный идентификатор перехода → «V10→V11», ссылки на утверждения — убрать."""
    labels = {t["transition_id"]: t["label"] for t in manifest["transitions"]}
    text = _CLAIM_ID.sub("", text)
    if pat:
        text = pat.sub(lambda m: labels[m.group(1)], text)
    return re.sub(r"\s+([,.;:])", r"\1", re.sub(r"\(\s*\)", "", text))


def word_count(ans: dict) -> int:
    return sum(len(re.findall(r"\w+", p)) for p in _user_texts(ans))


# ------------------------------------------------------------------ сборка report_v1

_MD_MARK = re.compile(r"\*\*|__|`+|</?[A-Za-z][^<>]{0,40}>")


def plain(text: str) -> str:
    """Текст для PDF без разметки Markdown/HTML, пришедшей из источника или ответа модели."""
    text = _MD_MARK.sub("", str(text or ""))
    text = re.sub(r"^\s*[-*•]\s+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _source(manifest: dict, r: dict) -> dict:
    loc = parse.locate(manifest, r["md_from"])
    t, s = loc["transition"], loc["sheet"]
    return {"transition": t["label"] if t else None, "sheet": (s.get("sheet") or s.get("title") or None) if s else None,
            "pdf_before": s.get("a_page") if s else None, "pdf_after": s.get("b_page") if s else None,
            "md_lines": [r["md_from"], r["md_to"]], "context": plain(r.get("context"))}


def _strip_label(text: str, label: str) -> str:
    """Обозначение перехода в начале текста строки таблицы лишнее: оно уже в первой колонке."""
    a, b = label.split("→", 1)
    m = re.match(rf"^\s*{re.escape(a)}\s*→\s*{re.escape(b)}\s*[:—–-]\s*", text)
    return text[m.end():] if m else text


def assemble(ans: dict, manifest: dict, verified: dict[str, dict], src: dict,
             lines: list[str] | None = None) -> dict[str, Any]:
    """report_v1 из ответа составителя: порядок, подписи A/B и даты — из разбора, оценки — проверенные."""
    pat = _id_pattern(manifest, lines or [])
    ans = json.loads(json.dumps(ans))
    for k in ("summary", "limitations"):
        ans[k] = [humanize(x, manifest, pat) for x in ans[k]]
    for t in ans["transitions"]:
        label = next(m["label"] for m in manifest["transitions"] if m["transition_id"] == t["transition_id"])
        for k in ("main_changes", "construction_impact"):
            t[k] = _strip_label(humanize(t[k], manifest, pat), label)
    for s in ans["significant_changes"]:
        for k in ("title", "before_after", "consequence", "version_range"):
            s[k] = humanize(s[k], manifest, pat)
    parts = verified_parts(verified)
    by = {t["transition_id"]: t for t in ans["transitions"]}
    trs = []
    for t in manifest["transitions"]:
        c = by[t["transition_id"]]
        scope, sig, _note = final_assessment(t["transition_id"], c, parts)
        trs.append({
            "transition_id": t["transition_id"],
            "from_version": t["from_version"], "to_version": t["to_version"],
            "from_label": t["a_label"], "to_label": t["b_label"],
            "from_date": t["from_date"], "to_date": t["to_date"],
            "change_scope": scope, "significance": sig,
            "main_changes": plain(c["main_changes"]), "construction_impact": plain(c["construction_impact"]),
            "sources": [_source(manifest, r) for r in c["sources"]],
        })
    sig = [{"priority": s["priority"], "title": plain(s["title"]), "version_range": plain(s["version_range"]),
            "before_after": plain(s["before_after"]), "consequence": plain(s["consequence"]), "claim_ids": s["claim_ids"],
            "sources": [_source(manifest, r) for r in s["sources"]]} for s in ans["significant_changes"]]
    return {
        "schema_version": "report_v1",
        "project_code": manifest.get("project_code") or Path(src["filename"]).stem,
        "title": "Оценка изменений версий документации",
        "input_filename": src["filename"],
        "analysis_basis": {"type": "ai_change_summary",
                           "description": "Анализ готовой ИИ-сводки изменений версий; исходные PDF не проверялись.",
                           "source_pdf_checked": False},
        "summary": [plain(x) for x in ans["summary"] if plain(x)],
        "summary_sources": [_source(manifest, r) for r in ans["summary_sources"]],
        "transitions": trs, "significant_changes": sig, "limitations": [plain(x) for x in ans["limitations"] if plain(x)],
    }


def distribution(report: dict) -> dict[str, int]:
    out = {"total": 0, "technical": 0, "no_content_change": 0, "formatting_only": 0, "unknown": 0}
    for t in report["transitions"]:
        out["total"] += 1
        sc = schemas.normalize_scope(t["change_scope"])
        key = ("technical" if sc in schemas.TECHNICAL_SCOPES else "no_content_change" if sc == "Нет изменений содержания"
               else "formatting_only" if sc == "Только оформление" else "unknown")
        out[key] += 1
    return out


def consistency(report: dict, manifest: dict) -> list[str]:
    """Соответствие отчёта разбору: ровно по одному переходу в исходном порядке, явные отметки соблюдены."""
    problems = []
    want = [(t["from_version"], t["to_version"]) for t in manifest["transitions"]]
    got = [(t["from_version"], t["to_version"]) for t in report["transitions"]]
    if want != got:
        problems.append("порядок или состав переходов отличается от источника")
    for t, m in zip(report["transitions"], manifest["transitions"]):
        sc = schemas.normalize_scope(t["change_scope"])
        if m.get("explicit_no_content_changes") and not m["sheets"] and sc != "Нет изменений содержания":
            problems.append(f"{m['label']}: в сводке «изменений содержания нет», в отчёте — «{t['change_scope']}»")
        if m["sheets"] and m["items"] and sc == "Нет изменений содержания":
            problems.append(f"{m['label']}: в сводке есть описанные изменения, в отчёте — «нет изменений содержания»")
    return problems
