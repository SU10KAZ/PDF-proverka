#!/usr/bin/env python3
"""Этапы 0.1–0.2 плана «Улучшение алгоритма» в ограниченном объёме по одной дисциплине.

Только чтение боевых данных, без вызовов моделей. Результат — в каталог --out:

  ledger.jsonl            наблюдения (решение + версия + входы + замечание + прогон)
  versions.json           сводка по версиям (прогоны, переаудит, входы)
  matcher_pairs.json      сопоставление «прогон ↔ прогон», «latest ↔ .bak», «версия ↔ версия»
  review_sample.json      выборка для ручной проверки сопоставителя (совпадения и несопоставленные)
  report.json / report.md итоговые счётчики

Пример:
  python backend/scripts/algorithm_improvement_ar.py --discipline AR \\
      --objects-root /srv/auditmanager/data/projects_v2/objects \\
      --decisions-log /srv/auditmanager/data/knowledge_base/decisions_log.json \\
      --out /tmp/aim_ar
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.algorithm_improvement import ledger as L  # noqa: E402
from backend.app.services.algorithm_improvement.matcher import (  # noqa: E402
    STATUS_AMBIGUOUS,
    STATUS_EXACT,
    STATUS_FUZZY,
    STATUS_UNMATCHED,
    match_findings,
    pair_features,
    review_candidates,
)

NON_REPLAYABLE_OBJECTS = ("213_",)   # исходные PDF утрачены (решение от 08.10.2026)
SIGNIFICANT = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ"}

# Предварительная таксономия причин отклонения (правила; порядок = приоритет).
CAUSES: list[tuple[str, str]] = [
    ("carried_or_precedent", r"↩|перенесено|авто-перенос|по\s+аналогии|аналогично\s+(отклон|предыдущ|ранее)"
                             r"|прецедент|ранее\s+отклон|как\s+в\s+(томе|книге|предыдущ)"),
    ("duplicate_or_derived", r"дубл|повторя\w*\s+(замечани|f-\d)|следстви\w*[^.]{0,60}f-\d|см\.?\s*f-\d"
                             r"|аналогичн\w+\s+замечани|уже\s+(учтено|указано|отражено)\s+в\s+(замечани|f-)"
                             r"|в\s+рамках\s+f-\d"),
    ("recognition_error", r"артефакт|распозна|\bocr\b|не\s*так\s+прочит|неверно\s+(прочит|считан|распознан)"
                          r"|прочитан\w*\s+неверно|ошибк\w+\s+(чтения|распозн)"),
    ("out_of_scope", r"не\s+относится\s+к|вне\s+(област|состав|рамок)|другого\s+раздела|смежн\w+\s+раздел"
                     r"|не\s+входит\s+в\s+(состав|объ[её]м)|не\s+предмет|в\s+рамках\s+(другого|смежного)"
                     r"|другого\s+комплекта"),
    ("info_elsewhere", r"(указан|приведен|показан|отражен|предусмотрен|имеется|есть|дан)\w*\s+(на|в)\s+"
                       r"(лист|л\.|друг|смежн|раздел|том|книг|специфик|ведомост|пояснит|общих\s+данн|экспликац"
                       r"|узл|разрез|фасад|план)|закрыто\s+в\s+другом|в\s+другом\s+(документ|раздел|том|комплект)"),
    ("norm_inapplicable", r"норм\w*\s+не\s+(применим|распростран|обязател|регламентир)|не\s+применяется"
                          r"|утратил\w*\s+сил|недействующ|добровольн|нет\s+(такого\s+)?требовани"
                          r"|требовани\w+\s+(нет|отсутству)|не\s+нормируется|пункт\w*\s+(не\s+существует|не\s+относится)"),
    ("customer_or_design_decision", r"заказчик|техническ\w+\s+задани|\bтз\b|согласован"
                                    r"|принят\w*\s+(проектн\w+\s+)?решени|по\s+решению|дизайн-проект"),
    ("insignificant", r"оформит|несуществ|не\s+влияет|практическ\w+\s+влияни|незначит"
                      r"|мелк\w+\s+(дефект|деффект|недочет|недочёт|неточн)|опечатк|не\s+несу\w*\s+(риск|последств)"),
    ("not_a_problem", r"(такой\s+)?проблемы\s+(нет|не\s+существует)|нет\s+(такой\s+)?проблемы"
                      r"|расхождени\w+\s+(нет|отсутству)|противоречи\w+\s+(нет|отсутству)"
                      r"|не\s+соответствует\s+действительност|неверн\w+\s+(вывод|утвержд|фактическ)|ошибочн"
                      r"|соответствует\s+(норм|требован|гост|сп)|указан\w*\s+верно|корректн"
                      r"|выполнен\w*\s+(верно|правильно)|не\s+является\s+(ошибк|нарушени)"),
]
_CAUSE_RX = [(name, re.compile(rx, re.IGNORECASE)) for name, rx in CAUSES]
_CITES = re.compile(r"(стр\.?|лист)\s*\d", re.IGNORECASE)


def causes_of(reason: str) -> list[str]:
    return [name for name, rx in _CAUSE_RX if rx.search(reason or "")]


def primary_cause(reason: str) -> str:
    found = causes_of(reason)
    return found[0] if found else "unclassified"


def is_replayable(obs: dict) -> bool:
    inputs = obs.get("inputs") or {}
    return (not obs["object"].startswith(NON_REPLAYABLE_OBJECTS)
            and bool(inputs.get("has_pdf")) and int(inputs.get("crops_present") or 0) > 0)


def unit_key(obs: dict) -> str:
    """Независимая единица: пачка (если есть), иначе группа зависимости."""
    return obs.get("batch_unit") or obs["group_id"]


def _pct(a: int, b: int) -> str:
    return f"{(100.0 * a / b):.1f} %" if b else "—"


# ─── 0.2: материал для калибровки сопоставителя ───────────────────────────

def matcher_pairs(objects_root: Path, versions: list[dict]) -> list[dict]:
    pairs = []
    for v in versions:
        variants = {fv.label: fv.items for fv in L.findings_variants(objects_root / v["path"])}
        runs = [lbl for lbl in variants if lbl.startswith("run:") and lbl.endswith(":03")]
        for a, b in zip(runs, runs[1:]):
            pairs.append({"kind": "run_run", "version": f"{v['object']}/{v['document']}/{v['version']}",
                          "left": a, "right": b, "left_items": variants[a], "right_items": variants[b]})
        for lbl in variants:
            if lbl.startswith("bak:") and "latest:03" in variants:
                pairs.append({"kind": "bak_latest", "version": f"{v['object']}/{v['document']}/{v['version']}",
                              "left": lbl, "right": "latest:03", "left_items": variants[lbl],
                              "right_items": variants["latest:03"]})
    return pairs


def run_matcher(pairs: list[dict]) -> tuple[list[dict], list[dict]]:
    summaries, cases = [], []
    for p in pairs:
        res = match_findings(p["left_items"].values(), p["right_items"].values())
        summaries.append({k: p[k] for k in ("kind", "version", "left", "right")} | {"counts": res["counts"]})
        for link in res["links"]:
            cases.append({"kind": p["kind"], "version": p["version"], "status": link["status"],
                          "left": p["left_items"][link["left"]], "right": p["right_items"][link["right"]],
                          "score": link["score"]})
        for un in res["left_unresolved"]:
            cases.append({"kind": p["kind"], "version": p["version"], "status": un["status"],
                          "left": p["left_items"][un["left"]], "right_pool": p["right_items"],
                          "reason": un["reason"]})
    return summaries, cases


def review_sample(cases: list[dict], *, per_status: int, seed: int = 20261020) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for status in (STATUS_EXACT, STATUS_FUZZY, STATUS_AMBIGUOUS, STATUS_UNMATCHED):
        pool = [c for c in cases if c["status"] == status]
        rng.shuffle(pool)
        for c in pool[:per_status]:
            left = c["left"]
            row = {"status": status, "kind": c["kind"], "version": c["version"],
                   "left": {"id": left.get("id"), **L.finding_snapshot(left)}, "label": None, "note": ""}
            if "right" in c:
                right = c["right"]
                row["right"] = {"id": right.get("id"), **L.finding_snapshot(right)}
                row["features"] = pair_features(left, right)
            else:
                row["nearest"] = [
                    {**cand, "snapshot": L.finding_snapshot(c["right_pool"][cand["id"]])}
                    for cand in review_candidates(left, c["right_pool"])
                ]
            out.append(row)
    return out


# ─── отчёт ─────────────────────────────────────────────────────────────────

def build_report(ledger: L.Ledger, pair_summaries: list[dict], cases: list[dict]) -> dict:
    obs = ledger.observations
    rep: dict = {"observations": len(obs), "versions": len(ledger.versions)}
    rep["decision"] = Counter(o["decision"] or "(пусто)" for o in obs)
    rep["provenance"] = Counter(o["provenance"] for o in obs)
    rep["link"] = Counter(o["link"] for o in obs)
    rep["provenance_x_link"] = Counter(f"{o['provenance']} | {o['link']}" for o in obs)
    rep["log_checked"] = sum(1 for o in obs if o["log_checked"])
    rep["by_object"] = Counter(o["object"] for o in obs)

    def usable(o: dict) -> bool:
        return (o["provenance"] == L.HUMAN and o["decision"] in ("accepted", "rejected")
                and o["link"] in L.RELIABLE_LINKS)

    use = [o for o in obs if usable(o)]
    rep["usable"] = len(use)
    rep["usable_by_link"] = Counter(o["link"] for o in use)
    rep["usable_by_decision"] = Counter(o["decision"] for o in use)
    rep["usable_replayable"] = Counter(o["decision"] for o in use if is_replayable(o))
    rep["usable_units"] = len({unit_key(o) for o in use})
    rep["usable_groups"] = len({o["group_id"] for o in use})
    rep["usable_families"] = len({o["family"] for o in use})
    rep["usable_families_replayable"] = len({o["family"] for o in use if is_replayable(o)})
    rep["usable_by_object_replayable"] = Counter(o["object"] for o in use if is_replayable(o))
    rep["flags_in_usable"] = Counter(f for o in use for f in o["flags"])
    rep["batched_in_usable"] = sum(1 for o in use if o.get("batch_unit"))
    rep["significant_accepted_usable"] = sum(
        1 for o in use if o["decision"] == "accepted" and (o["finding"] or {}).get("severity") in SIGNIFICANT)

    # повторные решения внутри групп зависимости
    groups: dict[str, list[dict]] = defaultdict(list)
    for o in obs:
        if o["provenance"] == L.HUMAN and o["decision"] in ("accepted", "rejected"):
            groups[o["group_id"]].append(o)
    multi = [g for g in groups.values() if len({x["version"] + x["document"] for x in g}) > 1]
    rep["dependency_groups_multi"] = len(multi)
    rep["dependency_groups_multi_obs"] = sum(len(g) for g in multi)
    rep["dependency_groups_agree"] = sum(1 for g in multi if len({x["decision"] for x in g}) == 1)

    # причины отклонения в достоверной части
    rej = [o for o in use if o["decision"] == "rejected"]
    acc = [o for o in use if o["decision"] == "accepted"]
    for o in use:
        o["_cause"] = primary_cause(o["reason"]) if o["decision"] == "rejected" else None
    rep["rejected_cause_obs"] = Counter(o["_cause"] for o in rej)
    rep["rejected_cause_units"] = {c: len({unit_key(o) for o in rej if o["_cause"] == c})
                                   for c in rep["rejected_cause_obs"]}
    rep["rejected_cites_document"] = sum(1 for o in rej if _CITES.search(o["reason"] or ""))

    def origin_stats(rows: list[dict]) -> Counter:
        return Counter(o["origin"] or "?" for o in rows)

    rep["origin_rejected"] = origin_stats(rej)
    rep["origin_accepted"] = origin_stats(acc)

    clusters = defaultdict(list)
    for o in rej:
        clusters[(o["_cause"], o["origin"] or "?")].append(o)
    rows = []
    for (cause, origin), items in clusters.items():
        cat_counter = Counter((o["finding"] or {}).get("category") or "?" for o in items)
        top_cats = [c for c, _ in cat_counter.most_common(3)]
        counter_acc = [o for o in acc if (o["origin"] or "?") == origin
                       and ((o["finding"] or {}).get("category") or "?") in top_cats]
        replay = [o for o in items if is_replayable(o)]
        rows.append({
            "cause": cause, "origin": origin, "obs": len(items),
            "units": len({unit_key(o) for o in items}),
            "families": len({o["family"] for o in items}),
            "objects": sorted({o["object"][:3] for o in items}),
            "replayable_units": len({unit_key(o) for o in replay}),
            "replayable_families": len({o["family"] for o in replay}),
            "replayable_objects": sorted({o["object"][:3] for o in replay}),
            "top_categories": cat_counter.most_common(3),
            "severity": Counter((o["finding"] or {}).get("severity") or "?" for o in items).most_common(3),
            "counter_accepted_units": len({unit_key(o) for o in counter_acc}),
            "counter_accepted_significant": sum(
                1 for o in counter_acc if (o["finding"] or {}).get("severity") in SIGNIFICANT),
            "examples": [{"obs_id": o["obs_id"], "problem": (o["finding"] or {}).get("problem", "")[:200],
                          "reason": (o["reason"] or "")[:240]} for o in items[:4]],
        })
    rows.sort(key=lambda r: -r["units"])
    rep["clusters"] = rows[:15]

    # сопоставитель
    agg = defaultdict(Counter)
    for s in pair_summaries:
        agg[s["kind"]].update(s["counts"])
        agg[s["kind"]]["pairs"] += 1
    fam = Counter()
    for m in ledger.family_matches:
        fam.update(m["counts"])
        fam["pairs"] += 1
    agg["version_version"] = fam
    rep["matcher"] = {k: dict(v) for k, v in agg.items()}
    rep["matcher_cases"] = Counter(c["status"] for c in cases)
    for k, v in list(rep.items()):
        if isinstance(v, Counter):
            rep[k] = dict(v.most_common())
    return rep


def write_markdown(rep: dict, path: Path) -> None:
    lines = [f"# Этапы 0.1–0.2 — отчёт ({rep['discipline']})", "",
             f"Наблюдений: {rep['observations']}, версий: {rep['versions']}.", ""]
    for key in ("decision", "provenance", "link", "provenance_x_link", "usable_by_link",
                "usable_by_decision", "usable_replayable", "usable_by_object_replayable",
                "flags_in_usable", "rejected_cause_obs", "rejected_cause_units",
                "origin_rejected", "origin_accepted", "matcher", "matcher_cases"):
        lines += [f"## {key}", "", "```", json.dumps(rep.get(key), ensure_ascii=False, indent=1), "```", ""]
    for key in ("usable", "usable_units", "usable_groups", "usable_families", "usable_families_replayable",
                "batched_in_usable", "significant_accepted_usable", "dependency_groups_multi",
                "dependency_groups_multi_obs", "dependency_groups_agree", "rejected_cites_document", "log_checked"):
        lines.append(f"- {key}: {rep.get(key)}")
    lines += ["", "## clusters", ""]
    for r in rep["clusters"]:
        lines.append(f"- **{r['cause']} / {r['origin']}** — наблюдений {r['obs']}, единиц {r['units']}, "
                     f"семейств {r['families']}, объекты {r['objects']}; воспроизводимо: единиц "
                     f"{r['replayable_units']}, семейств {r['replayable_families']}, объекты {r['replayable_objects']}; "
                     f"категории {r['top_categories']}; контрпримеры (принятые той же категории и происхождения): "
                     f"{r['counter_accepted_units']}, из них существенных {r['counter_accepted_significant']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--discipline", default="AR")
    ap.add_argument("--objects-root", type=Path, required=True)
    ap.add_argument("--decisions-log", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--no-hash", action="store_true", help="не считать sha256 входов (быстрее)")
    ap.add_argument("--sample-per-status", type=int, default=25)
    args = ap.parse_args(argv)

    objects_root = args.objects_root.resolve()
    out = args.out.resolve()
    for guarded in (objects_root, args.decisions_log.resolve().parent):
        if out == guarded or guarded in out.parents:
            ap.error(f"--out не может лежать внутри {guarded}: модуль только читает боевые данные")
    out.mkdir(parents=True, exist_ok=True)

    log_index = L.LogIndex.from_file(args.decisions_log)
    ledger = L.build_ledger(objects_root, args.discipline, log_index, hash_inputs=not args.no_hash)
    pairs = matcher_pairs(objects_root, ledger.versions)
    pair_summaries, cases = run_matcher(pairs)
    rep = build_report(ledger, pair_summaries, cases)
    rep["discipline"] = args.discipline

    with (out / "ledger.jsonl").open("w", encoding="utf-8") as fh:
        for o in ledger.observations:
            fh.write(json.dumps({k: v for k, v in o.items() if not k.startswith("_")}, ensure_ascii=False) + "\n")
    (out / "versions.json").write_text(json.dumps(ledger.versions, ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "matcher_pairs.json").write_text(
        json.dumps({"run_and_bak": pair_summaries, "version_version": ledger.family_matches},
                   ensure_ascii=False, indent=1), encoding="utf-8")
    (out / "review_sample.json").write_text(
        json.dumps(review_sample(cases, per_status=args.sample_per_status), ensure_ascii=False, indent=1),
        encoding="utf-8")
    (out / "report.json").write_text(json.dumps(rep, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    write_markdown(rep, out / "report.md")
    print(f"наблюдений: {rep['observations']}, пригодных: {rep['usable']}, отчёт: {out / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
