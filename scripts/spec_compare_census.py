"""Контрольный набор полного сравнения СО: прогон этапа на сохранённых ответах Луны, без обращений к модели.

Каждый раздел (``<VERSION_COMPARISON_ROOT>/objects/<объект>/whole_specifications/<проект>/<подпись>``)
копируется в рабочую папку, и копия проходит через настоящий ``specification_run.run``: проверка
ответов, автоисправление, перенос разметки между версиями, согласование соседних версий, сверка
значений с PDF, сравнение переходов. Исполнитель офлайн: сохранённые ответы (в том числе частей
и окон согласования) берутся из кэша, любой новый запрос к модели сразу получает отказ ``OFFLINE``.
Боевые файлы не меняются. Пометки об отказе (``rejected.json``) в копии снимаются: проверяется
текущий код, а не решение прошлого прогона.

Метрики (docs/whole_specification_v2_plan.md, этап 1):
- версии: полные / частичные (часть групп не разобрана) / отказ / нет ответа / не отправлялись;
- доля разобранных строк данных таблиц и потерянные строки (ни в позиции, ни в «не разобрано»);
- переходы: сравнены; ложные расхождения — на паре, где дословно совпадают ≥ 95 % строк,
  «добавлено + удалено» больше числа различающихся строк;
- обращения, которые понадобились бы модели (офлайн-отказы).

    python scripts/spec_compare_census.py --json after.json [--only ОВ3]
    python scripts/spec_compare_census.py --diff before.json after.json
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = "/srv/auditmanager/data/version_comparison"
NOISE = ("Markdown table data line is not extracted", "Extraction block ids", "Unclassified source lines",
         "Markdown table cell is missing")
DROP = ("rejected.json", "checked.json", "reconciled.json", "final.json", "pdf_values.json", "strict_autofix.json")


def _reason(issue: str) -> str:
    return re.sub(r"(blk_\w+: )|\d+|\{.*\}|\[.*\]", "", str(issue)).strip(" :;")[:70]


def _texts(src: dict) -> collections.Counter:
    return collections.Counter(ln["text"].strip() for b in src["blocks"] for ln in b["lines"] if ln["text"].strip())


class Offline:
    """Исполнитель без модели: тот же runtime, что в сохранённом прогоне (ключи кэша совпадают)."""
    executor_id = "offline"

    def __init__(self, runtime: dict):
        self.runtime = runtime
        self.model = runtime.get("model")
        self.calls = 0
        self.markers: list[str] = []

    def describe_runtime(self) -> dict:
        return dict(self.runtime)

    def validate_runtime(self) -> dict:
        return {"ok": True}

    def run(self, task, cancel):
        from backend.app.services.version_comparison_ai.executor import DescribeResult
        self.calls += 1
        self.markers.append(task.marker)
        return DescribeResult(False, error_kind="OFFLINE", error="контрольный набор: обращение к модели не выполняется")


def projects(root: str) -> list[tuple[str, Path]]:
    out = []
    for obj in sorted(glob.glob(f"{root}/objects/*/")):
        for proj in sorted(glob.glob(f"{obj}whole_specifications/*/")):
            found = sorted(glob.glob(f"{proj}*/run/plan.json"), key=os.path.getmtime)
            if found:
                out.append((Path(obj).name, Path(found[-1]).parent.parent))
    return out


def census_one(object_id: str, shared: Path, work: Path) -> dict:
    from backend.app.services.version_comparison import paths, specification_run as run_mod
    from backend.app.services.version_comparison import spec as engine_spec, specification_structure as st

    copy = work / shared.parent.name / shared.name
    shutil.copytree(shared, copy, ignore=shutil.ignore_patterns(*DROP, ".run.lock", ".worker.lock", "*.tmp"))
    plan = json.loads((copy / "run" / "plan.json").read_text(encoding="utf-8"))
    title = plan["tasks"][0]["document"] if plan["tasks"] else shared.parent.name
    prepared = run_mod.prepare(copy / "input" / "manifest.json", structure=True)
    old_tasks = plan["tasks"]

    def once():
        offline = Offline(plan["settings"]["runtime"])
        res = run_mod.run(prepared, copy / "run", execute=True, parallel=8, executor=offline, break_after=0,
                          pdf_values=lambda sha: engine_spec.load(str(paths.profile_dir(object_id, sha))) if sha else None)
        return offline, res, json.loads((copy / "run" / "plan.json").read_text(encoding="utf-8"))

    offline, result, plan = once()
    # Промпт/схема менялись после сохранённого прогона — ключи заданий другие. Ответы прежних ключей
    # (та же версия, тот же порядок заданий) переносятся в новые папки, и прогон повторяется.
    moved = 0
    if len(old_tasks) == len(plan["tasks"]):
        for old, new in zip(old_tasks, plan["tasks"]):
            src_dir, dst_dir = copy / "run" / "calls" / old["key"], copy / "run" / "calls" / new["key"]
            if old["key"] == new["key"] or old["label"] != new["label"] or not (src_dir / "answer.json").exists():
                continue
            shutil.copy2(src_dir / "answer.json", dst_dir / "answer.json")
            if (src_dir / "parts").exists():
                shutil.copytree(src_dir / "parts", dst_dir / "parts", dirs_exist_ok=True)
            for name in DROP:
                (dst_dir / name).unlink(missing_ok=True)
            moved += 1
    if moved:
        offline, result, plan = once()
    keys = {t["key"]: t for t in plan["tasks"]}
    versions, by_label = [], {}
    for key, task in keys.items():
        folder = copy / "run" / "calls" / key
        final = json.loads((folder / "final.json").read_text(encoding="utf-8")) if (folder / "final.json").exists() else None
        src = json.loads((folder / "source.json").read_text(encoding="utf-8")) if (folder / "source.json").exists() else None
        item = {"label": task["label"]}
        if final is None or src is None or not task["eligible"]:
            item["status"] = "not_sent"
        elif not (folder / "answer.json").exists():
            item["status"] = "no_answer"
        else:
            rows = final.get("rows") or []
            item["status"] = "complete" if final["complete"] else ("partial" if rows else "failed")
        if src is not None and final is not None:
            _, _, cells, _, _, _ = st._source_lines(src)
            data = {n for n, c in cells.items() if any(v.strip() for v in c.values())}
            cited = {ref["line_start"] for row in final.get("rows") or [] for ref in row.get("sources", [])}
            unresolved = {n for g in final.get("unresolved_groups") or [] for n in g.get("lines", [])}
            lost = sorted(data - cited - unresolved) if final.get("rows") else []
            item |= {"rows": len(final.get("rows") or []), "data_lines": len(data),
                     "resolved_lines": len(data & cited), "lost_lines": len(lost), "lost_sample": lost[:5],
                     "unresolved_groups": len(final.get("unresolved_groups") or []),
                     "needs_model": any("OFFLINE" in str(i) for i in final.get("issues") or []),
                     "reasons": [r for r, _ in collections.Counter(
                         _reason(i) for i in final.get("issues") or [] if not str(i).startswith(NOISE)).most_common(4)]}
        versions.append(item)
        by_label[task["label"]] = src
    pairs = []
    for rep in result["reports"]:
        c = rep["comparison"]
        a, b = c["old"]["label"], c["new"]["label"]
        entry = {"a": a, "b": b, "complete": c["complete"],
                 "compared": bool(c["old"].get("complete") or c["old"].get("partial"))
                 and bool(c["new"].get("complete") or c["new"].get("partial")),
                 "counts": {k: v for k, v in c["counts"].items() if v},
                 "not_compared_groups": len(c.get("not_compared") or [])}
        sa, sb = by_label.get(a), by_label.get(b)
        if entry["compared"] and sa and sb:
            ta, tb = _texts(sa), _texts(sb)
            total = sum(tb.values())
            differ = sum((ta - tb).values()) + sum((tb - ta).values())
            same = sum((ta & tb).values()) / total if total else 1.0
            moved = c["counts"]["added"] + c["counts"]["removed"]
            entry |= {"same_share": round(same, 3), "differing_lines": differ,
                      "false_diff": same >= 0.95 and moved > differ}
        pairs.append(entry)
    return {"title": title, "object_id": object_id, "versions": versions, "pairs": pairs, "answers_rekeyed": moved,
            "model_calls_needed": offline.calls, "model_call_markers": offline.markers}


def summarize(result: list[dict]) -> dict:
    s = collections.Counter()
    reasons = collections.Counter()
    for proj in result:
        s["model_calls_needed"] += proj.get("model_calls_needed", 0)
        for v in proj["versions"]:
            s["versions"] += 1
            s[v["status"]] += 1
            for k in ("data_lines", "resolved_lines", "lost_lines", "unresolved_groups"):
                s[k] += v.get(k, 0)
            for r in v.get("reasons", []):
                reasons[r] += 1
        for p in proj["pairs"]:
            s["pairs"] += 1
            s["pairs_compared"] += p["compared"]
            s["pairs_complete"] += bool(p.get("complete"))
            s["false_diff"] += bool(p.get("false_diff"))
            s["false_diff_rows"] += (p["counts"].get("added", 0) + p["counts"].get("removed", 0)) if p.get("false_diff") else 0
    return {"totals": dict(s), "reasons": reasons.most_common(15)}


def print_report(result: list[dict]) -> None:
    for proj in result:
        counts = collections.Counter(v["status"] for v in proj["versions"])
        pairs = proj["pairs"]
        fd = [f"{p['a'][:12]}→{p['b'][:12]} ±{p['counts'].get('added', 0)}/{p['counts'].get('removed', 0)}"
              for p in pairs if p.get("false_diff")]
        print(f"{proj['title'][:26]:28} {dict(counts)}  сравнено {sum(p['compared'] for p in pairs)}/{len(pairs)}"
              f"  к модели {proj['model_calls_needed']}" + (f"  ЛОЖНЫЕ: {fd}" if fd else ""))
    summ = summarize(result)
    t = summ["totals"]
    print("\nИТОГО:", json.dumps(t, ensure_ascii=False))
    if t.get("data_lines"):
        print(f"разобрано строк данных: {100 * t.get('resolved_lines', 0) / t['data_lines']:.1f} %")
    print("причины:")
    for reason, n in summ["reasons"]:
        print(f"  {n:3} {reason}")


def diff(before: str, after: str) -> None:
    a, b = (json.loads(Path(p).read_text(encoding="utf-8")) for p in (before, after))
    ta, tb = summarize(a)["totals"], summarize(b)["totals"]
    for key in sorted(set(ta) | set(tb)):
        if ta.get(key, 0) != tb.get(key, 0):
            print(f"{key:20} {ta.get(key, 0):>8} → {tb.get(key, 0):>8}")
    old = {p["title"]: p for p in a}
    for proj in b:
        prev = old.get(proj["title"])
        if not prev:
            continue
        for vo, vn in zip(prev["versions"], proj["versions"]):
            if vo["status"] != vn["status"]:
                print(f"  {proj['title'][:24]} {vn['label']}: {vo['status']} → {vn['status']}")
        for po, pn in zip(prev["pairs"], proj["pairs"]):
            if po["counts"] != pn["counts"] or po.get("false_diff") != pn.get("false_diff"):
                print(f"  {proj['title'][:24]} {pn['a'][:12]}→{pn['b'][:12]}: {po['counts']} → {pn['counts']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=os.environ.get("VERSION_COMPARISON_ROOT") or DEFAULT_ROOT)
    ap.add_argument("--json", help="записать полный результат в файл")
    ap.add_argument("--only", help="подстрока имени документа или папки проекта")
    ap.add_argument("--work", help="рабочая папка для копий (по умолчанию — временная)")
    ap.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"))
    args = ap.parse_args()
    if args.diff:
        diff(*args.diff)
        return
    # Профили версий (значения из PDF) читаются из того же корня; запись идёт только в копию.
    os.environ["VERSION_COMPARISON_ROOT"] = args.root
    sys.path.insert(0, str(ROOT))
    work = Path(args.work or tempfile.mkdtemp(prefix="spec-census-"))
    result = []
    for object_id, shared in projects(args.root):
        if args.only and args.only not in shared.parent.name:
            plan = json.loads((shared / "run" / "plan.json").read_text(encoding="utf-8"))
            if not plan["tasks"] or args.only not in plan["tasks"][0]["document"]:
                continue
        result.append(census_one(object_id, shared, work))
    print_report(result)
    if args.json:
        Path(args.json).write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
    if not args.work:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
