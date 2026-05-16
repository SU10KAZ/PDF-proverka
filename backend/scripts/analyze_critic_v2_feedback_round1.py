"""
Анализ ручной проверки Critic v2 (раунд 1).

Что делает:
  - читает все *.json в --feedback-dir (по умолчанию: critic v2 test/);
  - джойнит каждый feedback item с critic_v2_triage_ui.json по
    (project_name, finding_id);
  - обогащает признаками из UI export (queue, reason, taxonomy_reason,
    evidence_quality, score, source_dependency, risk_level, human_decision,
    human_reason, title, description, recommendation);
  - строит confusion-матрицу original_tab → preferred_tab;
  - запускает keyword clustering по reviewer_note (8 фиксированных групп);
  - для каждого ключевого направления переноса (primary → suggested_reject и
    т.д.) считает распределения признаков (score bucket, evidence, taxonomy);
  - пишет 3 артефакта в /tmp:
      critic_v2_feedback_round1_analysis.json
      critic_v2_feedback_round1_analysis.md
      critic_v2_feedback_round1_enriched.csv

Запрещено:
  - не запускает LLM;
  - не пишет в _output проектов;
  - не меняет production pipeline;
  - не модифицирует critic_v2 logic.

Запуск:
  python backend/scripts/analyze_critic_v2_feedback_round1.py \
    [--feedback-dir "critic v2 test"] \
    [--ui-export /tmp/.../critic_v2_triage_ui.json] \
    [--out-json /tmp/critic_v2_feedback_round1_analysis.json] \
    [--out-md   /tmp/critic_v2_feedback_round1_analysis.md] \
    [--out-csv  /tmp/critic_v2_feedback_round1_enriched.csv]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

DEFAULT_FEEDBACK_DIR = Path("critic v2 test")
DEFAULT_UI_EXPORT = Path(
    "/tmp/critic_v2_ui_export_for_manual_review/llm_no_context/critic_v2_triage_ui.json"
)
DEFAULT_OUT_JSON = Path("/tmp/critic_v2_feedback_round1_analysis.json")
DEFAULT_OUT_MD = Path("/tmp/critic_v2_feedback_round1_analysis.md")
DEFAULT_OUT_CSV = Path("/tmp/critic_v2_feedback_round1_enriched.csv")

KEYWORD_CLUSTERS: dict[str, list[str]] = {
    "ocr_artifact": [
        "ocr", "мусор", "неверное определение", "обозначение", "распознавание",
        "артефакт",
    ],
    "auxiliary_scheme": [
        "вспомогательная схема", "схема", "условное обозначение", "вспомог",
    ],
    "rd_vs_pz_calculation": [
        "пз", "расчёт", "расчет", "расчётный параметр", "расчетный параметр",
        "огнестойк", "rei", "экспертиз", "нормативная база", "обоснован",
    ],
    "already_in_adjacent_section": [
        "смежн", "дублирован", "присутствует в раздел",
    ],
    "already_in_drawing_or_spec": [
        "присутств", "указано", "есть схема", "спецификац", "ведомост",
        "имеется", "в марке",
    ],
    "not_required_or_optional": [
        "не требуется", "не обязательно", "не влечёт", "не влечет", "допустимо",
        "не существенно",
    ],
    "false_positive": [
        "не является замечанием", "не дефект", "отсутствует нарушение",
        "ложн", "не нарушение",
    ],
    "should_be_primary": [
        "влияет", "важно", "оставить", "основная проверка", "критич",
        "проверить эксперт",
    ],
}


def classify_note(note):
    if not note:
        return []
    s = note.lower()
    return [c for c, kws in KEYWORD_CLUSTERS.items() if any(k in s for k in kws)]


def score_bucket(score):
    if score is None or score == "":
        return "none"
    try:
        s = int(score)
    except (TypeError, ValueError):
        return "none"
    if s >= 10: return "10-11"
    if s >= 8:  return "8-9"
    if s >= 6:  return "6-7"
    if s >= 4:  return "4-5"
    return "0-3"


def load_feedback_files(feedback_dir):
    rows = []
    if not feedback_dir.exists():
        return rows
    for f in sorted(feedback_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        scope = d.get("scope") or {}
        for item in (d.get("feedback") or []):
            rows.append({
                "_source_file": f.name,
                "_scope_mode": scope.get("mode"),
                "_scope_project": scope.get("project_name") or scope.get("project_id"),
                **item,
            })
    return rows


def load_ui_export_index(path):
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    idx = {}
    for it in data.get("items") or []:
        key = (it.get("project_name") or "", it.get("finding_id") or "")
        idx[key] = it
    return idx


ENRICH_FIELDS = (
    "queue", "reason", "taxonomy_reason", "evidence_quality", "score",
    "source_dependency", "risk_level", "human_decision", "human_reason",
    "title", "description", "recommendation",
)


def enrich(feedback_rows, ui_index):
    """
    Join feedback with UI export by (project_name, finding_id).

    Fallback: если item.project_name пустой (старая версия экспорта), берём
    scope.project_name. Дополнительно finding_id уже содержит project в
    префиксе вида "<project>:F-NNN", это даёт второй fallback.
    """
    enriched = []
    for r in feedback_rows:
        pn = r.get("project_name") or r.get("_scope_project") or ""
        fid = r.get("finding_id") or ""
        # Try direct key
        ui = ui_index.get((pn, fid))
        # Fallback: take prefix from finding_id
        if not ui and ":" in fid:
            prefix = fid.rsplit(":", 1)[0]
            ui = ui_index.get((prefix, fid))
            if ui:
                pn = prefix
        ui = ui or {}
        merged = dict(r)
        # Always backfill project_name if empty
        if not merged.get("project_name"):
            merged["project_name"] = pn
        # Backfill section from UI (was empty for legacy exports)
        if not merged.get("section") and ui.get("section"):
            merged["section"] = ui["section"]
        for f in ENRICH_FIELDS:
            merged.setdefault(f, ui.get(f))
        if not merged.get("queue"):
            merged["queue"] = merged.get("original_queue")
        merged["_score_bucket"] = score_bucket(merged.get("score"))
        merged["_note_clusters"] = classify_note(merged.get("reviewer_note"))
        ot = r.get("original_tab")
        pt = r.get("preferred_tab")
        merged["_direction"] = f"{ot} → {pt}" if (pt and pt != ot) else None
        merged["_in_ui_export"] = bool(ui)
        enriched.append(merged)
    return enriched


KEY_DIRECTIONS = [
    ("primary", "suggested_reject"),
    ("primary", "hidden_by_critic"),
    ("primary", "needs_context"),
    ("needs_context", "primary"),
    ("needs_context", "suggested_reject"),
    ("needs_context", "hidden_by_critic"),
]

FEATURE_FIELDS = (
    "_score_bucket", "evidence_quality", "taxonomy_reason",
    "source_dependency", "reason", "risk_level", "section",
    "human_decision",
)


def distribution(rows, field, top_n=6):
    c = Counter(str(r.get(field) or "(none)") for r in rows)
    return c.most_common(top_n)


def analyze(enriched):
    total = len(enriched)
    by_section = defaultdict(list)
    for r in enriched:
        by_section[r.get("section") or "?"].append(r)

    tc = Counter(r.get("triage_correct") or "(empty)" for r in enriched)

    confusion = Counter()
    for r in enriched:
        ot = r.get("original_tab")
        pt = r.get("preferred_tab")
        if ot and pt and ot != pt:
            confusion[(ot, pt)] += 1

    section_summary = {}
    for sec, rows in by_section.items():
        sec_tc = Counter(r.get("triage_correct") or "(empty)" for r in rows)
        section_summary[sec] = {
            "items": len(rows),
            "yes": sec_tc.get("yes", 0),
            "no": sec_tc.get("no", 0),
            "unsure": sec_tc.get("unsure", 0),
            "empty": sec_tc.get("(empty)", 0),
        }

    priority = Counter(r.get("priority") or "normal" for r in enriched)
    critical_items = [r for r in enriched if r.get("priority") == "critical"]

    cluster_total = Counter()
    cluster_no_only = Counter()
    for r in enriched:
        for cl in r["_note_clusters"]:
            cluster_total[cl] += 1
            if r.get("triage_correct") == "no":
                cluster_no_only[cl] += 1

    direction_analysis = {}
    for (ot, pt) in KEY_DIRECTIONS:
        slice_ = [r for r in enriched
                  if r.get("original_tab") == ot and r.get("preferred_tab") == pt]
        if not slice_:
            continue
        direction_analysis[f"{ot} → {pt}"] = {
            "count": len(slice_),
            "by_section": Counter(r.get("section") or "?" for r in slice_).most_common(),
            "features": {f: distribution(slice_, f) for f in FEATURE_FIELDS},
            "clusters": Counter(
                c for r in slice_ for c in r["_note_clusters"]
            ).most_common(),
            "sample_notes": [
                (r.get("finding_id"), (r.get("reviewer_note") or "").strip()[:200])
                for r in slice_ if (r.get("reviewer_note") or "").strip()
            ][:3],
        }

    no_rows = [r for r in enriched if r.get("triage_correct") == "no"]
    no_features = {f: distribution(no_rows, f, top_n=10) for f in FEATURE_FIELDS}

    critical_features = {f: distribution(critical_items, f, top_n=6)
                         for f in FEATURE_FIELDS}

    STOP = {
        "и", "в", "на", "не", "что", "это", "то", "по", "к", "с", "из", "у",
        "за", "от", "до", "для", "при", "о", "об", "так", "как", "же", "а",
        "the", "is", "of", "to", "in",
    }
    word_counter = Counter()
    for r in enriched:
        note = (r.get("reviewer_note") or "").lower()
        for w in re.findall(r"[a-zа-я0-9-]{3,}", note):
            if w not in STOP:
                word_counter[w] += 1
    top_words = word_counter.most_common(25)

    join_stats = {
        "feedback_total": total,
        "matched_in_ui_export": sum(1 for r in enriched if r["_in_ui_export"]),
        "missing_in_ui_export": sum(1 for r in enriched if not r["_in_ui_export"]),
    }

    return {
        "feedback_total": total,
        "triage_breakdown": dict(tc),
        "priority_breakdown": dict(priority),
        "section_summary": section_summary,
        "confusion_matrix": [
            {"from": o, "to": p, "count": n}
            for (o, p), n in confusion.most_common()
        ],
        "direction_analysis": direction_analysis,
        "no_only_feature_distribution": no_features,
        "critical_features": critical_features,
        "critical_items_count": len(critical_items),
        "keyword_cluster_totals": dict(cluster_total),
        "keyword_cluster_no_only": dict(cluster_no_only),
        "top_reviewer_note_words": top_words,
        "join_stats": join_stats,
    }


def write_csv(enriched, path):
    fieldnames = [
        "_source_file", "project_name", "section", "finding_id",
        "original_tab", "preferred_tab", "triage_correct", "priority",
        "_direction", "queue", "reason", "taxonomy_reason",
        "evidence_quality", "score", "_score_bucket", "source_dependency",
        "risk_level", "human_decision", "_note_clusters",
        "reviewer_note", "title",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in enriched:
            row = {k: r.get(k, "") for k in fieldnames}
            row["_note_clusters"] = ";".join(r.get("_note_clusters") or [])
            w.writerow(row)


def fmt_dist(d):
    return ", ".join(f"`{k}`={v}" for k, v in d)


def write_markdown(report, path):
    out = []
    out.append("# Critic v2 — Round 1 Feedback Analysis")
    out.append("")
    js = report["join_stats"]
    out.append(f"- feedback items: **{js['feedback_total']}**")
    out.append(f"- сматчилось с UI export: **{js['matched_in_ui_export']}**")
    out.append(f"- не сматчилось: **{js['missing_in_ui_export']}**")
    out.append("")
    out.append("## Triage breakdown")
    out.append("")
    for k, v in sorted(report["triage_breakdown"].items(), key=lambda x: -x[1]):
        out.append(f"- `{k}`: {v}")
    out.append("")
    out.append("## По разделам")
    out.append("")
    out.append("| section | items | yes | no | unsure | empty |")
    out.append("|---|---:|---:|---:|---:|---:|")
    for sec, s in sorted(report["section_summary"].items()):
        out.append(f"| {sec} | {s['items']} | {s['yes']} | {s['no']} | "
                   f"{s['unsure']} | {s['empty']} |")
    out.append("")
    out.append("## Confusion matrix (original_tab → preferred_tab)")
    out.append("")
    out.append("| from | to | count |")
    out.append("|---|---|---:|")
    for row in report["confusion_matrix"]:
        out.append(f"| {row['from']} | {row['to']} | {row['count']} |")
    out.append("")
    out.append("## Direction analysis (топ-направления)")
    out.append("")
    for direction, info in report["direction_analysis"].items():
        out.append(f"### {direction} — {info['count']}")
        out.append("")
        out.append("По разделам: " + ", ".join(
            f"`{s}`={n}" for s, n in info["by_section"]))
        out.append("")
        for fname, dist in info["features"].items():
            out.append(f"- **{fname}**: {fmt_dist(dist)}")
        out.append("")
        if info["clusters"]:
            out.append("Keyword clusters: " + ", ".join(
                f"`{c}`={n}" for c, n in info["clusters"]))
            out.append("")
        if info["sample_notes"]:
            out.append("Примеры комментариев:")
            for fid, note in info["sample_notes"]:
                out.append(f"  - [{fid}] {note}")
            out.append("")
    out.append("## NO-only — топ-распределения признаков")
    out.append("")
    for fname, dist in report["no_only_feature_distribution"].items():
        out.append(f"- **{fname}**: {fmt_dist(dist)}")
    out.append("")
    out.append(f"## Critical priority — {report['critical_items_count']} карточек")
    out.append("")
    for fname, dist in report["critical_features"].items():
        out.append(f"- **{fname}**: {fmt_dist(dist)}")
    out.append("")
    out.append("## Keyword clusters в reviewer_note")
    out.append("")
    out.append("| cluster | total | among NO |")
    out.append("|---|---:|---:|")
    no_clusters = report["keyword_cluster_no_only"]
    for cl, n in sorted(report["keyword_cluster_totals"].items(), key=lambda x: -x[1]):
        out.append(f"| {cl} | {n} | {no_clusters.get(cl, 0)} |")
    out.append("")
    out.append("## Топ слов в комментариях ревьюеров")
    out.append("")
    out.append(", ".join(f"`{w}`({n})" for w, n in report["top_reviewer_note_words"]))
    out.append("")
    path.write_text("\n".join(out), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feedback-dir", default=str(DEFAULT_FEEDBACK_DIR))
    ap.add_argument("--ui-export", default=str(DEFAULT_UI_EXPORT))
    ap.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    feedback_dir = Path(args.feedback_dir)
    ui_export = Path(args.ui_export)
    out_json = Path(args.out_json)
    out_md = Path(args.out_md)
    out_csv = Path(args.out_csv)

    feedback = load_feedback_files(feedback_dir)
    ui_index = load_ui_export_index(ui_export)
    enriched = enrich(feedback, ui_index)
    report = analyze(enriched)

    out_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_markdown(report, out_md)
    write_csv(enriched, out_csv)

    if not args.quiet:
        js = report["join_stats"]
        n_files = len({r["_source_file"] for r in enriched}) if enriched else 0
        print(f"feedback files: {n_files}")
        print(f"items: {js['feedback_total']}  "
              f"matched={js['matched_in_ui_export']}  "
              f"unmatched={js['missing_in_ui_export']}")
        print(f"wrote: {out_json}")
        print(f"wrote: {out_md}")
        print(f"wrote: {out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
