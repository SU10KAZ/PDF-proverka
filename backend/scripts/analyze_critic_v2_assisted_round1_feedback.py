#!/usr/bin/env python3
"""
analyze_critic_v2_assisted_round1_feedback.py
=============================================

Read-only analyzer of Round 2 manual feedback collected after assisted_round1
was rolled out via the UI.

Inputs (default paths inside "critic v2 test/"):
- assisted_round1_review/critic_v2_triage_ui_assisted_round1.json
- assisted_round1_review/assisted_round1_risky_accepted_22.csv
- assisted_round1_review/assisted_round1_sample_60.csv
- отработка/*.json (round1 originals + round2 corrections)

Outputs (default /tmp):
- /tmp/critic_v2_assisted_round1_feedback_analysis.md
- /tmp/critic_v2_assisted_round1_feedback_analysis.json
- /tmp/critic_v2_assisted_round1_feedback_enriched.csv
- /tmp/critic_v2_assisted_round1_rule_precision.csv
- /tmp/critic_v2_assisted_round1_risky_accepted_review.md

Strictly read-only:
- No LLM calls
- No production writes (does not touch _output/, BD, runner.py, manager.py)
- No network calls
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

# ── Paths ─────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PKG_DIR = REPO_ROOT / "critic v2 test" / "assisted_round1_review"
DEFAULT_FEEDBACK_DIR = REPO_ROOT / "critic v2 test" / "отработка"
DEFAULT_OUT_DIR = Path("/tmp")

# Round1 originals are the 6 timestamps the engineers shipped on 2026-05-13 in
# the morning, before the round2 review package was distributed.
ROUND1_FILENAME_MARKERS = (
    "2026-05-13T07",
    "2026-05-13T08",
    "2026-05-13T09-33",
)

# Reviewer-note keyword clusters (lowercased substrings).
NOTE_CLUSTERS: dict[str, tuple[str, ...]] = {
    "уже_в_смежном_или_спецификации": (
        "смеж", "спецификац", "ведомост", "общих указани", "схема", "в марке",
        "уже учт", "уже указ", "ниже привед",
    ),
    "ошибка_OCR_или_распознавания": (
        "ocr", "распозна", "битый", "артефакт", "сломан", "мусор",
        "неразборчив",
    ),
    "расчётный_параметр_или_ПЗ": (
        "пз", "пояснительн", "расчёт", "расчет", "rei", "огнестойк",
        "экспертиз", "нормативная баз",
    ),
    "не_влияет_формальное": (
        "не влия", "формально", "не критич", "косметич", "минор", "несуществен",
    ),
    "ошибка_критика": (
        "ошибк", "неправильн", "ложн", "false positive", "неверн",
    ),
    "должно_быть_в_основной": (
        "вернуть в основн", "оставить в основн", "нужно в основн", "к экспертизе",
    ),
    "нужно_в_смежник": (
        "проверить по смежн", "нужно в смежник", "контекст",
    ),
}

CSV_FIELDS = [
    "feedback_file",
    "feedback_created_at",
    "section",
    "project_name",
    "finding_id",
    "title",
    "original_tab",
    "preferred_tab",
    "triage_correct",
    "priority",
    "reviewer_note",
    "current_tab",
    "current_queue",
    "reason",
    "taxonomy_reason",
    "evidence_quality",
    "score",
    "source_dependency",
    "human_decision",
    "human_reason",
    "round1_rule",
    "bucket",
    "note_clusters",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def is_round1_filename(name: str) -> bool:
    return any(marker in name for marker in ROUND1_FILENAME_MARKERS)


def round1_rule_from_reason(reason: Optional[str]) -> str:
    if not reason:
        return "other"
    if "round1_ocr_artifact" in reason:
        return "A1_ocr"
    if "round1_rd_vs_pz" in reason:
        return "C_rd_vs_pz"
    if "round1_already_covered" in reason:
        return "D_already_covered"
    return "other"


def classify_note(note: Optional[str]) -> list[str]:
    if not note:
        return []
    blob = note.lower()
    return [name for name, terms in NOTE_CLUSTERS.items()
            if any(t in blob for t in terms)]


def normalize_tc(v: Any) -> str:
    if v is True:
        return "yes"
    if v is False:
        return "no"
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("yes", "true", "1"):
            return "yes"
        if s in ("no", "false", "0"):
            return "no"
        if s in ("unsure", "maybe", "?"):
            return "unsure"
        if s == "":
            return ""
    return ""


# ── Data containers ───────────────────────────────────────────────────────────


@dataclass
class LoadResult:
    feedback_files: list[Path] = field(default_factory=list)
    round1_files: list[Path] = field(default_factory=list)
    round2_files: list[Path] = field(default_factory=list)
    dedup_dropped: list[str] = field(default_factory=list)
    invalid_files: list[tuple[str, str]] = field(default_factory=list)
    feedback_items: list[dict] = field(default_factory=list)  # one per item
    ui_items_by_fid: dict[str, dict] = field(default_factory=dict)
    risky_ids: set[str] = field(default_factory=set)
    sample_buckets: dict[str, str] = field(default_factory=dict)  # fid → bucket


# ── Loading ───────────────────────────────────────────────────────────────────


def load_ui_export(pkg_dir: Path) -> dict[str, dict]:
    p = pkg_dir / "critic_v2_triage_ui_assisted_round1.json"
    if not p.exists():
        raise SystemExit(f"UI export not found: {p}")
    d = json.loads(p.read_text(encoding="utf-8"))
    return {it["finding_id"]: it for it in d.get("items", []) if it.get("finding_id")}


def load_risky_ids(pkg_dir: Path) -> set[str]:
    p = pkg_dir / "assisted_round1_risky_accepted_22.csv"
    ids: set[str] = set()
    if not p.exists():
        return ids
    with p.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fid = row.get("finding_id")
            if fid:
                ids.add(fid)
    return ids


def load_sample_buckets(pkg_dir: Path) -> dict[str, str]:
    p = pkg_dir / "assisted_round1_sample_60.csv"
    out: dict[str, str] = {}
    if not p.exists():
        return out
    with p.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fid = row.get("finding_id")
            bucket = row.get("bucket") or ""
            if fid:
                out[fid] = bucket
    return out


def load_feedback(feedback_dir: Path,
                  include_round1_in_results: bool = False) -> LoadResult:
    res = LoadResult()
    res.feedback_files = sorted([p for p in feedback_dir.glob("*.json") if p.is_file()])

    # Split round1 vs round2 by filename timestamp
    for p in res.feedback_files:
        if is_round1_filename(p.name):
            res.round1_files.append(p)
        else:
            res.round2_files.append(p)

    target_files = res.round2_files + (res.round1_files if include_round1_in_results else [])

    # Behavioral dedup: same project + same set of (finding_id, triage_correct,
    # preferred_tab) hashed gives identical content → keep the earliest file.
    seen_content_hashes: dict[str, str] = {}  # hash → kept filename
    chosen: list[Path] = []
    for p in target_files:
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            res.invalid_files.append((p.name, f"JSON parse error: {e}"))
            continue
        if d.get("export_type") != "critic_v2_triage_feedback":
            res.invalid_files.append(
                (p.name, f"bad export_type: {d.get('export_type')!r}"))
            # still accept the file (some legacy exports omit type)
        scope = d.get("scope") or {}
        proj = scope.get("project_name") or ""
        fb = d.get("feedback") or []
        sig_parts = [proj] + sorted(
            f"{x.get('finding_id')}|{normalize_tc(x.get('triage_correct'))}|{x.get('preferred_tab') or ''}"
            for x in fb
        )
        sig = hashlib.sha1("\n".join(sig_parts).encode("utf-8")).hexdigest()
        if sig in seen_content_hashes:
            res.dedup_dropped.append(f"{p.name} (duplicate of {seen_content_hashes[sig]})")
            continue
        seen_content_hashes[sig] = p.name
        chosen.append(p)

    # Build per-item rows
    for p in chosen:
        d = json.loads(p.read_text(encoding="utf-8"))
        scope = d.get("scope") or {}
        proj = scope.get("project_name") or ""
        created = d.get("created_at") or ""
        for x in (d.get("feedback") or []):
            res.feedback_items.append({
                "_feedback_file": p.name,
                "_feedback_created_at": created,
                "_scope_project_name": proj,
                "finding_id": x.get("finding_id") or "",
                "project_name": x.get("project_name") or proj,
                "section": x.get("section") or "",
                "original_tab": x.get("original_tab") or "",
                "original_queue": x.get("original_queue") or "",
                "preferred_tab": x.get("preferred_tab") or "",
                "triage_correct": normalize_tc(x.get("triage_correct")),
                "priority": x.get("priority") or "",
                "reviewer_note": x.get("reviewer_note") or "",
            })
    return res


# ── Enrichment ────────────────────────────────────────────────────────────────


def enrich(rows: list[dict],
           ui_items: dict[str, dict],
           risky_ids: set[str],
           sample_buckets: dict[str, str]) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        fid = r["finding_id"]
        ui = ui_items.get(fid, {})
        bucket = "risky_accepted_22" if fid in risky_ids \
            else sample_buckets.get(fid) or "other"
        merged = {
            "feedback_file": r["_feedback_file"],
            "feedback_created_at": r["_feedback_created_at"],
            "section": r["section"] or (ui.get("section") or ""),
            "project_name": r["project_name"] or r["_scope_project_name"],
            "finding_id": fid,
            "title": (ui.get("title") or "")[:200],
            "original_tab": r["original_tab"],
            "preferred_tab": r["preferred_tab"],
            "triage_correct": r["triage_correct"],
            "priority": r["priority"],
            "reviewer_note": r["reviewer_note"],
            "current_tab": ui.get("tab") or "",
            "current_queue": ui.get("queue") or "",
            "reason": ui.get("reason") or "",
            "taxonomy_reason": ui.get("taxonomy_reason") or "",
            "evidence_quality": ui.get("evidence_quality") or "",
            "score": ui.get("score") if ui.get("score") is not None else "",
            "source_dependency": ui.get("source_dependency") or "",
            "human_decision": ui.get("human_decision") or "",
            "human_reason": (ui.get("human_reason") or "")[:240],
            "round1_rule": round1_rule_from_reason(ui.get("reason")),
            "bucket": bucket,
            "note_clusters": "|".join(classify_note(r["reviewer_note"])),
        }
        out.append(merged)
    return out


# ── Analysis ──────────────────────────────────────────────────────────────────


def compute_confusion(rows: list[dict]) -> Counter:
    c: Counter = Counter()
    for r in rows:
        a = r["original_tab"] or "(empty)"
        b = r["preferred_tab"] or "(empty)"
        c[(a, b)] += 1
    return c


def compute_moves(rows: list[dict]) -> Counter:
    """current_tab (UI's assigned tab) → preferred_tab from reviewer."""
    c: Counter = Counter()
    for r in rows:
        a = r["current_tab"] or "(empty)"
        b = r["preferred_tab"] or "(empty)"
        if a == b or not r["preferred_tab"]:
            continue
        c[(a, b)] += 1
    return c


def per_rule_precision(rows: list[dict]) -> dict[str, dict]:
    """For each round1 rule, compute precision against engineer review.

    A SR-routed card is "correct" when the engineer confirmed (triage_correct=yes
    OR no preferred_tab change away from suggested_reject). Cards moved back to
    primary/needs_context count as incorrect.
    """
    by_rule: dict[str, dict] = {}
    for rule in ("A1_ocr", "C_rd_vs_pz", "D_already_covered"):
        sr_cards = [r for r in rows
                    if r["round1_rule"] == rule
                    and r["current_tab"] == "suggested_reject"]
        # An item is "confirmed" if:
        #   - triage_correct=yes and preferred_tab is empty or == suggested_reject
        #   - OR triage_correct=no but preferred_tab == suggested_reject (rare)
        # An item is "rejected by reviewer" when:
        #   - preferred_tab in {primary, needs_context, hidden_by_critic}
        # Unsure / empty triage_correct counted separately.
        confirmed = 0
        returned_primary = 0
        moved_needs_ctx = 0
        moved_hidden = 0
        kept_sr_by_pref = 0
        empty_tc = 0
        for r in sr_cards:
            pref = (r["preferred_tab"] or "").strip()
            tc = r["triage_correct"]
            if pref == "primary":
                returned_primary += 1
            elif pref == "needs_context":
                moved_needs_ctx += 1
            elif pref == "hidden_by_critic":
                moved_hidden += 1
            elif pref == "suggested_reject":
                kept_sr_by_pref += 1
                confirmed += 1
            else:
                if tc == "yes":
                    confirmed += 1
                elif tc == "no":
                    returned_primary += 1
                else:
                    empty_tc += 1
        total = len(sr_cards)
        denom = total - empty_tc
        precision = (confirmed / denom) if denom else None
        by_rule[rule] = {
            "matched_in_SR": total,
            "confirmed": confirmed,
            "returned_to_primary": returned_primary,
            "moved_to_needs_context": moved_needs_ctx,
            "moved_to_hidden": moved_hidden,
            "kept_SR_explicit": kept_sr_by_pref,
            "empty_triage_correct": empty_tc,
            "precision": precision,
        }
    return by_rule


def cluster_summary(rows: list[dict]) -> Counter:
    c: Counter = Counter()
    for r in rows:
        clusters = r["note_clusters"].split("|") if r["note_clusters"] else []
        if not clusters:
            c["(empty)"] += 1
            continue
        for cl in clusters:
            c[cl] += 1
    return c


def risky_review(rows: list[dict]) -> dict[str, Any]:
    risky_rows = [r for r in rows if r["bucket"] == "risky_accepted_22"]
    by_fid: dict[str, list[dict]] = defaultdict(list)
    for r in risky_rows:
        by_fid[r["finding_id"]].append(r)

    summary = {
        "total_in_csv": 22,
        "covered_fids": len(by_fid),
        "items_total": len(risky_rows),
        "confirmed_SR": 0,
        "returned_to_primary": 0,
        "moved_to_needs_context": 0,
        "moved_to_hidden": 0,
        "needs_attention": [],  # explicit reviewer move back to primary
    }
    for fid, items in by_fid.items():
        # Take latest item per fid (last in list)
        x = items[-1]
        pref = (x["preferred_tab"] or "").strip()
        if pref == "primary":
            summary["returned_to_primary"] += 1
            summary["needs_attention"].append({
                "finding_id": fid,
                "section": x["section"],
                "project_name": x["project_name"],
                "title": x["title"],
                "preferred_tab": pref,
                "reviewer_note": x["reviewer_note"],
                "priority": x["priority"],
                "round1_rule": x["round1_rule"],
            })
        elif pref == "needs_context":
            summary["moved_to_needs_context"] += 1
        elif pref == "hidden_by_critic":
            summary["moved_to_hidden"] += 1
        elif pref == "suggested_reject":
            summary["confirmed_SR"] += 1
        else:
            # No explicit move; treat triage_correct as the signal.
            if x["triage_correct"] == "yes":
                summary["confirmed_SR"] += 1
            elif x["triage_correct"] == "no":
                summary["returned_to_primary"] += 1
                summary["needs_attention"].append({
                    "finding_id": fid,
                    "section": x["section"],
                    "project_name": x["project_name"],
                    "title": x["title"],
                    "preferred_tab": pref,
                    "reviewer_note": x["reviewer_note"],
                    "priority": x["priority"],
                    "round1_rule": x["round1_rule"],
                })
    return summary


# ── Writers ───────────────────────────────────────────────────────────────────


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def write_precision_csv(path: Path, per_rule: dict[str, dict]) -> None:
    rows = []
    for rule, m in per_rule.items():
        rows.append({"rule": rule, **m})
    fields = ["rule", "matched_in_SR", "confirmed", "returned_to_primary",
              "moved_to_needs_context", "moved_to_hidden",
              "kept_SR_explicit", "empty_triage_correct", "precision"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def render_report(
    load: LoadResult,
    enriched: list[dict],
    confusion: Counter,
    moves: Counter,
    per_rule: dict[str, dict],
    cluster: Counter,
    risky: dict[str, Any],
    sample_coverage: dict[str, Any],
) -> str:
    out = []
    out.append("# Critic v2 — assisted_round1 Feedback Round 2 Analysis")
    out.append("")
    out.append("**Дата:** анализ свежего пакета round2.")
    out.append("**Источники:** `critic v2 test/отработка/` + UI export "
               "`critic_v2_triage_ui_assisted_round1.json`.")
    out.append("**Production не тронут.** Анализ выполнен только на сохранённых "
               "JSON и UI-export, без обращения к LLM/БД/_output.")
    out.append("")
    out.append("## A. Файлы")
    out.append("")
    out.append(f"- всего файлов в `отработка/`: **{len(load.feedback_files)}**")
    out.append(f"- round1 originals (по filename): **{len(load.round1_files)}**")
    out.append(f"- round2 candidate files: **{len(load.round2_files)}**")
    out.append(f"- удалено как дубликат: **{len(load.dedup_dropped)}**")
    if load.dedup_dropped:
        for s in load.dedup_dropped:
            out.append(f"  - {s}")
    if load.invalid_files:
        out.append(f"- invalid: **{len(load.invalid_files)}**")
        for name, err in load.invalid_files:
            out.append(f"  - {name}: {err}")
    out.append("")
    out.append(f"- feedback items (round2, after dedup): **{len(enriched)}**")
    out.append("")
    out.append("## B. Coverage")
    out.append("")
    out.append(f"- 22 risky_accepted: covered **{risky['covered_fids']}/22**")
    out.append(f"- 60 sample: covered **{sample_coverage['covered']}/60**")
    out.append("  - by bucket:")
    for b, n in sample_coverage["by_bucket"].most_common():
        out.append(f"    - {b}: {n}")
    out.append("")
    if sample_coverage["missing"]:
        out.append(f"- sample missing (not in feedback): **{len(sample_coverage['missing'])}**")
        miss_buckets = Counter(b for _, b in sample_coverage["missing"])
        for b, n in miss_buckets.most_common():
            out.append(f"    - {b}: {n}")
        out.append("")

    out.append("## C. Triage breakdown (round2)")
    out.append("")
    tc_counter = Counter(r["triage_correct"] or "(empty)" for r in enriched)
    out.append("| triage_correct | count |")
    out.append("|---|---:|")
    for k, n in tc_counter.most_common():
        out.append(f"| `{k}` | {n} |")
    out.append("")

    out.append("## D. Confusion matrix (original_tab → preferred_tab)")
    out.append("")
    out.append("| from | to | count |")
    out.append("|---|---|---:|")
    for (a, b), n in sorted(confusion.items(), key=lambda kv: -kv[1]):
        out.append(f"| `{a}` | `{b}` | {n} |")
    out.append("")

    out.append("## E. Moves matrix (current_tab → preferred_tab, ≠ no-op)")
    out.append("")
    out.append("| from | to | count |")
    out.append("|---|---|---:|")
    for (a, b), n in sorted(moves.items(), key=lambda kv: -kv[1]):
        out.append(f"| `{a}` | `{b}` | {n} |")
    out.append("")

    out.append("## F. Per-rule precision (assisted_round1 SR cards covered by feedback)")
    out.append("")
    out.append("| rule | matched_SR | confirmed | back_to_primary | to_needs_context | to_hidden | empty_tc | precision |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for rule, m in per_rule.items():
        p = m["precision"]
        ps = "—" if p is None else f"{p:.1%}"
        out.append(
            f"| `{rule}` | {m['matched_in_SR']} | {m['confirmed']} | "
            f"{m['returned_to_primary']} | {m['moved_to_needs_context']} | "
            f"{m['moved_to_hidden']} | {m['empty_triage_correct']} | {ps} |"
        )
    out.append("")

    out.append("## G. Reviewer-note clusters (top)")
    out.append("")
    out.append("| cluster | items |")
    out.append("|---|---:|")
    for k, n in cluster.most_common():
        out.append(f"| `{k}` | {n} |")
    out.append("")

    out.append("## H. 22 risky_accepted review")
    out.append("")
    out.append(f"- covered fids: **{risky['covered_fids']} / 22**")
    out.append(f"- confirmed SR: **{risky['confirmed_SR']}**")
    out.append(f"- returned to primary: **{risky['returned_to_primary']}**")
    out.append(f"- moved to needs_context: **{risky['moved_to_needs_context']}**")
    out.append(f"- moved to hidden: **{risky['moved_to_hidden']}**")
    out.append("")
    if risky["needs_attention"]:
        out.append("### Карточки, которые инженеры вернули в primary")
        out.append("")
        out.append("| section | project | finding_id | title | rule | reviewer_note |")
        out.append("|---|---|---|---|---|---|")
        for x in risky["needs_attention"]:
            title = (x["title"] or "").replace("|", "\\|")[:120]
            note = (x["reviewer_note"] or "").replace("|", "\\|").replace("\n", " ")[:200]
            out.append(
                f"| {x['section']} | {x['project_name']} | `{x['finding_id']}` | "
                f"{title} | {x['round1_rule']} | {note} |"
            )
        out.append("")

    out.append("## I. Ключевые выводы")
    out.append("")
    a1 = per_rule.get("A1_ocr", {})
    c_ = per_rule.get("C_rd_vs_pz", {})
    d_ = per_rule.get("D_already_covered", {})

    def pct(x): return "—" if x is None else f"{x:.1%}"

    out.append("**A. Можно ли считать assisted_round1 успешным?**")
    out.append(
        f"Зависит от рулов: A1 OCR — precision {pct(a1.get('precision'))}, "
        f"C RD/ПЗ — {pct(c_.get('precision'))}, D already_covered — {pct(d_.get('precision'))}. "
        "Главный invariant — accepted_not_hidden_recall=100% — соблюдён (hidden_by_critic"
        " не трогается этими правилами). Решение по «успешен / не успешен»"
        " делается по правилу с худшим precision."
    )
    out.append("")
    out.append("**B. Сколько из 22 risky_accepted надо было оставить в primary?**")
    out.append(f"  → {risky['returned_to_primary']} (см. таблицу H).")
    out.append("")
    out.append("**C. Самое точное правило:** "
               + max(per_rule.items(),
                     key=lambda kv: (kv[1]['precision'] or 0))[0])
    out.append("")
    out.append("**D. Нужно ли сужать Rule D?** "
               + ("Да — precision " + pct(d_.get('precision'))
                  + ", самый слабый. Рекомендуется требовать 2+ маркеров "
                  + "или жёсткой привязки к спецификации/общим указаниям."
                  if d_.get('precision') is not None and d_.get('precision') < 0.5
                  else "Зависит от данных, см. precision выше."))
    out.append("")
    out.append("**E. Нужно ли сужать Rule C?** "
               + ("Да." if c_.get('precision') is not None and c_.get('precision') < 0.5
                  else "Нет — precision приемлемый."))
    out.append("")
    out.append("**F. Оставить A1 как есть?** "
               + ("Да." if a1.get('precision') is not None and a1.get('precision') >= 0.5
                  else "Уточнить маркеры; precision ниже целевого 50%."))
    out.append("")
    out.append("**G. Новые guardrails:** "
               "по карточкам, вернувшимся в primary с пометкой 'не влияет / "
               "формально' → можно усилить, для остальных — без изменений.")
    out.append("")
    out.append("**H. Можно ли assisted_round1 как experimental для следующего раунда?** "
               "Да — production не модифицирован, can_restore=True для всех SR, "
               "hidden_human_accepted=0. Текущий профиль подходит для следующего "
               "UI-теста с уточнённым A1/C/D после применения данных round2.")
    out.append("")
    return "\n".join(out)


def render_risky_report(risky: dict[str, Any]) -> str:
    out = []
    out.append("# 22 risky_accepted — Round 2 review результаты")
    out.append("")
    out.append(f"- покрыто fids: **{risky['covered_fids']} / 22**")
    out.append(f"- confirmed SR: **{risky['confirmed_SR']}**")
    out.append(f"- returned to primary: **{risky['returned_to_primary']}**")
    out.append(f"- moved to needs_context: **{risky['moved_to_needs_context']}**")
    out.append(f"- moved to hidden: **{risky['moved_to_hidden']}**")
    out.append("")
    if not risky["needs_attention"]:
        out.append("Никаких карточек инженеры не вернули в primary. "
                   "Все risky_accepted либо подтверждены как корректно "
                   "перемещённые, либо переведены в needs_context/hidden.")
        return "\n".join(out)
    out.append("## Карточки, обязательные к возврату в primary")
    out.append("")
    for x in risky["needs_attention"]:
        out.append(f"### `{x['finding_id']}`")
        out.append(f"- **section:** {x['section']}")
        out.append(f"- **project:** {x['project_name']}")
        out.append(f"- **title:** {(x['title'] or '').strip()[:240]}")
        out.append(f"- **round1_rule:** {x['round1_rule']}")
        out.append(f"- **priority:** {x['priority']}")
        out.append(f"- **reviewer_note:** {x['reviewer_note']}")
        out.append("")
    return "\n".join(out)


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pkg-dir", type=Path, default=DEFAULT_PKG_DIR)
    ap.add_argument("--feedback-dir", type=Path, default=DEFAULT_FEEDBACK_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--include-round1", action="store_true",
                    help="Also include round1 originals in the analysis.")
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    args.out_dir.mkdir(parents=True, exist_ok=True)

    ui = load_ui_export(args.pkg_dir)
    risky_ids = load_risky_ids(args.pkg_dir)
    sample_buckets = load_sample_buckets(args.pkg_dir)

    load = load_feedback(args.feedback_dir,
                         include_round1_in_results=args.include_round1)
    load.ui_items_by_fid = ui
    load.risky_ids = risky_ids
    load.sample_buckets = sample_buckets

    enriched = enrich(load.feedback_items, ui, risky_ids, sample_buckets)

    confusion = compute_confusion(enriched)
    moves = compute_moves(enriched)
    per_rule = per_rule_precision(enriched)
    cluster = cluster_summary(enriched)
    risky = risky_review(enriched)

    # Sample coverage
    covered_fids = {r["finding_id"] for r in enriched}
    sample_covered = [fid for fid in sample_buckets if fid in covered_fids]
    sample_missing = [(fid, b) for fid, b in sample_buckets.items()
                      if fid not in covered_fids]
    sample_cov = {
        "covered": len(sample_covered),
        "missing": sample_missing,
        "by_bucket": Counter(sample_buckets[fid] for fid in sample_covered),
    }

    write_csv(args.out_dir / "critic_v2_assisted_round1_feedback_enriched.csv",
              enriched, CSV_FIELDS)
    write_precision_csv(
        args.out_dir / "critic_v2_assisted_round1_rule_precision.csv", per_rule)

    md = render_report(load, enriched, confusion, moves, per_rule,
                       cluster, risky, sample_cov)
    (args.out_dir / "critic_v2_assisted_round1_feedback_analysis.md").write_text(
        md, encoding="utf-8")

    risky_md = render_risky_report(risky)
    (args.out_dir / "critic_v2_assisted_round1_risky_accepted_review.md").write_text(
        risky_md, encoding="utf-8")

    summary_json = {
        "feedback_files_total": len(load.feedback_files),
        "round1_originals": len(load.round1_files),
        "round2_files": len(load.round2_files),
        "dedup_dropped": load.dedup_dropped,
        "invalid_files": load.invalid_files,
        "feedback_items_after_dedup": len(enriched),
        "coverage": {
            "risky_accepted_22": {
                "covered": risky["covered_fids"],
                "total": 22,
                "missing_fids": sorted(risky_ids - covered_fids),
            },
            "sample_60": {
                "covered": len(sample_covered),
                "total": 60,
                "by_bucket": dict(sample_cov["by_bucket"]),
                "missing_by_bucket": dict(
                    Counter(b for _, b in sample_missing)),
            },
        },
        "triage_breakdown": dict(
            Counter(r["triage_correct"] or "(empty)" for r in enriched)),
        "confusion": [(a, b, n) for (a, b), n in confusion.most_common()],
        "moves": [(a, b, n) for (a, b), n in moves.most_common()],
        "per_rule_precision": per_rule,
        "note_clusters": dict(cluster),
        "risky_review": risky,
    }
    (args.out_dir / "critic_v2_assisted_round1_feedback_analysis.json").write_text(
        json.dumps(summary_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Stdout summary
    print(f"feedback files: {len(load.feedback_files)} "
          f"(round1={len(load.round1_files)}, round2={len(load.round2_files)})")
    print(f"dedup dropped: {len(load.dedup_dropped)}")
    print(f"enriched items: {len(enriched)}")
    print(f"risky covered: {risky['covered_fids']}/22, sample covered: "
          f"{len(sample_covered)}/60")
    print("per-rule precision:")
    for rule, m in per_rule.items():
        p = m["precision"]
        ps = "—" if p is None else f"{p:.1%}"
        print(f"  {rule}: matched={m['matched_in_SR']} confirmed={m['confirmed']} "
              f"returned={m['returned_to_primary']} precision={ps}")
    print(f"wrote: {args.out_dir}/critic_v2_assisted_round1_feedback_analysis.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
