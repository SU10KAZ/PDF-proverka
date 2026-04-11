"""
evaluate_v4.py
--------------
Новый evaluator для v4 архитектуры — сравнивает candidates/findings с etalon.jsonl
по структурным ключам (issue_class_id, subtype, entity_key, field) вместо fuzzy-text.

Устойчив к перефразированию finding и даёт per-class метрики.

Использование:
    python evaluate_v4.py --candidates prototype_v4/test_output/candidates.json
    python evaluate_v4.py --candidates projects/EOM/.../candidates_v2.json --etalon etalon.jsonl
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


# ─── Matching policies ────────────────────────────────────────────────────


def normalize_line_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().upper()
    s = s.replace("M", "М")  # latin → cyr
    s = re.sub(r"^(М)(\d)", r"\1-\2", s)
    return s


def split_entity_key(key: str) -> tuple[str, str]:
    """Разбить entity_key на (type, value).

    Примеры:
        'line:М-1.1' → ('line', 'М-1.1')
        'panel_pair:ЩУ-2/Т|ЩУ-12/Т' → ('panel_pair', 'ЩУ-2/Т|ЩУ-12/Т')
        'line_group:М-1.1|М-2.1' → ('line_group', 'М-1.1|М-2.1')
        'breaker_family:ВА-335А' → ('breaker_family', 'ВА-335А')
    """
    if ":" not in key:
        return ("unknown", key)
    parts = key.split(":", 1)
    return (parts[0], parts[1])


def match_entity_keys(candidate_key: str, etalon_key: str) -> bool:
    """Проверить соответствие entity_keys кандидата и эталона.

    Логика:
    - Прямое совпадение → match
    - line:М-1.1 vs line_group:М-1.1|М-2.1 → match (М-1.1 входит в группу)
    - breaker:QF1.1 vs breaker_family:ВА-335А → match только если QF1.1 имеет model=ВА-335А
      (но это требует lookup в memory — здесь допускаем loose match по типу)
    - line:М-1.4 vs line_group:unknown_exact_line_with_150_vs_120 → fuzzy match:
      если поля field совпадают, считаем что это тот же класс ошибки
    """
    if candidate_key == etalon_key:
        return True

    c_type, c_val = split_entity_key(candidate_key)
    e_type, e_val = split_entity_key(etalon_key)

    # line:X vs line_group:X|Y → проверяем вхождение
    if c_type == "line" and e_type == "line_group":
        c_line = normalize_line_id(c_val)
        group_lines = [normalize_line_id(x) for x in e_val.split("|")]
        return c_line in group_lines

    if c_type == "line_group" and e_type == "line":
        e_line = normalize_line_id(e_val)
        group_lines = [normalize_line_id(x) for x in c_val.split("|")]
        return e_line in group_lines

    # Для "unknown_exact_line_with_150_vs_120" или подобных — это fuzzy match
    # по классу и полю. Возвращаем False здесь, mathing на уровне class/field.
    if e_val.startswith("unknown_"):
        return False  # требует match по class/field, не по entity

    # panel_pair:A|B vs panel_pair:B|A → нормализуем пары
    if c_type == "panel_pair" and e_type == "panel_pair":
        c_pair = set(c_val.split("|"))
        e_pair = set(e_val.split("|"))
        return c_pair == e_pair

    # breaker:QF1.1 vs breaker_family:ВА-335А — нельзя определить без memory lookup
    # В judge stage это должно проверяться через breaker_kb.
    if c_type == "breaker" and e_type == "breaker_family":
        # loose match: если candidate поднимает breaker из той же марки — считаем match
        # Здесь допускаем loose match (judge решит)
        return True

    return False


def match_subtype(candidate_subtype: str, etalon_subtype: str) -> bool:
    """Проверить match subtype.

    Полное совпадение или prefix-match.
    Пример:
        candidate: "cross_view_attribute_mismatch"
        etalon:    "cross_view_attribute_mismatch.pe_count"
        → match (prefix)
    """
    if candidate_subtype == etalon_subtype:
        return True

    # Prefix match
    if etalon_subtype.startswith(candidate_subtype + "."):
        return True
    if candidate_subtype.startswith(etalon_subtype + "."):
        return True

    return False


# Группы взаимозаменяемых полей — модели часто путают PE ↔ нейтраль
# в русских проектах, терминологически это один класс ошибки
FIELD_SYNONYMS = [
    {"pe_count", "neutral_count", "pe_or_n_count"},
    {"pe_section_mm2", "neutral_section_mm2", "pe_or_n_section_mm2"},
    {"phase_section_mm2"},
    {"phase_count"},
    {"cable_mark", "mark"},
    {"source_panel"},
    {"destination_panel", "destination"},
]


def fields_are_equivalent(f1: str | None, f2: str | None) -> bool:
    """Проверить что два поля — взаимозаменяемые синонимы."""
    if not f1 or not f2:
        return f1 == f2  # оба None — ok, один None — нет
    if f1 == f2:
        return True
    for group in FIELD_SYNONYMS:
        if f1 in group and f2 in group:
            return True
    return False


def match_class_and_field(candidate: dict, etalon_record: dict) -> bool:
    """Более мягкий match — через (class, subtype, field) с учётом синонимов."""
    if candidate.get("issue_class_id") != etalon_record.get("issue_class_id"):
        return False

    if not match_subtype(candidate.get("subtype", ""), etalon_record.get("subtype", "")):
        return False

    c_field = candidate.get("field")
    e_field = etalon_record.get("field")

    # Field match (exact, synonym, or None-compatible)
    if not fields_are_equivalent(c_field, e_field):
        return False

    return True


def match_candidate_to_etalon(candidate: dict, etalon_record: dict) -> tuple[bool, str]:
    """Попытаться сматчить candidate с etalon записью.

    Returns:
        (matched, match_type) где match_type = "exact_key", "unknown_resolved", "loose_key"
    """
    etalon_entity = etalon_record.get("entity_key", "")

    # 1. Exact class + subtype + entity_key
    if (candidate.get("issue_class_id") == etalon_record.get("issue_class_id")
            and match_subtype(candidate.get("subtype", ""), etalon_record.get("subtype", ""))
            and match_entity_keys(candidate.get("entity_key", ""), etalon_record.get("entity_key", ""))):
        return (True, "exact_key")

    # 2. Для unknown_* в эталоне — loose match по class+field имеет высокий приоритет
    # (потому что эталон просто не знает конкретный entity, это не наша вина)
    if "unknown_" in etalon_entity:
        if match_class_and_field(candidate, etalon_record):
            # Дополнительно проверяем что тип entity совместим
            c_type, _ = split_entity_key(candidate.get("entity_key", ""))
            e_type, _ = split_entity_key(etalon_entity)
            if (c_type == "line" and e_type == "line_group") or c_type == e_type:
                return (True, "unknown_resolved")

    # 3. Loose match: same class+subtype+field, entity_key разный но совместим
    if match_class_and_field(candidate, etalon_record):
        c_type, _ = split_entity_key(candidate.get("entity_key", ""))
        e_type, _ = split_entity_key(etalon_entity)
        loose_pairs = [
            ("line", "line"),
            ("line", "line_group"),
            ("line_group", "line"),
            ("panel", "panel"),
            ("panel_pair", "panel_pair"),
            ("breaker", "breaker"),
            ("breaker", "breaker_family"),
            ("breaker_family", "breaker"),
        ]
        if (c_type, e_type) in loose_pairs:
            return (True, "loose_class_field")

    return (False, "no_match")


# ─── Evaluation ───────────────────────────────────────────────────────────


def evaluate(candidates: list[dict], etalon_records: list[dict]) -> dict:
    """Оценить candidates против etalon.

    Greedy assignment: каждый кандидат → один этiban (или None).
    Каждый эталон → один кандидат (или None).
    """
    used_candidates = set()
    used_etalons = set()
    matches = []

    # Генерируем все пары (candidate_idx, etalon_idx, match_type, priority)
    pairs = []
    priorities = {
        "exact_key": 3,
        "unknown_resolved": 3,  # высокий — эталон сам не знает entity
        "loose_class_field": 2,
        "no_match": 0,
    }
    for ci, c in enumerate(candidates):
        for ei, e in enumerate(etalon_records):
            matched, match_type = match_candidate_to_etalon(c, e)
            if matched:
                priority = priorities.get(match_type, 0)
                pairs.append((priority, ci, ei, match_type))

    # Сортируем по priority desc, назначаем жадно
    pairs.sort(key=lambda x: -x[0])
    for priority, ci, ei, match_type in pairs:
        if ci in used_candidates or ei in used_etalons:
            continue
        used_candidates.add(ci)
        used_etalons.add(ei)
        matches.append({
            "candidate_idx": ci,
            "candidate": candidates[ci],
            "etalon_idx": ei,
            "etalon": etalon_records[ei],
            "match_type": match_type,
        })

    # Nонматчnутые
    novel = [candidates[i] for i in range(len(candidates)) if i not in used_candidates]
    missed = [etalon_records[i] for i in range(len(etalon_records)) if i not in used_etalons]

    # Классификация matches по status
    tp = [m for m in matches if m["etalon"].get("status") == "KEEP"]
    fp = [m for m in matches if m["etalon"].get("status") == "FP"]
    partial = [m for m in matches if m["etalon"].get("status") == "PARTIAL"]
    unknown = [m for m in matches if m["etalon"].get("status") == "UNKNOWN"]

    # Missed KEEPs — важное!
    missed_keeps = [e for e in missed if e.get("status") == "KEEP"]

    # Per-class recall
    class_stats = defaultdict(lambda: {"keeps_total": 0, "keeps_matched": 0})
    for e in etalon_records:
        if e.get("status") == "KEEP":
            cls = e.get("issue_class_id", "?")
            class_stats[cls]["keeps_total"] += 1
    for m in tp:
        cls = m["etalon"].get("issue_class_id", "?")
        class_stats[cls]["keeps_matched"] += 1

    total_keeps = sum(1 for e in etalon_records if e.get("status") == "KEEP")
    recall_definite = len(tp) / total_keeps if total_keeps else 0

    return {
        "counts": {
            "total_candidates": len(candidates),
            "total_etalon": len(etalon_records),
            "etalon_keeps": total_keeps,
            "tp": len(tp),
            "fp": len(fp),
            "partial": len(partial),
            "unknown_matches": len(unknown),
            "novel": len(novel),
            "missed_keeps": len(missed_keeps),
        },
        "metrics": {
            "recall_definite": recall_definite,
            "precision_definite": (
                len(tp) / (len(tp) + len(fp)) if (len(tp) + len(fp)) else 0
            ),
            "precision_with_potential": (
                (len(tp) + len(unknown)) / (len(tp) + len(fp) + len(unknown))
                if (len(tp) + len(fp) + len(unknown)) else 0
            ),
        },
        "per_class_recall": dict(class_stats),
        "tp": tp,
        "fp": fp,
        "partial": partial,
        "unknown_matches": unknown,
        "novel": novel,
        "missed_keeps": missed_keeps,
    }


# ─── Output ───────────────────────────────────────────────────────────────


class C:
    """ANSI colors (no-op if no TTY)."""
    if sys.stdout.isatty():
        GREEN = "\033[92m"
        RED = "\033[91m"
        YELLOW = "\033[93m"
        BLUE = "\033[94m"
        DIM = "\033[2m"
        BOLD = "\033[1m"
        RESET = "\033[0m"
    else:
        GREEN = RED = YELLOW = BLUE = DIM = BOLD = RESET = ""


def short(text, n=120):
    text = str(text).replace("\n", " ").strip()
    if len(text) > n:
        return text[: n - 1] + "…"
    return text


def print_report(result: dict, title: str = "Evaluation v4"):
    # Force UTF-8 for Windows consoles
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    cnt = result["counts"]
    m = result["metrics"]

    print(f"\n{C.BOLD}=== {title} ==={C.RESET}")
    print(f"Candidates: {cnt['total_candidates']} | Etalon: {cnt['total_etalon']} (KEEPs: {cnt['etalon_keeps']})")
    print()

    # Главные метрики
    print(f"{C.BOLD}Метрики:{C.RESET}")
    recall_pct = m["recall_definite"] * 100
    recall_c = C.GREEN if recall_pct >= 80 else C.YELLOW if recall_pct >= 50 else C.RED
    print(f"  Recall (definite KEEPs):    {recall_c}{cnt['tp']}/{cnt['etalon_keeps']} = {recall_pct:.0f}%{C.RESET}")
    print(f"  Precision (definite):       {m['precision_definite']*100:.0f}% ({cnt['tp']}/{cnt['tp']+cnt['fp']})")
    print(f"  Precision (с потенциалом):  {m['precision_with_potential']*100:.0f}%")
    print()

    # Распределение
    print(f"{C.BOLD}Распределение:{C.RESET}")
    print(f"  {C.GREEN}[TP]      {cnt['tp']:3d}{C.RESET}")
    print(f"  {C.RED}[FP]      {cnt['fp']:3d}{C.RESET}")
    print(f"  {C.YELLOW}[Partial] {cnt['partial']:3d}{C.RESET}")
    print(f"  {C.BLUE}[Unknown] {cnt['unknown_matches']:3d}{C.RESET}")
    print(f"  {C.DIM}[Novel]   {cnt['novel']:3d}{C.RESET}")
    print(f"  {C.RED}[Missed]  {cnt['missed_keeps']:3d}{C.RESET}")
    print()

    # Per-class recall
    if result.get("per_class_recall"):
        print(f"{C.BOLD}Recall по классам:{C.RESET}")
        for cls, stats in sorted(result["per_class_recall"].items()):
            matched = stats["keeps_matched"]
            total = stats["keeps_total"]
            pct = (matched / total * 100) if total else 0
            print(f"  Class {cls}: {matched}/{total} ({pct:.0f}%)")
        print()

    # TP details
    if result["tp"]:
        print(f"{C.BOLD}{C.GREEN}[TP] details:{C.RESET}")
        for match in result["tp"]:
            cand = match["candidate"]
            et = match["etalon"]
            print(f"  [{match['match_type']}] Class {cand.get('issue_class_id')} {cand.get('subtype')}")
            print(f"     Candidate: {cand.get('entity_key')} / {cand.get('field')}")
            print(f"     Etalon:    #{et.get('etalon_no')} {et.get('entity_key')} / {et.get('field')}")
            print(f"     Text: {short(et.get('raw_text', ''), 100)}")
        print()

    # Missed KEEPs
    if result["missed_keeps"]:
        print(f"{C.BOLD}{C.RED}[Missed KEEPs]:{C.RESET}")
        for et in result["missed_keeps"]:
            print(f"  #{et.get('etalon_no')}: class={et.get('issue_class_id')} subtype={et.get('subtype')}")
            print(f"     entity_key: {et.get('entity_key')}")
            print(f"     field: {et.get('field')}")
            print(f"     {short(et.get('raw_text', ''), 100)}")
        print()

    # FP details (first 5)
    if result["fp"]:
        print(f"{C.BOLD}{C.RED}[FP] details (first 5):{C.RESET}")
        for match in result["fp"][:5]:
            cand = match["candidate"]
            et = match["etalon"]
            print(f"  Candidate: {cand.get('subtype')} / {cand.get('entity_key')}")
            print(f"  Matched to etalon #{et.get('etalon_no')}: {short(et.get('raw_text', ''), 80)}")
            print(f"  Reason: {short(et.get('status_reason', ''), 80)}")
            print()

    # Novel candidates
    if result["novel"]:
        print(f"{C.BOLD}+ Novel candidates ({len(result['novel'])}):{C.RESET}")
        for cand in result["novel"][:10]:
            print(f"  Class {cand.get('issue_class_id')} {cand.get('subtype')}: {cand.get('entity_key')}")
        if len(result["novel"]) > 10:
            print(f"  ... и ещё {len(result['novel']) - 10}")
        print()


# ─── Main ─────────────────────────────────────────────────────────────────


def load_candidates(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    # Format может быть {all_candidates: [...]} или просто [...]
    if isinstance(data, dict):
        return data.get("all_candidates", [])
    return data


def load_etalon(path: Path) -> list[dict]:
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except Exception as e:
            print(f"WARN: failed to parse line: {e}", file=sys.stderr)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", required=True, help="Путь к candidates JSON")
    parser.add_argument("--etalon", default="etalon.jsonl", help="Путь к etalon.jsonl")
    parser.add_argument("--json", help="Сохранить результат в JSON")
    args = parser.parse_args()

    candidates_path = Path(args.candidates)
    etalon_path = Path(args.etalon)

    if not candidates_path.exists():
        print(f"ERROR: {candidates_path} не найден", file=sys.stderr)
        sys.exit(1)
    if not etalon_path.exists():
        print(f"ERROR: {etalon_path} не найден", file=sys.stderr)
        sys.exit(1)

    candidates = load_candidates(candidates_path)
    etalon = load_etalon(etalon_path)

    result = evaluate(candidates, etalon)
    print_report(result, title=f"Evaluation: {candidates_path.name}")

    if args.json:
        # Serialize (remove non-serializable fields)
        out = {
            "counts": result["counts"],
            "metrics": result["metrics"],
            "per_class_recall": result["per_class_recall"],
            "tp_summary": [
                {
                    "match_type": m["match_type"],
                    "candidate_entity": m["candidate"].get("entity_key"),
                    "etalon_no": m["etalon"].get("etalon_no"),
                }
                for m in result["tp"]
            ],
            "missed_keeps": [
                {
                    "etalon_no": e.get("etalon_no"),
                    "class": e.get("issue_class_id"),
                    "subtype": e.get("subtype"),
                    "entity_key": e.get("entity_key"),
                    "text": e.get("raw_text", "")[:150],
                }
                for e in result["missed_keeps"]
            ],
        }
        Path(args.json).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nSaved: {args.json}")


if __name__ == "__main__":
    main()
