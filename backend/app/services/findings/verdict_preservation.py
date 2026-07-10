"""
verdict_preservation.py
-----------------------
Сохранение экспертных вердиктов при переаудите ТОЙ ЖЕ версии.

Проблема: F-NNN — позиционный номер; findings_merge перенумеровывает его на
каждом полном прогоне, из-за чего решения эксперта (accepted/rejected) в
expert_review.json «съезжают» на чужие замечания или сиротеют
(см. docs/stable_finding_id.md — стабильного uid в проде нет).

Механизм (детерминированный, без LLM, fail-soft):
  1. snapshot_for_project() — ПЕРЕД удалением 03_findings.json снимает слепок
     решённых вердиктов вместе с фингерпринтом замечания (лист, категория,
     severity, нормализованный паттерн проблемы, «значимые числа», полный текст).
     Слепок лежит в 04_review/ версии — папка переживает и переаудит,
     и «Очистить» (v2-primary удаляет только 03_analysis).
  2. rehydrate_for_project(item_types=...) — ПОСЛЕ регенерации артефактов
     матчит слепок с новыми items (двухфазно: сначала все exact-совпадения
     fingerprint, затем fuzzy по тексту) и переписывает вердикты на новые ID
     через kb.save_expert_review (carried_over=True — эксперт может
     переопределить; тот же паттерн, что у decision carryover между версиями).
     Одновременно РЕКОНСИЛИРУЕТ stale-решения: записи expert_review, чьи
     item_id взяты из слепка (это решения ПРОШЛОГО прогона), снимаются через
     removed_ids — иначе они висели бы на чужих новых F-ID и блокировали
     восстановление. Отчёт — verdict_preservation_report.json.

item_types: findings перегенерируются на findings_merge, оптимизации — позже
(этап 05), поэтому регидрация вызывается дважды: после merge для ("finding",)
и после post-findings блока для ("optimization",). Применённые типы
отмечаются в слепке раздельно (applied_types).

Известные ограничения (осознанно):
  - Если разметка УЖЕ рассинхронизирована до снятия слепка (переаудит,
    сделанный до включения фичи), слепок зафиксирует текущие — возможно
    неверные — пары. Слепок с не до конца применёнными типами не
    перезаписывается (kept_unapplied) — защита от отравления после краха
    между merge и регидрацией.
  - retry только этапа оптимизации (без merge) слепок не снимает — OPT-вердикты
    на этом пути пока не защищены.

Ручные решения никогда не перезаписываются; несматченные вердикты не
пропадают молча — попадают в отчёт (unmatched/ambiguous/target_already_decided).
"""
from __future__ import annotations

import difflib
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

SCHEMA_VERSION = 2
SNAPSHOT_FILENAME = "verdict_preservation_snapshot.json"
REPORT_FILENAME = "verdict_preservation_report.json"

ALL_ITEM_TYPES = ("finding", "optimization")

# Порог fuzzy-совпадения текста проблемы и минимальный отрыв от второго
# кандидата: ниже порога — unmatched, отрыв меньше margin — ambiguous
# (лучше честно отдать эксперту, чем прилепить вердикт не туда).
FUZZY_RATIO_MIN = 0.80
FUZZY_MARGIN_MIN = 0.03

_NUM_RE = re.compile(r"\d+(?:[.,]\d+)?")

# Сентинелы исходов exact-матча (не пересекаются с id замечаний)
_AMBIGUOUS = "__ambiguous__"
_OCCUPIED = "__occupied__"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def is_enabled() -> bool:
    try:
        from backend.app.core.config import VERDICT_PRESERVATION_ENABLED
        return bool(VERDICT_PRESERVATION_ENABLED)
    except Exception:
        return False


def is_shadow() -> bool:
    """Shadow-режим: матчинг и отчёт полные, запись вердиктов выключена."""
    try:
        from backend.app.core.config import VERDICT_PRESERVATION_SHADOW
        return bool(VERDICT_PRESERVATION_SHADOW)
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
# Фингерпринт замечания / оптимизации
# ═══════════════════════════════════════════════════════════════════════════

def _salient_numbers(text: str) -> list[str]:
    """Значимые числа из текста (сечения, токи, длины) — порядко-независимо."""
    return sorted({m.group(0).replace(",", ".") for m in _NUM_RE.finditer(text or "")})


def _normalize_sheet(sheet: Any) -> str:
    """«Лист 1 (из 1)» и «Лист 1» — один и тот же лист."""
    s = re.sub(r"\s*\(.*?\)\s*$", "", str(sheet or "").strip())
    return re.sub(r"\s+", " ", s).lower()


def _item_core_text(item: dict, item_type: str) -> str:
    if item_type == "optimization":
        return " ".join(
            str(item.get(k) or "") for k in ("current", "proposed")
        ).strip()
    return " ".join(
        str(item.get(k) or "") for k in ("problem", "solution")
    ).strip()


def _problem_pattern(text: str) -> str:
    try:
        from backend.app.services.findings.findings_service import (
            _normalize_problem_pattern,
        )
        return _normalize_problem_pattern(text or "")
    except Exception:
        return re.sub(r"\s+", " ", (text or "").lower()).strip()


def build_fingerprint(item: dict, item_type: str) -> dict:
    """Стабильный слепок сути замечания, не зависящий от F-NNN."""
    core = _item_core_text(item, item_type)
    if item_type == "optimization":
        kind = str(item.get("type") or "")
        head = str(item.get("problem") or item.get("current") or "")
    else:
        kind = str(item.get("category") or "")
        head = str(item.get("problem") or "")
    return {
        "sheet": _normalize_sheet(item.get("sheet")),
        "kind": kind.strip().lower(),
        "severity": str(item.get("severity") or "").strip().upper(),
        "pattern": _problem_pattern(head),
        "numbers": _salient_numbers(core),
        "text": re.sub(r"\s+", " ", core).strip(),
    }


def _exact_key(fp: dict, *, with_sheet: bool) -> tuple:
    key = (fp.get("pattern") or "", tuple(fp.get("numbers") or ()))
    if with_sheet:
        key = key + (fp.get("sheet") or "",)
    return key


# ═══════════════════════════════════════════════════════════════════════════
# Ядро матчинга (чистая функция — покрыта тестами без IO)
# ═══════════════════════════════════════════════════════════════════════════

def match_snapshot_to_items(
    snapshot_items: list[dict],
    new_items: dict[str, dict],
    item_type: str,
    already_decided_ids: set[str],
) -> tuple[list[dict], list[dict]]:
    """Сматчить слепок вердиктов с новыми items одного item_type.

    Двухфазно: фаза 1 — exact fingerprint для ВСЕХ снапшот-items (fuzzy более
    раннего item не может украсть exact-цель более позднего); фаза 2 — fuzzy
    для оставшихся. Ложная привязка хуже потери, поэтому: неоднозначный exact →
    ambiguous; exact-цель занята существующим решением → target_already_decided
    (не пробуем fuzzy на соседей); fuzzy требует равенства значимых чисел и
    совпадения категории (мягко), порог 0.80 + отрыв 0.03.

    Returns:
        (restored, leftovers) — restored: [{new_id, snapshot_item, match, score}],
        leftovers: [{snapshot_item, reason, ...}].
    """
    new_fps = {
        nid: build_fingerprint(item, item_type)
        for nid, item in new_items.items()
    }

    # exact-индексы по ВСЕМ items (занятость проверяется на этапе выбора —
    # это позволяет отличить «цель занята» от «цели нет»)
    exact_with_sheet: dict[tuple, list[str]] = {}
    exact_no_sheet: dict[tuple, list[str]] = {}
    for nid, fp in new_fps.items():
        if fp["pattern"]:
            exact_with_sheet.setdefault(_exact_key(fp, with_sheet=True), []).append(nid)
            exact_no_sheet.setdefault(_exact_key(fp, with_sheet=False), []).append(nid)

    restored: list[dict] = []
    leftovers: list[dict] = []
    taken: set[str] = set()

    def _tiebreak(candidates: list[str], snap_fp: dict) -> Optional[str]:
        """>1 exact-кандидатов: сузить по полному тексту, затем по листу."""
        snap_text = snap_fp.get("text") or ""
        by_text = [
            nid for nid in candidates
            if (new_fps[nid].get("text") or "") == snap_text
        ]
        pool = by_text or candidates
        if len(pool) == 1:
            return pool[0]
        snap_sheet = snap_fp.get("sheet") or ""
        by_sheet = [nid for nid in pool if new_fps[nid].get("sheet") == snap_sheet]
        pool = by_sheet or pool
        if len(pool) == 1:
            return pool[0]
        # истинные дубликаты (одинаковый текст И лист) — безопасно взять первый
        texts = {(new_fps[nid].get("text"), new_fps[nid].get("sheet")) for nid in pool}
        if len(texts) == 1:
            return sorted(pool)[0]
        return None  # различимые кандидаты — неоднозначность

    def _try_exact(snap_fp: dict) -> Optional[str]:
        """id | _AMBIGUOUS | _OCCUPIED | None (совпадений нет)."""
        if not snap_fp.get("pattern"):
            return None
        for index, _with_sheet in ((exact_with_sheet, True), (exact_no_sheet, False)):
            all_cands = index.get(_exact_key(snap_fp, with_sheet=_with_sheet), [])
            if not all_cands:
                continue
            free = [
                nid for nid in all_cands
                if nid not in taken and nid not in already_decided_ids
            ]
            if not free:
                # exact-цель существует, но уже занята решением — не отдаём fuzzy
                return _OCCUPIED
            if len(free) == 1:
                return free[0]
            chosen = _tiebreak(free, snap_fp)
            return chosen if chosen is not None else _AMBIGUOUS
        return None

    def _try_fuzzy(snap_fp: dict) -> tuple[Optional[str], float, float]:
        scored: list[tuple[float, str]] = []
        snap_text = snap_fp.get("text") or ""
        if not snap_text:
            return None, 0.0, 0.0
        snap_numbers = tuple(snap_fp.get("numbers") or ())
        snap_kind = snap_fp.get("kind") or ""
        for nid, fp in new_fps.items():
            if nid in taken or nid in already_decided_ids:
                continue
            # лист — мягкий гейт: если у обоих есть и различаются, пропускаем
            if snap_fp.get("sheet") and fp.get("sheet") and snap_fp["sheet"] != fp["sheet"]:
                continue
            # категория/тип — мягкий гейт (та же семантика)
            if snap_kind and fp.get("kind") and snap_kind != fp["kind"]:
                continue
            # значимые числа обязаны совпадать: «2,5 мм2» не матчится на «4 мм2»
            if tuple(fp.get("numbers") or ()) != snap_numbers:
                continue
            ratio = difflib.SequenceMatcher(
                None, snap_text, fp.get("text") or ""
            ).ratio()
            scored.append((ratio, nid))
        if not scored:
            return None, 0.0, 0.0
        scored.sort(reverse=True)
        best_ratio, best_id = scored[0]
        second_ratio = scored[1][0] if len(scored) > 1 else 0.0
        return best_id, best_ratio, second_ratio

    # ─── Фаза 1: exact для всех ────────────────────────────────────────────
    pending: list[dict] = []
    for snap in snapshot_items:
        fp = snap.get("fingerprint") or {}
        old_id = str(snap.get("old_id") or "")
        outcome = _try_exact(fp)
        if outcome == _AMBIGUOUS:
            leftovers.append(
                {"snapshot_item": snap, "reason": "ambiguous_exact", "old_id": old_id}
            )
        elif outcome == _OCCUPIED:
            leftovers.append(
                {"snapshot_item": snap, "reason": "target_already_decided",
                 "old_id": old_id}
            )
        elif outcome is not None:
            taken.add(outcome)
            restored.append(
                {"new_id": outcome, "snapshot_item": snap, "match": "exact", "score": 1.0}
            )
        else:
            pending.append(snap)

    # ─── Фаза 2: fuzzy для оставшихся ──────────────────────────────────────
    for snap in pending:
        fp = snap.get("fingerprint") or {}
        old_id = str(snap.get("old_id") or "")
        best_id, best_ratio, second_ratio = _try_fuzzy(fp)
        if best_id is None or best_ratio < FUZZY_RATIO_MIN:
            leftovers.append(
                {"snapshot_item": snap, "reason": "unmatched",
                 "best_ratio": round(best_ratio, 3), "old_id": old_id}
            )
            continue
        if best_ratio - second_ratio < FUZZY_MARGIN_MIN:
            leftovers.append(
                {"snapshot_item": snap, "reason": "ambiguous",
                 "best_ratio": round(best_ratio, 3),
                 "second_ratio": round(second_ratio, 3), "old_id": old_id}
            )
            continue
        taken.add(best_id)
        restored.append(
            {"new_id": best_id, "snapshot_item": snap, "match": "fuzzy",
             "score": round(best_ratio, 3)}
        )

    return restored, leftovers


# ═══════════════════════════════════════════════════════════════════════════
# IO-обёртки: снапшот и регидрация по project_id
# ═══════════════════════════════════════════════════════════════════════════

def _snapshot_path(project_id: str):
    from backend.app.services.knowledge_base import knowledge_base_service as kb
    return kb._review_path(project_id).parent / SNAPSHOT_FILENAME


def _report_path(project_id: str):
    from backend.app.services.knowledge_base import knowledge_base_service as kb
    return kb._review_path(project_id).parent / REPORT_FILENAME


def _pinned(version_id: Optional[str]):
    from backend.app.services.common import version_service
    if version_id:
        return version_service.pinned_version(version_id)
    import contextlib
    return contextlib.nullcontext()


def _load_fresh_item_maps(project_id: str) -> tuple[dict[str, dict], dict[str, dict]]:
    """Свежие items СТРОГО из 03_findings.json / optimization.json.

    Намеренно НЕ kb._load_source_item_maps: тот предпочитает
    03a_norms_verified.json, который на момент регидрации (сразу после
    findings_merge, до этапа норм) остался от ПРОШЛОГО прогона — матчинг
    против него привязал бы вердикты к устаревшим замечаниям.
    """
    from backend.app.services.knowledge_base import knowledge_base_service as kb

    findings_map: dict[str, dict] = {}
    opt_map: dict[str, dict] = {}
    for output_dir in kb._analysis_dirs(project_id):
        if not findings_map:
            fdata = kb._load_json(output_dir / "03_findings.json")
            if fdata:
                for item in fdata.get("findings", fdata.get("items", [])) or []:
                    if isinstance(item, dict) and item.get("id"):
                        findings_map[str(item["id"])] = item
        if not opt_map:
            odata = kb._load_json(output_dir / "optimization.json")
            if odata:
                for item in odata.get("items", []) or []:
                    if isinstance(item, dict) and item.get("id"):
                        opt_map[str(item["id"])] = item
        if findings_map and opt_map:
            break
    return findings_map, opt_map


def _applied_types(snapshot: dict) -> set[str]:
    applied = snapshot.get("applied_types")
    if isinstance(applied, list):
        return {str(t) for t in applied}
    # обратная совместимость со schema v1 (скалярный applied_at = всё применено)
    return set(ALL_ITEM_TYPES) if snapshot.get("applied_at") else set()


def snapshot_for_project(project_id: str, *, version_id: Optional[str] = None) -> dict:
    """Снять слепок решённых вердиктов ПЕРЕД удалением 03_findings.json.

    Merge-семантика:
      - findings уже удалены (например, после «Очистить») → существующий
        слепок НЕ затирается (kept_existing);
      - существующий слепок ещё не применён полностью (крах между merge и
        регидрацией) → НЕ перезаписывается (kept_unapplied): текущая пара
        review↔findings в переходном состоянии и снимать с неё слепок нельзя.
    """
    if not is_enabled():
        return {"status": "disabled"}

    from backend.app.services.common.atomic_json import atomic_write_json
    from backend.app.services.knowledge_base import knowledge_base_service as kb

    with _pinned(version_id):
        snap_path = _snapshot_path(project_id)
        if snap_path.exists():
            try:
                import json as _json
                existing = _json.loads(snap_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                existing = None
            if isinstance(existing, dict) and (existing.get("items") or []):
                missing = set(ALL_ITEM_TYPES) - _applied_types(existing)
                snapped_types = {
                    str(i.get("item_type") or "finding")
                    for i in existing["items"] if isinstance(i, dict)
                }
                if missing & snapped_types:
                    return {"status": "kept_unapplied"}

        review = kb.load_expert_review(project_id) or {}
        decisions = [
            d for d in (review.get("decisions") or [])
            if isinstance(d, dict) and str(d.get("decision") or "").strip()
        ]
        if not decisions:
            return {"status": "no_decisions"}

        findings_map, opt_map = _load_fresh_item_maps(project_id)
        maps = {"finding": findings_map, "optimization": opt_map}

        items = []
        for d in decisions:
            item_type = str(d.get("item_type") or "finding")
            source = maps.get(item_type, {}).get(str(d.get("item_id") or ""))
            if not source:
                continue  # замечания уже нет — фингерпринт снять не с чего
            items.append({
                "old_id": str(d.get("item_id")),
                "item_type": item_type,
                "decision": str(d.get("decision")),
                "rejection_reason": d.get("rejection_reason"),
                "reviewer": d.get("reviewer") or "",
                "timestamp": d.get("timestamp") or "",
                "carried_over": bool(d.get("carried_over")),
                "fingerprint": build_fingerprint(source, item_type),
            })

        if not items:
            # findings уже удалены/пусты — сохраняем ранее снятый слепок
            return {"status": "kept_existing" if snap_path.exists() else "nothing_to_snapshot"}

        atomic_write_json(snap_path, {
            "schema_version": SCHEMA_VERSION,
            "project_id": project_id,
            "version_id": version_id,
            "created_at": _now_iso(),
            "applied_types": [],
            "items": items,
        })
        return {"status": "ok", "items": len(items), "path": str(snap_path)}


def rehydrate_for_project(
    project_id: str,
    *,
    version_id: Optional[str] = None,
    item_types: Iterable[str] = ("finding",),
) -> dict:
    """Восстановить вердикты на новые ID ПОСЛЕ регенерации артефактов.

    item_types: какие типы обрабатывать в этом проходе. findings регенерируются
    на findings_merge, optimization — на этапе 05, поэтому вызовов два.
    """
    if not is_enabled():
        return {"status": "disabled"}

    import json as _json

    from backend.app.models.expert_review import ExpertDecision
    from backend.app.services.common.atomic_json import atomic_write_json
    from backend.app.services.knowledge_base import knowledge_base_service as kb

    wanted = [t for t in item_types if t in ALL_ITEM_TYPES]
    if not wanted:
        return {"status": "no_item_types"}

    with _pinned(version_id):
        snap_path = _snapshot_path(project_id)
        if not snap_path.exists():
            return {"status": "no_snapshot"}
        try:
            snapshot = _json.loads(snap_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"status": "snapshot_unreadable"}

        applied = _applied_types(snapshot)
        todo = [t for t in wanted if t not in applied]
        if not todo:
            return {"status": "already_applied"}

        snap_items = [
            i for i in (snapshot.get("items") or [])
            if isinstance(i, dict) and str(i.get("item_type") or "finding") in todo
        ]

        findings_map, opt_map = _load_fresh_item_maps(project_id)
        maps = {"finding": findings_map, "optimization": opt_map}

        review = kb.load_expert_review(project_id) or {}

        # Реконсиляция stale-решений: записи, чей item_id взят из слепка И чьё
        # содержимое совпадает со слепком, — это решения ПРОШЛОГО прогона.
        # После перенумерации они висят на чужих новых ID и блокируют free_ids.
        # Свежая ручная разметка (другой timestamp/decision) стирается НЕ будет.
        snap_by_key = {
            (str(s.get("item_type") or "finding"), str(s.get("old_id") or "")): s
            for s in snap_items
        }
        stale_ids: list[str] = []
        decided_by_type: dict[str, set[str]] = {t: set() for t in ALL_ITEM_TYPES}
        for d in review.get("decisions") or []:
            if not (isinstance(d, dict) and str(d.get("decision") or "").strip()):
                continue
            d_type = str(d.get("item_type") or "finding")
            d_id = str(d.get("item_id") or "")
            snap_match = snap_by_key.get((d_type, d_id))
            if (
                snap_match is not None
                and str(d.get("decision")) == str(snap_match.get("decision"))
                and str(d.get("timestamp") or "") == str(snap_match.get("timestamp") or "")
            ):
                stale_ids.append(d_id)
                continue  # НЕ считаем занятым: слот освобождается реконсиляцией
            decided_by_type.setdefault(d_type, set()).add(d_id)

        all_restored: list[dict] = []
        all_leftovers: list[dict] = []
        for item_type in todo:
            part = [i for i in snap_items if str(i.get("item_type") or "finding") == item_type]
            if not part:
                continue
            new_items = maps.get(item_type) or {}
            if not new_items:
                all_leftovers.extend(
                    {"snapshot_item": s, "reason": "no_new_items",
                     "old_id": s.get("old_id")} for s in part
                )
                continue
            restored, leftovers = match_snapshot_to_items(
                part, new_items, item_type, decided_by_type.get(item_type, set()),
            )
            all_restored.extend(restored)
            all_leftovers.extend(leftovers)

        shadow = is_shadow()
        saved = 0
        if (all_restored or stale_ids) and not shadow:
            decisions = []
            for r in all_restored:
                snap = r["snapshot_item"]
                decisions.append(ExpertDecision(
                    item_id=r["new_id"],
                    item_type=snap["item_type"],
                    decision=snap["decision"],
                    rejection_reason=snap.get("rejection_reason"),
                    reviewer=snap.get("reviewer") or "",
                    timestamp=_now_iso(),
                    carried_over=True,
                    carried_from_version=snapshot.get("version_id"),
                    carried_from_item_id=snap.get("old_id"),
                ))
            result = kb.save_expert_review(
                project_id, decisions,
                reviewer="Авто-восстановление вердиктов после переаудита",
                removed_ids=sorted(set(stale_ids)),
                stamp_schedule=False,
            )
            saved = int(result.get("saved", 0))

        report = {
            "schema_version": SCHEMA_VERSION,
            "project_id": project_id,
            "version_id": version_id,
            "item_types": todo,
            "shadow": shadow,
            "checked_at": _now_iso(),
            "snapshot_created_at": snapshot.get("created_at"),
            "summary": {
                "snapshot_items": len(snap_items),
                "restored": len(all_restored),
                "restored_exact": sum(1 for r in all_restored if r["match"] == "exact"),
                "restored_fuzzy": sum(1 for r in all_restored if r["match"] == "fuzzy"),
                "unmatched": sum(1 for l in all_leftovers if l["reason"] == "unmatched"),
                "ambiguous": sum(
                    1 for l in all_leftovers
                    if l["reason"] in ("ambiguous", "ambiguous_exact")
                ),
                "target_already_decided": sum(
                    1 for l in all_leftovers if l["reason"] == "target_already_decided"
                ),
                "stale_removed": len(set(stale_ids)),
                "saved": saved,
            },
            "restored": [
                {"old_id": r["snapshot_item"].get("old_id"), "new_id": r["new_id"],
                 "match": r["match"], "score": r["score"],
                 "decision": r["snapshot_item"].get("decision")}
                for r in all_restored
            ],
            "leftovers": [
                {k: v for k, v in l.items() if k != "snapshot_item"}
                | {"decision": (l.get("snapshot_item") or {}).get("decision"),
                   "problem_head": ((l.get("snapshot_item") or {}).get("fingerprint") or {}).get("text", "")[:160]}
                for l in all_leftovers
            ],
        }
        # отчёт по проходам не затирается: раздельные секции per item_types
        prev_report = None
        report_path = _report_path(project_id)
        if report_path.exists():
            try:
                prev_report = _json.loads(report_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                prev_report = None
        if isinstance(prev_report, dict) and prev_report.get("passes"):
            passes = prev_report["passes"]
        else:
            passes = []
        passes = [p for p in passes if set(p.get("item_types") or []) != set(todo)]
        passes.append(report)
        atomic_write_json(report_path, {"passes": passes})

        if not shadow:
            snapshot["applied_types"] = sorted(_applied_types(snapshot) | set(todo))
            atomic_write_json(snap_path, snapshot)
        return {"status": "ok", **report["summary"]}
