"""Экспертная оценка расхождений в сессии «Сравнение стадий».

Хранение — `comparison/sessions/<sid>/expert_review.json`. Ключ — СОСТАВНОЙ
`<pair_id>::<raw_id>`, где `raw_id` — стабильный id расхождения из
`unified_findings.json` (chg_… либо uf_…), а `pair_id` — PDF-пара, в контексте
которой эксперт принял решение. Решение по группе в UI агрегируется из
составных ключей её `source_finding_ids` × пары — это переживает регруппировку
(group_id меняется, raw id — нет) И не «протекает» между парами: одинаковые
штамповые id (chg_customer, chg_stamp_org …) в разных парах больше не делят
один вердикт.

Schema:
{
  "version": 2,
  "updated_at": "<iso>",
  "decisions": {
    "<pair_id>::<raw_id>": {
      "decision": "accepted" | "rejected",
      "rejection_reason": "" | "...",
      "reviewer": "",
      "timestamp": "<iso>"
    }
  }
}

Миграция v1→v2: старый файл хранил голый `<raw_id>` без привязки к паре.
`_maybe_migrate` восстанавливает пару через `build_unified_flat`: id,
встречающийся ровно в одной паре, привязывается однозначно; id, общий для
нескольких пар (12 штамповых на сессию Балчуг), сбрасывается — его нельзя
достоверно привязать, эксперт переразмечает заново.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from . import paths as paths_mod

VERSION = 2
_KEY_SEP = "::"
_lock = threading.RLock()


def make_key(pair_id: str, raw_id: str) -> str:
    """Составной ключ решения `<pair_id>::<raw_id>`."""
    return f"{str(pair_id).strip()}{_KEY_SEP}{str(raw_id).strip()}"


def _is_legacy_key(key: str) -> bool:
    """v1-ключ — голый raw_id без привязки к паре (нет `::`)."""
    return _KEY_SEP not in str(key)


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _empty_payload() -> dict:
    return {"version": VERSION, "updated_at": None, "decisions": {}}


def _iter_session_pair_changes(session_id: str):
    """Дёшево перечислить `(pair_id, raw_id)` по всем 'done'-парам сессии.

    Читает только `comparison_result.json` каждой пары — БЕЗ дорогого
    location-резолва и загрузки alignment (как в `build_unified_flat`).
    Поэтому годится для частых вызовов: колонка «Проверено экспертом» и
    миграция. `raw_id` может быть пустым (тогда change в UI получает
    синтетический uf_-id и всё равно не имеет экспертного решения).

    Набор пар/статусов согласован с `build_unified_flat`: items даёт только
    пара со `status == "done"`, по одному на каждый change.
    """
    from . import store as store_mod
    from . import enriched_comparison as enriched_mod

    session = store_mod.get_session(session_id)
    if not session:
        return
    for pair in session.get("pairs") or []:
        if not isinstance(pair, dict):
            continue
        if pair.get("status") == "disabled":
            continue
        pid = str(pair.get("id") or "")
        if not pid:
            continue
        result = enriched_mod.get_comparison_result(session_id, pid)
        if result is None or str(result.get("status") or "") != "done":
            continue
        for ch in (result.get("changes") or []):
            if not isinstance(ch, dict):
                continue
            yield pid, str(ch.get("id") or "").strip()


def _maybe_migrate(session_id: str, data: dict) -> dict:
    """v1→v2: привязать голые raw_id к паре по comparison_result'ам.

    Срабатывает один раз: пока есть хоть один legacy-ключ (без `::`), строим
    map `raw_id → {pair_ids}` и переписываем хранилище. Уникальный raw_id →
    `<pair>::<raw>`; общий для >1 пары — сбрасывается (нельзя достоверно
    привязать); не найденный — выкидывается (он всё равно не совпал бы с
    составным lookup'ом и не «протекает»).

    Тяжёлой работы при отсутствии legacy-ключей не делается.
    """
    decisions = data.get("decisions") or {}
    legacy_keys = [k for k in decisions if _is_legacy_key(k)]
    if not legacy_keys:
        return data

    raw_to_pairs: dict[str, set[str]] = {}
    try:
        for pid, rid in _iter_session_pair_changes(session_id):
            if rid and pid:
                raw_to_pairs.setdefault(rid, set()).add(pid)
    except Exception:  # noqa: BLE001 — миграция не должна ронять чтение
        return data

    new_store: dict = {}
    # Сначала уже-составные ключи (если файл частично мигрирован).
    for k, v in decisions.items():
        if not _is_legacy_key(k):
            new_store[k] = v
    # Затем legacy-ключи.
    for raw in legacy_keys:
        pairs = raw_to_pairs.get(raw)
        if pairs and len(pairs) == 1:
            new_store[make_key(next(iter(pairs)), raw)] = decisions[raw]
        # len > 1 (общий id) — сбрасываем; не найден в flat — выкидываем.

    data["decisions"] = new_store
    data["version"] = VERSION
    data["updated_at"] = _utc_now()
    data["migrated_pair_scoped"] = True
    try:
        _atomic_write_json(paths_mod.expert_review_path(session_id), data)
    except OSError:
        pass
    return data


def load(session_id: str) -> dict:
    """Прочитать решения; вернуть пустую структуру если файла нет."""
    path = paths_mod.expert_review_path(session_id)
    if not path.exists():
        return _empty_payload()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_payload()
    if not isinstance(data, dict):
        return _empty_payload()
    data.setdefault("version", VERSION)
    data.setdefault("decisions", {})
    if not isinstance(data["decisions"], dict):
        data["decisions"] = {}
    return _maybe_migrate(session_id, data)


def _summary(decisions: dict) -> dict:
    accepted = 0
    rejected = 0
    for entry in decisions.values():
        if not isinstance(entry, dict):
            continue
        d = (entry.get("decision") or "").lower()
        if d == "accepted":
            accepted += 1
        elif d == "rejected":
            rejected += 1
    return {"accepted": accepted, "rejected": rejected, "total": accepted + rejected}


def _per_pair_status(session_id: str, decisions: dict) -> dict:
    """Для каждой PDF-пары: сколько расхождений всего и сколько с решением.

    `fully_verified=True`, только если у пары есть хотя бы одно расхождение и
    КАЖДОЕ из них получило решение эксперта (accepted/rejected). Это сигнал для
    колонки «Проверено экспертом» на этапе «1. Загрузка документации».

    Счёт ведётся по дешёвому `_iter_session_pair_changes` (только чтение
    comparison_result'ов), а не по `build_unified_flat` с location-резолвом —
    иначе колонка грузилась минутами на больших сессиях. Набор пар/changes
    согласован с `build_unified_flat`, поэтому счётчики совпадают с вкладкой
    «3. Расхождения».
    """
    # Ключи хранятся как `<pair_id>::<raw_id>`; сверяем по составному ключу,
    # чтобы решение в одной паре не засчитывалось другим парам с тем же raw_id.
    decided_keys = {
        str(key)
        for key, entry in (decisions or {}).items()
        if isinstance(entry, dict) and (entry.get("decision") or "").lower() in ("accepted", "rejected")
    }

    per_pair: dict[str, dict] = {}
    try:
        for pid, rid in _iter_session_pair_changes(session_id):
            if not pid:
                continue
            bucket = per_pair.setdefault(pid, {"total": 0, "decided": 0})
            bucket["total"] += 1
            if rid and make_key(pid, rid) in decided_keys:
                bucket["decided"] += 1
    except Exception:  # noqa: BLE001 — пара не должна падать из-за per-pair сводки
        return per_pair

    for bucket in per_pair.values():
        bucket["fully_verified"] = bucket["total"] > 0 and bucket["decided"] >= bucket["total"]
    return per_pair


def _prune_decisions(session_id: str, decisions: dict) -> tuple[dict, dict]:
    """Разделить решения на (kept, removed_orphans). НИКОГДА не вызывается с
    диска автоматически — только из явного `prune_orphans` (см. инцидент ниже).

    Orphan — составной ключ `<pid>::<raw>`, где пара `pid` СЕЙЧАС done и
    перечислена `_iter_session_pair_changes`, но `raw` уже не встречается среди
    её изменений (остался от прошлой регенерации сравнения — id'шники `chg_…`
    переgenerировались).

    КРИТИЧЕСКИЙ guard «zero-overlap» (после инцидента 2026-06-04, когда пара была
    стёрта целиком): если у пары НИ ОДНО сохранённое решение не совпадает с
    текущими id её changes, пара НЕ чистится — это сигнатура того, что
    comparison пары прямо сейчас регенерируется / id полностью переgenerированы
    (race), и стирать всю разметку пары недопустимо. Чистим пару только при
    ЧАСТИЧНОМ совпадении (есть и совпавшие, и пропавшие id) — тогда пропавшие
    действительно orphan'ы. Пары, которые не удалось перечислить (не done /
    удалена), и legacy-ключи (без `::`) не трогаем.
    """
    valid_by_pair: dict[str, set[str]] = {}
    try:
        for pid, rid in _iter_session_pair_changes(session_id):
            if not pid:
                continue
            valid_by_pair.setdefault(pid, set())
            if rid:
                valid_by_pair[pid].add(rid)
    except Exception:  # noqa: BLE001 — prune не должен ронять чтение
        return dict(decisions or {}), {}

    # Группируем сохранённые составные ключи по паре.
    stored_by_pair: dict[str, list[str]] = {}
    kept: dict = {}
    removed: dict = {}
    for key, entry in (decisions or {}).items():
        skey = str(key)
        if _is_legacy_key(skey):
            kept[skey] = entry           # legacy → миграция, не трогаем
            continue
        pid = skey.split(_KEY_SEP, 1)[0]
        stored_by_pair.setdefault(pid, []).append(skey)

    for pid, keys in stored_by_pair.items():
        valid_ids = valid_by_pair.get(pid)
        if not valid_ids:
            # Пара не перечислена ИЛИ дала 0 валидных id (не done / race) →
            # сохраняем всю разметку пары как есть.
            for k in keys:
                kept[k] = decisions[k]
            continue
        matched = [k for k in keys if k.split(_KEY_SEP, 1)[1] in valid_ids]
        if not matched:
            # zero-overlap guard: вероятно id полностью переgenerированы /
            # пара регенерируется прямо сейчас. НЕ стираем пару целиком.
            for k in keys:
                kept[k] = decisions[k]
            continue
        for k in keys:
            raw = k.split(_KEY_SEP, 1)[1]
            if raw in valid_ids:
                kept[k] = decisions[k]
            else:
                removed[k] = decisions[k]
    return kept, removed


def prune_orphans(session_id: str, dry_run: bool = False) -> dict:
    """Удалить решения по `raw_id`, которых больше нет в текущих changes пары.

    ЯВНОЕ действие (endpoint/скрипт), НЕ авто-триггер на чтении. Безопасно:
    чистит только пары, что сейчас done, перечислимы и имеют частичное
    совпадение id (см. zero-overlap guard в `_prune_decisions`). `dry_run=True`
    — только посчитать, ничего не писать. Возвращает отчёт.
    """
    with _lock:
        data = load(session_id)
        decisions = data.get("decisions") or {}
        kept, removed = _prune_decisions(session_id, decisions)
        if removed and not dry_run:
            data["decisions"] = kept
            data["updated_at"] = _utc_now()
            data["version"] = VERSION
            _atomic_write_json(paths_mod.expert_review_path(session_id), data)
        return {
            "session_id": session_id,
            "removed_count": len(removed),
            "removed_keys": sorted(removed.keys()),
            "kept_count": len(kept),
            "dry_run": bool(dry_run),
        }


def get_with_summary(session_id: str, include_pairs: bool = False) -> dict:
    # ВАЖНО: чтение НЕ мутирует хранилище. Orphan-решения не накручивают
    # счётчик за счёт скоупинга на фронте (только видимые changes), а реальная
    # чистка диска — отдельное явное действие `prune_orphans`. Авто-prune на
    # каждом GET был убран после инцидента 2026-06-04 (race с регенерацией пары
    # стёр разметку пары целиком).
    data = load(session_id)
    decisions = data.get("decisions") or {}
    out = {
        "session_id": session_id,
        "version": data.get("version") or VERSION,
        "updated_at": data.get("updated_at"),
        "decisions": decisions,
        "summary": _summary(decisions),
    }
    if include_pairs:
        out["per_pair"] = _per_pair_status(session_id, decisions)
    return out


def apply_batch(
    session_id: str,
    decisions: Iterable[dict],
    removed_ids: Optional[Iterable[str]] = None,
    reviewer: str = "",
) -> dict:
    """Записать пачку решений; вернуть итоговый payload + summary.

    Каждый элемент `decisions` должен содержать:
      - `item_id` — СОСТАВНОЙ ключ `<pair_id>::<raw_id>` (фронт строит его из
        активной пары; см. `make_key`)
      - `decision` ("accepted" | "rejected")
      - `rejection_reason` (optional)

    `removed_ids` — составные ключи, для которых решение нужно очистить.
    """
    with _lock:
        data = load(session_id)
        store = data.get("decisions") or {}
        now = _utc_now()
        applied = 0
        for raw in decisions or ():
            if not isinstance(raw, dict):
                continue
            item_id = str(raw.get("item_id") or "").strip()
            decision = str(raw.get("decision") or "").strip().lower()
            if not item_id or decision not in ("accepted", "rejected"):
                continue
            store[item_id] = {
                "decision": decision,
                "rejection_reason": str(raw.get("rejection_reason") or "")[:1000],
                "reviewer": reviewer or "",
                "timestamp": now,
            }
            applied += 1
        for rid in removed_ids or ():
            rid_s = str(rid or "").strip()
            if rid_s and rid_s in store:
                store.pop(rid_s, None)
        data["decisions"] = store
        data["updated_at"] = now
        data["version"] = VERSION
        _atomic_write_json(paths_mod.expert_review_path(session_id), data)
        return {
            "session_id": session_id,
            "applied": applied,
            "removed": len(list(removed_ids or [])),
            "decisions": store,
            "summary": _summary(store),
            "updated_at": now,
        }


def apply_transfer(session_id: str, transfers: Iterable[dict]) -> dict:
    """Применить перенос решений (из «Расхождений» в V2) с пометкой конфликтов.

    Каждый элемент `transfers`:
      - `key` — СОСТАВНОЙ ключ `<pair_id>::<raw_id>` цели (обычно v2_id);
      - `decision` ("accepted" | "rejected");
      - `rejection_reason` (optional);
      - `method` ("exact" | "semantic"), `source_raw_id`, `confidence` —
        метаданные переноса;
      - `needs_review` (bool) — флаг «проверить» для неуверенных совпадений.

    Политика конфликта — НЕ перезаписывать существующее решение: если у ключа
    уже есть вердикт, он сохраняется. Совпадающий вердикт считается
    «consistent», расходящийся — конфликтом (помечается на записи флагом
    `conflict` + историей `transfer_conflicts`). Идемпотентно: повторный
    перенос того же не плодит дублей.
    """
    with _lock:
        data = load(session_id)
        store = data.get("decisions") or {}
        now = _utc_now()
        applied = 0
        consistent = 0
        needs_review = 0
        conflicts: list[dict] = []
        for t in transfers or ():
            if not isinstance(t, dict):
                continue
            key = str(t.get("key") or "").strip()
            decision = str(t.get("decision") or "").strip().lower()
            if not key or decision not in ("accepted", "rejected"):
                continue
            existing = store.get(key)
            if isinstance(existing, dict) and (existing.get("decision") or "").lower() in ("accepted", "rejected"):
                if (existing.get("decision") or "").lower() == decision:
                    consistent += 1
                    continue
                # Расхождение вердиктов — конфликт. Существующее не трогаем.
                src_raw = str(t.get("source_raw_id") or "")
                conflicts.append({
                    "key": key,
                    "existing_decision": (existing.get("decision") or "").lower(),
                    "incoming_decision": decision,
                    "source_raw_id": src_raw,
                })
                existing["conflict"] = True
                hist = existing.setdefault("transfer_conflicts", [])
                entry_c = {"incoming_decision": decision, "source_raw_id": src_raw}
                if entry_c not in hist:
                    hist.append(entry_c)
                continue
            entry = {
                "decision": decision,
                "rejection_reason": str(t.get("rejection_reason") or "")[:1000],
                "reviewer": "transfer",
                "timestamp": now,
                "transferred": True,
                "transfer_method": str(t.get("method") or "exact"),
                "transfer_source_raw_id": str(t.get("source_raw_id") or ""),
                "transfer_confidence": round(float(t.get("confidence") or 0.0), 3),
            }
            if t.get("needs_review"):
                entry["needs_review"] = True
                needs_review += 1
            store[key] = entry
            applied += 1
        data["decisions"] = store
        data["updated_at"] = now
        data["version"] = VERSION
        _atomic_write_json(paths_mod.expert_review_path(session_id), data)
        return {
            "applied": applied,
            "consistent_existing": consistent,
            "needs_review": needs_review,
            "conflicts": conflicts,
            "total_decisions": len(store),
        }
