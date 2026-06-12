# -*- coding: utf-8 -*-
"""Pipeline V2 — offline diagnostics: runtime artifact root audit.

Назначение
----------
Перед любой будущей runtime-write задачей (controlled enforce/skip, зеркалирование
отчётов) нужно ТОЧНО знать, какой ``comparison/`` root реально читает работающий
production backend и нет ли рассинхрона между worktree'ами:

    /home/coder/projects/PDF-proverka/comparison          (main worktree)
    /home/coder/projects/PDF-proverka-deploy/comparison   (deploy worktree)

Production backend резолвит comparison root так (см. ``paths.comparison_root_path``
→ ``config.ROOT_DIR``):

    COMPARISON_ROOT (env)  ─или─  AUDIT_ROOT_DIR/AUDIT_BASE_DIR (env)/comparison
                           ─или─  autodetect backend/../../comparison

Если ни одна env не задана, root = worktree, из которого запущен код. Поэтому
deploy-сервер читает ``PDF-proverka-deploy/comparison``, а не main worktree —
именно из-за этого в прошлой задаче пришлось зеркалировать
``skip_readiness_report.json`` в deploy worktree.

Гарантии
--------
* **read-only**: модуль НИЧЕГО не пишет в ``comparison/`` и не создаёт директорий
  под artifact roots (резолв путей без ``mkdir``);
* **offline**: не импортирует и не вызывает модели/джобы/LLM-runner'ы; сетевых
  запросов не делает — ``/api/info`` подаётся снаружи как готовый dict
  (``api_info=...``), модуль его только интерпретирует;
* **fail-soft**: битый/недоступный файл не роняет audit — он отражается в записи
  artifact'а (``exists=false`` + причина), а не как исключение наружу;
* **path traversal** отклоняется (``_safe_id``), как в read-only payload-сервисе.

Единственный разрешённый write этой задачи — diagnostics-артефакты в
``diagnostics_pipeline_v2/...`` — выполняется отдельным helper-скриптом, НЕ этим
модулем.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

AUDIT_KIND = "stage_comparison_pipeline_v2_runtime_root_audit"
AUDIT_VERSION = 1
PIPELINE_V2_DIRNAME = "pipeline_v2"

# Канонические worktree-роуты (можно переопределить аргументом ``roots=``).
MAIN_COMPARISON_ROOT = "/home/coder/projects/PDF-proverka/comparison"
DEPLOY_COMPARISON_ROOT = "/home/coder/projects/PDF-proverka-deploy/comparison"

DEFAULT_ROOTS: tuple[tuple[str, str], ...] = (
    ("main_worktree", MAIN_COMPARISON_ROOT),
    ("deploy_worktree", DEPLOY_COMPARISON_ROOT),
)

# Минимальный набор Pipeline V2 артефактов для сверки (см. задачу).
DEFAULT_ARTIFACT_NAMES: tuple[str, ...] = (
    "entity_diff_report.json",
    "entity_alignment_preview_report.json",
    "entity_mapping_overrides.json",
    "link_validation_report.json",
    "exclusion_preview_v2_report.json",
    "exclusion_review_overrides.json",
    "skip_readiness_report.json",
    "block_link_preview_report.json",
    "visual_equivalence_gate_report.json",
    "graphic_vision_enrichment_report.json",
    "graphic_vision_grounding_report.json",
    "grounded_evidence_report.json",
    "delta_explanation_report.json",
    "pipeline_v2_ui_payload.json",
    "pipeline_v2_summary.json",
    "pipeline_v2_summary.md",
    "pipeline_v2_manifest.json",
)

_HASH_CHUNK = 1 << 16  # 64 KiB


# ─── helpers ─────────────────────────────────────────────────────────────────


def _safe_id(value: str) -> str:
    """Отклонить path traversal / пустой id (как в paths._safe_id)."""
    safe = "".join(c for c in (value or "") if c.isalnum() or c in "-_")
    if not safe or safe != (value or ""):
        raise ValueError(f"invalid id: {value!r}")
    return safe


def _iso_mtime(path: Path) -> Optional[str]:
    try:
        ts = path.stat().st_mtime
    except OSError:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _sha256_file(path: Path) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """(sha256_hex, size_bytes, error) — read-only, никогда не бросает наружу."""
    try:
        size = path.stat().st_size
        h = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(_HASH_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest(), size, None
    except Exception as exc:  # noqa: BLE001 — fail-soft по контракту
        return None, None, f"{type(exc).__name__}: {exc}"


def _artifact_record(art_dir: Path, name: str) -> dict[str, Any]:
    """Запись одного артефакта в одном root'е (без mkdir, без записи)."""
    path = art_dir / name
    rec: dict[str, Any] = {
        "name": name,
        "exists": False,
        "size": None,
        "mtime": None,
        "sha256": None,
    }
    try:
        if not path.is_file():
            return rec
    except OSError as exc:
        rec["error"] = f"{type(exc).__name__}: {exc}"
        return rec
    sha, size, err = _sha256_file(path)
    rec["exists"] = err is None
    rec["size"] = size
    rec["mtime"] = _iso_mtime(path)
    rec["sha256"] = sha
    if err:
        rec["error"] = err
        rec["exists"] = False
    return rec


def pair_pipeline_v2_path(comparison_root: Path, session_id: str,
                          pair_id: Optional[str]) -> Path:
    """Каталог pipeline_v2 для пары (или сессии) внутри данного root'а, без mkdir."""
    base = comparison_root / "sessions" / _safe_id(session_id)
    if pair_id:
        return base / "pairs" / _safe_id(pair_id) / PIPELINE_V2_DIRNAME
    return base / PIPELINE_V2_DIRNAME


def _root_record(name: str, comparison_root: str, session_id: str,
                 pair_id: Optional[str],
                 artifact_names: tuple[str, ...]) -> dict[str, Any]:
    root_path = Path(comparison_root).expanduser()
    try:
        root_path = root_path.resolve()
    except OSError:
        pass
    art_dir = pair_pipeline_v2_path(root_path, session_id, pair_id)
    rec: dict[str, Any] = {
        "name": name,
        "comparison_root": str(root_path),
        "pair_pipeline_v2_path": str(art_dir),
        "exists": False,
        "artifact_count": 0,
        "artifacts": [],
    }
    try:
        rec["comparison_root_exists"] = root_path.is_dir()
        rec["exists"] = art_dir.is_dir()
    except OSError as exc:
        rec["error"] = f"{type(exc).__name__}: {exc}"
        return rec
    artifacts = [_artifact_record(art_dir, n) for n in artifact_names]
    rec["artifacts"] = artifacts
    rec["artifact_count"] = sum(1 for a in artifacts if a.get("exists"))
    return rec


def _compare_roots(roots: list[dict[str, Any]],
                   artifact_names: tuple[str, ...]) -> dict[str, Any]:
    """Сверить набор файлов и хэши между всеми существующими root'ами."""
    present_roots = [r for r in roots if r.get("exists")]
    differences: list[dict[str, Any]] = []
    same_file_set = True
    same_hashes = True

    # карта name -> {root_name -> artifact_record}
    by_name: dict[str, dict[str, dict[str, Any]]] = {n: {} for n in artifact_names}
    for r in present_roots:
        for a in r.get("artifacts", []):
            by_name.setdefault(a["name"], {})[r["name"]] = a

    for name in artifact_names:
        per_root = by_name.get(name, {})
        existing = {rn: a for rn, a in per_root.items() if a.get("exists")}
        present_in = sorted(existing.keys())
        all_root_names = sorted(r["name"] for r in present_roots)

        # file-set mismatch: артефакт есть не во всех (но хотя бы в одном) root'ах
        if existing and len(present_in) != len(all_root_names):
            same_file_set = False
            differences.append({
                "name": name,
                "kind": "missing_in_root",
                "present_in": present_in,
                "missing_in": [rn for rn in all_root_names if rn not in present_in],
            })
            continue

        if len(existing) < 2:
            # нечего сравнивать по хэшу (0 или 1 копия)
            continue

        hashes = {rn: a.get("sha256") for rn, a in existing.items()}
        distinct = set(hashes.values())
        if len(distinct) > 1:
            same_hashes = False
            differences.append({
                "name": name,
                "kind": "hash_mismatch",
                "hashes": hashes,
                "sizes": {rn: a.get("size") for rn, a in existing.items()},
                "mtimes": {rn: a.get("mtime") for rn, a in existing.items()},
            })

    return {
        "roots_compared": [r["name"] for r in present_roots],
        "same_file_set": same_file_set,
        "same_hashes": same_hashes,
        "differences": differences,
    }


def _backend_comparison_root() -> Optional[str]:
    """Лучший offline-резолв активного root'а через backend path-helper.

    Лениво, в try/except: модуль должен импортироваться и тестироваться без
    рабочего backend-окружения. Никаких моделей/джоб не импортирует.
    """
    try:
        from backend.app.services.stage_comparison.paths import comparison_root_path
        return str(comparison_root_path())
    except Exception:  # noqa: BLE001
        return None


def detect_active_runtime_root(roots: list[dict[str, Any]], *,
                               api_info: Optional[dict[str, Any]] = None
                               ) -> dict[str, Any]:
    """Определить, какой comparison root реально активен для production backend.

    Источники evidence (по убыванию надёжности):
      1. ``api_info["base_dir"]`` от живого ``/api/info`` → ``base_dir/comparison``;
      2. env ``COMPARISON_ROOT``;
      3. env ``AUDIT_ROOT_DIR`` / ``AUDIT_BASE_DIR`` → ``.../comparison``;
      4. backend path-helper ``comparison_root_path()`` (текущий процесс).
    """
    evidence: list[dict[str, Any]] = []
    candidate: Optional[str] = None
    confidence = "low"

    root_by_path = {r["comparison_root"]: r["name"] for r in roots}

    def _norm(p: str) -> str:
        try:
            return str(Path(p).expanduser().resolve())
        except OSError:
            return p

    # 1. /api/info base_dir
    if isinstance(api_info, dict):
        base_dir = api_info.get("base_dir")
        if isinstance(base_dir, str) and base_dir:
            comp = _norm(str(Path(base_dir) / "comparison"))
            evidence.append({
                "source": "api_info.base_dir",
                "value": base_dir,
                "implied_comparison_root": comp,
            })
            candidate = comp
            confidence = "high"

    # 2. COMPARISON_ROOT env
    env_comp = os.environ.get("COMPARISON_ROOT", "").strip()
    if env_comp:
        comp = _norm(env_comp)
        evidence.append({"source": "env.COMPARISON_ROOT", "value": env_comp,
                         "implied_comparison_root": comp})
        if candidate is None:
            candidate = comp
            confidence = "high"

    # 3. AUDIT_ROOT_DIR / AUDIT_BASE_DIR env
    for env_name in ("AUDIT_ROOT_DIR", "AUDIT_BASE_DIR"):
        val = os.environ.get(env_name, "").strip()
        if val:
            comp = _norm(str(Path(val) / "comparison"))
            evidence.append({"source": f"env.{env_name}", "value": val,
                             "implied_comparison_root": comp})
            if candidate is None:
                candidate = comp
                confidence = "medium"

    # 4. backend path-helper в текущем процессе
    helper = _backend_comparison_root()
    if helper:
        comp = _norm(helper)
        evidence.append({"source": "backend.comparison_root_path",
                         "implied_comparison_root": comp})
        if candidate is None:
            candidate = comp
            confidence = "medium"

    matched_root_name = root_by_path.get(candidate) if candidate else None
    # сверим candidate с известными root'ами для дружелюбного имени
    if candidate and matched_root_name is None:
        for rp, rn in root_by_path.items():
            if _norm(rp) == candidate:
                matched_root_name = rn
                break

    return {
        "detected": candidate,
        "detected_root_name": matched_root_name,
        "confidence": confidence if candidate else "unknown",
        "evidence": evidence,
    }


def _build_recommendations(roots: list[dict[str, Any]],
                           comparison: dict[str, Any],
                           active: dict[str, Any]) -> list[str]:
    recs: list[str] = []
    active_path = active.get("detected")
    active_name = active.get("detected_root_name")
    if active_path:
        label = f"{active_name} ({active_path})" if active_name else active_path
        recs.append(
            f"Source-of-truth для будущих runtime-write задач = АКТИВНЫЙ root: {label}. "
            "Писать в него (с бэкапом), а не только в main worktree."
        )
    else:
        recs.append(
            "Активный root не определён уверенно — НЕ выполнять runtime-write, "
            "пока не подтверждён реальный comparison root backend'а через /api/info."
        )

    if not comparison.get("same_file_set", True):
        recs.append(
            "Рассинхрон НАБОРА файлов между worktree'ами: часть артефактов есть "
            "не во всех root'ах. Перед enforce синхронизировать только нужную "
            "пару и только с явным отчётом о зеркалировании."
        )
    if not comparison.get("same_hashes", True):
        recs.append(
            "HASH-MISMATCH между worktree'ами: одноимённые артефакты различаются. "
            "Это критично — backend и main worktree видят РАЗНОЕ содержимое. "
            "Определить, какая версия каноническая, до любого enforce."
        )
    if comparison.get("same_file_set", True) and comparison.get("same_hashes", True):
        if len([r for r in roots if r.get("exists")]) >= 2:
            recs.append(
                "Worktree'ы согласованы (одинаковый набор и хэши) — зеркалирование "
                "не требуется для проверенной пары."
            )

    # активный root не существует / пуст
    for r in roots:
        if active_path and r.get("comparison_root") == active_path and not r.get("exists"):
            recs.append(
                f"ВНИМАНИЕ: активный root '{r['name']}' не содержит pipeline_v2 "
                "артефактов для этой пары — endpoint вернёт not_found. Зеркалировать "
                "отчёт в активный root перерд проверкой UI."
            )
    return recs


# ─── публичный API ───────────────────────────────────────────────────────────


def build_runtime_root_audit(session_id: str,
                             pair_id: Optional[str] = None,
                             *,
                             roots: Optional[list[tuple[str, str]]] = None,
                             api_info: Optional[dict[str, Any]] = None,
                             artifact_names: Optional[tuple[str, ...]] = None
                             ) -> dict[str, Any]:
    """Собрать diagnostics-отчёт сверки Pipeline V2 artifact roots.

    Read-only. Возвращает dict по схеме
    ``stage_comparison_pipeline_v2_runtime_root_audit``. Никогда не пишет на диск.

    :param session_id: id сессии (path traversal отклоняется).
    :param pair_id: id пары; ``None`` → session-level pipeline_v2 каталог.
    :param roots: список ``(name, comparison_root)``; по умолчанию main+deploy.
    :param api_info: опц. dict от ``/api/info`` (для detect active root).
    :param artifact_names: набор проверяемых артефактов; по умолчанию 17 штук.
    """
    # path traversal → ValueError наружу (как в payload-сервисе)
    _safe_id(session_id)
    if pair_id:
        _safe_id(pair_id)

    root_specs = roots if roots is not None else list(DEFAULT_ROOTS)
    names = artifact_names if artifact_names is not None else DEFAULT_ARTIFACT_NAMES

    root_records = [
        _root_record(name, comp_root, session_id, pair_id, names)
        for name, comp_root in root_specs
    ]
    comparison = _compare_roots(root_records, names)
    active = detect_active_runtime_root(root_records, api_info=api_info)
    recommendations = _build_recommendations(root_records, comparison, active)

    return {
        "version": AUDIT_VERSION,
        "kind": AUDIT_KIND,
        "status": "ok",
        "session_id": session_id,
        "pair_id": pair_id,
        "checked_artifact_names": list(names),
        "roots": root_records,
        "comparison": comparison,
        "active_runtime_root": active,
        "recommendations": recommendations,
    }


__all__ = [
    "AUDIT_KIND",
    "AUDIT_VERSION",
    "DEFAULT_ROOTS",
    "DEFAULT_ARTIFACT_NAMES",
    "MAIN_COMPARISON_ROOT",
    "DEPLOY_COMPARISON_ROOT",
    "pair_pipeline_v2_path",
    "detect_active_runtime_root",
    "build_runtime_root_audit",
]
