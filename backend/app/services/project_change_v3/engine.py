"""V3 production comparison entry — real map/mine/dedupe path, never legacy.

Fail-closed state machine (no new states beyond the existing V3 ones):

* RUNNING   — written once the provider gate admits the run;
* FAILED    — gate closed, source preparation, provider, validation, result
              persistence or any unexpected error;
* REVIEW    — ProjectChanges persisted, but unresolved hints exist or the
              Human Mapping publication failed;
* COMPLETED — ProjectChanges persisted and Human Mapping published.

Every caught exception is logged with session/pair/stage context.  A state
that cannot be persisted is returned as FAILED (``state_persisted=False``),
never as a silent COMPLETED.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import (
    DEDUPE_PROMPT,
    DEDUPE_SCHEMA,
    ENGINE_NAME,
    ENGINE_VERSION,
    MAPPER_PROMPT,
    MAP_SCHEMA,
    MINER_PROMPT,
    MINER_SCHEMA,
    SOURCE_PACKAGING_VERSION,
)
from .dedupe import apply_dedupe, compact_change
from .hm_builder import build_human_mapping_ui_data, materialize_hm_assets
from .provenance import build_provenance
from .provider import ProviderError, get_provider
from .provider_gate import check_provider_readiness
from .source_prep import (
    SourcePreparationError,
    load_page_record,
    mapping_images,
    optimized_region_bundle,
    prepare_comparison_sources,
)
from .validate import validate_map, validate_miner

logger = logging.getLogger(__name__)

RESULT_SCHEMA = "projectchange_v3_final/2"
UNAVAILABLE_RU = (
    "Сравнение проектов движком V3 недоступно: модель gpt-6-astra "
    "не готова или квота исчерпана. Результат не сгенерирован. "
    "Legacy (subject-first) не запускался."
)
KILL_SWITCH_RU = (
    "Движок сравнения проектов V3 выбран, но живой inference "
    "запрещён (PROJECT_COMPARISON_V3_ALLOW_INFERENCE!=1). "
    "Legacy не запускался."
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_state(
    session_id: str,
    pair_id: str,
    *,
    status: str,
    message: str,
    reason_code: str,
    provenance: dict[str, Any],
    extra: dict[str, Any] | None = None,
    model_calls: int = 0,
    run_id: str | None = None,
    started_at: str | None = None,
) -> dict[str, Any]:
    now = _now()
    state: dict[str, Any] = {
        "kind": "stage_comparison_production_state",
        "schema_version": 1,
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "run_id": run_id,
        "status": status,
        "progress": 100 if status in {"FAILED", "COMPLETED", "REVIEW"} else 50,
        "message": message,
        "reason_code": reason_code,
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "provenance": provenance,
        "technical_provenance": provenance,
        "started_at": started_at or now,
        "last_activity_at": now,
        "failed_at": now if status == "FAILED" else None,
        "completed_at": now if status in {"COMPLETED", "REVIEW"} else None,
        "current_stage": None,
        "current_substage": None,
        "model_calls": model_calls,
        "legacy_invoked": False,
        "state_persisted": True,
    }
    if extra:
        state.update(extra)
    from backend.app.services.stage_comparison import production_store

    try:
        production_store.save_artifact(session_id, pair_id, "state", state)
    except Exception as exc:  # noqa: BLE001 — never report an unsaved state as success
        logger.exception(
            "V3 state persistence failed: session=%s pair=%s status=%s", session_id, pair_id, status
        )
        state = {
            **state,
            "status": "FAILED",
            "progress": 100,
            "failed_at": now,
            "completed_at": None,
            "reason_code": "state_persistence_failed",
            "message": f"V3: состояние прогона не сохранено ({type(exc).__name__}); результат не публикуется.",
            "attempted_status": status,
            "attempted_reason_code": reason_code,
            "state_persisted": False,
        }
        try:
            production_store.save_artifact(session_id, pair_id, "state", state)
            state["state_persisted"] = True
        except Exception:  # noqa: BLE001
            logger.exception(
                "V3 FAILED state persistence also failed: session=%s pair=%s", session_id, pair_id
            )
    return state


def _save_artifact(session_id: str, pair_id: str, name: str, value: dict[str, Any]) -> None:
    """Persist one V3 artifact; failures propagate (no silent fallback path)."""
    from backend.app.services.stage_comparison import production_store

    production_store.save_artifact(session_id, pair_id, name, value)


def _resolve_pair_paths(session_id: str, pair_id: str) -> tuple[dict[str, Path], dict[str, Path], dict[str, Any]]:
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison.production_orchestrator import (
        _resolved_document_paths,
    )

    pair = store.get_pair_for_production(session_id, pair_id)
    left = _resolved_document_paths(pair.get("left") or {})
    right = _resolved_document_paths(pair.get("right") or {})
    return left, right, pair


def _work_dir(session_id: str, pair_id: str) -> Path:
    from backend.app.services.stage_comparison import paths

    d = paths.production_dir(session_id, pair_id) / "project_change_v3"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _source_identity(paths_by_side: dict[str, dict[str, Path]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for side, paths in paths_by_side.items():
        out[side] = {}
        for key in ("pdf", "blocks", "markdown"):
            path = Path(paths[key])
            stat = path.stat()
            out[side][key] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    return out


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _publish_human_mapping(
    *,
    session_id: str,
    pair_id: str,
    object_id: str | None,
    semantic_map: dict[str, Any],
    work_dir: Path,
    run_id: str,
) -> dict[str, Any]:
    """Build and publish comparison-scoped Human Mapping data (errors propagate)."""
    from backend.app.services.human_mapping_production import storage

    from .scope import object_id_for_session

    oid = object_id or object_id_for_session(session_id)
    hm_dir = storage.pair_dir(oid, pair_id, smoke=False)  # validates both IDs
    ui_data = build_human_mapping_ui_data(
        pair_id=pair_id, object_id=oid, semantic_map=semantic_map, work_dir=work_dir,
    )
    ui_data["session_id"] = session_id
    ui_data["run_id"] = run_id
    _atomic_write_json(work_dir / "HUMAN_MAPPING_UI_DATA.json", ui_data)
    materialize_hm_assets(work_dir, hm_dir / "assets")
    _atomic_write_json(hm_dir / "ui_data.json", ui_data)
    pointer = {
        "pair_id": pair_id,
        "object_id": oid,
        "session_id": session_id,
        "run_id": run_id,
        "region_count": len(ui_data.get("regions") or []),
        "review_region_count": ui_data.get("review_region_count", 0),
    }
    _save_artifact(session_id, pair_id, "project_change_v3_human_mapping_ui", pointer)
    return pointer


def _region_of_changes(mined_regions: list[dict[str, Any]]) -> dict[str, str]:
    return {
        change["projectchange_id"]: region["region_id"]
        for region in mined_regions
        for change in region.get("projectchanges") or []
    }


def run_v3_pipeline(
    *,
    session_id: str,
    pair_id: str,
    object_id: str | None = None,
    old_paths: dict[str, Path] | None = None,
    new_paths: dict[str, Path] | None = None,
    skip_provider_gate: bool = False,
) -> dict[str, Any]:
    """Execute source prep -> map -> mine -> dedupe -> persist -> publish.

    skip_provider_gate is for tests with FakeProvider only.
    """
    base_provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)
    run_id = uuid.uuid4().hex
    started_at = _now()
    from . import provider as provider_mod

    using_test_provider = provider_mod._test_provider is not None

    def state(
        status: str,
        message: str,
        reason: str,
        *,
        calls: int = 0,
        provenance: dict[str, Any] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        return _write_state(
            session_id, pair_id, status=status, message=message, reason_code=reason,
            provenance=provenance or base_provenance, extra=extra or None,
            model_calls=calls, run_id=run_id, started_at=started_at,
        )

    if not skip_provider_gate and not using_test_provider:
        gate = check_provider_readiness()
        if not gate.get("available"):
            closed = not gate.get("allow_inference")
            return state(
                "FAILED",
                KILL_SWITCH_RU if closed else UNAVAILABLE_RU,
                "v3_inference_kill_switch" if closed else "v3_provider_unavailable",
                provenance={**base_provenance, "provider_gate": gate},
            )

    running = state("RUNNING", "V3: подготовка источников", "v3_running")
    if running["status"] == "FAILED":
        return running

    try:
        return _run_admitted(
            session_id=session_id, pair_id=pair_id, object_id=object_id,
            old_paths=old_paths, new_paths=new_paths, run_id=run_id,
            using_test_provider=using_test_provider, state=state,
        )
    except _V3Failure as failure:
        return state("FAILED", failure.message, failure.reason,
                     calls=failure.model_calls, provenance=failure.provenance)
    except Exception as exc:  # noqa: BLE001 — a crash must never leave RUNNING
        logger.exception("V3 pipeline crashed: session=%s pair=%s run=%s", session_id, pair_id, run_id)
        return state("FAILED", f"V3: внутренняя ошибка ({type(exc).__name__})", "v3_internal_error")


class _V3Failure(Exception):
    def __init__(self, reason: str, message: str, model_calls: int, provenance: dict[str, Any] | None = None):
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.model_calls = model_calls
        self.provenance = provenance


def _run_admitted(
    *,
    session_id: str,
    pair_id: str,
    object_id: str | None,
    old_paths: dict[str, Path] | None,
    new_paths: dict[str, Path] | None,
    run_id: str,
    using_test_provider: bool,
    state,
) -> dict[str, Any]:
    calls = 0
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)

    def fail(reason: str, message: str, exc: BaseException | None = None) -> _V3Failure:
        if exc is not None:
            logger.error(
                "V3 %s: session=%s pair=%s run=%s: %s: %s",
                reason, session_id, pair_id, run_id, type(exc).__name__, exc,
            )
        return _V3Failure(reason, message, calls, provenance)

    # 1. Source preparation (resolution + packaging), fail-closed.
    try:
        if old_paths is None or new_paths is None:
            old_paths, new_paths, _pair = _resolve_pair_paths(session_id, pair_id)
        work_dir = _work_dir(session_id, pair_id)
        prepared = prepare_comparison_sources(
            pair_id=pair_id, old_paths=old_paths, new_paths=new_paths,
            work_dir=work_dir, object_id=object_id,
        )
        source_identity = _source_identity({"OLD": old_paths, "NEW": new_paths})
    except (SourcePreparationError, FileNotFoundError, KeyError, ValueError, OSError) as exc:
        raise fail("source_preparation_failed", f"V3: подготовка источников не удалась: {exc}", exc) from exc
    provenance = build_provenance(
        source_prep_version=prepared["source_packaging_version"],
        structure_sha256=prepared["structure_sha256"],
    )
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_source_manifest", prepared["manifest"])
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: манифест источников не сохранён", exc) from exc

    provider = get_provider()
    structure = prepared["structure"]

    # 2. Semantic mapping.
    try:
        semantic_map = provider.complete(
            stage="MAPPING", call_id=f"{pair_id}_SEMANTIC_MAPPING", pair_id=pair_id,
            prompt=MAPPER_PROMPT, data={"pair": pair_id, "pages": structure},
            schema=MAP_SCHEMA, images=mapping_images(structure),
        )
        calls += 0 if using_test_provider else 1
        validate_map(pair_id, semantic_map, structure)
    except ProviderError as exc:
        raise fail(exc.code, f"V3 Mapper failed: {exc.message}", exc) from exc
    except Exception as exc:  # noqa: BLE001
        raise fail("mapper_validation_failed", f"V3 Mapper validation failed: {exc}", exc) from exc
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_semantic_map", semantic_map)
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: семантическая карта не сохранена", exc) from exc

    pages_by_key = {
        (page["side"], page["physical_page"]): load_page_record(work_dir, page["side"], page["physical_page"])
        for page in structure
    }

    # 3. Mining per semantic region.
    mined_regions: list[dict[str, Any]] = []
    all_changes: list[dict[str, Any]] = []
    all_hints: list[dict[str, Any]] = []
    for region in semantic_map.get("regions") or []:
        data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
        try:
            mined = provider.complete(
                stage="MINING", call_id=f"{pair_id}_{region['region_id']}", pair_id=pair_id,
                prompt=MINER_PROMPT, data=data, schema=MINER_SCHEMA, images=images,
            )
            calls += 0 if using_test_provider else 1
            validate_miner(pair_id, region, mined, pages_by_key)
        except ProviderError as exc:
            raise fail(exc.code, f"V3 Miner failed: {exc.message}", exc) from exc
        except Exception as exc:  # noqa: BLE001
            raise fail("miner_validation_failed", f"V3 Miner validation failed: {exc}", exc) from exc
        mined_regions.append(mined)
        all_changes.extend(mined.get("projectchanges") or [])
        all_hints.extend(mined.get("unresolved_hints") or [])
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_miner_results", {
            "pair_id": pair_id, "run_id": run_id, "regions": mined_regions,
            "projectchanges": all_changes, "unresolved_hints": all_hints,
        })
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: результаты майнера не сохранены", exc) from exc

    # 4. Lightweight dedupe.
    try:
        if all_changes:
            dedupe_raw = provider.complete(
                stage="DEDUPE", call_id=f"{pair_id}_DEDUPE", pair_id=pair_id,
                prompt=DEDUPE_PROMPT,
                data={"pair": pair_id, "projectchanges": [compact_change(c) for c in all_changes]},
                schema=DEDUPE_SCHEMA, images=[],
            )
            calls += 0 if using_test_provider else 1
            if str(dedupe_raw.get("pair")) != str(pair_id):
                raise RuntimeError("Dedupe pair mismatch")
            final_changes = apply_dedupe(pair_id, all_changes, dedupe_raw)
        else:
            dedupe_raw = {"pair": pair_id, "decisions": [], "notes": ["empty"]}
            final_changes = []
    except ProviderError as exc:
        raise fail(exc.code, f"V3 Dedupe failed: {exc.message}", exc) from exc
    except Exception as exc:  # noqa: BLE001
        raise fail("dedupe_failed", f"V3 Dedupe failed: {exc}", exc) from exc

    # 5. Result persistence — without it nothing is published.
    final = {
        "schema": RESULT_SCHEMA,
        "run_id": run_id,
        "session_id": session_id,
        "pair_id": pair_id,
        "object_id": object_id,
        "created_at": _now(),
        "provenance": provenance,
        "source_manifest": {
            key: prepared["manifest"][key]
            for key in ("old_pdf_sha256", "new_pdf_sha256", "structure_sha256", "source_packaging_version")
        },
        "source_identity": source_identity,
        "projectchanges": final_changes,
        "projectchange_regions": _region_of_changes(mined_regions),
        "unresolved_hints": all_hints,
        "dedupe": dedupe_raw,
        "model_calls": calls,
        "legacy_invoked": False,
    }
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_result", final)
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: результат не сохранён, публикация отменена", exc) from exc

    # 6. Human Mapping publication — a failure is REVIEW, never COMPLETED.
    summary = {
        "projectchange_count": len(final_changes),
        "unresolved_hint_count": len(all_hints),
        "semantic_region_count": len(semantic_map.get("regions") or []),
    }
    try:
        hm = _publish_human_mapping(
            session_id=session_id, pair_id=pair_id, object_id=object_id,
            semantic_map=semantic_map, work_dir=work_dir, run_id=run_id,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("V3 Human Mapping publication failed: session=%s pair=%s run=%s", session_id, pair_id, run_id)
        return state(
            "REVIEW", f"V3: {len(final_changes)} ProjectChanges сохранены, но Human Mapping не опубликован "
            f"({type(exc).__name__}: {exc}).", "v3_human_mapping_unavailable",
            calls=calls, provenance=provenance, **summary, human_mapping_published=False,
            human_mapping_error=f"{type(exc).__name__}: {exc}",
        )

    return state(
        "REVIEW" if all_hints else "COMPLETED",
        f"V3 comparison finished: {len(final_changes)} ProjectChanges, {len(all_hints)} unresolved hints.",
        "v3_completed", calls=calls, provenance=provenance, **summary,
        human_mapping_published=True, human_mapping_object_id=hm["object_id"],
        human_mapping_review_regions=hm["review_region_count"],
    )


def run_v3_production_comparison(
    session_id: str,
    pair_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Entry for PROJECT_COMPARISON_ENGINE=v3. Never invokes legacy."""
    engine_env = os.environ.get("PROJECT_COMPARISON_ENGINE", "v3").strip().lower()
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)
    if engine_env not in {"v3", "projectchange_v3", "project_change_v3"}:
        return _write_state(
            session_id,
            pair_id,
            status="FAILED",
            message=UNAVAILABLE_RU,
            reason_code="engine_mismatch",
            provenance=provenance,
        )
    object_id = kwargs.get("object_id")
    skip_gate = bool(kwargs.get("skip_provider_gate"))
    return run_v3_pipeline(
        session_id=session_id,
        pair_id=pair_id,
        object_id=str(object_id) if object_id else None,
        skip_provider_gate=skip_gate,
    )
