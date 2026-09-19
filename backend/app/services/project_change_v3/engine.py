"""V3 production comparison entry — real map/mine/dedupe path, never legacy."""
from __future__ import annotations

import json
import os
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
from .provider import ProviderError, get_provider, reset_test_provider
from .provider_gate import check_provider_readiness
from .source_prep import (
    mapping_images,
    optimized_region_bundle,
    prepare_comparison_sources,
)
from .validate import validate_map, validate_miner

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
) -> dict[str, Any]:
    now = _now()
    state: dict[str, Any] = {
        "kind": "stage_comparison_production_state",
        "schema_version": 1,
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "status": status,
        "progress": 100 if status in {"FAILED", "COMPLETED", "REVIEW"} else 50,
        "message": message,
        "reason_code": reason_code,
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "provenance": provenance,
        "technical_provenance": provenance,
        "started_at": now,
        "last_activity_at": now,
        "failed_at": now if status == "FAILED" else None,
        "completed_at": now if status in {"COMPLETED", "REVIEW"} else None,
        "current_stage": None,
        "current_substage": None,
        "model_calls": model_calls,
        "legacy_invoked": False,
    }
    if extra:
        state.update(extra)
    try:
        from backend.app.services.stage_comparison import production_store

        production_store.save_artifact(session_id, pair_id, "state", state)
    except Exception:
        pass
    return state


def _save_artifact(session_id: str, pair_id: str, name: str, value: dict[str, Any]) -> None:
    try:
        from backend.app.services.stage_comparison import production_store

        production_store.save_artifact(session_id, pair_id, name, value)
    except Exception:
        # Fallback: write under production_dir when artifact registry lags.
        try:
            from backend.app.services.stage_comparison import paths

            target = paths.production_dir(session_id, pair_id) / f"{name}.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass


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


def run_v3_pipeline(
    *,
    session_id: str,
    pair_id: str,
    object_id: str | None = None,
    old_paths: dict[str, Path] | None = None,
    new_paths: dict[str, Path] | None = None,
    skip_provider_gate: bool = False,
) -> dict[str, Any]:
    """Execute source prep -> map -> mine -> dedupe -> persist.

    skip_provider_gate is for tests with FakeProvider only.
    """
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)
    model_calls = 0
    using_test_provider = False
    try:
        from . import provider as provider_mod

        using_test_provider = provider_mod._test_provider is not None
    except Exception:
        using_test_provider = False

    if not skip_provider_gate and not using_test_provider:
        gate = check_provider_readiness()
        allow = os.environ.get("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0").strip() == "1"
        if not gate.get("available"):
            reason = "v3_provider_unavailable"
            msg = UNAVAILABLE_RU
            if not allow:
                reason = "v3_inference_kill_switch"
                msg = KILL_SWITCH_RU
            return _write_state(
                session_id,
                pair_id,
                status="FAILED",
                message=msg,
                reason_code=reason,
                provenance={**provenance, "provider_gate": gate},
                model_calls=0,
            )

    if old_paths is None or new_paths is None:
        left, right, pair_meta = _resolve_pair_paths(session_id, pair_id)
        old_paths = left
        new_paths = right
        object_id = object_id or str(pair_meta.get("object_id") or "")
    else:
        pair_meta = {}

    work_dir = _work_dir(session_id, pair_id)
    prepared = prepare_comparison_sources(
        pair_id=pair_id,
        old_paths=old_paths,
        new_paths=new_paths,
        work_dir=work_dir,
        object_id=object_id,
    )
    provenance = build_provenance(
        source_prep_version=prepared["source_packaging_version"],
        structure_sha256=prepared["structure_sha256"],
    )
    _save_artifact(
        session_id,
        pair_id,
        "project_change_v3_source_manifest",
        prepared["manifest"],
    )

    provider = get_provider()
    structure = prepared["structure"]
    try:
        semantic_map = provider.complete(
            stage="MAPPING",
            call_id=f"{pair_id}_SEMANTIC_MAPPING",
            pair_id=pair_id,
            prompt=MAPPER_PROMPT,
            data={"pair": pair_id, "pages": structure},
            schema=MAP_SCHEMA,
            images=mapping_images(structure),
        )
        model_calls += 0 if using_test_provider else 1
        validate_map(pair_id, semantic_map, structure)
    except ProviderError as exc:
        return _write_state(
            session_id,
            pair_id,
            status="FAILED",
            message=f"V3 Mapper failed: {exc.message}",
            reason_code=exc.code,
            provenance=provenance,
            model_calls=model_calls,
        )
    except Exception as exc:
        return _write_state(
            session_id,
            pair_id,
            status="FAILED",
            message=f"V3 Mapper validation failed: {exc}",
            reason_code="mapper_validation_failed",
            provenance=provenance,
            model_calls=model_calls,
        )

    _save_artifact(session_id, pair_id, "project_change_v3_semantic_map", semantic_map)

    pages_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for page in structure:
        # reload full page records for miner validation
        from .source_prep import load_page_record

        pages_by_key[(page["side"], page["physical_page"])] = load_page_record(
            work_dir, page["side"], page["physical_page"]
        )

    mined_regions: list[dict[str, Any]] = []
    all_changes: list[dict[str, Any]] = []
    all_hints: list[dict[str, Any]] = []
    for region in semantic_map.get("regions") or []:
        data, images = optimized_region_bundle(
            pair_id=pair_id, region=region, work_dir=work_dir
        )
        try:
            mined = provider.complete(
                stage="MINING",
                call_id=f"{pair_id}_{region['region_id']}",
                pair_id=pair_id,
                prompt=MINER_PROMPT,
                data=data,
                schema=MINER_SCHEMA,
                images=images,
            )
            model_calls += 0 if using_test_provider else 1
            validate_miner(pair_id, region, mined, pages_by_key)
        except ProviderError as exc:
            return _write_state(
                session_id,
                pair_id,
                status="FAILED",
                message=f"V3 Miner failed: {exc.message}",
                reason_code=exc.code,
                provenance=provenance,
                model_calls=model_calls,
            )
        except Exception as exc:
            return _write_state(
                session_id,
                pair_id,
                status="FAILED",
                message=f"V3 Miner validation failed: {exc}",
                reason_code="miner_validation_failed",
                provenance=provenance,
                model_calls=model_calls,
            )
        mined_regions.append(mined)
        all_changes.extend(mined.get("projectchanges") or [])
        all_hints.extend(mined.get("unresolved_hints") or [])

    miner_results = {
        "pair_id": pair_id,
        "regions": mined_regions,
        "projectchanges": all_changes,
        "unresolved_hints": all_hints,
    }
    _save_artifact(session_id, pair_id, "project_change_v3_miner_results", miner_results)

    dedupe_payload = {
        "pair": pair_id,
        "projectchanges": [compact_change(c) for c in all_changes],
    }
    try:
        if all_changes:
            dedupe_raw = provider.complete(
                stage="DEDUPE",
                call_id=f"{pair_id}_DEDUPE",
                pair_id=pair_id,
                prompt=DEDUPE_PROMPT,
                data=dedupe_payload,
                schema=DEDUPE_SCHEMA,
                images=[],
            )
            model_calls += 0 if using_test_provider else 1
            if str(dedupe_raw.get("pair")) != str(pair_id):
                raise RuntimeError("Dedupe pair mismatch")
            final_changes = apply_dedupe(pair_id, all_changes, dedupe_raw)
        else:
            dedupe_raw = {"pair": pair_id, "decisions": [], "notes": ["empty"]}
            final_changes = []
    except ProviderError as exc:
        return _write_state(
            session_id,
            pair_id,
            status="FAILED",
            message=f"V3 Dedupe failed: {exc.message}",
            reason_code=exc.code,
            provenance=provenance,
            model_calls=model_calls,
        )
    except Exception as exc:
        return _write_state(
            session_id,
            pair_id,
            status="FAILED",
            message=f"V3 Dedupe failed: {exc}",
            reason_code="dedupe_failed",
            provenance=provenance,
            model_calls=model_calls,
        )

    final = {
        "schema": "projectchange_v3_final/1",
        "pair_id": pair_id,
        "object_id": object_id,
        "provenance": provenance,
        "projectchanges": final_changes,
        "unresolved_hints": all_hints,
        "dedupe": dedupe_raw,
        "model_calls": model_calls,
        "legacy_invoked": False,
    }
    _save_artifact(session_id, pair_id, "project_change_v3_result", final)

    # Human Mapping UI data for this comparison (generic)
    oid = object_id or "unknown"
    ui_data = build_human_mapping_ui_data(
        pair_id=pair_id,
        object_id=oid,
        semantic_map=semantic_map,
        work_dir=work_dir,
    )
    ui_path = work_dir / "HUMAN_MAPPING_UI_DATA.json"
    ui_path.write_text(json.dumps(ui_data, ensure_ascii=False, indent=2), encoding="utf-8")
    assets_dir = work_dir / "hm_assets"
    materialize_hm_assets(work_dir, assets_dir)
    _save_artifact(
        session_id,
        pair_id,
        "project_change_v3_human_mapping_ui",
        {
            "pair_id": pair_id,
            "object_id": oid,
            "ui_data_path": str(ui_path),
            "assets_dir": str(assets_dir),
            "region_count": len(ui_data.get("regions") or []),
        },
    )

    # Also publish under comparison-scoped HM store for API reads
    try:
        from backend.app.services.human_mapping_production import storage

        hm_dir = storage.pair_dir(oid, pair_id, smoke=False)
        hm_dir.mkdir(parents=True, exist_ok=True)
        (hm_dir / "ui_data.json").write_text(
            json.dumps(ui_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        materialize_hm_assets(work_dir, hm_dir / "assets")
    except Exception:
        pass

    status = "REVIEW" if all_hints and not final_changes else "COMPLETED"
    if all_hints and final_changes:
        status = "REVIEW"
    message = (
        f"V3 comparison finished: {len(final_changes)} ProjectChanges, "
        f"{len(all_hints)} unresolved hints."
    )
    return _write_state(
        session_id,
        pair_id,
        status=status,
        message=message,
        reason_code="v3_completed",
        provenance=provenance,
        extra={
            "projectchange_count": len(final_changes),
            "unresolved_hint_count": len(all_hints),
            "semantic_region_count": len(semantic_map.get("regions") or []),
        },
        model_calls=model_calls,
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
