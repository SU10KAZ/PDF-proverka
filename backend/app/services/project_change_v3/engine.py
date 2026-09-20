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

RUNNING is re-written at every stage boundary (mapping, each mined region,
dedupe) so the user sees what the run is doing;
a progress write that cannot be persisted stops the run as FAILED.  A user
cancel (``cancel_token``) stops the run as FAILED/``v3_cancelled``: between
calls at the next stage boundary, inside a call by the gateway killing the
CLI session.  Neither touches what the model sees.
"""
from __future__ import annotations

import hashlib
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
    MODEL,
    REASONING,
    SOURCE_PACKAGING_VERSION,
)
from .dedupe import apply_dedupe, compact_change
from .hm_builder import build_human_mapping_ui_data, materialize_hm_assets
from .provenance import build_provenance
from .provider import ProviderError, build_codex_payload, get_provider
from .provider_gate import check_provider_readiness
from .source_prep import (
    SourcePreparationError,
    load_page_record,
    mapping_images,
    optimized_region_bundle,
    prepare_comparison_sources,
)
from .transport import sha256_text
from .validate import EvidenceTraceabilityError, validate_map, validate_miner

logger = logging.getLogger(__name__)

RESULT_SCHEMA = "projectchange_v3_final/2"
# A Miner answer rejected by the evidence-traceability check is asked ONCE
# more with the identical model-visible input (research A-R017: a complete
# answer failed with ``Untraceable evidence`` and the same input passed on the
# next call).  Nothing else is retried: provider, quota, transport, schema,
# persistence, cancel and every other validation failure stay final.
MINER_MAX_ATTEMPTS = 2
MINER_RETRY_POLICY = {
    "max_attempts": MINER_MAX_ATTEMPTS,
    "retry_only_on": ["untraceable_evidence", "graphic_crop_mismatch"],
    "same_model_visible_input_required": True,
}
USAGE_KEYS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")
UNAVAILABLE_RU = (
    f"Сравнение проектов движком V3 недоступно: модель {MODEL} "
    "не готова или лимит подписки исчерпан. Результат не сгенерирован. "
    "Другая модель и legacy (subject-first) не запускались."
)
CANCELLED_RU = "V3: анализ остановлен пользователем. Результат не опубликован. Legacy не запускался."
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


def model_visible_input(
    *, prompt: str, data: Any, images: list[dict[str, Any]], schema: dict[str, Any],
    model: str, reasoning: str, region: dict[str, Any],
) -> dict[str, Any]:
    """Fingerprint of everything a call shows the model (no text, only hashes).

    The payload hash is the one the transport receipts carry: the same
    ``build_codex_payload`` text, hashed the same way.
    """
    payload, image_paths, _labels = build_codex_payload(prompt, data, images)
    return {
        "model_visible_payload_sha256": sha256_text(payload),
        "model_visible_payload_size": len(payload),
        "image_sha256": [hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in image_paths],
        "schema_sha256": sha256_text(json.dumps(schema, ensure_ascii=False, sort_keys=True)),
        "prompt_sha256": sha256_text(prompt),
        "semantic_region_sha256": sha256_text(json.dumps(region, ensure_ascii=False, sort_keys=True)),
        "model": model,
        "reasoning": reasoning,
    }


def usage_total(transport_calls: list[dict[str, Any]]) -> dict[str, Any]:
    """Actual token usage summed over every receipted call, retries included."""
    total = {key: 0 for key in USAGE_KEYS}
    reported = 0
    for call in transport_calls:
        usage = call.get("usage")
        if not usage:
            continue
        reported += 1
        for key in USAGE_KEYS:
            total[key] += int(usage.get(key) or 0)
    return {**total, "calls": len(transport_calls), "calls_with_usage": reported,
            "calls_without_usage": len(transport_calls) - reported}


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
    run_id: str | None = None,
    cancel_token: Any = None,
) -> dict[str, Any]:
    """Execute source prep -> map -> mine -> dedupe -> persist -> publish.

    skip_provider_gate is for tests with FakeProvider only.  ``run_id`` and
    ``cancel_token`` come from the orchestrator's run control, so a user
    cancel of this pair reaches exactly this run.
    """
    base_provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)
    run_id = run_id or uuid.uuid4().hex
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
            cancel_token=cancel_token,
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
    cancel_token: Any = None,
) -> dict[str, Any]:
    calls = 0
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION)
    transport_calls: list[dict[str, Any]] = []
    miner_attempts: list[dict[str, Any]] = []

    def receipts() -> dict[str, Any]:
        return {
            "transport_calls": list(transport_calls),
            "miner_retry_policy": MINER_RETRY_POLICY,
            "miner_attempts": list(miner_attempts),
            "usage_total": usage_total(transport_calls),
        }

    def fail(reason: str, message: str, exc: BaseException | None = None) -> _V3Failure:
        if exc is not None:
            logger.error(
                "V3 %s: session=%s pair=%s run=%s: %s: %s",
                reason, session_id, pair_id, run_id, type(exc).__name__, exc,
            )
        return _V3Failure(reason, message, calls, {**provenance, **receipts()})

    def cancelled() -> bool:
        return bool(cancel_token is not None and getattr(cancel_token, "cancelled", False))

    def provider_failure(label: str, exc: ProviderError) -> _V3Failure:
        # Whatever the transport reported for the killed call, a cancel of THIS
        # run is one outcome; any other provider failure keeps its own code.
        if cancelled():
            return fail("v3_cancelled", CANCELLED_RU, exc)
        return fail(exc.code, f"V3 {label} failed: {exc.message}", exc)

    def progress(message: str, stage: str, **extra: Any) -> None:
        """Stage boundary: honour a cancel, then publish what the run is doing."""
        if cancelled():
            raise fail("v3_cancelled", CANCELLED_RU)
        written = state("RUNNING", message, "v3_running", calls=calls, provenance=provenance,
                        current_stage=stage, **extra)
        if written["status"] == "FAILED":  # progress could not be persisted: stop, never run blind
            raise _V3Failure(written["reason_code"], written["message"], calls, {**provenance, **receipts()})

    def complete(**kwargs: Any) -> dict[str, Any]:
        # Every contacted call is receipted, including a failed one.
        try:
            return provider.complete(**kwargs)
        finally:
            receipt = getattr(provider, "last_transport", None)
            if receipt:
                transport_calls.append({"stage": kwargs["stage"], "call_id": kwargs["call_id"], **receipt})

    def call_receipt(call_id: str) -> dict[str, Any]:
        return transport_calls[-1] if transport_calls and transport_calls[-1]["call_id"] == call_id else {}

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
    if cancel_token is not None and hasattr(provider, "cancel_token"):
        provider.cancel_token = cancel_token  # the gateway kills the CLI session on cancel
    structure = prepared["structure"]

    # 2. Semantic mapping.
    progress("V3: семантическое сопоставление OLD↔NEW (Mapper)", "MAPPING")
    try:
        semantic_map = complete(
            stage="MAPPING", call_id=f"{pair_id}_SEMANTIC_MAPPING", pair_id=pair_id,
            prompt=MAPPER_PROMPT, data={"pair": pair_id, "pages": structure},
            schema=MAP_SCHEMA, images=mapping_images(structure),
        )
        calls += 0 if using_test_provider else 1
        validate_map(pair_id, semantic_map, structure)
    except ProviderError as exc:
        raise provider_failure("Mapper", exc) from exc
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
    model = str(getattr(provider, "model", MODEL))
    reasoning = str(getattr(provider, "reasoning", REASONING))
    regions = semantic_map.get("regions") or []
    for index, region in enumerate(regions, start=1):
        progress(f"V3: поиск изменений, регион {index} из {len(regions)} (Miner)", "MINING",
                 processed=index - 1, total=len(regions), unit="region", current_item=region["region_id"])
        data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
        base_call_id = f"{pair_id}_{region['region_id']}"

        def visible() -> dict[str, Any]:
            return model_visible_input(prompt=MINER_PROMPT, data=data, images=images, schema=MINER_SCHEMA,
                                       model=model, reasoning=reasoning, region=region)

        try:
            first_input = visible()
        except ProviderError as exc:
            raise provider_failure("Miner", exc) from exc
        mined: dict[str, Any] | None = None
        for attempt in range(1, MINER_MAX_ATTEMPTS + 1):
            call_id = base_call_id if attempt == 1 else f"{base_call_id}_RETRY_{attempt - 1}"
            if attempt > 1:
                # The retry is the SAME call: nothing may have changed since attempt 1.
                again = visible()
                if again != first_input:
                    raise fail("miner_retry_input_drift",
                               f"V3 Miner: вход повтора {call_id} отличается от первой попытки; повтор не выполнен")
            row: dict[str, Any] = {
                "region_id": region["region_id"], "attempt": attempt, "call_id": call_id,
                "model_visible_payload_sha256": first_input["model_visible_payload_sha256"],
                "model_visible_input_sha256": sha256_text(json.dumps(first_input, sort_keys=True)),
                "transport": None, "usage": None, "validation": None,
                "rejection_kind": None, "rejection_reason": None, "accepted": False,
            }
            miner_attempts.append(row)
            try:
                mined = complete(
                    stage="MINING", call_id=call_id, pair_id=pair_id,
                    prompt=MINER_PROMPT, data=data, schema=MINER_SCHEMA, images=images,
                )
                calls += 0 if using_test_provider else 1
            except ProviderError as exc:
                receipt = call_receipt(call_id)
                row.update(transport=receipt.get("transport"), usage=receipt.get("usage"),
                           validation="PROVIDER_FAILED", rejection_kind=exc.code, rejection_reason=exc.message)
                raise provider_failure("Miner", exc) from exc
            receipt = call_receipt(call_id)
            row.update(transport=receipt.get("transport") or ("test_provider" if using_test_provider else None),
                       usage=receipt.get("usage"),
                       transport_payload_sha256=receipt.get("model_visible_payload_sha256"))
            try:
                validate_miner(pair_id, region, mined, pages_by_key)
            except EvidenceTraceabilityError as exc:
                row.update(validation="REJECTED", rejection_kind=exc.kind, rejection_reason=str(exc))
                if attempt < MINER_MAX_ATTEMPTS:
                    logger.warning("V3 Miner answer rejected by provenance check, one retry: session=%s pair=%s "
                                   "run=%s call=%s: %s", session_id, pair_id, run_id, call_id, exc)
                    continue
                raise fail("miner_provenance_rejected",
                           f"V3 Miner: ответ отвергнут проверкой провенанса в {MINER_MAX_ATTEMPTS} попытках "
                           f"из {MINER_MAX_ATTEMPTS} ({exc})", exc) from exc
            except Exception as exc:  # noqa: BLE001 — not a traceability rejection: final
                row.update(validation="REJECTED", rejection_kind="not_retryable", rejection_reason=str(exc))
                raise fail("miner_validation_failed", f"V3 Miner validation failed: {exc}", exc) from exc
            row.update(validation="ACCEPTED", accepted=True)
            break
        assert mined is not None
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
    progress("V3: поиск дублей (Dedupe)", "DEDUPE", processed=len(regions), total=len(regions), unit="region")
    try:
        if all_changes:
            dedupe_raw = complete(
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
        raise provider_failure("Dedupe", exc) from exc
    except Exception as exc:  # noqa: BLE001
        raise fail("dedupe_failed", f"V3 Dedupe failed: {exc}", exc) from exc

    # One model configuration per run: a result whose call receipts name more
    # than one (provider, model, effort) is never persisted or published.
    configurations = sorted({
        (str(c.get("provider")), str(c.get("model")), str(c.get("reasoning")))
        for c in transport_calls if c.get("model")
    })
    expected = (str(provenance.get("provider")), str(provenance.get("model")), str(provenance.get("reasoning")))
    if any(configuration != expected for configuration in configurations):
        raise fail("v3_model_mixing", f"V3: вызовы прогона выполнены разными конфигурациями модели: {configurations}")

    # 5. Result persistence — without it nothing is published.
    provenance = {**provenance, **receipts()}
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
    run_id = kwargs.get("run_id")
    return run_v3_pipeline(
        session_id=session_id,
        pair_id=pair_id,
        object_id=str(object_id) if object_id else None,
        skip_provider_gate=skip_gate,
        run_id=str(run_id) if run_id else None,
        cancel_token=kwargs.get("cancel_token"),
    )
