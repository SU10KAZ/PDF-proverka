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
every accepted Miner region is persisted at once to a run-scoped checkpoint
(``miner_checkpoints/project_change_v3_miner_checkpoint_<run_id>.json``), so a
later failure does not lose paid answers.  The checkpoint is an internal run
artifact: it is never published, never read back by a run, and a checkpoint
that cannot be written stops the run as FAILED;
EVERY completed Miner answer — accepted or rejected — is first written to the
append-only attempt store (``miner_attempts/<run_id>/``), before validation
decides anything: a paid answer that cannot be kept stops the run, and no
further model call is made;
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
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .contracts import (
    DEDUPE_PROMPT,
    DEDUPE_PROMPT_SHA256,
    DEDUPE_SCHEMA,
    ENGINE_NAME,
    ENGINE_VERSION,
    MAPPER_PROMPT,
    MAPPER_PROMPT_SHA256,
    MAP_SCHEMA,
    MINER_FORMAT_V3,
    MINER_FORMAT_V31_COMPACT,
    MINER_PROMPT,
    MINER_PROMPT_SHA256,
    MINER_PROMPT_V31,
    MINER_PROMPT_V31_SHA256,
    MINER_SCHEMA,
    MINER_SCHEMA_V31,
    MINER_V31_EXPANSION_VERSION,
    MINER_V31_SCHEMA_VERSION,
    MODEL,
    PROVIDER,
    REASONING,
    SOURCE_PACKAGING_VERSION,
)
from .dedupe import apply_dedupe, compact_change
from .hm_builder import build_human_mapping_ui_data, materialize_hm_assets
from .provenance import build_provenance
from .provider import ProviderError, build_codex_payload, get_provider
from .provider_gate import check_provider_readiness
from .source_ref import HINT_SOURCE_REF_POLICY, SourceRefError, expand_miner_output
from .source_prep import (
    SourcePreparationError,
    load_page_record,
    mapping_images,
    optimized_region_bundle,
    prepare_comparison_sources,
)
from .transport import sha256_text
from .validate import EvidenceTraceabilityError, MinerStructuralError, validate_map, validate_miner

logger = logging.getLogger(__name__)

RESULT_SCHEMA = "projectchange_v3_final/2"
# A Miner answer rejected by the evidence-traceability check is asked ONCE
# more with the identical model-visible input (research A-R017: a complete
# answer failed with ``Untraceable evidence`` and the same input passed on the
# next call).  Since 3.5.2 the same single identical retry answers the two
# structural output failures (a ProjectChange that is one-sided or names a page
# outside its region): a clean resampling, never a corrective prompt — the
# acceptance rule itself is unchanged.  Nothing else is retried: provider,
# quota, transport, schema, persistence, cancel and every other validation
# failure stay final.
MINER_MAX_ATTEMPTS = 2
MINER_RETRY_POLICY = {
    "max_attempts": MINER_MAX_ATTEMPTS,
    "retry_only_on": ["untraceable_evidence", "graphic_crop_mismatch",
                      "MINER_PAGE_OUTSIDE_REGION", "MINER_ONE_SIDED_PROJECTCHANGE"],
    "same_model_visible_input_required": True,
    "corrective_prompt": False,
}
MINER_ATTEMPT_SCHEMA = "projectchange_v3_miner_attempt/1"
# V3.1-B COMPACT MINER OUTPUT (research, OFF by default).  ON: the Miner gets
# the V3.1 prompt/compact schema, its answer is expanded to the V3 shape by
# source_ref.py BEFORE validate_miner, and a region gets ONE generating call —
# no retry of any kind (the V3 policy above is not changed by this).
V31_COMPACT_MINER_ENV = "PROJECT_COMPARISON_V31_COMPACT_MINER"
MINER_V31_MAX_ATTEMPTS = 1
MINER_V31_RETRY_POLICY = {"max_attempts": MINER_V31_MAX_ATTEMPTS, "retry_only_on": [],
                          "same_model_visible_input_required": True, "corrective_prompt": False}
MINER_ATTEMPT_SCHEMA_V31 = "projectchange_v31_miner_attempt/1"
MINER_CHECKPOINT_SCHEMA_V31 = "projectchange_v31_miner_checkpoint/1"
MINER_ATTEMPTS_DIR = "miner_attempts"
MINER_CHECKPOINT_SCHEMA = "projectchange_v3_miner_checkpoint/1"
MINER_CHECKPOINT_DIR = "miner_checkpoints"
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


def v31_compact_miner_enabled() -> bool:
    return os.environ.get(V31_COMPACT_MINER_ENV, "0").strip() == "1"


def miner_contract(compact: bool) -> dict[str, Any]:
    """Prompt, schema and retry policy of the Miner call in the selected output format."""
    if not compact:
        return {"format": MINER_FORMAT_V3, "prompt": MINER_PROMPT, "prompt_sha256": MINER_PROMPT_SHA256,
                "schema": MINER_SCHEMA, "max_attempts": MINER_MAX_ATTEMPTS, "retry_policy": MINER_RETRY_POLICY}
    return {"format": MINER_FORMAT_V31_COMPACT, "prompt": MINER_PROMPT_V31,
            "prompt_sha256": MINER_PROMPT_V31_SHA256, "schema": MINER_SCHEMA_V31,
            "max_attempts": MINER_V31_MAX_ATTEMPTS, "retry_policy": MINER_V31_RETRY_POLICY}


def miner_format_provenance(compact: bool) -> dict[str, Any]:
    """Extra provenance of a V3.1 run; nothing at all for V3 (its provenance stays byte-identical)."""
    if not compact:
        return {}
    return {
        "miner_format": MINER_FORMAT_V31_COMPACT,
        "miner_prompt_version": MINER_PROMPT_V31_SHA256,
        "miner_prompt_sha256": MINER_PROMPT_V31_SHA256,
        "miner_output": {
            "miner_format": MINER_FORMAT_V31_COMPACT,
            "compact_schema_version": MINER_V31_SCHEMA_VERSION,
            "compact_schema_sha256": sha256_text(json.dumps(MINER_SCHEMA_V31, ensure_ascii=False, sort_keys=True)),
            "expanded_to_schema_sha256": sha256_text(json.dumps(MINER_SCHEMA, ensure_ascii=False, sort_keys=True)),
            "miner_prompt_sha256": MINER_PROMPT_V31_SHA256,
            "v3_miner_prompt_sha256": MINER_PROMPT_SHA256,
            "source_ref_expansion_version": MINER_V31_EXPANSION_VERSION,
            "hint_source_refs": HINT_SOURCE_REF_POLICY,
            "max_generating_attempts_per_region": MINER_V31_MAX_ATTEMPTS,
        },
    }


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

    from . import run_storage
    d = (run_storage.active_dir(session_id, pair_id) or paths.production_dir(session_id, pair_id)) / "project_change_v3"
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


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def miner_checkpoint_path(work_dir: Path, run_id: str) -> Path:
    """Run-scoped: one file per run, so no run ever sees another run's regions."""
    safe = run_id if re.fullmatch(r"[A-Za-z0-9._-]{1,128}", run_id or "") else hashlib.sha256(
        str(run_id).encode("utf-8")).hexdigest()
    return Path(work_dir) / MINER_CHECKPOINT_DIR / f"project_change_v3_miner_checkpoint_{safe}.json"


def write_miner_checkpoint(path: Path, checkpoint: dict[str, Any]) -> str:
    """tmp file -> fsync -> atomic rename, then read back; any failure propagates.

    Returns the SHA256 of the bytes on disk.
    """
    from backend.app.services.common.atomic_json import atomic_write_json

    atomic_write_json(path, checkpoint)
    data = path.read_bytes()
    back = json.loads(data.decode("utf-8"))
    if back.get("run_id") != checkpoint["run_id"] or len(back.get("regions") or []) != len(checkpoint["regions"]):
        raise OSError(f"V3 Miner checkpoint read-back differs from what was written: {path}")
    return hashlib.sha256(data).hexdigest()


def _safe_name(value: str) -> str:
    return value if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value or "") and ".." not in value else hashlib.sha256(
        str(value).encode("utf-8")).hexdigest()


def miner_attempt_path(work_dir: Path, run_id: str, ordinal: int, region_id: str, attempt: int) -> Path:
    """Append-only: one file per (run, region, attempt); an existing file is never rewritten."""
    return (Path(work_dir) / MINER_ATTEMPTS_DIR / _safe_name(run_id)
            / f"{ordinal:03d}_{_safe_name(region_id).replace(':', '_')}_attempt_{attempt}.json")


def write_miner_attempt(path: Path, record: dict[str, Any]) -> str:
    """Atomic write of one completed Miner answer; refuses to overwrite; failures propagate."""
    from backend.app.services.common.atomic_json import atomic_write_json

    if path.exists():
        raise FileExistsError(f"V3 Miner attempt already recorded (append-only store): {path}")
    atomic_write_json(path, record)
    data = path.read_bytes()
    if json.loads(data.decode("utf-8")).get("response_sha256") != record["response_sha256"]:
        raise OSError(f"V3 Miner attempt read-back differs from what was written: {path}")
    return hashlib.sha256(data).hexdigest()


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
    hm_dir = storage.pair_dir(oid, pair_id, smoke=False, session_id=session_id, run_id=run_id)  # validates both IDs
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


def _validate_json_schema(value: Any, schema: dict[str, Any]) -> None:
    import jsonschema

    jsonschema.validate(value, schema)


def _region_of_changes(mined_regions: list[dict[str, Any]]) -> dict[str, str]:
    return {
        change["projectchange_id"]: region["region_id"]
        for region in mined_regions
        for change in region.get("projectchanges") or []
    }


def run_v3_pipeline(**kwargs: Any) -> dict[str, Any]:
    from . import run_storage
    session_id, pair_id = kwargs['session_id'], kwargs['pair_id']
    run_id = kwargs.get('run_id') or uuid.uuid4().hex
    kwargs['run_id'] = run_id
    run_storage.adopt_legacy(session_id, pair_id)
    run_storage.create(session_id, pair_id, run_id, kwargs.get('object_id'))
    with run_storage.selected(session_id, pair_id, run_id):
        state = _run_v3_pipeline(**kwargs)
        try:
            run_storage.finalize(session_id, pair_id, run_id, state)
        except Exception:
            logger.exception('V3 finalization failed; previous current pointer retained')
            return {**state, 'status': 'FAILED', 'reason_code': 'run_finalization_failed'}
        return state


def _run_v3_pipeline(
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
    compact = v31_compact_miner_enabled()
    contract = miner_contract(compact)
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION, **miner_format_provenance(compact))
    transport_calls: list[dict[str, Any]] = []
    miner_attempts: list[dict[str, Any]] = []
    # Where the accepted Miner regions of this run are kept (set at the first write).
    checkpoint_ref: dict[str, Any] = {}
    attempts_ref: dict[str, Any] = {}

    def receipts() -> dict[str, Any]:
        return {
            "transport_calls": list(transport_calls),
            "miner_retry_policy": contract["retry_policy"],
            "miner_attempts": list(miner_attempts),
            "usage_total": usage_total(transport_calls),
            **({"miner_checkpoint": dict(checkpoint_ref)} if checkpoint_ref else {}),
            **({"miner_attempt_store": dict(attempts_ref)} if attempts_ref else {}),
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
        **miner_format_provenance(compact),
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
    regions = semantic_map.get("regions") or []
    model = str(getattr(provider, "model", MODEL))
    reasoning = str(getattr(provider, "reasoning", REASONING))
    checkpoint_path = miner_checkpoint_path(work_dir, run_id)
    checkpoint: dict[str, Any] = {
        "schema": MINER_CHECKPOINT_SCHEMA,
        "internal_run_artifact": True,
        "published": False,
        "note": "Accepted Miner regions of ONE run, for audit/recovery only. Not a ProjectChange result; "
                "never read back by a run; not a source for another run.",
        "run_id": run_id,
        "session_id": session_id,
        "pair_id": pair_id,
        "engine": ENGINE_NAME,
        "engine_version": ENGINE_VERSION,
        "provider": str(getattr(provider, "provider", PROVIDER)),
        "model": model,
        "reasoning": reasoning,
        "mapper_prompt_sha256": MAPPER_PROMPT_SHA256,
        "miner_prompt_sha256": contract["prompt_sha256"],
        "dedupe_prompt_sha256": DEDUPE_PROMPT_SHA256,
        "miner_schema_sha256": sha256_text(json.dumps(contract["schema"], ensure_ascii=False, sort_keys=True)),
        "semantic_map_sha256": _canonical_sha256(semantic_map),
        "source": {
            key: prepared["manifest"].get(key)
            for key in ("old_pdf_sha256", "new_pdf_sha256", "structure_sha256", "source_packaging_version")
        },
        "regions_total": len(regions),
        "regions": [],
        "created_at": _now(),
        "updated_at": None,
    }
    if compact:
        checkpoint.update(schema=MINER_CHECKPOINT_SCHEMA_V31, miner_format=MINER_FORMAT_V31_COMPACT,
                          miner_output=provenance["miner_output"],
                          note=checkpoint["note"] + " V3.1: each result is the EXPANDED V3-shaped answer; the raw "
                               "compact answer is in the attempt store.")

    # 3. Mining per semantic region.
    mined_regions: list[dict[str, Any]] = []
    all_changes: list[dict[str, Any]] = []
    all_hints: list[dict[str, Any]] = []
    for index, region in enumerate(regions, start=1):
        progress(f"V3: поиск изменений, регион {index} из {len(regions)} (Miner)", "MINING",
                 processed=index - 1, total=len(regions), unit="region", current_item=region["region_id"])
        data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
        base_call_id = f"{pair_id}_{region['region_id']}"

        def visible() -> dict[str, Any]:
            return model_visible_input(prompt=contract["prompt"], data=data, images=images,
                                       schema=contract["schema"], model=model, reasoning=reasoning, region=region)

        try:
            first_input = visible()
        except ProviderError as exc:
            raise provider_failure("Miner", exc) from exc
        mined: dict[str, Any] | None = None
        expansion: dict[str, Any] | None = None

        def keep_attempt(row: dict[str, Any], response: Any, receipt: dict[str, Any],
                         checks: dict[str, str], violations: list[dict[str, Any]],
                         v31: dict[str, Any] | None = None) -> None:
            """The paid answer goes to disk first; if it cannot, the run stops with no further call.

            V3.1: ``response`` is the raw compact answer (the paid output);
            ``v31`` carries the expanded V3 answer (None if expansion failed)
            and the expansion receipt.
            """
            raw = json.dumps(response, ensure_ascii=False)
            record = {
                "schema": MINER_ATTEMPT_SCHEMA, "internal_run_artifact": True, "published": False,
                "note": "One completed Miner answer, accepted or rejected. Audit/debug only: never a ProjectChange "
                        "result, never shown as accepted, never read back by a run.",
                "run_id": run_id, "session_id": session_id, "pair_id": pair_id,
                "region_id": region["region_id"], "region_ordinal": index, "attempt": row["attempt"],
                "call_id": row["call_id"], "engine_version": ENGINE_VERSION,
                "provider": receipt.get("provider", checkpoint["provider"]),
                "model": receipt.get("model", model), "reasoning": receipt.get("reasoning", reasoning),
                "mapper_prompt_sha256": MAPPER_PROMPT_SHA256, "miner_prompt_sha256": MINER_PROMPT_SHA256,
                "miner_schema_sha256": checkpoint["miner_schema_sha256"],
                "semantic_map_sha256": checkpoint["semantic_map_sha256"],
                "model_visible_input": first_input,
                "model_visible_payload_sha256": first_input["model_visible_payload_sha256"],
                "region_source_data_sha256": _canonical_sha256(data),
                "source": checkpoint["source"],
                "raw_response": raw,
                "raw_response_kind": "structured_output_json_as_returned_by_the_provider",
                "response_sha256": sha256_text(raw),
                "parsed_response": response if isinstance(response, dict) else None,
                "validation": {**checks, "verdict": row["validation"]},
                "accepted": bool(row["accepted"]),
                "rejection_code": row.get("rejection_kind"), "rejection_codes": row.get("rejection_codes") or (
                    [row["rejection_kind"]] if row.get("rejection_kind") else []),
                "rejection_message": row.get("rejection_reason"),
                "offending_projectchange_ids": row.get("offending_projectchange_ids") or [],
                "structural_violations": violations,
                "transport_receipt": receipt or ("test_provider" if using_test_provider else None),
                "image_transport": {key: receipt.get(key) for key in (
                    "images", "image_sha256", "images_reencoded_by_provider_cli",
                    "images_reencoded_by_provider_cli_sha256", "image_transport_lossy",
                    "image_byte_identity_after_provider")} if receipt else None,
                "usage": receipt.get("usage"),
                "recorded_at": _now(),
            }
            if compact:
                expanded = (v31 or {}).get("expanded")
                record.update({
                    "schema": MINER_ATTEMPT_SCHEMA_V31, "miner_format": MINER_FORMAT_V31_COMPACT,
                    "parent_region_id": region["region_id"],
                    "compact_schema_sha256": checkpoint["miner_schema_sha256"],
                    "prompt_sha256": contract["prompt_sha256"],
                    "semantic_region_sha256": first_input["semantic_region_sha256"],
                    "source_package_sha256": (v31 or {}).get("source_package_sha256"),
                    "raw_compact_response_sha256": record["response_sha256"],
                    "raw_response_kind": "v31_compact_structured_output_json_as_returned_by_the_provider",
                    "expanded_v3_response": expanded,
                    "expanded_v3_sha256": _canonical_sha256(expanded) if expanded is not None else None,
                    "source_ref_expansion": (v31 or {}).get("receipt"),
                    "source_ref_count": ((v31 or {}).get("receipt") or {}).get("source_ref_count"),
                    "source_ref_resolution": ((v31 or {}).get("receipt") or {}).get("resolution", "NOT_REACHED"),
                })
            path = miner_attempt_path(work_dir, run_id, index, region["region_id"], row["attempt"])
            try:
                digest = write_miner_attempt(path, record)
            except Exception as exc:  # noqa: BLE001
                raise fail("miner_attempt_persistence_failed",
                           f"V3: ответ майнера {row['call_id']} не сохранён ({type(exc).__name__}); прогон "
                           "остановлен, следующий вызов модели не выполняется", exc) from exc
            row.update(attempt_file=str(path), attempt_file_sha256=digest, response_sha256=record["response_sha256"])
            attempts_ref.update(dir=str(path.parent), schema=MINER_ATTEMPT_SCHEMA,
                                saved=attempts_ref.get("saved", 0) + 1,
                                rejected=attempts_ref.get("rejected", 0) + (0 if row["accepted"] else 1))

        for attempt in range(1, contract["max_attempts"] + 1):
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
                    prompt=contract["prompt"], data=data, schema=contract["schema"], images=images,
                )
                calls += 0 if using_test_provider else 1
            except ProviderError as exc:
                receipt = call_receipt(call_id)
                row.update(transport=receipt.get("transport"), usage=receipt.get("usage"),
                           validation="PROVIDER_FAILED", rejection_kind=exc.code, rejection_reason=exc.message)
                answered = getattr(provider, "last_response", None)
                if answered is not None:  # the provider did answer (e.g. schema_invalid): keep the paid answer
                    keep_attempt(row, answered, receipt, {
                        "schema": "FAILED" if exc.code == "schema_invalid" else "NOT_REACHED",
                        "provenance": "NOT_REACHED", "structural": "NOT_REACHED",
                        **({"source_ref_resolution": "NOT_REACHED"} if compact else {})}, [])
                raise provider_failure("Miner", exc) from exc
            receipt = call_receipt(call_id)
            row.update(transport=receipt.get("transport") or ("test_provider" if using_test_provider else None),
                       usage=receipt.get("usage"),
                       transport_payload_sha256=receipt.get("model_visible_payload_sha256"))
            # Validate, then persist the answer WITH its verdict — before any retry
            # or failure handling acts on that verdict.
            rejection: Exception | None = None
            raw_answer, v31 = mined, None
            if compact:
                # Compact schema (again, provider-independent) -> fail-closed source_ref
                # expansion -> the V3 shape; the V3 validator below then runs unchanged.
                try:
                    _validate_json_schema(raw_answer, MINER_SCHEMA_V31)
                    expanded, expansion = expand_miner_output(
                        raw_answer, region=region, pages_by_key=pages_by_key, model_visible_pages=data["pages"],
                        expected_pdf_sha256={"OLD": prepared["manifest"].get("old_pdf_sha256"),
                                             "NEW": prepared["manifest"].get("new_pdf_sha256")})
                    _validate_json_schema(expanded, MINER_SCHEMA)
                    v31 = {"expanded": expanded, "receipt": expansion,
                           "source_package_sha256": expansion["source_package_sha256"]}
                    mined = expanded
                except SourceRefError as exc:
                    rejection = exc
                    v31 = {"expanded": None, "receipt": exc.receipt()}
                except Exception as exc:  # noqa: BLE001 — compact or expanded shape invalid
                    rejection = exc
                    v31 = {"expanded": None, "receipt": {"resolution": "NOT_REACHED",
                                                         "schema_error": f"{type(exc).__name__}: {exc}"}}
            if rejection is None:
                try:
                    validate_miner(pair_id, region, mined, pages_by_key)
                except Exception as exc:  # noqa: BLE001 — classified below, after the answer is on disk
                    rejection = exc
            v31_checks = {"source_ref_resolution": (v31 or {}).get("receipt", {}).get("resolution", "NOT_REACHED")
                          } if compact else {}
            if isinstance(rejection, SourceRefError):
                row.update(validation="REJECTED", rejection_kind=rejection.kind, rejection_reason=str(rejection),
                           rejection_codes=[rejection.kind, rejection.reason],
                           offending_projectchange_ids=[rejection.owner] if rejection.owner else [])
                checks, violations = {"schema": "PASS", "provenance": "NOT_REACHED", "structural": "NOT_REACHED",
                                      **v31_checks}, []
            elif compact and rejection is not None and v31 and v31["expanded"] is None:
                row.update(validation="REJECTED", rejection_kind="v31_schema_invalid", rejection_reason=str(rejection))
                checks, violations = {"schema": "FAILED", "provenance": "NOT_REACHED", "structural": "NOT_REACHED",
                                      **v31_checks}, []
            elif rejection is None:
                row.update(validation="ACCEPTED", accepted=True)
                checks, violations = {"schema": "PASS", "provenance": "PASS", "structural": "PASS", **v31_checks}, []
            elif isinstance(rejection, MinerStructuralError):
                row.update(validation="REJECTED", rejection_kind=rejection.kind, rejection_reason=str(rejection),
                           rejection_codes=rejection.codes, offending_projectchange_ids=rejection.projectchange_ids)
                checks, violations = {"schema": "PASS", "provenance": "NOT_REACHED", "structural": "FAILED",
                                      **v31_checks}, rejection.violations
            elif isinstance(rejection, EvidenceTraceabilityError):
                row.update(validation="REJECTED", rejection_kind=rejection.kind, rejection_reason=str(rejection),
                           offending_projectchange_ids=[rejection.projectchange_id])
                checks, violations = {"schema": "PASS", "provenance": "FAILED", "structural": "PASS", **v31_checks}, []
            else:
                row.update(validation="REJECTED", rejection_kind="not_retryable", rejection_reason=str(rejection))
                checks, violations = {"schema": "PASS", "provenance": "UNKNOWN", "structural": "UNKNOWN",
                                      "other": "FAILED", **v31_checks}, []
            keep_attempt(row, raw_answer, receipt, checks, violations, v31)
            if rejection is None:
                break
            try:
                raise rejection
            except SourceRefError as exc:  # V3.1 only: never retried, never repaired
                raise fail("miner_source_ref_unresolvable",
                           f"V3.1 Miner: ссылка на источник не разрешается однозначно ({exc})", exc) from exc
            except MinerStructuralError as exc:
                if attempt < contract["max_attempts"]:
                    logger.warning("V3 Miner answer rejected by structural check, one identical retry: session=%s "
                                   "pair=%s run=%s call=%s: %s", session_id, pair_id, run_id, call_id, exc)
                    continue
                raise fail("miner_structural_rejected",
                           f"V3 Miner: ответ отвергнут структурной проверкой в {contract['max_attempts']} попытках "
                           f"из {contract['max_attempts']} ({exc})", exc) from exc
            except EvidenceTraceabilityError as exc:
                if attempt < contract["max_attempts"]:
                    logger.warning("V3 Miner answer rejected by provenance check, one retry: session=%s pair=%s "
                                   "run=%s call=%s: %s", session_id, pair_id, run_id, call_id, exc)
                    continue
                raise fail("miner_provenance_rejected",
                           f"V3 Miner: ответ отвергнут проверкой провенанса в {contract['max_attempts']} попытках "
                           f"из {contract['max_attempts']} ({exc})", exc) from exc
            except Exception as exc:  # noqa: BLE001 — neither traceability nor structural: final
                raise fail("miner_validation_failed", f"V3 Miner validation failed: {exc}", exc) from exc
        assert mined is not None
        mined_regions.append(mined)
        all_changes.extend(mined.get("projectchanges") or [])
        all_hints.extend(mined.get("unresolved_hints") or [])
        # Paid and accepted: persist it NOW, before anything else can fail.
        accepted_receipt = call_receipt(call_id)
        checkpoint["regions"].append({
            "region_id": region["region_id"],
            "ordinal": index,
            "accepted_call_id": call_id,
            "result": mined,
            "attempts": [dict(row) for row in miner_attempts if row["region_id"] == region["region_id"]],
            "validation": "ACCEPTED",
            "model_visible_input": first_input,
            "model_visible_payload_sha256": first_input["model_visible_payload_sha256"],
            "region_source_data_sha256": _canonical_sha256(data),
            "semantic_region_sha256": first_input["semantic_region_sha256"],
            "provider": accepted_receipt.get("provider", checkpoint["provider"]),
            "model": accepted_receipt.get("model", model),
            "reasoning": accepted_receipt.get("reasoning", reasoning),
            "transport_receipt": accepted_receipt or ("test_provider" if using_test_provider else None),
            "usage": accepted_receipt.get("usage"),
            "accepted_at": _now(),
            **({"miner_format": MINER_FORMAT_V31_COMPACT,
                "raw_compact_result_sha256": [r for r in miner_attempts if r["call_id"] == call_id][-1]["response_sha256"],
                "expanded_v3_sha256": expansion["expanded_v3_sha256"],
                "source_package_sha256": expansion["source_package_sha256"],
                "source_ref_expansion": expansion} if compact else {}),
        })
        checkpoint["updated_at"] = _now()
        try:
            checkpoint_sha256 = write_miner_checkpoint(checkpoint_path, checkpoint)
            checkpoint_ref.update(path=str(checkpoint_path), sha256=checkpoint_sha256,
                                  regions=len(checkpoint["regions"]), schema=MINER_CHECKPOINT_SCHEMA)
        except Exception as exc:  # noqa: BLE001 — a paid answer that cannot be kept stops the run
            raise fail("miner_checkpoint_persistence_failed",
                       f"V3: контрольная точка майнера не сохранена после региона {region['region_id']} "
                       f"({type(exc).__name__}); прогон остановлен, результат не публикуется", exc) from exc
    miner_checkpoint = dict(checkpoint_ref)
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_miner_results", {
            "pair_id": pair_id, "run_id": run_id, "regions": mined_regions,
            "projectchanges": all_changes, "unresolved_hints": all_hints,
            "miner_checkpoint": miner_checkpoint,
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
