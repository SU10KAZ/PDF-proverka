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
artifact: it is never published and never read back by a run — with ONE
explicit exception (3.11.0, ``resume.py``): a run started with
``resume_from_run_id`` reuses the map, Miner and unmatched answers of a FAILED
run whose sources and replay contract are identical, re-validates every answer
and continues from Dedupe. Since 3.19.0 a partial Miner checkpoint can also be
resumed: only missing regions are planned/called, on the same model. The model
stays the same unless remaining_stages explicitly hands over the tail of a
fully mined donor (3.12.0), or all stages after a completed Mapper when a
planning stop proves no Miner calls were made (3.25.0). Since 3.26.0 a COMPLETED
run with failed review batches is a donor too: on the same model only those
batches are called again. A checkpoint that cannot be written stops the run
as FAILED;
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
    UNMATCHED_PROMPT,
    UNMATCHED_PROMPT_SHA256,
    VERIFICATION_SCHEMA,
    VERIFY_PROMPT,
    VERIFY_PROMPT_SHA256,
)
from .dedupe import apply_dedupe, compact_change, dedupe_lossless_enabled, repair_decisions
from .source_checks import enabled_checks, run_source_checks
from .coverage import build_page_coverage, validate_page_coverage
from .hm_builder import build_human_mapping_ui_data, materialize_hm_assets
from .hint_identity import hint_identities
from . import mapper_portions as mapper_portions_mod
from . import parallel_calls
from . import resume as resume_mod
from . import miner_plan
from .provenance import build_provenance
from .provider import ProviderError, build_codex_payload, get_provider
from .provider_gate import check_provider_readiness
from .quality import build_quality_ledger
from .source_ref import HINT_SOURCE_REF_POLICY, SourceRefError, expand_miner_output
from .source_prep import (
    SourcePreparationError,
    load_page_record,
    mapping_images,
    optimized_region_bundle,
    prepare_comparison_sources,
)
from .supplemental import build_supplemental_batches, validate_supplemental_answer
from .transport import TransportIntegrityError, plan_claude, plan_codex_mapper, plan_codex_miner, sha256_text
from .validate import EvidenceTraceabilityError, MinerStructuralError, validate_map, validate_miner
from .verification import apply_verification_results, build_verification_batches, validate_verification_answer

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
UNMATCHED_REVIEW_ENV = "PROJECT_COMPARISON_V3_UNMATCHED_REVIEW"
UNMATCHED_REVIEW_MAX_BATCHES_ENV = "PROJECT_COMPARISON_V3_UNMATCHED_MAX_BATCHES"
SOURCE_VERIFICATION_ENV = "PROJECT_COMPARISON_V3_SOURCE_VERIFICATION"
SOURCE_VERIFICATION_MAX_BATCHES_ENV = "PROJECT_COMPARISON_V3_SOURCE_VERIFICATION_MAX_BATCHES"
# Пакеты разбора unmatched-страниц и проверки параметров: отказ провайдера, который
# проходит сам (TRANSIENT — 429, перегрузка, обрыв потока), повторяется тем же
# вызовом после паузы. Codex к этому моменту уже исчерпал свои быстрые повторы:
# 19 пакетов объекта 256 (15.10) упали за одну минуту на 429, а через 6 минут
# проверка параметров той же моделью прошла целиком. Пауза идёт в рабочем потоке
# (parallel_calls.DELAY_KEY). Miner этим не затронут — его политика выше.
REVIEW_TRANSIENT_RETRY_DELAYS_S: tuple[float, ...] = (120.0, 300.0)
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


def unavailable_ru() -> str:
    """UNAVAILABLE_RU for the model of the current run (3.9.0: chosen per run)."""
    from .contracts import active_profile
    return UNAVAILABLE_RU.replace(f"модель {MODEL} ", f"модель {active_profile().model} ", 1)


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
        result = {"format": MINER_FORMAT_V3, "prompt": MINER_PROMPT, "prompt_sha256": MINER_PROMPT_SHA256,
                "schema": MINER_SCHEMA, "max_attempts": MINER_MAX_ATTEMPTS, "retry_policy": MINER_RETRY_POLICY}
    else:
        result = {"format": MINER_FORMAT_V31_COMPACT, "prompt": MINER_PROMPT_V31,
            "prompt_sha256": MINER_PROMPT_V31_SHA256, "schema": MINER_SCHEMA_V31,
            "max_attempts": MINER_V31_MAX_ATTEMPTS, "retry_policy": MINER_V31_RETRY_POLICY}
    from .comparison_mode import current, SUBJECTS
    if current() == SUBJECTS:
        from .subject_contracts import AUTHORITY
        result['prompt'] += '\n' + AUTHORITY
        result['prompt_sha256'] = sha256_text(result['prompt'])
    return result


def _base_miner_format_provenance(compact: bool) -> dict[str, Any]:
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


def miner_format_provenance(compact: bool) -> dict[str, Any]:
    result = _base_miner_format_provenance(compact)
    from .comparison_mode import current, SUBJECTS
    if current() == SUBJECTS:
        sha = miner_contract(compact)['prompt_sha256']
        result.update(miner_prompt_sha256=sha, miner_prompt_version=sha)
        if 'miner_output' in result:
            result['miner_output']['miner_prompt_sha256'] = sha
    return result


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
        active = state.get("pipeline_current")
        if active in (state.get("pipeline_stages") or {}):
            state["pipeline_stages"][active].update(
                status="FAILED", message=state["message"], finished_at=now,
            )
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
    """``model_profile``: the run's model (3.9.0); ``None`` = the startup default."""
    from .contracts import resolve_profile, use_profile
    requested = kwargs.pop('model_profile', None)
    parallelism = kwargs.pop('parallelism', None)
    from . import comparison_mode, run_storage
    mode = kwargs.pop('comparison_mode', None)
    if kwargs.get('resume_from_run_id'):
        source_state = run_storage.read(run_storage.run_dir(kwargs['session_id'], kwargs['pair_id'],
                                           kwargs['resume_from_run_id']) / 'state.json') or {}
        source_mode = (source_state.get('provenance') or {}).get('comparison_mode', 'standard')
        if mode is not None and mode != source_mode:
            raise ValueError('cannot change comparison mode when resuming a run')
        mode = source_mode
    mode = mode or comparison_mode.STANDARD
    policy = kwargs.get('resume_model_policy', 'same_model')
    if policy not in {'same_model', 'remaining_stages'}:
        raise ValueError('unknown resume model policy')
    if policy == 'remaining_stages' and (not kwargs.get('resume_from_run_id') or not requested):
        raise ValueError('model handoff requires a donor run and an explicit target model')
    if requested is None and kwargs.get('resume_from_run_id'):
        # A resumed run keeps the donor's model by default. A different model
        # requires the explicit remaining_stages policy and an explicit profile.
        requested = resume_mod.donor_model_profile(kwargs['session_id'], kwargs['pair_id'],
                                                   kwargs['resume_from_run_id'])
    profile = resolve_profile(requested)
    with use_profile(profile), parallel_calls.use_concurrency(parallelism), comparison_mode.selected(mode):
        return _run_v3_pipeline_stored(**kwargs)


def _run_v3_pipeline_stored(**kwargs: Any) -> dict[str, Any]:
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
    resume_from_run_id: str | None = None,
    resume_model_policy: str = "same_model",
    max_new_miner_calls: int | None = None,
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
    from .pipeline_progress import PipelineProgress

    pipeline = PipelineProgress()

    def state(
        status: str,
        message: str,
        reason: str,
        *,
        calls: int = 0,
        provenance: dict[str, Any] | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        extra.update(pipeline.update(status, message, reason, _now(), extra))
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
                KILL_SWITCH_RU if closed else unavailable_ru(),
                "v3_inference_kill_switch" if closed else "v3_provider_unavailable",
                provenance={**base_provenance, "provider_gate": gate},
            )

    running = state("RUNNING", "V3: подготовка источников", "v3_running",
                    current_stage="SOURCE_PREPARATION")
    if running["status"] == "FAILED":
        return running

    try:
        return _run_admitted(
            session_id=session_id, pair_id=pair_id, object_id=object_id,
            old_paths=old_paths, new_paths=new_paths, run_id=run_id,
            using_test_provider=using_test_provider, state=state,
            cancel_token=cancel_token, resume_from_run_id=resume_from_run_id,
            resume_model_policy=resume_model_policy,
            max_new_miner_calls=max_new_miner_calls,
        )
    except _V3Failure as failure:
        return state("FAILED", failure.message, failure.reason,
                     calls=failure.model_calls, provenance=failure.provenance)
    except Exception as exc:  # noqa: BLE001 — a crash must never leave RUNNING
        logger.exception("V3 pipeline crashed: session=%s pair=%s run=%s", session_id, pair_id, run_id)
        return state("FAILED", f"V3: внутренняя ошибка ({type(exc).__name__})", "v3_internal_error")


def _assembly_sources(paths: dict[str, Path] | None) -> list[dict[str, Any]]:
    """``assembly_origin.json["sources"]`` of one side, or [] for an ordinary document."""
    if not paths:
        return []
    origin = Path(paths["pdf"]).parent / "assembly_origin.json"
    try:
        data = json.loads(origin.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    return [row for row in data.get("sources") or [] if isinstance(row, dict)]


def mapper_portions_mod_envelope(pair_id: str, data: dict[str, Any]) -> None:
    """Check the selected provider's envelope for the complete Mapper call."""
    from .contracts import active_profile

    payload, image_paths, _labels = build_codex_payload(MAPPER_PROMPT, data, mapping_images(data["pages"]))
    if active_profile().selection == "codex":
        plan_codex_mapper(payload, image_paths, MAP_SCHEMA)
    else:
        plan_claude(payload, image_paths, MAP_SCHEMA)


def mapper_portions_mod_single_envelope_error(pair_id: str, structure: list[dict[str, Any]]) -> str:
    try:
        mapper_portions_mod_envelope(pair_id, {"pair": pair_id, "pages": structure})
    except TransportIntegrityError as exc:
        return str(exc)
    return ""


def _miner_fits(pair_id: str, region: dict[str, Any], work_dir: Path, contract: dict[str, Any]) -> bool:
    """Check the complete Miner call, including full-page images and output schema."""
    from .contracts import active_profile

    data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
    payload, image_paths, _labels = build_codex_payload(contract["prompt"], data, images)
    try:
        if active_profile().selection == "codex":
            plan_codex_miner(payload, image_paths, contract["schema"])
        else:
            plan_claude(payload, image_paths, contract["schema"])
    except TransportIntegrityError:
        return False
    return True


def _miner_measure(pair_id, region, work_dir, contract, *, read_image=None):
    from . import transport
    from .contracts import active_profile

    reader = read_image or transport.image_info
    data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
    payload, paths, _ = build_codex_payload(contract["prompt"], data, images,
                                           image_digest=lambda path: reader(path)["sha256"])
    return transport.measure_request(payload, paths, contract["schema"], selection=active_profile().selection,
                                      stage="Miner", read_image=reader)


def mapper_portions_mod_fits(pair_id: str, pages: list[dict[str, Any]]) -> bool:
    """Would a portion with these pages pass the exact transport plan (with its portion note)?"""
    probe = mapper_portions_mod.MapperPortion(
        index=99, total=99, label="х" * 200,
        new_pages=[p["physical_page"] for p in pages if p["side"] == "NEW"],
        # Reserve the longer two-sided note too: a fallback OLD slice must not
        # fit in planning only to fail with its actual scope metadata later.
        old_pages=[p["physical_page"] for p in pages if p["side"] == "OLD"],
    )
    try:
        mapper_portions_mod_envelope(pair_id, mapper_portions_mod.portion_data(pair_id, pages, probe))
    except TransportIntegrityError:
        return False
    return True


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
    resume_from_run_id: str | None = None,
    resume_model_policy: str = "same_model",
    max_new_miner_calls: int | None = None,
) -> dict[str, Any]:
    calls = 0
    compact = v31_compact_miner_enabled()
    contract = miner_contract(compact)
    provenance = build_provenance(source_prep_version=SOURCE_PACKAGING_VERSION, **miner_format_provenance(compact))
    transport_calls: list[dict[str, Any]] = []
    miner_attempts: list[dict[str, Any]] = []
    mapper_attempts: list[dict[str, Any]] = []
    review_attempts: list[dict[str, Any]] = []
    # Where the accepted Miner regions of this run are kept (set at the first write).
    checkpoint_ref: dict[str, Any] = {}
    attempts_ref: dict[str, Any] = {}

    def receipts() -> dict[str, Any]:
        return {
            "transport_calls": list(transport_calls),
            "miner_retry_policy": contract["retry_policy"],
            "miner_attempts": list(miner_attempts),
            **({"mapper_attempts": list(mapper_attempts)} if mapper_attempts else {}),
            **({"review_attempts": list(review_attempts)} if review_attempts else {}),
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
        if exc.code == "CONTEXT_WINDOW_EXCEEDED":
            return fail(
                "provider_context_window_exceeded",
                f"V3 {label}: превышено окно контекста модели. Нужно уменьшить объём данных "
                "в одном запросе; повтор с тем же пакетом не устранит причину.",
                exc,
            )
        if exc.code == "CONTEXT_COMPACTED":
            return fail(
                "provider_context_compacted",
                f"V3 {label}: клиент сжал контекст во время анализа. Результат не принят: "
                "полнота исходных данных после сжатия не подтверждена. "
                "Нужно разделить запрос на меньшие части.",
                exc,
            )
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

    try:
        miner_workers = parallel_calls.concurrency(parallel_calls.MINER_CONCURRENCY_ENV)
    except ValueError as exc:
        raise fail("miner_concurrency_invalid", str(exc), exc) from exc
    try:
        max_new_miner_calls = miner_plan.max_new_calls(max_new_miner_calls)
        miner_planning_mode = miner_plan.planner_mode()
    except ValueError as exc:
        raise fail("miner_plan_limit_invalid", str(exc), exc) from exc
    provenance["miner_concurrency"] = miner_workers
    try:
        mapper_workers = parallel_calls.concurrency(parallel_calls.MAPPER_CONCURRENCY_ENV)
    except ValueError as exc:
        raise fail("mapper_concurrency_invalid", str(exc), exc) from exc
    provenance["mapper_concurrency"] = mapper_workers

    try:
        unmatched_workers = parallel_calls.concurrency(parallel_calls.UNMATCHED_CONCURRENCY_ENV)
        verification_workers = parallel_calls.concurrency(parallel_calls.VERIFICATION_CONCURRENCY_ENV)
        def batch_limit(name: str) -> int | None:
            raw = os.environ.get(name, "all").strip().lower()
            if raw == "all":
                return None
            value = int(raw)
            if value < 0:
                raise ValueError(f"{name} must be all or a nonnegative integer")
            return value
        unmatched_max_batches = batch_limit(UNMATCHED_REVIEW_MAX_BATCHES_ENV)
        verification_max_batches = batch_limit(SOURCE_VERIFICATION_MAX_BATCHES_ENV)
    except ValueError as exc:
        raise fail("review_settings_invalid", str(exc), exc) from exc
    review_settings = {
        "unmatched_concurrency": unmatched_workers,
        "source_verification_concurrency": verification_workers,
        "unmatched_max_batches": unmatched_max_batches,
        "source_verification_max_batches": verification_max_batches,
        "unmatched_existing_subjects_policy": "frozen_after_mining",
    }
    provenance.update(review_settings)

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
        miner_concurrency=miner_workers, mapper_concurrency=mapper_workers, **review_settings,
        **miner_format_provenance(compact),
    )
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_source_manifest", prepared["manifest"])
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: манифест источников не сохранён", exc) from exc

    # 3.11.0: resume a FAILED run from its saved map, Miner and unmatched answers.
    donor: resume_mod.Donor | None = None
    from .contracts import MODEL_PROFILES, active_profile
    from . import comparison_mode, subject_search, run_storage
    subjects_mode = comparison_mode.current() == comparison_mode.SUBJECTS
    subject_checkpoint = None
    subject_donor_profile = None
    subject_ledger = None
    if resume_from_run_id and subjects_mode:
        prior = run_storage.read(run_storage.run_dir(session_id, pair_id, resume_from_run_id) / 'state.json') or {}
        if subject_search.recoverable_search_state(prior):
            try:
                subject_checkpoint, prior = subject_search.load_checkpoint(session_id, pair_id, resume_from_run_id)
                prior_provenance = prior.get('provenance') or {}
                subject_donor_profile = MODEL_PROFILES.get(prior_provenance.get('model_profile'))
                if (subject_donor_profile is None or
                        (subject_donor_profile.provider, subject_donor_profile.model, REASONING) !=
                        (prior_provenance.get('provider'), prior_provenance.get('model'), prior_provenance.get('reasoning'))):
                    raise ValueError('subject donor model identity changed')
                provenance['subject_recovery'] = {'run_id': resume_from_run_id,
                    'prior_usage_total': prior_provenance.get('usage_total'),
                    'source_model': resume_mod.model_identity(prior_provenance),
                    'accepted_requests': len(subject_checkpoint['entries'])}
                if resume_model_policy == 'remaining_stages' and subject_donor_profile != active_profile():
                    provenance['model_handoff'] = {
                        'policy': resume_model_policy, 'donor_run_id': resume_from_run_id,
                        'reused_stages': ['SUBJECT_SEARCH_PARTIAL'],
                        'remaining_stages': ['MAPPING', 'MINING', 'UNMATCHED_REVIEW', 'DEDUPE', 'SOURCE_VERIFICATION'],
                        'source_model': resume_mod.model_identity(prior_provenance),
                        'remaining_model': resume_mod.model_identity(provenance)}
                    provenance['engine_variant'] = (
                        f"ProjectChange V3 / {subject_donor_profile.model} → {active_profile().model} (досборка)")
                elif prior_provenance.get('model_handoff'):
                    provenance['model_handoff'] = prior_provenance['model_handoff']
                    provenance['engine_variant'] = prior_provenance['engine_variant']
            except Exception as exc:
                raise fail('resume_refused', f'Не удалось восстановить поиск по предметам: {exc}', exc) from exc
    if resume_from_run_id and subject_checkpoint is None:
        try:
            donor = resume_mod.load_donor(session_id, pair_id, resume_from_run_id)
            resume_mod.check_compatible(donor, provenance=provenance, manifest=prepared["manifest"],
                                        structure=prepared["structure"], work_dir=work_dir,
                                        model_policy=resume_model_policy)
            if donor.checkpoint.get("miner_schema_sha256") != _canonical_sha256(contract["schema"]):
                raise resume_mod.ResumeRefused("схема Miner отличается от схемы прогона-донора")
        except resume_mod.ResumeRefused as exc:
            raise fail("resume_refused", f"V3: досборка из прогона {resume_from_run_id} невозможна: {exc}",
                       exc) from exc
        donor_calls, donor_attempts = resume_mod.donor_receipts(donor)
        bad = resume_mod.mismatched_calls(donor_calls, provenance, donor)
        if bad:
            raise fail("resume_refused", f"V3: квитанции донора содержат другую модель: {bad}")
        if resume_mod.model_identity(donor.provenance) != resume_mod.model_identity(provenance):
            provenance["model_handoff"] = {
                "policy": resume_model_policy,
                "donor_run_id": donor.run_id,
                "reused_stages": (["MAPPING"] if donor.before_mining else ["MAPPING", "MINING", "UNMATCHED_REVIEW"]),
                "remaining_stages": sorted(resume_mod.POST_MAPPING_STAGES if donor.before_mining else resume_mod.TAIL_STAGES),
                "source_model": resume_mod.model_identity(donor.provenance),
                "remaining_model": resume_mod.model_identity(provenance),
            }
            provenance["engine_variant"] = (
                f"ProjectChange V3 / {donor.provenance.get('model')} → {provenance['model']} (досборка)"
            )
        elif donor.provenance.get("model_handoff"):
            # Continuing the target model retains the Mapper's original authorship.
            provenance["model_handoff"] = donor.provenance["model_handoff"]
            provenance["engine_variant"] = donor.provenance["engine_variant"]
        transport_calls.extend(donor_calls)
        miner_attempts.extend(donor_attempts)
        calls = int(donor.state.get("model_calls") or 0)
        if not donor.partial_mining:
            checkpoint_ref.update(donor.miner_results.get("miner_checkpoint") or {}, resumed_from_run_id=donor.run_id)
        provenance["resumed_from"] = donor.summary()
        for key in ("mapper_portioning", "mapper_plan", "miner_region_splits", "miner_region_merges", "subject_recovery"):
            if donor.provenance.get(key):
                provenance[key] = donor.provenance[key]
        if subjects_mode:
            subject_ledger = run_storage.read(run_storage.run_dir(session_id, pair_id, donor.run_id) /
                                             (subject_search.ARTIFACT + '.json'))
            if not subject_ledger or subject_search.digest(subject_ledger) != donor.provenance.get('subject_ledger_sha256'):
                raise fail('resume_refused', 'Журнал поиска по предметам отсутствует или изменён')
            subject_ledger = donor.rebase(subject_ledger)
            subject_ledger['resumed_from_run_id'] = donor.run_id
            subject_ledger['run_id'] = run_id
            _save_artifact(session_id, pair_id, subject_search.ARTIFACT, subject_ledger)
            provenance['subject_search'] = subject_ledger['summary']
            provenance['subject_ledger_sha256'] = subject_search.digest(subject_ledger)

    structure = prepared["structure"]
    from . import mapper_planning
    from .contracts import active_profile

    try:
        planning_mode = mapper_planning.planner_mode()
    except ValueError as exc:
        raise fail("mapper_planner_invalid", str(exc), exc) from exc
    planner = mapper_planning.MapperPlanner(pair_id, structure, active_profile(),
                                            _assembly_sources(new_paths), cancelled=cancelled)
    planned_packets = []

    # Plan both providers before any call. Codex injected-history chunks still
    # share one context: large inputs need separate Mapper calls too.
    mapper_portions: list[mapper_portions_mod.MapperPortion] = []
    if donor is not None or subjects_mode:
        pass  # the donor's map is reused: its envelope was planned by the donor run
    else:
        single_error = mapper_portions_mod_single_envelope_error(pair_id, structure)
        if single_error:
            try:
                choice = planner.plan(mode=planning_mode, allow_single=False,
                                      fits=(lambda pages: mapper_portions_mod_fits(pair_id, pages))
                                      if planning_mode == "legacy" else None)
                mapper_portions = choice.portions
                provenance["mapper_plan"] = choice.report
                # The planner uses a probe portion note. Check ALL exact final
                # notes/labels before spending on even the first portion.
                for portion in mapper_portions:
                    mapper_portions_mod_envelope(
                        pair_id, mapper_portions_mod.portion_data(pair_id, structure, portion))
                if not mapper_portions:
                    raise mapper_portions_mod.PortioningImpossible(single_error)
                planned_packets = planner.verify(mapper_portions)
            except (mapper_portions_mod.PortioningImpossible, TransportIntegrityError) as exc:
                if cancelled():
                    raise fail("v3_cancelled", CANCELLED_RU, exc) from exc
                raise fail(
                    "assembly_mapper_envelope_exceeded",
                    f"Пара превышает возможности транспорта V3 даже порциями: {exc}",
                    exc,
                ) from exc
            provenance["mapper_portioning"] = {
                "version": mapper_portions_mod.PORTIONING_VERSION,
                "mode": "PORTIONED",
                "split_sides": (["OLD", "NEW"] if any(p.old_pages is not None for p in mapper_portions)
                                else ["NEW"]),
                "single_call_refused": single_error,
                "portions": [portion.summary() for portion in mapper_portions],
            }
        else:
            try:
                provenance["mapper_plan"] = planner.plan(mode=planning_mode).report
                planned_packets = planner.verify([])
            except (mapper_portions_mod.PortioningImpossible, TransportIntegrityError) as exc:
                raise fail("v3_cancelled" if cancelled() else "assembly_mapper_envelope_exceeded",
                           CANCELLED_RU if cancelled() else str(exc), exc) from exc
        provenance["mapper_plan"]["final_packets"] = planned_packets
        provenance["mapper_plan"]["accepted_portions"] = []
        provenance["mapper_plan"]["complete"] = False
        try:
            _save_artifact(session_id, pair_id, "project_change_v3_mapper_plan", provenance["mapper_plan"])
        except Exception as exc:
            raise fail("result_persistence_failed", "V3: план Mapper не сохранён", exc) from exc
    provider = get_provider()
    if cancel_token is not None and hasattr(provider, "cancel_token"):
        provider.cancel_token = cancel_token
    # 2. Semantic mapping.
    if donor is not None:
        progress("Сопоставление восстановлено из сохранённого запуска", "MAPPING",
                 pipeline_updates={"MAPPING": {"status": "REUSED"}})
        semantic_map = donor.rebase(donor.semantic_map)
        if donor.mapper_portions is not None:
            try:
                _save_artifact(session_id, pair_id, "project_change_v3_mapper_portions",
                               donor.rebase(donor.mapper_portions))
            except Exception as exc:  # noqa: BLE001
                raise fail("result_persistence_failed", "V3: ответы порций Mapper не сохранены", exc) from exc
    elif subjects_mode:
        progress('По инженерным предметам: подготовка каталога', 'MAPPING')

        def subject_outcome(request, outcome):
            nonlocal calls
            if outcome.receipt:
                transport_calls.append({'stage': request['stage'], 'call_id': request['call_id'], **outcome.receipt})
            if not using_test_provider and (outcome.error is None or outcome.receipt):
                calls += 1
            provenance.update(receipts())

        def subject_save(name, value):
            _save_artifact(session_id, pair_id, name, value)
            if name == subject_search.CHECKPOINT:
                provenance['subject_checkpoint_sha256'] = subject_search.digest(value)
                provenance['subject_accepted_requests'] = len(value['entries'])
            elif name == subject_search.ARTIFACT:
                provenance['subject_ledger_sha256'] = subject_search.digest(value)
                details = value.get('progress') or {}
                message = details.get('message') or (
                    f"По инженерным предметам: сохранено {provenance.get('subject_accepted_requests', 0)} ответов")
                counters = {key: details[key] for key in ('processed', 'total', 'unit') if key in details}
                written = state('RUNNING', message, 'v3_running', calls=calls, provenance=provenance,
                                current_stage='MAPPING', **counters)
                if written['status'] == 'FAILED':
                    raise fail('result_persistence_failed', 'Не сохранено состояние поиска по предметам')

        try:
            search = subject_search.SubjectSearch(
                pair_id=pair_id, run_id=run_id, structure=structure, work_dir=work_dir,
                source_manifest=prepared['manifest'], profile=active_profile(), provider=provider,
                on_outcome=subject_outcome, workers=mapper_workers,
                save=subject_save, progress=progress, cancelled=cancelled, donor_checkpoint=subject_checkpoint,
                donor_profile=subject_donor_profile, resume_model_policy=resume_model_policy)
            semantic_map, subject_ledger = search.run()
            provenance['subject_search'] = subject_ledger['summary']
            provenance['mapper_plan'] = {'mode': 'subjects', 'coverage_contract': 'candidate_search',
                                         'complete': True, **subject_ledger['summary']}
        except subject_search.SubjectRecoveryError as exc:
            raise fail('resume_refused', f'Не удалось восстановить поиск по предметам: {exc}', exc) from exc
        except ProviderError as exc:
            raise provider_failure('поиск по предметам', exc) from exc
        except _V3Failure:
            raise
        except Exception as exc:
            raise fail('v3_cancelled' if cancelled() else 'subject_search_failed',
                       CANCELLED_RU if cancelled() else f'Поиск по предметам остановлен: {exc}', exc) from exc
    elif not mapper_portions:
        progress("V3: семантическое сопоставление OLD↔NEW (Mapper)", "MAPPING")
        try:
            if planner.packet({"pair": pair_id, "pages": structure}, fresh=True)["request_key"] != planned_packets[0]["request_key"]:
                raise fail("mapper_sources_changed", "V3: источники изменились после планирования Mapper")
            semantic_map = complete(
                stage="MAPPING", call_id=f"{pair_id}_SEMANTIC_MAPPING", pair_id=pair_id,
                prompt=MAPPER_PROMPT, data={"pair": pair_id, "pages": structure},
                schema=MAP_SCHEMA, images=mapping_images(structure),
            )
            calls += 0 if using_test_provider else 1
            validate_map(pair_id, semantic_map, structure)
            provenance["mapper_plan"].update(accepted_portions=[1], complete=True)
        except _V3Failure:
            raise
        except ProviderError as exc:
            raise provider_failure("Mapper", exc) from exc
        except Exception as exc:  # noqa: BLE001
            raise fail("mapper_validation_failed", f"V3 Mapper validation failed: {exc}", exc) from exc
    else:
        portion_maps_by_index: dict[int, dict[str, Any]] = {}
        mapping_control = parallel_calls.CallControl()

        def map_portion(_ordinal: int, portion: mapper_portions_mod.MapperPortion):
            nonlocal calls
            data = mapper_portions_mod.portion_data(pair_id, structure, portion)
            if planner.packet(data, fresh=True)["request_key"] != planned_packets[portion.index - 1]["request_key"]:
                raise fail("mapper_sources_changed", "V3: источники изменились после планирования Mapper")
            call_id = f"{pair_id}_SEMANTIC_MAPPING_P{portion.index:02d}"
            outcome = yield dict(
                stage="MAPPING", call_id=call_id, pair_id=pair_id, prompt=MAPPER_PROMPT,
                data=data, schema=MAP_SCHEMA, images=mapping_images(data["pages"]),
            )
            receipt = outcome.receipt
            if receipt:
                transport_calls.append({"stage": "MAPPING", "call_id": call_id, **receipt})
            if not using_test_provider and (outcome.error is None or receipt):
                calls += 1
            error = outcome.error
            if error is None:
                try:
                    validate_map(pair_id, outcome.response, data["pages"])
                except Exception as exc:  # preserve a rejected paid answer too
                    error = exc
            record = {
                "schema": "projectchange_v3_mapper_attempt/1", "internal_run_artifact": True,
                "published": False, "run_id": run_id, "session_id": session_id, "pair_id": pair_id,
                "call_id": call_id, "portion": portion.summary(), "recorded_at": _now(),
                "prompt_sha256": MAPPER_PROMPT_SHA256, "source_data_sha256": _canonical_sha256(data),
                "request_key": planned_packets[portion.index - 1]["request_key"],
                "planner_version": mapper_planning.pp.VERSION,
                "payload_version": mapper_planning.PAYLOAD_VERSION,
                "response": outcome.response if outcome.error is None else outcome.last_response,
                "transport_receipt": receipt, "accepted": error is None,
                "error": str(error) if error is not None else None,
            }
            path = work_dir / "mapper_attempts" / run_id / f"{portion.index:03d}.json"
            try:
                from .run_storage import atomic
                atomic(path, record)
            except Exception as exc:
                raise fail("mapper_attempt_persistence_failed",
                           f"V3: ответ порции Mapper {portion.index} не сохранён; прогон остановлен", exc) from exc
            mapper_attempts.append({"call_id": call_id, "portion_index": portion.index,
                                    "accepted": error is None, "path": str(path),
                                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
            mapper_attempts.sort(key=lambda row: row["portion_index"])
            if isinstance(error, ProviderError):
                raise provider_failure(f"Mapper (порция {portion.index} из {portion.total})", error) from error
            if error is not None:
                raise fail("mapper_validation_failed",
                           f"V3 Mapper validation failed (порция {portion.index} из {portion.total}): {error}",
                           error) from error
            portion_maps_by_index[portion.index] = outcome.response
            provenance["mapper_plan"]["accepted_portions"] = sorted(portion_maps_by_index)
            try:
                _save_artifact(session_id, pair_id, "project_change_v3_mapper_plan", provenance["mapper_plan"])
            except Exception as exc:
                raise fail("result_persistence_failed", "V3: прогресс плана Mapper не сохранён", exc) from exc

        def mapping_progress() -> None:
            done = len(portion_maps_by_index)
            progress(f"V3: сопоставление OLD↔NEW, готово {done} из {len(mapper_portions)} порций "
                     f"(до {mapper_workers} одновременно)", "MAPPING",
                     processed=done, total=len(mapper_portions), unit="portion")

        mapping_progress()
        try:
            parallel_calls.run_items(
                enumerate(mapper_portions, start=1), start_item=map_portion, provider=provider,
                workers=mapper_workers, control=mapping_control, cancelled=cancelled,
                on_completed=mapping_progress, thread_name="v3-mapper",
            )
        except _V3Failure as exc:
            raise fail(exc.reason, exc.message) from exc
        except Exception as exc:
            raise fail("v3_internal_error", f"V3 Mapper: внутренняя ошибка ({type(exc).__name__})", exc) from exc
        if cancelled():
            raise fail("v3_cancelled", CANCELLED_RU)
        portion_maps = [portion_maps_by_index[portion.index] for portion in mapper_portions]
        try:
            semantic_map = mapper_portions_mod.merge_portion_maps(pair_id, portion_maps, mapper_portions, structure)
            if not using_test_provider:
                # Каждая порция уже прошла схему у провайдера; слияние обязано её сохранить.
                _validate_json_schema(semantic_map, MAP_SCHEMA)
            validate_map(pair_id, semantic_map, structure)
            provenance["mapper_plan"]["complete"] = True
        except Exception as exc:  # noqa: BLE001
            raise fail("mapper_validation_failed", f"V3 Mapper: порции не сливаются в одну карту: {exc}", exc) from exc
        try:
            _save_artifact(session_id, pair_id, "project_change_v3_mapper_portions", {
                **provenance["mapper_portioning"],
                "portion_maps": portion_maps,
            })
        except Exception as exc:  # noqa: BLE001
            raise fail("result_persistence_failed", "V3: ответы порций Mapper не сохранены", exc) from exc

    if donor is None:
        try:
            _save_artifact(session_id, pair_id, "project_change_v3_mapper_plan", provenance["mapper_plan"])
        except Exception as exc:
            raise fail("result_persistence_failed", "V3: завершённый план Mapper не сохранён", exc) from exc
    reuse_complete_mining = donor is not None and not donor.partial_mining
    pages_by_key = {
        (page["side"], page["physical_page"]): load_page_record(work_dir, page["side"], page["physical_page"])
        for page in structure
    }
    reused_rows: list[dict[str, Any]] = []
    if donor is not None and donor.partial_mining:
        try:
            validate_map(pair_id, semantic_map, structure)
            reused_rows = resume_mod.replay_partial(
                donor, pair_id=pair_id, pages_by_key=pages_by_key, validate_miner=validate_miner)
        except Exception as exc:  # noqa: BLE001 — no new calls until every saved answer is checked
            raise fail("resume_refused", f"V3: сохранённые участки Miner не прошли проверку: {exc}", exc) from exc
    reused_ids = {row["region_id"] for row in reused_rows}
    if not reuse_complete_mining:
        progress("V3: проверка размера запросов поиска изменений", "MINING")
        from functools import lru_cache
        from .transport import PlanningImageCache

        miner_images = PlanningImageCache()

        @lru_cache(maxsize=1024)
        def measured_miner(serialized_region):
            return _miner_measure(pair_id, json.loads(serialized_region), work_dir, contract, read_image=miner_images)

        def measure_region(region):
            return measured_miner(json.dumps(region, ensure_ascii=False, sort_keys=True))

        miner_candidate_reports = []
        if donor is not None:
            try:
                replanned, merges = miner_plan.coalesce(
                    semantic_map["regions"], reused_ids=reused_ids,
                    splits=(provenance.get("miner_region_splits") or {}).get("regions", []),
                    previous_merges=provenance.get("miner_region_merges") or [], pages=pages_by_key,
                    fits=lambda candidate: _miner_fits(pair_id, candidate, work_dir, contract))
                if merges:
                    semantic_map = {**semantic_map, "regions": replanned}
                    validate_map(pair_id, semantic_map, structure)
                    provenance["miner_region_merges"] = list(provenance.get("miner_region_merges") or []) + merges
            except Exception as exc:
                raise fail("resume_refused", f"V3: план оставшихся участков не удалось проверить: {exc}", exc) from exc
        # Plan ALL regions for the selected provider BEFORE any Miner call.
        try:
            split_regions: list[dict[str, Any]] = []
            splits: list[dict[str, Any]] = []
            for region in semantic_map.get("regions") or []:
                if region["region_id"] in reused_ids:
                    split_regions.append(region)
                    continue  # Accepted answers keep their original regions, even if now over budget.
                parts = mapper_portions_mod.split_region_for_envelope(
                    region, lambda candidate: _miner_fits(pair_id, candidate, work_dir, contract),
                    native_texts={key: page.get("native_page_text") or "" for key, page in pages_by_key.items()})
                parts, region_plan = miner_plan.balance_region(region, parts, measure_region, mode=miner_planning_mode)
                miner_candidate_reports.append(region_plan)
                for part in parts:
                    if not _miner_fits(pair_id, part, work_dir, contract):
                        raise mapper_portions_mod.PortioningImpossible(f"область {part['region_id']}: окончательный запрос не помещается")
                if len(parts) > 1:
                    splits.append({"region_id": region["region_id"],
                                   "parts": [{"region_id": part["region_id"], "old_pages": part["old_pages"],
                                              "new_pages": part["new_pages"],
                                              **({"native_text_windows": part["native_text_windows"]}
                                                 if "native_text_windows" in part else {})}
                                             for part in parts]})
                split_regions.extend(parts)
        except mapper_portions_mod.PortioningImpossible as exc:
            raise fail("miner_envelope_exceeded", f"V3: область не помещается в вызов модели даже частями: {exc}",
                       exc) from exc
        if splits:
            semantic_map = {**semantic_map, "regions": split_regions}
            validate_map(pair_id, semantic_map, structure)
            provenance["miner_region_splits"] = {
                "version": mapper_portions_mod.PORTIONING_VERSION,
                "regions": list((provenance.get("miner_region_splits") or {}).get("regions") or []) + splits,
            }
        provenance["miner_plan"] = {"version": miner_plan.VERSION,
                                    "reused_regions": len(reused_ids),
                                    "new_calls": sum(r["region_id"] not in reused_ids for r in split_regions),
                                    "max_new_calls": max_new_miner_calls,
                                    "mode": miner_planning_mode, "regions": miner_candidate_reports,
                                    "estimated_new_input_tokens": sum(measure_region(r)["estimated_input_tokens"]
                                                                      for r in split_regions if r["region_id"] not in reused_ids)}
        try:
            _save_artifact(session_id, pair_id, "project_change_v3_miner_plan", provenance["miner_plan"])
        except Exception as exc:
            raise fail("result_persistence_failed", "V3: план Miner не сохранён", exc) from exc
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_semantic_map", semantic_map)
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: семантическая карта не сохранена", exc) from exc

    regions = semantic_map.get("regions") or []
    analysed_region_ids: list[str] = []
    reviewed_unmatched_pages: set[tuple[str, int]] = set()
    failed_unmatched_pages: set[tuple[str, int]] = set()

    def persist_coverage() -> dict[str, Any]:
        coverage = build_page_coverage(
            pair_id=pair_id,
            run_id=run_id,
            structure=structure,
            semantic_map=semantic_map,
            analysed_region_ids=analysed_region_ids,
            reviewed_unmatched_pages=reviewed_unmatched_pages,
            failed_unmatched_pages=failed_unmatched_pages,
            page_records=pages_by_key,
        )
        validate_page_coverage(coverage)
        _save_artifact(session_id, pair_id, "project_change_v3_coverage", coverage)
        return coverage

    try:
        coverage = persist_coverage()
    except Exception as exc:  # noqa: BLE001
        raise fail("coverage_persistence_failed", "V3: учет покрытия страниц не сохранён", exc) from exc
    model = str(getattr(provider, "model", MODEL))
    reasoning = str(getattr(provider, "reasoning", REASONING))
    checkpoint_path = miner_checkpoint_path(work_dir, run_id)
    checkpoint: dict[str, Any] = {
        "schema": MINER_CHECKPOINT_SCHEMA,
        "internal_run_artifact": True,
        "published": False,
        "note": "Accepted Miner regions, including explicitly resumed and revalidated answers. "
                "Internal recovery checkpoint, not a published ProjectChange result.",
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
    mining_control = parallel_calls.CallControl()
    mined_by_ordinal: dict[int, dict[str, Any]] = {}

    def persist_checkpoint() -> None:
        checkpoint["regions"].sort(key=lambda row: row["ordinal"])
        checkpoint["updated_at"] = _now()
        digest = write_miner_checkpoint(checkpoint_path, checkpoint)
        checkpoint_ref.update(path=str(checkpoint_path), sha256=digest,
                              regions=len(checkpoint["regions"]), schema=checkpoint["schema"])

    if reused_rows:
        # A new run owns its checkpoint. A second failure can resume these rows
        # plus any newly accepted parts without modifying the original donor.
        ordinals = {region["region_id"]: i for i, region in enumerate(regions, 1)}
        for row in reused_rows:
            index = ordinals[row["region_id"]]
            checkpoint["regions"].append({**row, "ordinal": index})
            mined_by_ordinal[index] = row["result"]
            analysed_region_ids.append(row["region_id"])
        progress(f"V3: восстановлено {len(reused_rows)} из {len(regions)} участков Miner", "MINING",
                 processed=len(reused_rows), total=len(regions), unit="region")
        try:
            persist_checkpoint()
            coverage = persist_coverage()
        except Exception as exc:  # noqa: BLE001
            raise fail("miner_checkpoint_persistence_failed",
                       "V3: восстановленные участки Miner не удалось сохранить; новые вызовы не начаты", exc) from exc

    if not reuse_complete_mining and provenance["miner_plan"]["new_calls"] > max_new_miner_calls:
        try:
            persist_checkpoint()
        except Exception as exc:
            raise fail("miner_checkpoint_persistence_failed",
                       "V3: план Miner не удалось сохранить; новые вызовы не начаты", exc) from exc
        raise fail("miner_plan_budget_exceeded",
                   f"V3: план требует {provenance['miner_plan']['new_calls']} новых заданий Miner "
                   f"при лимите {max_new_miner_calls}. Поиск изменений остановлен до новых вызовов; "
                   "сопоставление и принятые ответы сохранены для продолжения.")

    def mine_region(index: int, region: dict[str, Any]):
        nonlocal calls, coverage
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
            if mining_control.stopped or cancelled():
                return
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
                outcome = yield dict(
                    stage="MINING", call_id=call_id, pair_id=pair_id,
                    prompt=contract["prompt"], data=data, schema=contract["schema"], images=images,
                )
                receipt = outcome.receipt
                if receipt:
                    transport_calls.append({"stage": "MINING", "call_id": call_id, **receipt})
                if outcome.error is not None:
                    if not using_test_provider and receipt:
                        calls += 1
                    raise outcome.error
                mined = outcome.response
                calls += 0 if using_test_provider else 1
            except ProviderError as exc:
                row.update(transport=receipt.get("transport"), usage=receipt.get("usage"),
                           validation="PROVIDER_FAILED", rejection_kind=exc.code, rejection_reason=exc.message)
                answered = outcome.last_response
                if answered is not None:  # the provider did answer (e.g. schema_invalid): keep the paid answer
                    keep_attempt(row, answered, receipt, {
                        "schema": "FAILED" if exc.code == "schema_invalid" else "NOT_REACHED",
                        "provenance": "NOT_REACHED", "structural": "NOT_REACHED",
                        **({"source_ref_resolution": "NOT_REACHED"} if compact else {})}, [])
                raise provider_failure("Miner", exc) from exc
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
        mined_by_ordinal[index] = mined
        # Paid and accepted: persist it NOW, before anything else can fail.
        accepted_receipt = receipt
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
        try:
            persist_checkpoint()
        except Exception as exc:  # noqa: BLE001 — a paid answer that cannot be kept stops the run
            raise fail("miner_checkpoint_persistence_failed",
                       f"V3: контрольная точка майнера не сохранена после региона {region['region_id']} "
                       f"({type(exc).__name__}); прогон остановлен, результат не публикуется", exc) from exc
        analysed_region_ids.append(str(region["region_id"]))
        try:
            coverage = persist_coverage()
        except Exception as exc:  # noqa: BLE001
            raise fail(
                "coverage_persistence_failed",
                f"V3: учет покрытия не сохранён после региона {region['region_id']}",
                exc,
            ) from exc
    if not reuse_complete_mining:

        def mining_progress() -> None:
            done = len(analysed_region_ids)
            progress(f"V3: поиск изменений, готово {done} из {len(regions)} участков "
                     f"(до {miner_workers} одновременно)", "MINING",
                     processed=done, total=len(regions), unit="region")

        mining_progress()
        try:
            parallel_calls.run_items(
                ((i, region) for i, region in enumerate(regions, start=1)
                 if region["region_id"] not in reused_ids), start_item=mine_region, provider=provider,
                workers=miner_workers, control=mining_control, cancelled=cancelled,
                on_completed=mining_progress, thread_name="v3-miner",
            )
        except _V3Failure as exc:
            # In-flight answers were drained and saved after the first error.
            # Refresh counts/receipts rather than publishing its earlier snapshot.
            raise fail(exc.reason, exc.message) from exc
        except Exception as exc:
            raise fail("v3_internal_error", f"V3 Miner: внутренняя ошибка ({type(exc).__name__})", exc) from exc
        if cancelled():
            raise fail("v3_cancelled", CANCELLED_RU)
        # Completion order must not change downstream prompts, IDs or dedupe.
        mined_regions.extend(mined_by_ordinal[i] for i in sorted(mined_by_ordinal))
        all_changes.extend(c for result in mined_regions for c in result.get("projectchanges") or [])
        all_hints.extend(h for result in mined_regions for h in result.get("unresolved_hints") or [])
        analysed_region_ids[:] = [str(region["region_id"]) for region in regions]
    if reuse_complete_mining:
        progress("V3: досборка — повторная проверка сохранённых ответов Miner", "MINING",
                 processed=len(regions), total=len(regions), unit="region")
        try:
            replayed = resume_mod.replay(donor, pair_id=pair_id, pages_by_key=pages_by_key,
                                         validate_miner=validate_miner,
                                         validate_supplemental_answer=validate_supplemental_answer)
        except resume_mod.ResumeRefused as exc:
            raise fail("resume_refused", f"V3: досборка из прогона {donor.run_id} невозможна: {exc}", exc) from exc
        mined_regions.extend(replayed["mined_regions"])
        all_changes.extend(replayed["changes"])
        all_hints.extend(replayed["hints"])
        analysed_region_ids.extend(replayed["analysed_region_ids"])
        reviewed_unmatched_pages.update(replayed["reviewed_unmatched_pages"])
        failed_unmatched_pages.update(replayed["failed_unmatched_pages"])
        try:
            coverage = persist_coverage()
        except Exception as exc:  # noqa: BLE001
            raise fail("coverage_persistence_failed", "V3: учет покрытия страниц не сохранён", exc) from exc
    miner_checkpoint = dict(checkpoint_ref)

    def review_answer(*, stage, batch_id, call_id, prompt_sha256, request, validate, validation_code):
        nonlocal calls
        delays = (0.0, *REVIEW_TRANSIENT_RETRY_DELAYS_S)
        for attempt, delay in enumerate(delays):
            attempt_call_id = call_id if not attempt else f"{call_id}_RETRY_{attempt}"
            if subjects_mode and stage == 'SOURCE_VERIFICATION':
                from .provider import build_codex_payload
                from .transport import measure_request
                payload, image_paths, _ = build_codex_payload(request['prompt'], request['data'], request['images'])
                measurement = measure_request(payload, image_paths, request['schema'], selection=active_profile().selection)
                if not measurement['fits']:
                    outcome = parallel_calls.CallOutcome(error=ProviderError(
                        'verification_envelope_exceeded', 'Полные источники проверки не помещаются: ' +
                        '; '.join(measurement['errors'])))
                    break  # local refusal, zero provider calls, retained as a pending review
            outcome = yield {"stage": stage, "call_id": attempt_call_id, "pair_id": pair_id, **request,
                             **({parallel_calls.DELAY_KEY: delay} if delay else {})}
            if outcome.receipt:
                transport_calls.append({"stage": stage, "call_id": attempt_call_id, **outcome.receipt})
            if not using_test_provider and (outcome.error is None or outcome.receipt):
                calls += 1
            transient = isinstance(outcome.error, ProviderError) and outcome.error.code == "TRANSIENT"
            if not transient or attempt + 1 == len(delays) or cancelled():
                break
            logger.warning("V3 %s %s: provider TRANSIENT, retry %d in %.0f s: session=%s pair=%s run=%s",
                           stage, batch_id, attempt + 1, delays[attempt + 1], session_id, pair_id, run_id)
        call_id = attempt_call_id
        error = outcome.error
        if error is None:
            try:
                _validate_json_schema(outcome.response, request["schema"])
                validate(outcome.response)
            except Exception as exc:
                error = exc
        row = {
            "call_id": call_id, "prompt_sha256": prompt_sha256,
            "status": "ACCEPTED" if error is None else "FAILED",
            "result": outcome.response if error is None else None,
            "error_code": error.code if isinstance(error, ProviderError) else validation_code if error else None,
            "error_message": error.message if isinstance(error, ProviderError) else f"{type(error).__name__}: {error}" if error else None,
        }
        path = work_dir / "review_attempts" / run_id / stage / f"{batch_id}.json"
        record = {
            "schema": "projectchange_v3_review_attempt/1", "internal_run_artifact": True,
            "published": False, "run_id": run_id, "session_id": session_id, "pair_id": pair_id,
            "stage": stage, "batch_id": batch_id, "recorded_at": _now(), **row,
            "source_data_sha256": _canonical_sha256(request["data"]),
            "response": outcome.response if outcome.error is None else outcome.last_response,
            "transport_receipt": outcome.receipt,
            "attempts": attempt + 1,
        }
        try:
            from .run_storage import atomic
            atomic(path, record)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except Exception as exc:
            raise fail("review_attempt_persistence_failed",
                       f"V3: ответ {stage}/{batch_id} не сохранён; прогон остановлен", exc) from exc
        review_attempts.append({"stage": stage, "batch_id": batch_id, "call_id": call_id,
                                "status": row["status"], "path": str(path), "sha256": digest})
        review_attempts.sort(key=lambda r: (r["stage"], r["batch_id"]))
        return row

    def run_review_batches(batches, start_item, workers, stage, label, completed):
        if not batches:
            return
        def report():
            progress(f"V3: {label}, обработано {len(completed)} из {len(batches)} пакетов "
                     f"(до {workers} одновременно)", stage,
                     processed=len(completed), total=len(batches), unit="batch")
        report()
        try:
            parallel_calls.run_items(
                enumerate(batches, start=1), start_item=start_item, provider=provider,
                workers=workers, control=parallel_calls.CallControl(), cancelled=cancelled,
                on_completed=report, thread_name=f"v3-{stage.lower()}",
            )
        except _V3Failure as exc:
            # Every in-flight answer has now been drained and saved.
            raise fail(exc.reason, exc.message) from exc
        except Exception as exc:
            raise fail("v3_internal_error", f"V3 {stage}: внутренняя ошибка ({type(exc).__name__})", exc) from exc
        if cancelled():
            raise fail("v3_cancelled", CANCELLED_RU)

    # 3b. Bounded review of meaningful pages left unmatched by Mapper.  This
    # uses a separate prompt and never mutates the frozen semantic map or the
    # ordinary Miner answers.  Failure leaves an explicit coverage gap and the
    # accepted bilateral results remain publishable for review.
    supplemental_batches: list[dict[str, Any]] = []
    progress("Подготовка разбора страниц без пары", "UNMATCHED_REVIEW")
    supplemental_enabled = os.environ.get(UNMATCHED_REVIEW_ENV, "1").strip() == "1"
    # Every concurrent batch sees the same completed Miner subjects. Later
    # supplemental answers cannot change another batch's model-visible input.
    existing_subjects: list[dict[str, Any]] = []
    supplemental_by_index: dict[int, dict[str, Any]] = {}
    retried_unmatched: list[str] = []

    def frozen_subjects(changes):
        return [{"projectchange_id": c.get("projectchange_id"), "engineering_subject": c.get("engineering_subject")}
                for c in changes]

    def review_unmatched(index, batch):
        nonlocal coverage
        region = batch["region"]
        data, images = optimized_region_bundle(pair_id=pair_id, region=region, work_dir=work_dir)
        data["coverage_review"] = {
            "primary_side": batch["primary_side"], "primary_pages": batch["primary_pages"],
            "candidate_side": batch["candidate_side"], "candidate_pages": batch["candidate_pages"],
            "existing_subjects": existing_subjects,
        }
        def validate(answer):
            validate_miner(pair_id, region, answer, pages_by_key)
            validate_supplemental_answer(batch, answer)
        row = yield from review_answer(
            stage="UNMATCHED_REVIEW", batch_id=batch["batch_id"],
            call_id=f"{pair_id}_{batch['batch_id']}_UNMATCHED", prompt_sha256=UNMATCHED_PROMPT_SHA256,
            request={"prompt": UNMATCHED_PROMPT, "data": data, "schema": MINER_SCHEMA, "images": images},
            validate=validate, validation_code="supplemental_validation_failed",
        )
        supplemental_by_index[index] = {**batch, **row}
        if row["status"] == "ACCEPTED":
            reviewed_unmatched_pages.update(batch["primary_keys"])
            failed_unmatched_pages.difference_update(batch["primary_keys"])
        else:
            failed_unmatched_pages.update(batch["primary_keys"])
        try:
            coverage = persist_coverage()
        except Exception as exc:
            raise fail("coverage_persistence_failed", "V3: учет разбора unmatched-страниц не сохранён", exc) from exc

    if reuse_complete_mining:
        supplemental_enabled = donor.replayed["supplemental_enabled"]
        supplemental_batches = list(donor.replayed["supplemental_batches"])
        # 3.26.0: on the same model, the donor's failed batches are asked again with
        # exactly the donor's question: the same frozen batch and the same subjects
        # (bilateral Miner answers only, as the donor's batches saw them).
        retry = ([row for row in supplemental_batches if row.get("status") != "ACCEPTED"]
                 if supplemental_enabled and resume_mod.same_model(donor, provenance, "UNMATCHED_REVIEW") else [])
        if retry:
            existing_subjects[:] = frozen_subjects(
                c for result in mined_regions[:len(regions)] for c in result.get("projectchanges") or [])
            plans = [{key: value for key, value in row.items() if key not in {
                "call_id", "prompt_sha256", "status", "result", "error_code", "error_message"}}
                | {"primary_keys": [(str(side), int(page)) for side, page in row["primary_keys"]]}
                for row in retry]
            run_review_batches(plans, review_unmatched, unmatched_workers, "UNMATCHED_REVIEW",
                               "повтор упавших пакетов разбора страниц без пары", supplemental_by_index)
            again = {supplemental_by_index[i]["batch_id"]: supplemental_by_index[i] for i in supplemental_by_index}
            retried_unmatched[:] = sorted(again)
            supplemental_batches = [again.get(row["batch_id"], row) for row in supplemental_batches]
            # Same order as an uninterrupted run: bilateral regions, then accepted batches in batch order.
            del mined_regions[len(regions):]
            mined_regions.extend(row["result"] for row in supplemental_batches if row["status"] == "ACCEPTED")
            all_changes[:] = [c for result in mined_regions for c in result.get("projectchanges") or []]
            all_hints[:] = [h for result in mined_regions for h in result.get("unresolved_hints") or []]
    elif supplemental_enabled:
        try:
            batches = build_supplemental_batches(
                semantic_map=semantic_map,
                page_records=pages_by_key,
                max_batches=unmatched_max_batches,
            )
        except Exception as exc:  # noqa: BLE001
            raise fail("supplemental_planning_failed", f"V3: план разбора unmatched-страниц не построен: {exc}", exc) from exc
        existing_subjects[:] = frozen_subjects(all_changes)
        run_review_batches(batches, review_unmatched, unmatched_workers, "UNMATCHED_REVIEW",
                           "разбор страниц без пары", supplemental_by_index)
        supplemental_batches = [supplemental_by_index[i] for i in sorted(supplemental_by_index)]
        for row in supplemental_batches:
            if row["status"] == "ACCEPTED":
                answer = row["result"]
                mined_regions.append(answer)
                all_changes.extend(answer.get("projectchanges") or [])
                all_hints.extend(answer.get("unresolved_hints") or [])

    try:
        _save_artifact(session_id, pair_id, "project_change_v3_supplemental_results", {
            "schema": "projectchange_v3_supplemental_results/1",
            "pair_id": pair_id,
            "run_id": run_id,
            "enabled": supplemental_enabled,
            "prompt_sha256": UNMATCHED_PROMPT_SHA256,
            "batches": supplemental_batches,
            "limits": {
                "max_batches": unmatched_max_batches if supplemental_enabled else 0,
                "concurrency": unmatched_workers,
                "existing_subjects_policy": "reused_from_donor" if reuse_complete_mining else "frozen_after_mining",
                **({"retried_from_donor": retried_unmatched} if retried_unmatched else {}),
                "max_primary_pages_per_batch": 8,
                "candidate_pages_per_primary": 2,
            },
        })
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: результаты разбора unmatched-страниц не сохранены", exc) from exc

    try:
        _save_artifact(session_id, pair_id, "project_change_v3_miner_results", {
            "pair_id": pair_id, "run_id": run_id, "regions": mined_regions,
            "projectchanges": all_changes, "unresolved_hints": all_hints,
            "miner_checkpoint": miner_checkpoint,
        })
    except Exception as exc:  # noqa: BLE001
        raise fail("result_persistence_failed", "V3: результаты майнера не сохранены", exc) from exc

    # 4. Lightweight dedupe.
    dedupe_lossless = dedupe_lossless_enabled()
    unmatched_pending = coverage["summary"].get("content_unmatched_pending", 0)
    unmatched_status = ("PARTIAL" if unmatched_pending else "COMPLETED") if supplemental_batches else "SKIPPED"
    unmatched_message = (f"Осталось непроверенных страниц: {unmatched_pending}." if unmatched_pending
                         else "Разбор страниц без пары завершён.")
    if not supplemental_batches:
        unmatched_message = ("Разбор отключён." if not supplemental_enabled else
                             "Нет подходящих пакетов для разбора." if unmatched_pending else
                             "Нет страниц, требующих дополнительного разбора.")
    progress("V3: поиск дублей (Dedupe)", "DEDUPE", pipeline_updates={
        "MINING": {"status": "REUSED" if reuse_complete_mining else "COMPLETED",
                   "processed": len(regions), "total": len(regions), "unit": "region"},
        "UNMATCHED_REVIEW": {"status": unmatched_status, "message": unmatched_message,
                             "processed": len(supplemental_batches), "total": len(supplemental_batches),
                             "unit": "batch"},
    })
    dedupe_answer: dict[str, Any] | None = None
    dedupe_repairs: list[dict[str, Any]] = []
    # 3.26.0: a completed donor's Dedupe answer is the answer to this very question
    # when the cards are byte-identical (no failed unmatched batch was answered now).
    # Only a COMPLETED donor: a donor that failed on Dedupe must be asked again.
    donor_dedupe = donor.dedupe if donor is not None and donor.completed else None
    reuse_dedupe = bool(
        all_changes and donor_dedupe and resume_mod.same_model(donor, provenance, "DEDUPE")
        and donor_dedupe.get("prompt_sha256") == DEDUPE_PROMPT_SHA256
        and donor_dedupe.get("projectchange_ids") == [c["projectchange_id"] for c in all_changes]
        and _canonical_sha256(donor.rebase(donor.miner_results.get("projectchanges"))) == _canonical_sha256(all_changes)
        and isinstance(donor_dedupe.get("raw_answer"), dict)
        and donor_dedupe.get("raw_answer_sha256") == _canonical_sha256(donor_dedupe["raw_answer"])
    )
    if reuse_dedupe:
        dedupe_answer = donor_dedupe["raw_answer"]
        provenance["dedupe_reused_from_run_id"] = donor.run_id
    try:
        if all_changes and not reuse_dedupe:
            dedupe_answer = complete(
                stage="DEDUPE", call_id=f"{pair_id}_DEDUPE", pair_id=pair_id,
                prompt=DEDUPE_PROMPT,
                data={"pair": pair_id, "projectchanges": [compact_change(c) for c in all_changes]},
                schema=DEDUPE_SCHEMA, images=[],
            )
            calls += 0 if using_test_provider else 1
    except ProviderError as exc:
        raise provider_failure("Dedupe", exc) from exc

    def save_dedupe_answer() -> None:
        # 3.11.0: the paid answer goes to disk before anything judges it.
        try:
            _save_artifact(session_id, pair_id, "project_change_v3_dedupe", {
                "schema": "projectchange_v3_dedupe/1", "pair_id": pair_id, "run_id": run_id,
                "prompt_sha256": DEDUPE_PROMPT_SHA256,
                "projectchange_ids": [c["projectchange_id"] for c in all_changes],
                "raw_answer": dedupe_answer, "raw_answer_sha256": _canonical_sha256(dedupe_answer),
                "repairs": dedupe_repairs,
            })
        except Exception as exc:  # noqa: BLE001
            raise fail("result_persistence_failed", "V3: ответ Dedupe не сохранён", exc) from exc

    if dedupe_answer is not None:
        save_dedupe_answer()
    try:
        if dedupe_answer is not None:
            if str(dedupe_answer.get("pair")) != str(pair_id):
                raise RuntimeError("Dedupe pair mismatch")
            # A decision that cannot be applied as given is not applied: its cards stay separate.
            dedupe_raw, dedupe_repairs = repair_decisions(all_changes, dedupe_answer)
            final_changes = apply_dedupe(pair_id, all_changes, dedupe_raw, lossless=dedupe_lossless)
        else:
            dedupe_raw = {"pair": pair_id, "decisions": [], "notes": ["empty"]}
            final_changes = []
    except Exception as exc:  # noqa: BLE001
        raise fail("dedupe_failed", f"V3 Dedupe failed: {exc}", exc) from exc
    if dedupe_repairs:
        save_dedupe_answer()
        provenance["dedupe_repair"] = {
            "repaired_decisions": sum(1 for r in dedupe_repairs if r["decision_index"] is not None),
            "cards_kept_separate": sum(len(r["projectchange_ids"]) for r in dedupe_repairs),
            "problems": sorted({p for r in dedupe_repairs for p in r["problems"]}),
        }
        logger.warning("V3 Dedupe answer repaired: session=%s pair=%s run=%s: %s",
                       session_id, pair_id, run_id, provenance["dedupe_repair"])

    # Historical calls retain their donor's model. All new calls must use this
    # run's model; a handoff permits only stages still to be performed.
    def verify_call_models() -> None:
        bad = resume_mod.mismatched_calls(transport_calls, provenance, donor)
        if bad:
            raise fail("v3_model_mixing", f"V3: неожиданная конфигурация модели в вызовах: {bad}")

    verify_call_models()

    miner_result_artifact = {
        "run_id": run_id,
        "regions": mined_regions,
    }
    progress("Подготовка проверки параметров по источникам", "SOURCE_VERIFICATION")
    try:
        quality = build_quality_ledger(
            pair_id=pair_id,
            run_id=run_id,
            projectchanges=final_changes,
            unresolved_hints=all_hints,
            hint_identities=hint_identities(
                {"run_id": run_id, "unresolved_hints": all_hints},
                miner_result_artifact,
            ),
            coverage=coverage,
            page_records=pages_by_key,
            review_all_parameters=subjects_mode,
        )
    except Exception as exc:  # noqa: BLE001
        raise fail("quality_build_failed", "V3: реестр качества результата не построен", exc) from exc

    verification_batches: list[dict[str, Any]] = []
    verification_results: list[dict[str, Any]] = []
    verification_enabled = os.environ.get(SOURCE_VERIFICATION_ENV, "1").strip() == "1"
    if verification_enabled and quality["verification_work_items"]:
        try:
            review_context = subject_search.verification_context(final_changes, subject_ledger, structure) if subjects_mode else None
            if subjects_mode:
                _save_artifact(session_id, pair_id, subject_search.ARTIFACT, subject_ledger)
                provenance['subject_search'] = subject_ledger['summary']
                provenance['subject_ledger_sha256'] = subject_search.digest(subject_ledger)
            planned_verification = build_verification_batches(
                work_items=quality["verification_work_items"],
                projectchanges=final_changes,
                page_records=pages_by_key,
                max_batches=verification_max_batches,
                **({'context_pages': review_context, 'max_items_per_batch': 1} if subjects_mode else {}),
            )
        except Exception as exc:  # noqa: BLE001
            raise fail("verification_planning_failed", f"V3: проверка параметров не спланирована: {exc}", exc) from exc
        verification_by_index: dict[int, dict[str, Any]] = {}
        # 3.26.0: on the same model, a batch whose question is byte-identical to one
        # the donor had accepted takes the donor's answer, re-validated here.
        donor_verification = (
            resume_mod.donor_review_answers(donor, "SOURCE_VERIFICATION", VERIFY_PROMPT_SHA256)
            if donor is not None and resume_mod.same_model(donor, provenance, "SOURCE_VERIFICATION") else {})

        def reuse_parameters(batch, data):
            saved = donor_verification.get(_canonical_sha256(data))
            if saved is None:
                return None
            answer = saved["record"]["response"]
            try:
                _validate_json_schema(answer, VERIFICATION_SCHEMA)
                validate_verification_answer(pair_id=pair_id, batch=batch, answer=answer)
            except Exception:  # noqa: BLE001 — not the same answer after all: ask again
                return None
            review_attempts.append({**saved["entry"], "batch_id": batch["batch_id"],
                                    "donor_batch_id": saved["entry"].get("batch_id"),
                                    "resumed_from_run_id": donor.run_id})
            review_attempts.sort(key=lambda r: (r["stage"], r["batch_id"]))
            return {"call_id": saved["record"].get("call_id"), "prompt_sha256": VERIFY_PROMPT_SHA256,
                    "status": "ACCEPTED", "result": answer, "error_code": None, "error_message": None,
                    "resumed_from_run_id": donor.run_id}

        def review_parameters(index, batch):
            data = {"pair": pair_id, **batch["data"]}
            row = reuse_parameters(batch, data)
            if row is None:
                row = yield from review_answer(
                    stage="SOURCE_VERIFICATION", batch_id=batch["batch_id"],
                    call_id=f"{pair_id}_{batch['batch_id']}", prompt_sha256=VERIFY_PROMPT_SHA256,
                    request={"prompt": VERIFY_PROMPT, "data": data,
                             "schema": VERIFICATION_SCHEMA, "images": batch["images"]},
                    validate=lambda answer: validate_verification_answer(pair_id=pair_id, batch=batch, answer=answer),
                    validation_code="verification_validation_failed",
                )
            verification_by_index[index] = {"batch_id": batch["batch_id"],
                                            "work_item_ids": batch["work_item_ids"], **row}

        run_review_batches(planned_verification, review_parameters, verification_workers, "SOURCE_VERIFICATION",
                           "независимая проверка параметров", verification_by_index)
        verification_batches = [verification_by_index[i] for i in sorted(verification_by_index)]
        for row in verification_batches:
            if row["status"] == "ACCEPTED":
                verification_results.extend(row["result"]["results"])
            else:
                for work in quality["verification_work_items"]:
                    if work["work_item_id"] in row["work_item_ids"]:
                        work["status"] = "FAILED"
    apply_verification_results(quality, verification_results)
    try:
        _save_artifact(session_id, pair_id, "project_change_v3_verification_results", {
            "schema": "projectchange_v3_verification_results/1",
            "pair_id": pair_id,
            "run_id": run_id,
            "enabled": verification_enabled,
            "prompt_sha256": VERIFY_PROMPT_SHA256,
            "batches": verification_batches,
            "limits": {"max_batches": verification_max_batches if verification_enabled else 0,
                       "max_items_per_batch": 8, "concurrency": verification_workers},
            "summary": {
                key: value for key, value in quality["summary"].items()
                if key.startswith("source_verification_")
            },
        })
        _save_artifact(session_id, pair_id, "project_change_v3_quality", quality)
    except Exception as exc:  # noqa: BLE001
        raise fail("quality_persistence_failed", "V3: реестр качества результата не сохранён", exc) from exc

    verify_call_models()

    # Deterministic source checks (0 model calls, each behind its own flag):
    # review signals next to the cards; nothing is edited or removed.
    checks_enabled = enabled_checks()
    source_checks: dict[str, Any] | None = None
    if any(checks_enabled.values()):
        try:
            source_checks = {"pair_id": pair_id, "run_id": run_id, **run_source_checks(
                projectchanges=final_changes, unresolved_hints=all_hints,
                page_records=pages_by_key, enabled=checks_enabled,
            )}
            _save_artifact(session_id, pair_id, "project_change_v3_source_checks", source_checks)
        except Exception as exc:  # noqa: BLE001
            raise fail("source_checks_failed", f"V3: детерминированные проверки источника не выполнены: {exc}", exc) from exc

    # 5. Result persistence — without it nothing is published.
    verification_summary = quality["summary"]
    verification_attention = any(verification_summary.get(key) for key in (
        "source_verification_corrected", "source_verification_conflicts",
        "source_verification_unreadable", "source_verification_pending"))
    verification_status = ("PARTIAL" if verification_attention else "COMPLETED") if verification_batches else "SKIPPED"
    verification_message = (
        "Проверка отключена." if not verification_enabled else
        "Нет параметров для проверки." if not quality["verification_work_items"] else
        f"Проверено параметров: {verification_summary.get('source_verification_completed', 0)}; "
        f"осталось: {verification_summary.get('source_verification_pending', 0)}."
    )
    progress("Сохранение результатов сравнения", "PERSISTING", pipeline_updates={
        "SOURCE_VERIFICATION": {"status": verification_status, "message": verification_message,
                                "processed": verification_summary.get("source_verification_completed", 0),
                                "verification": {key.removeprefix("source_verification_"): value
                                                 for key, value in verification_summary.items()
                                                 if key.startswith("source_verification_")},
                                "total": len(quality["verification_work_items"]), "unit": "parameter"},
    })
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
        "coverage": coverage["summary"],
        "unmatched_work_items": coverage["unmatched_work_items"],
        "supplemental_review": {
            "enabled": supplemental_enabled,
            "batches": len(supplemental_batches),
            "accepted": sum(row["status"] == "ACCEPTED" for row in supplemental_batches),
            "failed": sum(row["status"] == "FAILED" for row in supplemental_batches),
            "prompt_sha256": UNMATCHED_PROMPT_SHA256,
        },
        "quality": quality["summary"],
        "source_verification": {
            "enabled": verification_enabled,
            "batches": len(verification_batches),
            "accepted": sum(row["status"] == "ACCEPTED" for row in verification_batches),
            "failed": sum(row["status"] == "FAILED" for row in verification_batches),
            "prompt_sha256": VERIFY_PROMPT_SHA256,
        },
    }
    if source_checks is not None:
        final["source_checks"] = source_checks["summary"]
    if dedupe_repairs:  # absent unless the Dedupe answer had to be repaired
        final["dedupe_repair"] = {**provenance["dedupe_repair"], "repairs": dedupe_repairs,
                                  "raw_answer_sha256": _canonical_sha256(dedupe_answer)}
    if dedupe_lossless:  # without the flag the result stays byte-identical to 3.7.0
        final["dedupe_policy"] = {
            "lossless": True,
            "canonical_reselected": sum(bool(c.get("dedupe_canonical_reselected")) for c in final_changes),
            "members_with_residual": sum(len(c.get("dedupe_residual") or {}) for c in final_changes),
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
        "coverage_complete": coverage["summary"]["complete"],
        "coverage_pages_pending": coverage["summary"]["pages_pending"],
        "coverage_content_unmatched": coverage["summary"]["content_unmatched"],
        "coverage_content_unmatched_pending": coverage["summary"]["content_unmatched_pending"],
        "quality_independently_verified": quality["summary"]["independently_verified"],
        "quality_literal_review_flags": quality["summary"]["literal_review_flags"],
        "quality_source_verification_pending": quality["summary"]["source_verification_pending"],
    }
    if subjects_mode:
        summary['comparison_mode'] = 'subjects'
        summary['subject_search'] = provenance.get('subject_search')
    if source_checks is not None:
        summary["source_check_findings"] = source_checks["summary"]["findings_total"]
    if dedupe_repairs:
        summary["dedupe_repaired_decisions"] = provenance["dedupe_repair"]["repaired_decisions"]
    if donor is not None:
        summary["resumed_from_run_id"] = donor.run_id
    from backend.app.services.project_change_consolidator.stage import enabled as consolidation_enabled

    summary["consolidation_expected"] = consolidation_enabled() and not using_test_provider
    progress("Подготовка сопоставлений для просмотра", "HUMAN_MAPPING")
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

    needs_review = bool(
        subjects_mode
        or all_hints
        or not coverage["summary"]["complete"]
        or quality["summary"].get("source_verification_corrected")
        or quality["summary"].get("source_verification_conflicts")
        or quality["summary"].get("source_verification_unreadable")
        or quality["summary"].get("source_verification_pending")
        or bool(source_checks and source_checks["summary"]["findings_total"])
        or bool(dedupe_repairs)
    )
    coverage_message = (
        f", {coverage['summary']['content_unmatched_pending']} unmatched content pages pending"
        if coverage["summary"]["content_unmatched_pending"] else ""
    )
    return state(
        "REVIEW" if needs_review else "COMPLETED",
        f"V3 comparison finished: {len(final_changes)} ProjectChanges, {len(all_hints)} unresolved hints"
        f"{coverage_message}.",
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
        model_profile=kwargs.get("model_profile") or None,
        parallelism=kwargs.get("parallelism"),
        resume_from_run_id=kwargs.get("resume_from_run_id") or None,
        resume_model_policy=kwargs.get("resume_model_policy", "same_model"),
        comparison_mode=kwargs.get("comparison_mode"),
        max_new_miner_calls=kwargs.get("max_new_miner_calls"),
    )
