"""ИИ-слой сравнения: схема, верификатор, кэш, приоритет человека, отказы.

Ни один тест здесь не вызывает модель. Шлюз подменяется, потому что проверять
надо не провайдера, а поведение системы вокруг него: что публикуется, что не
публикуется никогда, и что происходит, когда модели нет.
"""
from __future__ import annotations

import json
import time

import pytest

from backend.app.services.stage_comparison.ai import (
    cache as cache_module,
    evidence as evidence_module,
    gateway,
    resolution as resolution_module,
    schemas,
    settings,
    verifier,
)


# ── Фикстуры ──────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _standard_mode(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "STANDARD")
    monkeypatch.setenv("STAGE_COMPARISON_AI_CACHE_ENABLED", "false")
    monkeypatch.setenv("STAGE_COMPARISON_AI_BATCH_SIZE", "10")
    monkeypatch.setenv("STAGE_COMPARISON_AI_CONCURRENCY", "1")
    monkeypatch.setenv("STAGE_COMPARISON_AI_MAX_RETRIES", "0")


def _fragment(fragment_id: str, side: str, page: int, text: str, order: int) -> dict:
    return {
        "id": fragment_id,
        "pdf_page": page,
        "text": text,
        "canonical_text": text.casefold(),
        "source_kind": "table_row",
        "source_group": f"{side}-block:table",
        "location_parts": text.split(" | "),
        "order": order,
    }


def _preparation() -> dict:
    return {
        "fragments": {
            "left": [
                _fragment("l0", "left", 29, "Экспликация помещений", 1),
                _fragment("l1", "left", 29, "24.5 | Кладовая | 6,02", 2),
                _fragment("l2", "left", 29, "24.6 | Холл | 15,71", 3),
            ],
            "right": [
                _fragment("r0", "right", 8, "Экспликация помещений", 1),
                _fragment("r1", "right", 8, "24.5 | Кладовая | 6,40", 2),
                _fragment("r2", "right", 8, "24.6 | Холл | 15,71", 3),
            ],
        },
    }


def _sheet_relations() -> dict:
    return {
        "kind": "stage_comparison_sheet_relations",
        "input_signature": "sheet-input",
        "sheet_labels": {
            "LEFT": {"29": "Корпуса 1, 2. План 3 этажа"},
            "RIGHT": {"8": "Корпуса 1, 2. План 3 этажа"},
        },
        "relations": [{
            "relation_id": "srel_a",
            "left_pages": [29],
            "right_pages": [8],
            "relation_type": "MATCHED",
            "status": "HIGH",
            "confidence": 1.0,
            "primary_source": "STAMP_EXACT",
            "reason_codes": ["stamp_key_exact"],
        }],
    }


def _groups() -> list[dict]:
    return [{"id": "srel_a", "left_pages": [29], "right_pages": [8]}]


def _review_item(item_id: str = "ureview_1") -> dict:
    scope = evidence_module.scope_ref_for_group(_groups()[0])
    return {
        "review_evidence_id": item_id,
        "atom_id": "tatom_1",
        "source": "TEXT",
        "scope_ref": scope,
        "dimension": "UNKNOWN_DIMENSION",
        "direction": "ALTERED",
        "outcome": "REVIEW_REQUIRED",
        "before_value": "24.5 | Кладовая | 6,02",
        "after_value": "24.5 | Кладовая | 6,40",
        "reason_codes": ["dimension_unknown"],
        "evidence_refs": [{"evidence_ref": "teva_1", "atom_id": "tatom_1"}],
        "provenance": {
            "source_atom": {
                "stage3_bucket": "changed",
                "structured_fact": False,
                "locations": {
                    "LEFT": [{"page": 29, "fragment_id": "l1"}],
                    "RIGHT": [{"page": 8, "fragment_id": "r1"}],
                },
            },
            "source_atom_outcome": "REVIEW_REQUIRED",
        },
    }


def _packages(item: dict | None = None) -> list:
    return evidence_module.build_packages(
        review_items=[item or _review_item()],
        preparation=_preparation(),
        sheet_relations=_sheet_relations(),
        comparison_groups=_groups(),
        batch_size=10,
    )


def _good_resolution(item_id: str = "ureview_1") -> dict:
    return {
        "item_id": item_id,
        "resolution_status": "AI_RESOLVED",
        "dimension": "PARAMETER",
        "direction": "INCREASED",
        "outcome": "MATERIAL_CHANGE",
        "object_label": "помещение 24.5",
        "facet_label": "площадь",
        "before_value": "24.5 | Кладовая | 6,02",
        "after_value": "24.5 | Кладовая | 6,40",
        "confidence": "HIGH",
        "evidence_quotes": [
            {"side": "LEFT", "quote": "24.5 | Кладовая | 6,02"},
            {"side": "RIGHT", "quote": "24.5 | Кладовая | 6,40"},
        ],
        "needs_human_review": False,
        "human_reason": "NOT_APPLICABLE",
        "human_question": None,
        "engineering_summary": "Площадь кладовой 24.5 увеличена с 6,02 до 6,40 м².",
    }


def _fake_call(payload, *, ok=True, error_kind="", delay=0.0):
    def call(provider_family, prompt, **kwargs):
        if delay:
            time.sleep(delay)
        return gateway.CallResult(
            provider_family, kwargs.get("model", "m"),
            kwargs.get("reasoning_level"), ok,
            parsed=payload if ok else None,
            error="" if ok else "модель недоступна",
            error_kind=error_kind,
        )
    return call


# ── A. Схема ──────────────────────────────────────────────────────────────


def test_analyst_schema_forbids_internal_refs_and_boxes():
    properties = schemas.ANALYST_SCHEMA["properties"]["resolutions"]["items"][
        "properties"
    ]
    assert "project_entity_ref" not in properties
    assert "subject_ref" not in properties
    assert "bbox" not in properties
    assert "object_label" in properties
    assert schemas.ANALYST_SCHEMA["additionalProperties"] is False


def test_analyst_schema_enums_follow_the_policy_contract():
    from backend.app.services.stage_comparison.unified_change_policy.contract import (
        DIRECTIONS,
        EVIDENCE_DIMENSIONS,
    )

    item = schemas.ANALYST_SCHEMA["properties"]["resolutions"]["items"]["properties"]
    assert item["dimension"]["enum"] == list(EVIDENCE_DIMENSIONS)
    assert item["direction"]["enum"] == list(DIRECTIONS)


# ── B. Верификатор ────────────────────────────────────────────────────────


def _item_view():
    return _packages()[0].items[0].model_view()


def test_verifier_accepts_a_grounded_resolution():
    assert verifier.verify_resolution(_item_view(), _good_resolution()).ok


def test_verifier_rejects_a_hallucinated_value():
    resolution = _good_resolution() | {"after_value": "24.5 | Кладовая | 9,99"}
    result = verifier.verify_resolution(_item_view(), resolution)
    assert not result.ok
    assert any("after_value" in error for error in result.errors)


def test_verifier_rejects_swapped_sides():
    resolution = _good_resolution() | {
        "before_value": "24.5 | Кладовая | 6,40",
        "after_value": "24.5 | Кладовая | 6,02",
        "evidence_quotes": [],
    }
    result = verifier.verify_resolution(_item_view(), resolution)
    assert not result.ok
    assert any("переставлен" in error for error in result.errors)


def test_verifier_rejects_a_quote_that_is_not_in_the_package():
    resolution = _good_resolution()
    resolution["evidence_quotes"] = [
        {"side": "LEFT", "quote": "24.5 | Кладовая | 6,02"},
        {"side": "RIGHT", "quote": "здесь такого текста нет"},
    ]
    result = verifier.verify_resolution(_item_view(), resolution)
    assert not result.ok
    assert any("цитата" in error for error in result.errors)


def test_verifier_rejects_an_invented_internal_reference():
    resolution = _good_resolution() | {
        "object_label": "project_text_entity_deadbeef"
    }
    result = verifier.verify_resolution(_item_view(), resolution)
    assert not result.ok
    assert any("идентификаторы" in error for error in result.errors)


def test_verifier_rejects_resolved_with_unknown_dimension():
    resolution = _good_resolution() | {"dimension": "UNKNOWN_DIMENSION"}
    result = verifier.verify_resolution(_item_view(), resolution)
    assert not result.ok


def test_verifier_reports_missing_and_extra_items():
    items = [_item_view()]
    verified, problems = verifier.verify_batch(items, {"resolutions": [
        _good_resolution("ureview_other"),
    ]})
    assert verified == {}
    assert any("вне пакета" in problem for problem in problems)
    assert any("полнота" in problem for problem in problems)


def test_verifier_rejects_a_duplicated_item():
    items = [_item_view()]
    _verified, problems = verifier.verify_batch(items, {"resolutions": [
        _good_resolution(), _good_resolution(),
    ]})
    assert any("дважды" in problem for problem in problems)


# ── C. Слой разрешения ────────────────────────────────────────────────────


def _resolve(call, **kwargs):
    layer = resolution_module.AiResolutionLayer(call=call, **kwargs)
    return layer.resolve(
        review_items=[_review_item()],
        preparation=_preparation(),
        sheet_relations=_sheet_relations(),
        comparison_groups=_groups(),
        generated_at="fixed",
    )


def test_a_verified_resolution_becomes_a_typed_resolution():
    artifact = _resolve(_fake_call({"resolutions": [_good_resolution()]}))
    assert artifact["diagnostics"]["ai_resolved"] == 1
    entry = artifact["resolutions"][0]
    assert entry["status"] == "AI_RESOLVED"
    typed = entry["typed_resolution"]
    assert typed["dimension"] == "PARAMETER"
    assert typed["object_label"] == "помещение 24.5"
    # Внутренние ссылки чеканит бэкенд, а не модель.
    assert "project_entity_ref" not in typed


def test_a_rejected_resolution_is_never_published():
    bad = _good_resolution() | {"after_value": "24.5 | Кладовая | 9,99"}
    artifact = _resolve(_fake_call({"resolutions": [bad]}))
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_VERIFIER_REJECTED
    assert entry["typed_resolution"] is None
    assert artifact["diagnostics"]["verifier_rejected"] == 1


def test_an_unavailable_model_leaves_the_item_to_a_human():
    artifact = _resolve(_fake_call(None, ok=False))
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_MODEL_FAILED
    assert artifact["diagnostics"]["model_failures"] == 1


def test_a_timeout_leaves_the_item_to_a_human():
    artifact = _resolve(_fake_call(None, ok=False, error_kind="TIMEOUT"))
    entry = artifact["resolutions"][0]
    assert entry["reason_code"] == resolution_module.REASON_MODEL_TIMEOUT
    assert artifact["diagnostics"]["model_timeouts"] == 1


def test_a_cancelled_run_stops_calling_the_model():
    token = gateway.CancelToken()
    token.cancel()
    calls: list[str] = []

    def call(provider_family, prompt, **kwargs):
        calls.append(provider_family)
        return gateway.CallResult(provider_family, "m", None, True, parsed={})

    artifact = _resolve(call, cancel=token)
    assert calls == []
    assert artifact["resolutions"][0]["reason_code"] == (
        resolution_module.REASON_CANCELLED
    )


def test_a_declining_model_is_not_a_failure(monkeypatch):
    declined = _good_resolution() | {
        "resolution_status": "HUMAN_REQUIRED",
        "needs_human_review": True,
        "human_reason": "SHEET_RELATION_WRONG",
        "human_question": "Это точно один и тот же лист?",
    }
    artifact = _resolve(_fake_call({"resolutions": [declined]}))
    entry = artifact["resolutions"][0]
    assert entry["reason_code"] == resolution_module.REASON_MODEL_DECLINED
    assert entry["human_question"] == "Это точно один и тот же лист?"


def test_the_item_budget_sends_the_remainder_to_a_human(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MAX_ITEMS", "0")
    artifact = _resolve(_fake_call({"resolutions": [_good_resolution()]}))
    entry = artifact["resolutions"][0]
    assert entry["reason_code"] == resolution_module.REASON_BUDGET_EXHAUSTED
    assert "max_items" in artifact["diagnostics"]["budgets_hit"]


def test_the_batch_budget_sends_the_remainder_to_a_human(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MAX_BATCHES", "0")
    artifact = _resolve(_fake_call({"resolutions": [_good_resolution()]}))
    assert artifact["resolutions"][0]["reason_code"] == (
        resolution_module.REASON_BUDGET_EXHAUSTED
    )


def test_the_audit_trail_records_the_provider_without_secrets():
    artifact = _resolve(_fake_call({"resolutions": [_good_resolution()]}))
    audit = artifact["resolutions"][0]["audit"]
    assert audit["provider_family"] == settings.CODEX_SESSION
    assert audit["model"] == settings.analyst_model()
    assert audit["reasoning_level"] == settings.analyst_effort()
    assert audit["prompt_version"] == schemas.PROMPT_VERSION
    assert audit["verifier_version"] == verifier.VERIFIER_VERSION
    assert audit["evidence_digest"] and audit["output_digest"]
    blob = json.dumps(artifact, ensure_ascii=False).lower()
    for secret in ("api_key", "authorization", "sk-", "token="):
        assert secret not in blob


def test_the_off_mode_makes_no_calls_and_claims_nothing():
    artifact = resolution_module.empty_artifact(generated_at="fixed")
    assert artifact["resolutions"] == []
    assert artifact["diagnostics"]["uses_model"] is False


# ── D. Критик ─────────────────────────────────────────────────────────────


def test_the_critic_can_reject_a_verified_resolution(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")
    seen: list[str] = []

    def call(provider_family, prompt, **kwargs):
        seen.append(provider_family)
        if provider_family == settings.CLAUDE_SESSION:
            return gateway.CallResult(
                provider_family, "claude-opus-5", None, True,
                parsed={
                    "verdict": "REJECT",
                    "problems": [{"code": "WRONG_ENTITY", "detail": "не тот объект"}],
                    "explanation": "Слева речь о соседнем помещении.",
                },
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    artifact = _resolve(call)
    assert settings.CLAUDE_SESSION in seen
    entry = artifact["resolutions"][0]
    assert entry["reason_code"] == resolution_module.REASON_CRITIC_REJECTED
    assert entry["typed_resolution"] is None
    assert artifact["diagnostics"]["critic_rejected"] == 1


def test_the_critic_is_not_called_in_standard_mode():
    seen: list[str] = []

    def call(provider_family, prompt, **kwargs):
        seen.append(provider_family)
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    _resolve(call)
    assert seen == [settings.CODEX_SESSION]


def test_an_unavailable_critic_neither_accepts_nor_rejects(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    def call(provider_family, prompt, **kwargs):
        if provider_family == settings.CLAUDE_SESSION:
            return gateway.CallResult(
                provider_family, "claude-opus-5", None, False,
                error="недоступен", error_kind="TRANSIENT",
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    artifact = _resolve(call)
    entry = artifact["resolutions"][0]
    assert entry["status"] == "AI_RESOLVED"
    assert entry["critic"] is None


# ── E. Кэш ────────────────────────────────────────────────────────────────


def test_the_cache_serves_the_second_run_without_calling_the_model(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_CACHE_ENABLED", "true")
    calls: list[int] = []

    def call(provider_family, prompt, **kwargs):
        calls.append(1)
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    first = _resolve(call, cache_dir=tmp_path / "cache")
    second = _resolve(call, cache_dir=tmp_path / "cache")
    assert len(calls) == 1
    assert first["diagnostics"]["ai_resolved"] == 1
    assert second["diagnostics"]["ai_resolved"] == 1
    assert second["diagnostics"]["cache"]["hits"] == 1


def test_the_cache_key_changes_with_every_input_it_names():
    base = dict(
        evidence_digest="d", model="m", reasoning_level="low",
        prompt_version="p", schema_version="s", role="analyst",
    )
    reference = cache_module.cache_key(**base)
    for field, value in (
        ("evidence_digest", "other"), ("model", "other"),
        ("reasoning_level", "high"), ("prompt_version", "other"),
        ("schema_version", "other"), ("role", "critic"),
    ):
        assert cache_module.cache_key(**{**base, field: value}) != reference


# ── F. Шлюз ───────────────────────────────────────────────────────────────


def test_the_gateway_strips_provider_keys_from_the_session(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "secret")
    monkeypatch.setenv("OPENAI_API_KEY", "secret")
    monkeypatch.setenv("CLAUDECODE", "1")
    env = gateway._clean_env("run-1")
    assert "ANTHROPIC_API_KEY" not in env
    assert "OPENAI_API_KEY" not in env
    assert "CLAUDECODE" not in env
    assert env[gateway.RUN_MARKER_ENV] == "run-1"


def test_the_gateway_refuses_an_unknown_provider_family():
    with pytest.raises(gateway.GatewayError):
        gateway.call("HTTP_API", "prompt")


@pytest.mark.parametrize(
    "text,kind",
    [
        ("Selected model is at capacity", "TRANSIENT"),
        ("usage limit reached", "PERMANENT"),
        ("что-то странное", "UNKNOWN"),
    ],
)
def test_the_gateway_classifies_provider_failures(text, kind):
    assert gateway.classify_failure(text) == kind


def test_the_gateway_reads_json_out_of_a_noisy_answer():
    assert gateway.extract_json('шум\n```json\n{"a": 1}\n```\nещё шум') == {"a": 1}


# ── G. Пакет доказательств ────────────────────────────────────────────────


def test_the_package_shows_sheet_titles_and_neighbouring_lines():
    package = _packages()[0]
    assert package.sheet_relation["left_sheets"][0]["title"] == (
        "Корпуса 1, 2. План 3 этажа"
    )
    view = package.items[0].model_view()
    assert any(line.startswith("»") for line in view["left_context"])
    assert any("24.6" in line for line in view["left_context"])


def test_the_package_view_carries_no_internal_identifiers():
    view = _packages()[0].items[0].model_view()
    blob = json.dumps(view, ensure_ascii=False)
    for marker in ("tatom_", "teva_", "srel_", "text_scope_"):
        assert marker not in blob


def test_batches_split_by_size_and_stay_inside_one_sheet_relation():
    items = [_review_item(f"ureview_{index}") for index in range(25)]
    packages = evidence_module.build_packages(
        review_items=items,
        preparation=_preparation(),
        sheet_relations=_sheet_relations(),
        comparison_groups=_groups(),
        batch_size=10,
    )
    assert [len(package.items) for package in packages] == [10, 10, 5]
    assert {package.relation_id for package in packages} == {"srel_a"}
