"""ИИ-слой сравнения: схема, верификатор, кэш, приоритет человека, отказы.

Ни один тест здесь не вызывает модель. Шлюз подменяется, потому что проверять
надо не провайдера, а поведение системы вокруг него: что публикуется, что не
публикуется никогда, и что происходит, когда модели нет.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock
import json
import tempfile
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
                # Лист прочитан надёжно: тесты этого файла про привязку ответа
                # модели, а не про полноту распознавания.
                "recognition_coverage": {
                    "status": "SUFFICIENT", "reason_codes": [],
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
        "object_evidence_ref": "L2",
        "facet_label": "площадь",
        "before_value": "24.5 | Кладовая | 6,02",
        "before_evidence_ref": "L2",
        "after_value": "24.5 | Кладовая | 6,40",
        "after_evidence_ref": "R2",
        "confidence": "HIGH",
        "evidence_quotes": [
            {"side": "LEFT", "evidence_ref": "L2", "quote": "24.5 | Кладовая | 6,02"},
            {"side": "RIGHT", "evidence_ref": "R2", "quote": "24.5 | Кладовая | 6,40"},
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


def test_verifier_accepts_a_quote_copied_with_the_context_marker():
    """«»» — наша разметка пакета. Ловить за неё надо себя, а не модель."""
    resolution = _good_resolution()
    resolution["evidence_quotes"] = [
        {"side": "LEFT", "evidence_ref": "L2", "quote": "» 24.5 | Кладовая | 6,02"},
        {"side": "RIGHT", "evidence_ref": "R2", "quote": "  24.5 | Кладовая | 6,40"},
    ]
    assert verifier.verify_resolution(_item_view(), resolution).ok


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
            # Существенное изменение, в котором модель не уверена, — повод
            # позвать критика.
            parsed={"resolutions": [_good_resolution() | {"confidence": "MEDIUM"}]},
        )

    artifact = _resolve(call)
    assert settings.CLAUDE_SESSION in seen
    entry = artifact["resolutions"][0]
    assert entry["reason_code"] == resolution_module.REASON_CRITIC_REJECTED
    assert entry["typed_resolution"] is None
    assert artifact["diagnostics"]["critic_rejected"] == 1


def test_the_critic_is_not_spent_on_a_resolution_with_nothing_to_doubt(monkeypatch):
    """Разбор, прошедший проверку без замечаний и с высокой уверенностью, уже
    доказан цитатами: вторая модель здесь ничего не добавляет, а стоит."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")
    seen: list[str] = []

    def call(provider_family, prompt, **kwargs):
        seen.append(provider_family)
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    artifact = _resolve(call)

    assert seen == [settings.CODEX_SESSION]
    entry = artifact["resolutions"][0]
    assert entry["status"] == "AI_RESOLVED"
    assert entry["critic_triggers"] == []
    assert artifact["diagnostics"]["critic_required"] == 0
    assert artifact["diagnostics"]["mode_completeness"] == "COMPLETE"


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


def test_a_required_critic_that_cannot_answer_blocks_the_resolution(monkeypatch):
    """«Не проверено» и «проверено, возражений нет» — разные утверждения."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    def call(provider_family, prompt, **kwargs):
        if provider_family == settings.CLAUDE_SESSION:
            return gateway.CallResult(
                provider_family, "claude-opus-5", None, False,
                error="недоступен", error_kind="TRANSIENT",
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution() | {"confidence": "MEDIUM"}]},
        )

    artifact = _resolve(call)
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_CRITIC_UNAVAILABLE
    assert entry["typed_resolution"] is None
    assert entry["critic"] is None
    assert artifact["diagnostics"]["critic_unavailable"] == 1
    # Глубокий режим не притворяется завершённым.
    assert artifact["diagnostics"]["mode_completeness"] == "PARTIAL"


def test_a_malformed_critic_answer_is_not_an_acceptance(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    def call(provider_family, prompt, **kwargs):
        if provider_family == settings.CLAUDE_SESSION:
            return gateway.CallResult(
                provider_family, "claude-opus-5", None, True,
                parsed={"verdict": "МОЖЕТ БЫТЬ", "problems": [], "explanation": ""},
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution() | {"confidence": "MEDIUM"}]},
        )

    artifact = _resolve(call)
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_CRITIC_UNAVAILABLE


def test_the_critic_budget_running_out_also_blocks_rather_than_publishes(monkeypatch):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")
    monkeypatch.setenv("STAGE_COMPARISON_AI_MAX_CRITIC_PASSES", "0")

    def call(provider_family, prompt, **kwargs):
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution() | {"confidence": "MEDIUM"}]},
        )

    artifact = _resolve(call)
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_CRITIC_UNAVAILABLE


def test_standard_mode_owes_no_critic_and_stays_complete():
    def call(provider_family, prompt, **kwargs):
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_good_resolution() | {"confidence": "MEDIUM"}]},
        )

    artifact = _resolve(call)

    assert artifact["resolutions"][0]["status"] == "AI_RESOLVED"
    assert artifact["diagnostics"]["critic_required"] == 0
    assert artifact["diagnostics"]["mode_completeness"] == "COMPLETE"


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


def test_the_session_environment_is_an_allowlist_not_a_blacklist(monkeypatch):
    # Чёрный список защищает ровно от тех имён, которые кто-то успел в него
    # внести. Каждое из этих появилось бы в проде позже правки — и проехало бы.
    for name in (
        "DATABASE_URL", "POSTGRES_PASSWORD", "REDIS_PASSWORD", "JWT_SECRET",
        "PORTAL_AUTH_PASSWORD", "SOME_VENDOR_TOKEN", "APP_SIGNING_KEY",
        "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY", "OPENROUTER_API_KEY",
        "MY_COMPLETELY_UNKNOWN_VARIABLE",
    ):
        monkeypatch.setenv(name, "secret")

    env = gateway._clean_env("run-1")

    assert "DATABASE_URL" not in env
    assert "MY_COMPLETELY_UNKNOWN_VARIABLE" not in env
    assert not [name for name in env if "SECRET" in name or "TOKEN" in name]
    assert not [name for name in env if "PASSWORD" in name]
    assert env[gateway.RUN_MARKER_ENV] == "run-1"


def test_the_allowlist_extension_can_never_be_used_to_smuggle_a_secret(monkeypatch):
    monkeypatch.setenv("SAFE_EXTRA", "value")
    monkeypatch.setenv("VENDOR_API_KEY", "secret")
    monkeypatch.setenv(
        gateway.ENV_ALLOWLIST_EXTENSION, "SAFE_EXTRA,VENDOR_API_KEY"
    )

    env = gateway._clean_env("run-1")

    assert env.get("SAFE_EXTRA") == "value"
    assert "VENDOR_API_KEY" not in env


def test_a_proxy_carrying_credentials_is_dropped_rather_than_forwarded(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.local:3128")
    monkeypatch.setenv("HTTP_PROXY", "http://user:pass@proxy.local:3128")

    env = gateway._clean_env("run-1")

    assert env.get("HTTPS_PROXY") == "http://proxy.local:3128"
    assert "HTTP_PROXY" not in env


def test_the_analyst_session_is_started_without_a_shell_or_the_repository():
    captured: dict[str, object] = {}

    def fake_run(command, *, cwd, env, timeout_s, stdin_text, cancel, run_id=""):
        captured["command"] = list(command)
        captured["cwd"] = cwd
        captured["stdin"] = stdin_text
        return 0, '{"resolutions": []}', "", ""

    with mock.patch.object(gateway, "_resolve_codex_binary", return_value="/bin/codex"), \
            mock.patch.object(gateway, "_run_process", side_effect=fake_run):
        gateway.call_codex("промпт", model="m", schema={"type": "object"})

    command = captured["command"]
    pairs = {
        (command[index], command[index + 1])
        for index in range(len(command) - 1)
    }
    for feature in gateway.CODEX_REQUIRED_OFF:
        assert ("--disable", feature) in pairs
    assert "--ignore-user-config" in command
    assert "--ignore-rules" in command
    assert command[command.index("-s") + 1] == "read-only"
    # Рабочий каталог — пустой временный, а не репозиторий и не артефакты.
    assert str(captured["cwd"]).startswith(tempfile.gettempdir())
    assert "PDF-proverka" not in str(captured["cwd"])
    # Промпт уходит через stdin: он не виден в `ps` и не упирается в ARG_MAX.
    assert captured["stdin"] == "промпт"
    assert "промпт" not in command
    assert command[-1] == "-"


def test_the_critic_session_carries_the_same_security_contract():
    captured: dict[str, object] = {}

    def fake_run(command, *, cwd, env, timeout_s, stdin_text, cancel, run_id=""):
        captured["command"] = list(command)
        captured["cwd"] = cwd
        captured["stdin"] = stdin_text
        return 0, '{"result": "{}", "usage": {}}', "", ""

    with mock.patch.object(gateway, "_resolve_claude_binary", return_value="/bin/claude"), \
            mock.patch.object(gateway, "_run_process", side_effect=fake_run):
        gateway.call_claude("промпт", model="m", schema={"type": "object"})

    command = captured["command"]
    assert command[command.index("--tools") + 1] == ""
    assert command[command.index("--setting-sources") + 1] == ""
    assert "--strict-mcp-config" in command
    assert "--no-session-persistence" in command
    assert "--disable-slash-commands" in command
    assert str(captured["cwd"]).startswith(tempfile.gettempdir())
    assert captured["stdin"] == "промпт"
    assert "промпт" not in command


_CODEX_HELP = (
    "--output-schema --sandbox --ignore-user-config --disable --config --image"
)


def _features(states: dict[str, str]) -> str:
    return "\n".join(f"{name} stable {value}" for name, value in states.items())


def _all_off() -> dict[str, str]:
    states = {name: "false" for name in gateway.CODEX_REQUIRED_OFF}
    states.update({name: "true" for name in gateway.CODEX_MUST_BE_KNOWN})
    return states


def test_runtime_validation_confirms_isolation_by_state_not_by_flag(monkeypatch):
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    monkeypatch.setattr(
        gateway, "_cli_probe",
        lambda command, timeout_s=30: (
            _features(_all_off()) if "features" in command else _CODEX_HELP
        ),
    )

    report = gateway.validate_runtime(require_vision=True)

    assert report["ok"] is True
    assert report["checks"]["codex_isolation_features"]["shell_tool"] == "false"


def test_runtime_validation_fails_when_the_shell_stays_enabled(monkeypatch):
    states = _all_off()
    states["shell_tool"] = "true"
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    monkeypatch.setattr(
        gateway, "_cli_probe",
        lambda command, timeout_s=30: (
            _features(states) if "features" in command else _CODEX_HELP
        ),
    )

    report = gateway.validate_runtime()

    assert report["ok"] is False
    assert any("shell_tool" in problem for problem in report["problems"])


def test_runtime_validation_fails_when_a_feature_was_renamed_away(monkeypatch):
    # Переименованная возможность превращает `--disable` в неиспользуемый ключ
    # конфигурации: флаг передан, изоляции нет, и молчать об этом нельзя.
    states = {
        name: "false" for name in gateway.CODEX_REQUIRED_OFF if name != "shell_tool"
    }
    states.update({name: "true" for name in gateway.CODEX_MUST_BE_KNOWN})
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    monkeypatch.setattr(
        gateway, "_cli_probe",
        lambda command, timeout_s=30: (
            _features(states) if "features" in command else _CODEX_HELP
        ),
    )

    report = gateway.validate_runtime()

    assert report["ok"] is False
    assert any("shell_tool" in problem for problem in report["problems"])


def test_runtime_validation_fails_when_structured_output_is_unsupported(monkeypatch):
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    monkeypatch.setattr(
        gateway, "_cli_probe",
        lambda command, timeout_s=30: (
            _features(_all_off()) if "features" in command
            else "--sandbox --ignore-user-config --disable --config"
        ),
    )

    report = gateway.validate_runtime()

    assert report["ok"] is False
    assert any("--output-schema" in problem for problem in report["problems"])


def test_runtime_validation_reports_no_secret_in_the_child_environment(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@host/db")
    monkeypatch.setattr(gateway, "_resolve_codex_binary", lambda: "/bin/codex")
    monkeypatch.setattr(
        gateway, "_cli_probe",
        lambda command, timeout_s=30: (
            _features(_all_off()) if "features" in command else _CODEX_HELP
        ),
    )

    report = gateway.validate_runtime()

    assert report["checks"]["environment_leaked_secrets"] == []
    assert "DATABASE_URL" not in report["checks"]["environment_names"]


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
    assert any(line["focus"] for line in view["left_context"])
    assert any("24.6" in line["text"] for line in view["left_context"])
    # У каждой строки есть адрес: без него привязать ответ модели не к чему.
    assert [line["ref"] for line in view["left_context"]] == ["L1", "L2", "L3"]


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


# ── H. Визуальный резерв ──────────────────────────────────────────────────


def _declined(reason: str = "GRAPHIC_EVIDENCE_REQUIRED") -> dict:
    return _good_resolution() | {
        "resolution_status": "HUMAN_REQUIRED",
        "needs_human_review": True,
        "human_reason": reason,
        "human_question": "Что показано на чертеже?",
    }


def test_vision_is_needed_only_on_two_explicit_signals():
    from backend.app.services.stage_comparison.ai import vision

    assert vision.needs_vision(
        resolution=None, graphic_route="VISION_REQUIRED", source="GRAPHIC"
    )
    assert vision.needs_vision(
        resolution=_declined(), graphic_route="MODE_1_APPLICABLE"
    )
    assert not vision.needs_vision(
        resolution=_declined("ENTITY_AMBIGUOUS"), graphic_route="MODE_1_APPLICABLE"
    )
    assert not vision.needs_vision(
        resolution=_good_resolution(), graphic_route="VISION_REQUIRED",
        source="GRAPHIC",
    )


def test_a_graphic_route_does_not_send_text_rows_to_the_drawing():
    """VISION_REQUIRED сказано про геометрию блока, а не про строку таблицы."""
    from backend.app.services.stage_comparison.ai import vision

    assert not vision.needs_vision(
        resolution=_declined("ENTITY_AMBIGUOUS"),
        graphic_route="VISION_REQUIRED",
        source="TEXT",
    )
    assert vision.needs_vision(
        resolution=_declined("ENTITY_AMBIGUOUS"),
        graphic_route="VISION_REQUIRED",
        source="GRAPHIC",
    )


def test_vision_is_not_called_in_standard_mode(monkeypatch, tmp_path):
    seen: list[str] = []

    def call(provider_family, prompt, **kwargs):
        seen.append("vision" if kwargs.get("images") else "text")
        return gateway.CallResult(
            provider_family, "m", None, True, parsed={"resolutions": [_declined()]}
        )

    layer = resolution_module.AiResolutionLayer(
        call=call, pdf_paths={"LEFT": str(tmp_path / "a.pdf")}
    )
    layer.resolve(
        review_items=[_review_item()], preparation=_preparation(),
        sheet_relations=_sheet_relations(), comparison_groups=_groups(),
    )
    assert seen == ["text"]


def test_a_contradicting_drawing_never_overrides_the_text(monkeypatch, tmp_path):
    """Модель уже читала «Корпус 1» вместо «Корпус 4». Картинка не главнее."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")

    def call(provider_family, prompt, **kwargs):
        if kwargs.get("images"):
            return gateway.CallResult(
                provider_family, "gpt-5.6-sol", "medium", True,
                parsed={
                    "item_id": "ureview_1",
                    "observed_left": "EI 60",
                    "observed_right": "EI 45",
                    "verdict": "CONTRADICTS_TEXT",
                    "confidence": "MEDIUM",
                    "explanation": "На чертеже другой предел.",
                },
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_declined()]},
        )

    monkeypatch.setattr(
        "backend.app.services.stage_comparison.ai.vision.render_crops",
        lambda **kwargs: [
            __import__(
                "backend.app.services.stage_comparison.ai.vision",
                fromlist=["Crop"],
            ).Crop(side="LEFT", page=29, path=str(pdf)),
        ],
    )
    layer = resolution_module.AiResolutionLayer(
        call=call, pdf_paths={"LEFT": str(pdf), "RIGHT": str(pdf)}
    )
    artifact = layer.resolve(
        review_items=[_review_item()], preparation=_preparation(),
        sheet_relations=_sheet_relations(), comparison_groups=_groups(),
    )
    entry = artifact["resolutions"][0]
    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_VISION_CONTRADICTS
    assert entry["typed_resolution"] is None
    assert entry["vision"]["verdict"] == "CONTRADICTS_TEXT"


def test_an_unreadable_drawing_is_an_honest_answer(monkeypatch, tmp_path):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")

    def call(provider_family, prompt, **kwargs):
        if kwargs.get("images"):
            return gateway.CallResult(
                provider_family, "gpt-5.6-sol", "medium", True,
                parsed={
                    "item_id": "ureview_1",
                    "observed_left": None,
                    "observed_right": None,
                    "verdict": "INSUFFICIENT_IMAGE",
                    "confidence": "UNKNOWN",
                    "explanation": "Фрагмент нечитаем.",
                },
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_declined()]},
        )

    monkeypatch.setattr(
        "backend.app.services.stage_comparison.ai.vision.render_crops",
        lambda **kwargs: [
            __import__(
                "backend.app.services.stage_comparison.ai.vision",
                fromlist=["Crop"],
            ).Crop(side="LEFT", page=29, path=str(pdf)),
        ],
    )
    layer = resolution_module.AiResolutionLayer(
        call=call, pdf_paths={"LEFT": str(pdf), "RIGHT": str(pdf)}
    )
    artifact = layer.resolve(
        review_items=[_review_item()], preparation=_preparation(),
        sheet_relations=_sheet_relations(), comparison_groups=_groups(),
    )
    assert artifact["resolutions"][0]["reason_code"] == (
        resolution_module.REASON_VISION_INSUFFICIENT
    )


def test_an_observation_enters_the_evidence_with_an_explicit_mark():
    from backend.app.services.stage_comparison.ai import vision

    observations = vision.observations_to_context({
        "observed_left": "толщина 200 мм", "observed_right": None,
    })
    assert observations["LEFT"] == ["по чертежу: толщина 200 мм"]
    assert observations["RIGHT"] == []


# ── I. Отмена, таймаут, сироты ────────────────────────────────────────────


def test_the_gateway_kills_the_whole_process_group_on_timeout(monkeypatch):
    """CLI поднимает дочерние процессы; убить надо всю группу, а не лидера."""
    killed: list[tuple[int, int]] = []
    monkeypatch.setattr(
        gateway.os, "killpg", lambda pgid, sig: killed.append((pgid, sig))
    )
    monkeypatch.setattr(gateway.os, "getpgid", lambda pid: 4242)

    class _Process:
        pid = 999
        returncode = None

        def wait(self, timeout=None):
            return 0

    gateway._kill_process_group(_Process())
    assert killed and killed[0][0] == 4242


def test_orphan_recovery_only_looks_at_processes_we_marked(monkeypatch, tmp_path):
    """Чужой codex exec трогать нельзя: метка — единственный признак нашего."""
    proc = tmp_path / "proc"
    ours = proc / "101"
    theirs = proc / "102"
    child = proc / "103"
    for directory in (ours, theirs, child):
        directory.mkdir(parents=True)
    ours.joinpath("environ").write_bytes(
        b"PATH=/usr/bin\x00" + gateway.RUN_MARKER_ENV.encode() + b"=run-old\x00"
    )
    ours.joinpath("status").write_text("Name:\tcodex\nPPid:\t1\n")
    theirs.joinpath("environ").write_bytes(b"PATH=/usr/bin\x00")
    theirs.joinpath("status").write_text("Name:\tcodex\nPPid:\t1\n")
    child.joinpath("environ").write_bytes(
        gateway.RUN_MARKER_ENV.encode() + b"=run-live\x00"
    )
    child.joinpath("status").write_text("Name:\tcodex\nPPid:\t500\n")
    monkeypatch.setattr(gateway, "Path", lambda value: proc if value == "/proc" else tmp_path)

    orphans = gateway.find_orphaned_processes()

    assert [item["run_id"] for item in orphans] == ["run-old"]


def test_orphan_recovery_never_kills_the_current_run(monkeypatch):
    monkeypatch.setattr(
        gateway, "find_orphaned_processes",
        lambda: [{"pid": 1, "run_id": "live"}, {"pid": 2, "run_id": "old"}],
    )
    killed: list[int] = []
    monkeypatch.setattr(gateway.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(gateway.os, "killpg", lambda pgid, sig: killed.append(pgid))

    assert gateway.reap_orphaned_processes(keep_run_id="live") == 1
    assert killed == [2]


def test_a_run_that_dies_mid_flight_leaves_no_live_processes():
    assert gateway.live_process_count() == 0
    assert gateway.kill_live_processes() == 0


def test_a_verifier_failure_is_counted_even_when_the_retry_saves_it(monkeypatch):
    """Успешный повтор не имеет права спрятать цену первого прохода."""
    monkeypatch.setenv("STAGE_COMPARISON_AI_MAX_RETRIES", "1")
    attempts: list[str] = []

    def call(provider_family, prompt, **kwargs):
        attempts.append(kwargs.get("reasoning_level") or "")
        if len(attempts) == 1:
            bad = _good_resolution() | {"after_value": "24.5 | Кладовая | 9,99"}
            return gateway.CallResult(
                provider_family, "m", "low", True, parsed={"resolutions": [bad]}
            )
        return gateway.CallResult(
            provider_family, "m", "high", True,
            parsed={"resolutions": [_good_resolution()]},
        )

    artifact = _resolve(call)

    assert artifact["diagnostics"]["verifier_failed_first_pass"] == 1
    assert artifact["diagnostics"]["retries_used"] == 1
    assert artifact["diagnostics"]["verifier_rejected"] == 0
    assert artifact["diagnostics"]["ai_resolved"] == 1
    assert attempts == ["low", "high"]


# ── Резерв: пустая сторона показывается листом целиком ─────────────────────

def _one_page_pdf(path, text):
    import fitz

    document = fitz.open()
    page = document.new_page(width=595, height=842)
    page.insert_text((72, 200), text, fontsize=14)
    document.save(str(path))
    document.close()
    return str(path)


def test_a_side_without_coordinates_is_shown_as_the_whole_sheet(tmp_path):
    """Строка «добавлена» — значит слева координат нет вовсе.

    Первый боевой прогон отдал 15 обращений из 15 с вердиктом
    INSUFFICIENT_IMAGE ровно потому, что модели показывали одну сторону из
    двух. Ответить «этой строки на листе нет» можно только по листу целиком.
    """
    from backend.app.services.stage_comparison.ai import vision

    left = _one_page_pdf(tmp_path / "left.pdf", "left sheet")
    right = _one_page_pdf(tmp_path / "right.pdf", "right sheet")
    crops = vision.render_crops(
        pdf_paths={"LEFT": left, "RIGHT": right},
        locations={
            "LEFT": [],
            "RIGHT": [{
                "page": 1,
                "bboxes": [{"x": 0.2, "y": 0.2, "width": 0.3, "height": 0.1}],
            }],
        },
        out_dir=tmp_path / "crops",
        sheet_pages={"LEFT": [1], "RIGHT": [1]},
    )
    by_side = {crop.side: crop for crop in crops}
    assert set(by_side) == {"LEFT", "RIGHT"}
    assert by_side["LEFT"].whole_sheet is True
    assert by_side["RIGHT"].whole_sheet is False
    assert "ЛИСТ ЦЕЛИКОМ" in by_side["LEFT"].caption()
    assert "фрагмент" in by_side["RIGHT"].caption()
    # лист целиком крупнее вырезанного места находки
    assert Path(by_side["LEFT"].path).stat().st_size > 0


def test_without_a_sheet_page_an_empty_side_stays_empty(tmp_path):
    """Догадываться о номере страницы нельзя: нет пары листов — нет картинки."""
    from backend.app.services.stage_comparison.ai import vision

    left = _one_page_pdf(tmp_path / "left.pdf", "left sheet")
    right = _one_page_pdf(tmp_path / "right.pdf", "right sheet")
    crops = vision.render_crops(
        pdf_paths={"LEFT": left, "RIGHT": right},
        locations={
            "LEFT": [],
            "RIGHT": [{
                "page": 1,
                "bboxes": [{"x": 0.2, "y": 0.2, "width": 0.3, "height": 0.1}],
            }],
        },
        out_dir=tmp_path / "crops",
        sheet_pages={"RIGHT": [1]},
    )
    assert [crop.side for crop in crops] == ["RIGHT"]


def test_the_prompt_names_every_image_it_actually_sends():
    """Промпт обещал «два фрагмента» даже когда картинка одна."""
    from backend.app.services.stage_comparison.ai import prompts

    text = prompts.vision_prompt(
        {"item_id": "ureview_1"}, None,
        captions=[
            "левая (старая) редакция, стр. PDF 37, ЛИСТ ЦЕЛИКОМ",
            "правая (новая) редакция, стр. PDF 45, фрагмент вокруг места находки",
        ],
    )
    assert "1. левая (старая) редакция, стр. PDF 37, ЛИСТ ЦЕЛИКОМ" in text
    assert "2. правая (новая) редакция, стр. PDF 45," in text
    assert "Отсутствие искомого значения на листе" in text


def test_sheet_pages_reach_the_item_but_not_the_analyst():
    """Лист целиком — дело резерва; отпечаток доказательств от него не зависит."""
    from backend.app.services.stage_comparison.ai import evidence as evidence_module

    packages = evidence_module.build_packages(
        review_items=[_review_item()], preparation=_preparation(),
        sheet_relations=_sheet_relations(), comparison_groups=_groups(),
        batch_size=10,
    )
    item = packages[0].items[0]
    assert item.sheet_pages == {"LEFT": [29], "RIGHT": [8]}
    assert "sheet_pages" not in item.model_view()


# ── Визуальный резерв: наблюдение привязано к своей картинке ──────────────

def _canary_crops(pdf, module):
    """Два кропа с обеих сторон — ровно как в боевом прогоне."""
    return [
        module.Crop(
            side="LEFT", page=29, path=str(pdf), digest="aaaaaaaa",
            document_digest="docleft",
        ),
        module.Crop(
            side="RIGHT", page=8, path=str(pdf), digest="bbbbbbbb",
            document_digest="docright",
        ),
    ]


def _vision_layer(monkeypatch, tmp_path, vision_payload):
    from backend.app.services.stage_comparison.ai import vision as vision_module

    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")
    crops = _canary_crops(pdf, vision_module)
    payload = vision_payload(crops)

    def call(provider_family, prompt, **kwargs):
        if kwargs.get("images"):
            return gateway.CallResult(
                provider_family, "gpt-5.6-sol", "medium", True, parsed=payload,
            )
        return gateway.CallResult(
            provider_family, "gpt-5.6-sol", "low", True,
            parsed={"resolutions": [_declined()]},
        )

    monkeypatch.setattr(
        "backend.app.services.stage_comparison.ai.vision.render_crops",
        lambda **kwargs: crops,
    )
    layer = resolution_module.AiResolutionLayer(
        call=call, pdf_paths={"LEFT": str(pdf), "RIGHT": str(pdf)}
    )
    artifact = layer.resolve(
        review_items=[_review_item()], preparation=_preparation(),
        sheet_relations=_sheet_relations(), comparison_groups=_groups(),
    )
    return artifact["resolutions"][0], crops


def test_a_vision_observation_naming_the_opposite_image_is_never_published(
    monkeypatch, tmp_path
):
    """Состязательная проба: обе картинки показаны, ссылки переставлены.

    ЛЕВАЯ-КАНАРЕЙКА видна только на левом кадре, ПРАВАЯ-КАНАРЕЙКА — только на
    правом. Модель кладёт содержимое правого кадра в наблюдение о левой
    стороне и ссылается на правый кадр. Проверки «была ли вообще картинка этой
    стороны» здесь недостаточно: картинки были обе.
    """
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    entry, crops = _vision_layer(monkeypatch, tmp_path, lambda crops: {
        "item_id": "ureview_1",
        "observed_left": "ПРАВАЯ-КАНАРЕЙКА",
        "observed_left_image_ref": crops[1].vision_image_ref,
        "observed_right": "ЛЕВАЯ-КАНАРЕЙКА",
        "observed_right_image_ref": crops[0].vision_image_ref,
        "verdict": "CONFIRMS_TEXT",
        "confidence": "HIGH",
        "explanation": "Обе метки видны.",
    })

    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["typed_resolution"] is None
    assert entry["reason_code"] == resolution_module.REASON_VISION_INSUFFICIENT
    problems = entry["vision"]["side_problems"]
    assert len(problems) == 2
    assert all(
        problem.startswith(
            __import__(
                "backend.app.services.stage_comparison.ai.vision",
                fromlist=["IMAGE_SIDE_MISMATCH"],
            ).IMAGE_SIDE_MISMATCH
        )
        for problem in problems
    ), problems
    assert entry["vision"]["observed_left"] is None
    assert entry["vision"]["observed_right"] is None


def test_a_vision_observation_without_an_image_reference_is_not_evidence(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    entry, _crops = _vision_layer(monkeypatch, tmp_path, lambda crops: {
        "item_id": "ureview_1",
        "observed_left": "ЛЕВАЯ-КАНАРЕЙКА",
        "observed_left_image_ref": None,
        "observed_right": None,
        "observed_right_image_ref": None,
        "verdict": "CONFIRMS_TEXT",
        "confidence": "HIGH",
        "explanation": "Метка видна.",
    })

    assert entry["status"] == "HUMAN_REQUIRED"
    assert entry["reason_code"] == resolution_module.REASON_VISION_INSUFFICIENT


def test_the_vision_audit_trail_records_which_image_each_observation_named(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("STAGE_COMPARISON_AI_MODE", "DEEP")

    entry, crops = _vision_layer(monkeypatch, tmp_path, lambda crops: {
        "item_id": "ureview_1",
        "observed_left": "ЛЕВАЯ-КАНАРЕЙКА",
        "observed_left_image_ref": crops[0].vision_image_ref,
        "observed_right": "ПРАВАЯ-КАНАРЕЙКА",
        "observed_right_image_ref": crops[1].vision_image_ref,
        "verdict": "CONFIRMS_TEXT",
        "confidence": "HIGH",
        "explanation": "Обе метки видны.",
    })

    assert entry["vision"]["side_problems"] == []
    assert entry["vision"]["observation_image_refs"] == {
        "LEFT": crops[0].vision_image_ref,
        "RIGHT": crops[1].vision_image_ref,
    }
    assert [crop["vision_image_ref"] for crop in entry["vision"]["crops"]] == [
        crops[0].vision_image_ref, crops[1].vision_image_ref,
    ]
