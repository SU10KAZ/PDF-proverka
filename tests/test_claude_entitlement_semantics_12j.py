"""12J — «авторизован» ещё не значит «работает».

Дефект, ради которого написан этот файл, наблюдался на боевом воркере 11l
19.08.2026. Claude сообщал `installed`, `logged_in`, `subscriptionType=max`,
и карточка VPS называла провайдера доступным. Первый настоящий запрос вернул:

    HTTP 403 · "Your organization has disabled Claude subscription access
                for Claude Code"
    usage: input=0 output=0 cost=0

То есть учётные данные были приняты, а работать провайдер не мог — и центр об
этом не знал, потому что спрашивал только про авторизацию.

Что здесь доказывается:

  1. отказ провайдера распознаётся как ОТДЕЛЬНАЯ причина (не `auth_required`,
     не `unknown`) — от этого зависит, куда пойдёт оператор: логиниться или к
     администратору организации;
  2. такой провайдер перестаёт считаться доступным и не обещает центру
     профили маршрутизации, которым он нужен;
  3. успешный вызов снимает блокировку сам — защёлки нет;
  4. отсутствие КВОТЫ по-прежнему не делает провайдера недоступным: это
     разные вопросы, и смешивать их нельзя;
  5. ни одно поле учётных данных не покидает воркер.

Ни один тест не обращается к модели и не ходит в сеть.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from audit_worker.providers import errors, quota, runtime_state  # noqa: E402
from audit_worker.providers.claude_adapter import _provider_refusal_code  # noqa: E402

#: Дословный ответ боевого воркера 11l. Служит фикстурой намеренно: если
#: формулировка провайдера изменится, тест обязан упасть здесь, а не в бою.
LIVE_403_ENVELOPE = {
    "is_error": True,
    "terminal_reason": "api_error",
    "api_error_status": 403,
    "subtype": "success",
    "result": (
        "Your organization has disabled Claude subscription access for Claude "
        "Code · Use an Anthropic API key instead, or ask your admin to enable "
        "access"
    ),
    "usage": {"input_tokens": 0, "output_tokens": 0},
    "total_cost_usd": 0,
    "session_id": "ea780705-4748-44d3-84fe-d8bbbc1b41ea",
}


# ─── 1. Отказ провайдера — своя причина, а не «перелогиньтесь» ──────────────

def test_live_403_is_classified_as_entitlement_not_auth():
    assert _provider_refusal_code(LIVE_403_ENVELOPE) == errors.ERR_ENTITLEMENT_BLOCKED
    # Ключевое отличие: обычный разлогин по-прежнему ведёт к входу.
    assert errors.classify_text("Not logged in. Please run /login") == (
        errors.ERR_AUTH_REQUIRED
    )


def test_successful_envelope_has_no_refusal():
    assert _provider_refusal_code(
        {"is_error": False, "result": "PROVIDER_PROBE_OK", "usage": {}}
    ) is None


@pytest.mark.parametrize("status,expected", [
    (429, errors.ERR_RATE_LIMITED),
    (503, errors.ERR_PROVIDER_UNAVAILABLE),
    (401, errors.ERR_AUTH_REQUIRED),
])
def test_other_server_refusals_keep_their_own_meaning(status, expected):
    assert _provider_refusal_code(
        {"is_error": True, "api_error_status": status, "result": ""}
    ) == expected


# ─── 2–3. Состояние переживает перезапуск и снимается успехом ───────────────

def test_state_survives_process_and_is_not_latched(tmp_path):
    runtime_state.record(
        tmp_path, "claude", success=False, error_code=errors.ERR_ENTITLEMENT_BLOCKED
    )
    # Читает «другой процесс» — то есть просто заново с диска.
    blocked = runtime_state.read(tmp_path, "claude")
    assert blocked.state == runtime_state.RUNTIME_ENTITLEMENT_BLOCKED
    assert blocked.blocked is True and blocked.proven_usable is False

    runtime_state.record(tmp_path, "claude", success=True)
    freed = runtime_state.read(tmp_path, "claude")
    assert freed.state == runtime_state.RUNTIME_READY
    assert freed.proven_usable is True
    assert freed.last_success_at is not None

    # И обратно: запрет вернулся — состояние снова блокирующее, но «когда
    # работало» не стирается.
    runtime_state.record(
        tmp_path, "claude", success=False, error_code=errors.ERR_ENTITLEMENT_BLOCKED
    )
    again = runtime_state.read(tmp_path, "claude")
    assert again.blocked is True
    assert again.last_success_at == freed.last_success_at


def test_network_failure_is_not_entitlement(tmp_path):
    runtime_state.record(
        tmp_path, "claude", success=False, error_code=errors.ERR_NETWORK
    )
    result = runtime_state.read(tmp_path, "claude")
    assert result.state == runtime_state.RUNTIME_ERROR
    assert result.blocked is False


def test_unknown_before_any_call(tmp_path):
    result = runtime_state.read(tmp_path, "claude")
    assert result.state == runtime_state.RUNTIME_UNKNOWN
    assert result.blocked is False and result.proven_usable is False


# ─── 4. Снимок воркера: блокировка перекрывает квоту ────────────────────────

def _manager(tmp_path):
    from audit_worker.providers.manager import ProviderManager

    return ProviderManager(worker_root=tmp_path)


def test_blocked_provider_reports_policy_blocked_quota(tmp_path):
    """Отказ обязан быть виден центру через ЕДИНСТВЕННОЕ доступное поле."""
    from audit_worker.providers.manager import _entitlement_applied

    ready = quota.ProviderQuotaSnapshot(
        provider="claude", quota_state=quota.QUOTA_READY, observed_at=time.time(),
        source=quota.SOURCE_LOCAL_USAGE_STATS, confidence=quota.CONFIDENCE_MEDIUM,
        auth_state="logged_in", estimated_remaining_pct=84.0,
        raw_remaining_supported=True, cli_version="2.1.220",
    )
    blocked_state = runtime_state.RuntimeResult(
        provider="claude", state=runtime_state.RUNTIME_ENTITLEMENT_BLOCKED,
        error_code=errors.ERR_ENTITLEMENT_BLOCKED, observed_at=time.time(),
    )
    out = _entitlement_applied(
        ready, blocked_state, provider="claude", identity=None, now=time.time()
    )
    assert out.quota_state == quota.QUOTA_POLICY_BLOCKED
    assert out.probe_error_code == errors.ERR_ENTITLEMENT_BLOCKED
    # Остаток обнулён: предлагать 84 % там, где работать нельзя, — обман.
    assert out.estimated_remaining_pct is None
    assert out.cli_version == "2.1.220"


def test_healthy_provider_snapshot_untouched(tmp_path):
    from audit_worker.providers.manager import _entitlement_applied

    ready = quota.ProviderQuotaSnapshot(
        provider="claude", quota_state=quota.QUOTA_READY, observed_at=time.time(),
        source=quota.SOURCE_LOCAL_USAGE_STATS, confidence=quota.CONFIDENCE_MEDIUM,
        auth_state="logged_in", estimated_remaining_pct=84.0,
        raw_remaining_supported=True,
    )
    for state in (runtime_state.RUNTIME_READY, runtime_state.RUNTIME_UNKNOWN,
                  runtime_state.RUNTIME_ERROR):
        result = runtime_state.RuntimeResult(provider="claude", state=state)
        out = _entitlement_applied(
            ready, result, provider="claude", identity=None, now=time.time()
        )
        assert out is ready, state


# ─── 5. Профили маршрутизации: не обещать того, что не работает ────────────

def _snapshot(quota_state="ready", policy="allowed", install="installed"):
    return {
        "provider": "claude", "policy_state": policy,
        "installation_status": install, "auth_state": "logged_in",
        "quota": {"quota_state": quota_state},
    }


def test_blocked_claude_removes_claude_routing_presets():
    from contracts.agent_stream.v1.adapters import usable_routing_compatibility

    declared = ["claude_gpt_codex", "codex_exec"]
    blocked = {"claude": _snapshot(quota_state="policy_blocked")}
    assert usable_routing_compatibility(declared, blocked) == ["codex_exec"]


@pytest.mark.parametrize("state", ["ready", "unknown", "low", "stale"])
def test_unknown_quota_does_not_remove_presets(state):
    """Незнание — не повод отказываться от работы."""
    from contracts.agent_stream.v1.adapters import usable_routing_compatibility

    declared = ["claude_gpt_codex", "codex_exec"]
    ok = {"claude": _snapshot(quota_state=state)}
    assert usable_routing_compatibility(declared, ok) == declared


def test_codex_presets_unaffected_by_claude_block():
    from contracts.agent_stream.v1.adapters import usable_routing_compatibility

    both = {
        "claude": _snapshot(quota_state="policy_blocked"),
        "codex": {"provider": "codex", "policy_state": "allowed",
                  "installation_status": "installed",
                  "quota": {"quota_state": "ready"}},
    }
    assert usable_routing_compatibility(["codex_exec"], both) == ["codex_exec"]


# ─── 6–9. Центр и интерфейс ────────────────────────────────────────────────

def _center_view(*, quota_state, policy="allowed", auth="logged_in",
                 remaining=None, source="unavailable", confidence="none"):
    from backend.app.services.distributed_workers import distributed_ui, provider_accounts
    from backend.app.services.distributed_workers.settings import get_settings

    now = time.time()
    snapshot = {
        "provider": "claude", "installation_status": "installed",
        "auth_state": auth, "policy_state": policy, "inference_allowed": False,
        "credential_present": True, "cli_version": "2.1.220", "observed_at": now,
        "quota": {
            "quota_state": quota_state, "source": source, "confidence": confidence,
            "observed_at": now, "auth_state": auth,
            "estimated_remaining_pct": remaining,
            "raw_remaining_supported": remaining is not None,
        },
        # Приманки: центр обязан собирать поля перечислением.
        "accessToken": "sk-ant-secret-value",
        "oauthAccount": {"accountUuid": "acc-secret-uuid",
                         "emailAddress": "owner@example.com"},
    }
    clean = provider_accounts.sanitize_provider_snapshot(snapshot)
    return distributed_ui._provider_quota(clean, now=now, settings=get_settings()), clean


def test_entitlement_block_is_not_available():
    view, _ = _center_view(quota_state="policy_blocked")
    assert view["status"] == "entitlement_blocked"
    assert view["availability"] == "unavailable"
    assert view["routeReady"] is False
    # Вход при этом исправен, и интерфейс обязан это показывать.
    assert view["loggedIn"] is True


def test_entitlement_reason_code_is_safe_and_specific():
    view, _ = _center_view(quota_state="policy_blocked")
    assert view["reason"] == "organization_subscription_access_disabled"


def test_operator_policy_block_is_not_confused_with_entitlement():
    """Собственный запрет оператора выглядит иначе — иначе и лечится."""
    view, _ = _center_view(quota_state="policy_blocked", policy="policy_blocked")
    assert view["status"] == "unavailable"
    assert view["reason"] != "organization_subscription_access_disabled"


def test_missing_quota_alone_keeps_provider_available():
    view, _ = _center_view(quota_state="unknown")
    assert view["availability"] == "available"
    assert view["routeReady"] is True
    assert view["percentageRemaining"] is None


def test_successful_run_returns_provider_to_ready():
    """После успеха воркер шлёт обычный снимок — центр снова видит рабочего."""
    view, _ = _center_view(
        quota_state="ready", remaining=78.0,
        source="local_usage_statistics", confidence="medium",
    )
    assert view["status"] in ("ok", "warning", "critical")
    assert view["routeReady"] is True
    assert view["percentageRemaining"] == 78.0


def test_no_credentials_and_no_raw_provider_text_in_api():
    view, clean = _center_view(quota_state="policy_blocked")
    blob = json.dumps({"view": view, "clean": clean}, ensure_ascii=False, default=str)
    for secret in ("sk-ant-secret-value", "acc-secret-uuid", "owner@example.com"):
        assert secret not in blob, f"утечка {secret!r}"
    # Дословный текст провайдера в основной модели интерфейса не нужен: там код.
    assert "Your organization has disabled" not in blob


def test_codex_untouched_by_all_of_this():
    from backend.app.services.distributed_workers import distributed_ui, provider_accounts
    from backend.app.services.distributed_workers.settings import get_settings

    now = time.time()
    codex = {
        "provider": "codex", "installation_status": "installed",
        "auth_state": "logged_in", "policy_state": "allowed",
        "inference_allowed": False, "credential_present": True,
        "cli_version": "0.147.0", "observed_at": now,
        "quota": {"quota_state": "ready", "source": "official_app_server_rpc",
                  "confidence": "high", "observed_at": now, "auth_state": "logged_in",
                  "estimated_remaining_pct": 3.0, "raw_remaining_supported": True},
    }
    clean = provider_accounts.sanitize_provider_snapshot(codex)
    view = distributed_ui._provider_quota(clean, now=now, settings=get_settings())
    assert view["percentageRemaining"] == 3.0
    assert view["status"] == "critical"          # 3 % — действительно мало
    assert view["routeReady"] is True
    assert view["reason"] is None
    assert view["isEstimated"] is False
