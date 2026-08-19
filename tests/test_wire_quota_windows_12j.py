"""12J — расширение проводного контракта: окна, код причины, ноль и «не опрошен».

Одна выкатка шлюза закрывает четыре разных умолчания, каждое из которых
показывало оператору неправду:

  1. остаток 0 % приезжал как «нет данных» — исчерпанный провайдер выглядел
     неопрошенным (наблюдалось на Codex 19.08.2026);
  2. недельное окно Claude не доезжало вовсе: провод вёз один остаток;
  3. код причины «почему остатка нет» центр вынужден был угадывать по
     состоянию снимка;
  4. «ещё не опрашивали» подменялось на `missing` — «CLI не установлен» про
     исправный провайдер (принятый остаточный дефект 12I.3).

Тесты держат ОБЕ стороны провода: отправку воркером и приём шлюзом.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

pytest.importorskip("google.protobuf")

from contracts.agent_stream.v1 import adapters  # noqa: E402


def _snapshot(**quota_overrides):
    now = 1787133000.0
    quota = {
        "quota_state": "ready",
        "source": "local_usage_statistics",
        "confidence": "medium",
        "source_stability": "undocumented",
        "reason_code": "local_cache_available",
        "estimated_remaining_pct": 78.0,
        "raw_remaining_supported": True,
        "next_reset_at": now + 1260,
        "primary_window": {
            "window_id": "five_hour", "used_pct": 22.0, "remaining_pct": 78.0,
            "reset_at": now + 1260, "duration_sec": 18000,
        },
        "secondary_windows": [{
            "window_id": "seven_day", "used_pct": 14.0, "remaining_pct": 86.0,
            "reset_at": now + 400000, "duration_sec": 604800,
        }],
    }
    quota.update(quota_overrides)
    return {
        "provider": "claude", "installation_status": "installed",
        "auth_state": "logged_in", "policy_state": "allowed",
        "inference_allowed": False, "credential_present": True,
        "cli_version": "2.1.220", "observed_at": now, "quota": quota,
    }


def _roundtrip(snapshot):
    proto = adapters._provider_snapshot_to_proto(snapshot)
    return adapters.provider_capability_to_center(proto)


# ─── 1. Два окна ───────────────────────────────────────────────────────────

def test_both_windows_survive_the_wire():
    quota = _roundtrip(_snapshot())["quota"]
    assert quota["primary_window"]["window_id"] == "five_hour"
    assert quota["primary_window"]["remaining_pct"] == 78.0
    assert quota["primary_window"]["used_pct"] == 22.0
    assert [w["window_id"] for w in quota["secondary_windows"]] == ["seven_day"]
    assert quota["secondary_windows"][0]["remaining_pct"] == 86.0


def test_window_order_matches_the_headline_number():
    """Главный остаток и первое окно обязаны относиться к одному лимиту."""
    quota = _roundtrip(_snapshot())["quota"]
    assert quota["estimated_remaining_pct"] == quota["primary_window"]["remaining_pct"]


def test_windows_inherit_source_and_confidence_of_the_snapshot():
    quota = _roundtrip(_snapshot())["quota"]
    for window in [quota["primary_window"], *quota["secondary_windows"]]:
        assert window["source"] == "local_usage_statistics"
        assert window["confidence"] == "medium"


def test_snapshot_without_windows_stays_empty():
    quota = _roundtrip(_snapshot(primary_window=None, secondary_windows=[]))["quota"]
    assert quota.get("primary_window") is None
    assert not quota.get("secondary_windows")


# ─── 2. Ноль процентов ─────────────────────────────────────────────────────

def test_zero_percent_is_a_number_not_a_gap():
    codex = _snapshot(
        source="official_app_server_rpc", confidence="high", reason_code=None,
        estimated_remaining_pct=0.0,
        primary_window={"window_id": "codex:primary", "used_pct": 100.0,
                        "remaining_pct": 0.0, "reset_at": None, "duration_sec": 18000},
        secondary_windows=[],
    )
    quota = _roundtrip(codex)["quota"]
    assert quota["estimated_remaining_pct"] == 0.0
    assert quota["primary_window"]["remaining_pct"] == 0.0
    assert quota["primary_window"]["used_pct"] == 100.0


def test_absent_number_stays_absent():
    quota = _roundtrip(_snapshot(
        estimated_remaining_pct=None, raw_remaining_supported=False,
        source="unavailable", confidence="none",
        primary_window=None, secondary_windows=[],
    ))["quota"]
    assert quota["estimated_remaining_pct"] is None


def test_window_without_values_carries_only_its_reset():
    quota = _roundtrip(_snapshot(
        primary_window={"window_id": "five_hour", "used_pct": None,
                        "remaining_pct": None, "reset_at": 1787200000.0,
                        "duration_sec": 18000},
        secondary_windows=[],
    ))["quota"]
    window = quota["primary_window"]
    assert window["remaining_pct"] is None and window["used_pct"] is None
    assert window["reset_at"] == pytest.approx(1787200000.0, abs=1)


# ─── 3. Код причины и стабильность источника ──────────────────────────────

@pytest.mark.parametrize("code", [
    "local_cache_available", "local_cache_stale", "local_cache_missing",
    "local_cache_schema_unsupported", "no_safe_supported_source",
    "organization_subscription_access_disabled",
])
def test_reason_code_survives(code):
    quota = _roundtrip(_snapshot(reason_code=code))["quota"]
    assert quota["reason_code"] == code


def test_missing_reason_code_is_none_not_empty_string():
    quota = _roundtrip(_snapshot(reason_code=None))["quota"]
    assert quota["reason_code"] is None


def test_source_stability_survives():
    quota = _roundtrip(_snapshot(source_stability="experimental"))["quota"]
    assert quota["source_stability"] == "experimental"


# ─── 4. «Ещё не опрашивали» больше не выдаётся за «не установлен» ─────────

def test_not_observed_is_no_longer_downgraded_to_missing():
    snapshot = _snapshot()
    snapshot.pop("installation_status")
    assert _roundtrip(snapshot)["installation_status"] == "not_observed"


def test_explicit_states_are_passed_through():
    for state in ("installed", "missing", "broken", "not_observed"):
        snapshot = _snapshot()
        snapshot["installation_status"] = state
        assert _roundtrip(snapshot)["installation_status"] == state


# ─── 5. Совместимость: старый воркер без новых полей ──────────────────────

def test_old_worker_payload_still_understood():
    """Снимок без окон, кода причины и признака остатка — прежнее поведение."""
    old = {
        "provider": "codex", "installation_status": "installed",
        "auth_state": "logged_in", "policy_state": "allowed",
        "inference_allowed": False, "credential_present": True,
        "cli_version": "0.147.0", "observed_at": 1787133000.0,
        "quota": {"quota_state": "ready", "source": "official_app_server_rpc",
                  "confidence": "high", "estimated_remaining_pct": 3.0,
                  "raw_remaining_supported": True},
    }
    quota = _roundtrip(old)["quota"]
    assert quota["estimated_remaining_pct"] == 3.0
    assert quota["reason_code"] is None
    assert quota.get("primary_window") is None


def test_center_sanitizer_accepts_the_new_wire_shape():
    """Провод → санитайзер центра: окна и код причины доживают до базы."""
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_roundtrip(_snapshot()))
    assert clean["quota"]["reason_code"] == "local_cache_available"
    assert clean["quota"]["source_stability"] == "undocumented"
    assert clean["quota"]["primary_window"]["remaining_pct"] == 78.0
    assert len(clean["quota"]["secondary_windows"]) == 1


def test_ui_shows_both_windows_from_the_wire():
    from backend.app.services.distributed_workers import distributed_ui, provider_accounts
    from backend.app.services.distributed_workers.settings import get_settings

    clean = provider_accounts.sanitize_provider_snapshot(_roundtrip(_snapshot()))
    view = distributed_ui._provider_quota(
        clean, now=1787133100.0, settings=get_settings()
    )
    assert [w["windowId"] for w in view["windows"]] == ["five_hour", "seven_day"]
    assert view["reason"] == "local_cache_available"
    assert view["sourceStability"] == "undocumented"


def test_no_credentials_in_the_wire_payload():
    snapshot = _snapshot()
    snapshot["accessToken"] = "sk-ant-secret-value"
    snapshot["oauthAccount"] = {"accountUuid": "acc-secret-uuid"}
    blob = adapters._provider_snapshot_to_proto(snapshot).SerializeToString()
    assert b"sk-ant-secret-value" not in blob
    assert b"acc-secret-uuid" not in blob
