"""12J — остаток лимита Claude из локального кеша Claude Code.

Что здесь доказывается и почему именно это.

Claude Code не имеет команды про остаток, а официальные поля `rate_limits`
публикуются только скрипту статусной строки и только после ответа модели.
Поэтому единственный допустимый источник — файл, который CLI пишет сам по ходу
обычной работы (`cachedUsageUtilization`). Отсюда три группы требований:

  1. ЧИСЛА. `utilization` — это ИСПОЛЬЗОВАНО. На экран идёт ОСТАТОК, и путаница
     здесь стоит ровно наоборот: «осталось 16 %» вместо «осталось 84 %».
  2. ГРАНИЦА. Файл соседствует с учётными данными. Разбор обязан быть
     allowlist'ом, а не «уберём лишнее»: ни один посторонний ключ не имеет
     права оказаться ни в снимке, ни в базе центра, ни в ответе API.
  3. ЧЕСТНОСТЬ. Отсутствие кеша, незнакомая форма и протухший снимок — три
     разных новости, и ни одна из них не делает провайдера недоступным.

Ни один тест не запускает CLI и не обращается к сети: весь ввод — фикстуры.
Настоящий `~/.claude.json` в тестах не участвует (§16 задания).
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

from audit_worker.providers import claude_local_usage as clu  # noqa: E402
from audit_worker.providers import quota  # noqa: E402
from audit_worker.providers.claude_adapter import (  # noqa: E402
    _quota_unavailable_reason,
    _snapshot_from_local_usage,
)

#: Значения, которых на пути «воркер → провод → база → API → браузер» не должно
#: появиться НИ РАЗУ. Все они лежат в фикстуре рядом с полезными полями — ровно
#: так, как в настоящем конфигурационном файле Claude Code.
SECRETS = (
    "sk-ant-secret-value",
    "refresh-secret-value",
    "session-secret-value",
    "cookie-secret-value",
    "acc-secret-uuid",
    "org-secret-uuid",
    "api-key-secret-value",
)


def _config(**overrides):
    """Фикстура конфигурационного файла: полезные поля + приманки-секреты."""
    cache = {
        "fetchedAtMs": overrides.pop("fetchedAtMs", None),
        "accountUuid": "acc-secret-uuid",
        "utilization": overrides.pop("utilization", None),
    }
    if cache["fetchedAtMs"] is None:
        cache.pop("fetchedAtMs")
    if cache["utilization"] is None:
        cache.pop("utilization")
    payload = {
        "accessToken": "sk-ant-secret-value",
        "refreshToken": "refresh-secret-value",
        "oauthAccount": {
            "accountUuid": "acc-secret-uuid",
            "organizationUuid": "org-secret-uuid",
            "emailAddress": "owner@example.com",
        },
        "sessionId": "session-secret-value",
        "cookie": "cookie-secret-value",
        "apiKey": "api-key-secret-value",
        "cachedUsageUtilization": cache,
    }
    payload.update(overrides)
    return payload


def _window(utilization, resets_at=None):
    row = {
        "utilization": utilization,
        "limit_dollars": None,
        "used_dollars": None,
        "remaining_dollars": None,
    }
    if resets_at is not None:
        row["resets_at"] = resets_at
    return row


def _write(tmp_path: Path, payload) -> Path:
    config_dir = tmp_path / ".claude"
    config_dir.mkdir(exist_ok=True)
    target = config_dir / ".claude.json"
    if isinstance(payload, str):
        target.write_text(payload, encoding="utf-8")
    else:
        target.write_text(json.dumps(payload), encoding="utf-8")
    return config_dir


def _read(tmp_path: Path, payload, *, now=None):
    config_dir = _write(tmp_path, payload)
    return clu.read_local_usage(
        config_dir=config_dir, home_dir=tmp_path, now=now or time.time()
    )


# ─── 1–7. Числа: «использовано» превращается в «осталось» ───────────────────

def test_five_hour_window_parsed(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int((now - 60) * 1000),
        utilization={"five_hour": _window(16, "2099-01-01T00:00:00+00:00")},
    ), now=now)
    window = reading.window("five_hour")
    assert reading.reason == clu.REASON_AVAILABLE
    assert window is not None and window.used_pct == 16.0


def test_seven_day_window_parsed(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000),
        utilization={
            "five_hour": _window(16),
            "seven_day": _window(12, "2099-01-01T00:00:00+00:00"),
        },
    ), now=now)
    assert {w.window_id for w in reading.windows} == {"five_hour", "seven_day"}
    assert reading.window("seven_day").used_pct == 12.0


def test_remaining_is_hundred_minus_utilization(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000),
        utilization={"five_hour": _window(16), "seven_day": _window(12)},
    ), now=now)
    assert reading.window("five_hour").remaining_pct == 84.0
    assert reading.window("seven_day").remaining_pct == 88.0


@pytest.mark.parametrize("used,expected", [(0, 100.0), (100, 0.0), (100.4, 0.0)])
def test_boundary_utilization_values(tmp_path, used, expected):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000), utilization={"five_hour": _window(used)},
    ), now=now)
    assert reading.window("five_hour").remaining_pct == expected


@pytest.mark.parametrize("bad", [-500, 250, "16", True, None, float("nan")])
def test_invalid_utilization_rejected(tmp_path, bad):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000), utilization={"five_hour": _window(bad)},
    ), now=now)
    # Ни одного разобранного окна → «форма незнакома», а не выдуманный процент.
    assert reading.reason == clu.REASON_SCHEMA_UNSUPPORTED
    assert reading.windows == ()


def test_reset_timestamp_parsed_as_utc(tmp_path):
    # Момент наблюдения задан явно: дата сброса проверяется по календарю, а не
    # по часам машины, на которой идут тесты.
    now = 1787125562.0
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000),
        utilization={"five_hour": _window(16, "2026-08-19T09:10:00.328860+00:00")},
    ), now=now)
    assert reading.window("five_hour").reset_at == pytest.approx(1787130600.32886)


def test_fetched_at_is_snapshot_time_not_read_time(tmp_path):
    now = time.time()
    fetched = now - 2013
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(fetched * 1000), utilization={"five_hour": _window(16)},
    ), now=now)
    assert reading.fetched_at == pytest.approx(fetched, abs=0.01)
    assert reading.read_at == pytest.approx(now, abs=0.01)
    assert reading.age_sec == pytest.approx(2013, abs=1)


# ─── 8–14. Исходы, каждый из которых нормальный ─────────────────────────────

def test_fresh_cache_is_ready_snapshot(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int((now - 30) * 1000),
        utilization={"five_hour": _window(16), "seven_day": _window(12)},
    ), now=now)
    snapshot = _snapshot_from_local_usage(
        reading, provider="claude", auth_state="logged_in",
        account_group_id=None, stale_after_sec=1800.0, low_threshold_pct=25.0,
    )
    assert snapshot.quota_state == quota.QUOTA_READY
    assert snapshot.estimated_remaining_pct == 84.0
    assert snapshot.is_stale(now=now) is False


def test_stale_cache_keeps_number_but_marks_state(tmp_path):
    """Протухший снимок — «последнее известное», а не «текущее» и не пустота."""
    from audit_worker.providers.manager import _staleness_applied

    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int((now - 7200) * 1000), utilization={"five_hour": _window(16)},
    ), now=now)
    snapshot = _snapshot_from_local_usage(
        reading, provider="claude", auth_state="logged_in",
        account_group_id=None, stale_after_sec=1800.0, low_threshold_pct=25.0,
    )
    applied = _staleness_applied(snapshot, now=now)
    assert applied.quota_state == quota.QUOTA_STALE
    assert applied.estimated_remaining_pct == 84.0
    assert applied.reason_code == quota.REASON_LOCAL_CACHE_STALE


def test_missing_file(tmp_path):
    reading = clu.read_local_usage(
        config_dir=tmp_path / "nope", home_dir=tmp_path / "nope", now=time.time()
    )
    assert reading.reason == clu.REASON_MISSING
    assert reading.ok is False


def test_missing_cache_key(tmp_path):
    reading = _read(tmp_path, {"oauthAccount": {"accountUuid": "acc-secret-uuid"}})
    assert reading.reason == clu.REASON_MISSING


def test_malformed_json(tmp_path):
    reading = _read(tmp_path, "{ это не json ")
    assert reading.reason == clu.REASON_SCHEMA_UNSUPPORTED


def test_unexpected_schema(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000), utilization={"five_hour": "какая-то строка"},
    ), now=now)
    assert reading.reason == clu.REASON_SCHEMA_UNSUPPORTED


def test_snapshot_without_fetched_at_is_not_trusted(tmp_path):
    """Без метки снимка возраст неизвестен — значит и свежесть недоказуема."""
    reading = _read(tmp_path, _config(utilization={"five_hour": _window(16)}))
    assert reading.reason == clu.REASON_SCHEMA_UNSUPPORTED


def test_future_fetched_at_rejected(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int((now + 86400) * 1000), utilization={"five_hour": _window(16)},
    ), now=now)
    # Метка из будущего сделала бы снимок вечно свежим.
    assert reading.reason == clu.REASON_SCHEMA_UNSUPPORTED


# ─── 15–17. Граница безопасности ───────────────────────────────────────────

def test_unknown_fields_ignored(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000),
        utilization={
            "five_hour": _window(16),
            "nimbus_quill": _window(0),
            "tangelo": _window(50),
            "extra_usage": {"is_enabled": False, "used_credits": 42},
        },
    ), now=now)
    assert {w.window_id for w in reading.windows} == {"five_hour"}


def test_no_secret_ever_leaves_the_parser(tmp_path):
    now = time.time()
    reading = _read(tmp_path, _config(
        fetchedAtMs=int(now * 1000),
        utilization={"five_hour": _window(16), "seven_day": _window(12)},
    ), now=now)
    snapshot = _snapshot_from_local_usage(
        reading, provider="claude", auth_state="logged_in",
        account_group_id=None, stale_after_sec=1800.0, low_threshold_pct=25.0,
    )
    blob = json.dumps(
        {"reading": [w.__dict__ for w in reading.windows], "detail": reading.detail,
         "snapshot": snapshot.as_dict()},
        ensure_ascii=False, default=str,
    )
    for secret in SECRETS:
        assert secret not in blob, f"утечка {secret!r}"
    assert "owner@example.com" not in blob


def test_quota_failure_does_not_touch_provider_availability(tmp_path):
    """Нет кеша — закрыта КВОТА. Установка и вход живут отдельными полями."""
    reading = clu.read_local_usage(config_dir=tmp_path, home_dir=tmp_path, now=time.time())
    snapshot = quota.unknown_snapshot(
        "claude", auth_state="logged_in",
        reason=_quota_unavailable_reason(reading),
        reason_code=reading.reason, observed_at=time.time(),
    )
    assert snapshot.auth_state == "logged_in"
    assert snapshot.quota_state == quota.QUOTA_UNKNOWN
    assert snapshot.estimated_remaining_pct is None
    assert snapshot.reason_code == clu.REASON_MISSING


# ─── 18–19. Снимок на проводе: изменения ровно тогда, когда они есть ────────

def _wire_payload(remaining, reset_at, *, state="ready"):
    return [{
        "provider": "claude",
        "installation_status": "installed",
        "auth_state": "logged_in",
        "policy_state": "allowed",
        "inference_allowed": False,
        "credential_present": True,
        "observed_at": 111.0,
        "cli_version": "2.1.220",
        "quota": {
            "quota_state": state,
            "source": "local_usage_statistics",
            "confidence": "medium",
            "estimated_remaining_pct": remaining,
            "raw_remaining_supported": True,
            "next_reset_at": reset_at,
        },
    }]


def test_same_cache_produces_same_wire_digest():
    pytest.importorskip("google.protobuf")
    from contracts.agent_stream.v1.adapters import provider_status_digest

    first = provider_status_digest(_wire_payload(84.0, 1787130600.0))
    second = provider_status_digest(_wire_payload(84.0, 1787130600.0))
    assert first == second and first != ""


def test_changed_quota_changes_wire_digest():
    pytest.importorskip("google.protobuf")
    from contracts.agent_stream.v1.adapters import provider_status_digest

    before = provider_status_digest(_wire_payload(84.0, 1787130600.0))
    after = provider_status_digest(_wire_payload(83.0, 1787130600.0))
    assert before != after


def test_wire_carries_local_cache_percentage():
    """Проводной контракт довозит остаток и источник до центра как есть."""
    pytest.importorskip("google.protobuf")
    from contracts.agent_stream.v1 import adapters

    proto = adapters._provider_snapshot_to_proto(_wire_payload(84.0, 1787130600.0)[0])
    back = adapters.provider_capability_to_center(proto)
    assert back["quota"]["estimated_remaining_pct"] == 84.0
    assert back["quota"]["source"] == "local_usage_statistics"
    assert back["quota"]["confidence"] == "medium"
    assert back["auth_state"] == "logged_in"


# ─── 20–23. Центр: приём, окна, причина, отсутствие регресса у Codex ────────

def _center_snapshot(**overrides):
    quota_payload = {
        "quota_state": "ready",
        "observed_at": 1787125562.0,
        "source": "local_usage_statistics",
        "confidence": "medium",
        "source_stability": "undocumented",
        "estimated_remaining_pct": 84.0,
        "raw_remaining_supported": True,
        "next_reset_at": 1787130600.0,
        "auth_state": "logged_in",
        "reason_code": "local_cache_available",
        "primary_window": {
            "window_id": "five_hour", "used_pct": 16.0, "remaining_pct": 84.0,
            "reset_at": 1787130600.0, "duration_sec": 18000,
            "source": "local_usage_statistics", "confidence": "medium",
        },
        "secondary_windows": [{
            "window_id": "seven_day", "used_pct": 12.0, "remaining_pct": 88.0,
            "reset_at": 1787544000.0, "duration_sec": 604800,
            "source": "local_usage_statistics", "confidence": "medium",
        }],
    }
    quota_payload.update(overrides.pop("quota", {}))
    snapshot = {
        "provider": "claude",
        "installation_status": "installed",
        "auth_state": "logged_in",
        "policy_state": "allowed",
        "inference_allowed": False,
        "credential_present": True,
        "cli_version": "2.1.220",
        "observed_at": 1787125562.0,
        "quota": quota_payload,
        # Приманки: центр обязан собрать верхний уровень перечислением полей.
        "accessToken": "sk-ant-secret-value",
        "oauthAccount": {"accountUuid": "acc-secret-uuid"},
    }
    snapshot.update(overrides)
    return snapshot


def test_center_accepts_local_cache_percentage():
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_center_snapshot())
    assert clean["quota"]["estimated_remaining_pct"] == 84.0
    assert clean["quota"]["source"] == "local_usage_statistics"
    assert clean["quota"]["source_stability"] == "undocumented"
    assert clean["quota"]["reason_code"] == "local_cache_available"
    assert len(clean["quota"]["secondary_windows"]) == 1


def test_center_drops_unknown_reason_code():
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(
        _center_snapshot(quota={"reason_code": "выдуманный_код"})
    )
    assert clean["quota"]["reason_code"] is None


def test_center_never_serializes_secrets():
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_center_snapshot())
    blob = json.dumps(clean, ensure_ascii=False, default=str)
    for secret in SECRETS:
        assert secret not in blob, f"утечка {secret!r}"


def _ui_quota(state, *, settings=None):
    from backend.app.services.distributed_workers import distributed_ui
    from backend.app.services.distributed_workers.settings import get_settings

    return distributed_ui._provider_quota(
        state, now=1787125600.0, settings=settings or get_settings()
    )


def test_ui_exposes_both_windows_ordered_by_constraint():
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_center_snapshot())
    view = _ui_quota(clean)
    assert [w["windowId"] for w in view["windows"]] == ["five_hour", "seven_day"]
    assert view["windows"][0]["remainingPercent"] == 84.0
    assert view["windows"][0]["usedPercent"] == 16.0
    assert view["windows"][1]["remainingPercent"] == 88.0
    assert view["percentageRemaining"] == 84.0
    assert view["reason"] == "local_cache_available"
    assert view["sourceStability"] == "undocumented"
    assert view["isEstimated"] is False


def test_ui_reports_observation_age_not_read_time():
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_center_snapshot())
    view = _ui_quota(clean)
    assert view["ageSec"] == pytest.approx(38, abs=2)


def test_ui_derives_reason_when_worker_sent_none():
    """Проводной контракт кода не несёт — центр обязан вывести его сам."""
    from backend.app.services.distributed_workers import provider_accounts

    clean = provider_accounts.sanitize_provider_snapshot(_center_snapshot(quota={
        "reason_code": None,
        "estimated_remaining_pct": None,
        "raw_remaining_supported": False,
        "quota_state": "unknown",
        "primary_window": None,
        "secondary_windows": [],
    }))
    view = _ui_quota(clean)
    assert view["reason"] == "no_safe_supported_source"
    assert view["percentageRemaining"] is None
    # И это НЕ делает провайдера недоступным.
    assert view["availability"] == "available"


def test_codex_official_source_unchanged():
    """Codex остаётся `official_app_server_rpc` с high и без пометки «оценка»."""
    from backend.app.services.distributed_workers import provider_accounts

    codex = _center_snapshot(provider="codex", quota={
        "source": "official_app_server_rpc",
        "confidence": "high",
        "source_stability": "experimental",
        "estimated_remaining_pct": 3.0,
        "reason_code": None,
    })
    clean = provider_accounts.sanitize_provider_snapshot(codex)
    view = _ui_quota(clean)
    assert clean["quota"]["source"] == "official_app_server_rpc"
    assert clean["quota"]["confidence"] == "high"
    assert view["percentageRemaining"] == 3.0
    assert view["isEstimated"] is False
    assert view["reason"] is None
