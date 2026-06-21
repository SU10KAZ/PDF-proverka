"""reserc.md #73 — атомарный daily-limit с резервированием.

Несколько одновременных платных вызовов читали spent ДО первого record_paid →
все проходили лимит и вместе перебирали потолок. Теперь reserve_paid_api()
резервирует оценку под локом; проверка лимита учитывает сумму активных
резерваций. TTL-самозалечивание защищает от утечки при пропущенном release.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def guard(monkeypatch):
    mod = importlib.import_module("backend.app.services.llm.paid_api_guard")
    # чистый ledger на каждый тест
    with mod._reservation_lock:
        mod._reservations.clear()
    # kill-switch включён, лимит задан, потрачено 0
    monkeypatch.setenv("PAID_API_ENABLED", "true")
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "10.0")
    monkeypatch.setattr(mod, "_today_spent_usd", lambda: 0.0)
    yield mod
    with mod._reservation_lock:
        mod._reservations.clear()


def _ctx(mod, est):
    return mod.PaidApiContext(
        source="test", model="openai/gpt-5.4", project_id="OBJ/PROJ",
        stage="stage02", estimated_cost_usd=est,
    )


def test_reservations_accumulate_and_block(guard):
    mod = guard
    # лимит 10; резервируем по 4 → третий вызов (12 > 10) должен заблокироваться
    r1 = mod.reserve_paid_api(_ctx(mod, 4.0))
    r2 = mod.reserve_paid_api(_ctx(mod, 4.0))
    assert r1 is not None and r2 is not None
    assert mod.active_reservation_count() == 2
    with pytest.raises(mod.PaidApiBlockedError) as ei:
        mod.reserve_paid_api(_ctx(mod, 4.0))
    assert ei.value.reason == "daily_limit_exceeded"
    # неудачный reserve не создаёт резервацию
    assert mod.active_reservation_count() == 2


def test_release_frees_budget(guard):
    mod = guard
    r1 = mod.reserve_paid_api(_ctx(mod, 6.0))
    # ещё 6 не влезет (12 > 10)
    with pytest.raises(mod.PaidApiBlockedError):
        mod.reserve_paid_api(_ctx(mod, 6.0))
    # освободили — теперь влезает
    r1.release()
    assert mod.active_reservation_count() == 0
    r2 = mod.reserve_paid_api(_ctx(mod, 6.0))
    assert r2 is not None


def test_release_is_idempotent_and_none_safe(guard):
    mod = guard
    r = mod.reserve_paid_api(_ctx(mod, 1.0))
    r.release()
    r.release()  # второй раз — no-op
    assert mod.active_reservation_count() == 0
    mod.release_reservation(None)  # None-safe


def test_assert_accounts_active_reservations(guard):
    mod = guard
    # held-резервация близко к потолку
    mod.reserve_paid_api(_ctx(mod, 9.5))
    # пре-флайт assert (он сам НЕ резервирует) должен видеть резервацию и блокнуть
    with pytest.raises(mod.PaidApiBlockedError) as ei:
        mod.assert_paid_api_allowed(_ctx(mod, 1.0))
    assert ei.value.reason == "daily_limit_exceeded"


def test_assert_does_not_reserve(guard):
    mod = guard
    mod.assert_paid_api_allowed(_ctx(mod, 1.0))
    mod.assert_paid_api_allowed(_ctx(mod, 1.0))
    # assert ничего не резервирует
    assert mod.active_reservation_count() == 0


def test_limit_disabled_no_reservation(guard, monkeypatch):
    mod = guard
    monkeypatch.setenv("PAID_API_DAILY_LIMIT_USD", "0")
    res = mod.reserve_paid_api(_ctx(mod, 100.0))
    assert res is None  # лимит выключен → резервирование не нужно
    assert mod.active_reservation_count() == 0


def test_spent_plus_reservation_blocks(guard, monkeypatch):
    mod = guard
    # уже потрачено 8 сегодня; резерв 1 ок (9<=10), ещё 2 — нет (8+1+2=11>10)
    monkeypatch.setattr(mod, "_today_spent_usd", lambda: 8.0)
    r1 = mod.reserve_paid_api(_ctx(mod, 1.0))
    assert r1 is not None
    with pytest.raises(mod.PaidApiBlockedError):
        mod.reserve_paid_api(_ctx(mod, 2.0))


def test_ttl_expiry_frees_stale_reservation(guard, monkeypatch):
    mod = guard
    monkeypatch.setenv("PAID_API_RESERVATION_TTL_SEC", "100")
    # подсунем «старую» резервацию вручную (ts далеко в прошлом)
    with mod._reservation_lock:
        mod._reservations[999] = (9.5, mod.time.monotonic() - 1000.0)
    # протухшая не должна мешать новой
    r = mod.reserve_paid_api(_ctx(mod, 5.0))
    assert r is not None
    # стерев протухшую при purge
    assert 999 not in mod._reservations


def test_blocked_when_kill_switch_off(guard, monkeypatch):
    mod = guard
    monkeypatch.setenv("PAID_API_ENABLED", "false")
    with pytest.raises(mod.PaidApiBlockedError) as ei:
        mod.reserve_paid_api(_ctx(mod, 1.0))
    assert ei.value.reason == "paid_api_disabled"
    assert mod.active_reservation_count() == 0


def test_estimate_request_cost_helpers():
    from backend.app.services.llm import llm_runner as lr
    msgs = [{"role": "user", "content": "x" * 400}]
    # 400 символов ≈ 100 input токенов
    assert lr._estimate_input_tokens(msgs) == 100
    # multimodal: считаем только текст
    mm = [{"role": "user", "content": [
        {"type": "text", "text": "y" * 80},
        {"type": "image_url", "image_url": {"url": "data:..."}},
    ]}]
    assert lr._estimate_input_tokens(mm) == 20
    # стоимость с известной ценой модели > 0
    cost = lr._estimate_request_cost("openai/gpt-5.4", msgs, 1000)
    assert cost > 0.0
    # неизвестная модель → 0 (под-учёт), не падает
    assert lr._estimate_request_cost("unknown/model-xyz", msgs, 1000) == 0.0
