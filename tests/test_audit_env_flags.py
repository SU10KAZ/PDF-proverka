"""reserc.md #102/#106 — тесты реестра env-флагов (scripts/audit_env_flags.py).

Главное, что проверяем: детекция читателей видит хелперы-обёртки (_env_bool/
_env_int), иначе живые флаги ложно попадают в orphan. И классификация
orphan/documented/read_only_default корректна.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_MOD_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_env_flags.py"
_spec = importlib.util.spec_from_file_location("audit_env_flags", _MOD_PATH)
aef = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(aef)


def test_extract_readers_sees_helpers_and_getenv():
    code = (
        'x = os.getenv("DIRECT_GETENV", "d")\n'
        'y = os.environ.get("ENVIRON_GET")\n'
        'z = os.environ["SUBSCRIPT_FLAG"]\n'
        'a = _env_bool("HELPER_BOOL", False)\n'
        'b = _env_int("HELPER_INT", 6000)\n'
        'c = _env_bool_runtime("HELPER_RUNTIME", default=True)\n'
    )
    readers = aef.extract_readers(code)
    for flag in (
        "DIRECT_GETENV", "ENVIRON_GET", "SUBSCRIPT_FLAG",
        "HELPER_BOOL", "HELPER_INT", "HELPER_RUNTIME",
    ):
        assert flag in readers, f"не увидел читателя {flag}"


def test_extract_defaults():
    code = 'v = _env_int("MAX_TOKENS", 6000)\nw = os.getenv("MODE", "legacy")\n'
    d = aef.extract_defaults(code)
    assert d["MAX_TOKENS"] == "6000"
    assert d["MODE"].strip("'\"") == "legacy"


def test_parse_env_file_active_vs_commented():
    text = (
        "ACTIVE_ONE=true\n"
        "# COMMENTED_ONE=false\n"
        "ACTIVE_TWO=123\n"
        "\n"
        "# просто комментарий без флага\n"
    )
    active, commented = aef.parse_env_file(text)
    assert active == {"ACTIVE_ONE", "ACTIVE_TWO"}
    assert commented == {"COMMENTED_ONE"}


def test_classify_orphan_documented_read_only_default():
    readers = {"LIVE_FLAG": {"backend/x.py"}}
    referenced = {"LIVE_FLAG"}                      # читается в коде
    defaults = {"LIVE_FLAG": "False"}
    env_active = {"LIVE_FLAG", "ORPHAN_FLAG"}       # ORPHAN объявлен, но не читается
    res = aef.classify(
        readers, referenced, defaults,
        env_active=env_active, env_commented=set(),
        example_active=set(), example_commented=set(),
    )
    reg = res["registry"]
    assert reg["LIVE_FLAG"]["status"] == "documented"
    assert reg["ORPHAN_FLAG"]["status"] == "orphan_in_env"
    assert "ORPHAN_FLAG" in res["orphan_in_env"]
    # LIVE_FLAG читается, но если бы не было в env — read_only_default:
    res2 = aef.classify(
        readers, referenced, defaults,
        env_active=set(), env_commented=set(),
        example_active=set(), example_commented=set(),
    )
    assert res2["registry"]["LIVE_FLAG"]["status"] == "read_only_default"


def test_real_repo_registry_v2_flags_not_false_orphans():
    """На реальном репозитории живые флаги (читаемые через хелперы) НЕ должны
    попадать в orphan. Регресс против старой узкой детекции (80 ложных orphan)."""
    reg = aef.build_registry()
    orphans = set(reg["orphan_in_env"])
    # Эти точно читаются в коде через _env_* — orphan'ом быть не могут.
    for live in ("PAID_API_ENABLED", "STAGE_COMPARISON_UPLOAD_MAX_MEMBERS"):
        assert live not in orphans, f"{live} ложно помечен orphan"
    # Реальных orphan мало (узкий список), а не десятки.
    assert reg["summary"]["orphan_in_env"] < 10
