"""Smoke-тесты для профилей дисциплин.

Проверяет, что у ключевых дисциплин есть непустые checklist.md и role.md.
Тесты используют _registry.json для резолвинга profile_dir, не зависят от
кириллических имён папок — переносимы между Windows/Linux/macOS.
"""
import json
import pytest
from pathlib import Path

DISCIPLINES_DIR = Path(__file__).resolve().parent.parent / "prompts" / "disciplines"
REGISTRY_PATH = DISCIPLINES_DIR / "_registry.json"

# Ключевые дисциплины (коды из registry, НЕ имена папок)
KEY_DISCIPLINES = ["EOM", "OV", "AR", "AI", "TX", "VK", "KM"]

# Файлы стандартного профиля
PROFILE_FILES = ["role.md", "checklist.md", "finding_categories.md"]


def _resolve_profile_dir(code: str, registry: dict) -> Path:
    """Резолвить папку профиля через profile_dir из реестра."""
    disc_info = registry.get("disciplines", {}).get(code, {})
    profile_dir_name = disc_info.get("profile_dir", code)
    return DISCIPLINES_DIR / profile_dir_name


@pytest.fixture(scope="module")
def registry():
    if not REGISTRY_PATH.exists():
        pytest.skip("disciplines/_registry.json не найден")
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


class TestRegistry:
    def test_registry_has_disciplines(self, registry):
        assert "disciplines" in registry
        assert len(registry["disciplines"]) >= 7

    def test_key_disciplines_registered(self, registry):
        codes = set(registry["disciplines"].keys())
        for d in KEY_DISCIPLINES:
            assert d in codes, f"Дисциплина {d} не зарегистрирована"

    def test_disciplines_have_names(self, registry):
        for code, info in registry["disciplines"].items():
            assert info.get("name"), f"{code}: отсутствует name"

    def test_disciplines_have_profile_dir(self, registry):
        """Каждая дисциплина должна иметь profile_dir (ASCII)."""
        for code, info in registry["disciplines"].items():
            pd = info.get("profile_dir")
            assert pd, f"{code}: отсутствует profile_dir"
            # profile_dir должен быть ASCII
            assert pd.isascii(), f"{code}: profile_dir '{pd}' содержит не-ASCII символы"


class TestProfiles:
    @pytest.mark.parametrize("discipline", KEY_DISCIPLINES)
    def test_profile_dir_exists(self, discipline, registry):
        d = _resolve_profile_dir(discipline, registry)
        assert d.exists(), (
            f"Папка профиля для {discipline} не найдена: {d}. "
            f"profile_dir={registry['disciplines'].get(discipline, {}).get('profile_dir')}"
        )

    @pytest.mark.parametrize("discipline", KEY_DISCIPLINES)
    def test_role_md_not_empty(self, discipline, registry):
        d = _resolve_profile_dir(discipline, registry)
        role = d / "role.md"
        assert role.exists(), f"{discipline}/role.md не найден (dir={d.name})"
        content = role.read_text(encoding="utf-8").strip()
        assert len(content) > 50, f"{discipline}/role.md слишком короткий ({len(content)} символов)"

    @pytest.mark.parametrize("discipline", KEY_DISCIPLINES)
    def test_checklist_md_not_empty(self, discipline, registry):
        d = _resolve_profile_dir(discipline, registry)
        cl = d / "checklist.md"
        assert cl.exists(), f"{discipline}/checklist.md не найден (dir={d.name})"
        content = cl.read_text(encoding="utf-8").strip()
        assert len(content) > 100, f"{discipline}/checklist.md слишком короткий ({len(content)} символов)"
        assert "- " in content or "* " in content or "1." in content, \
            f"{discipline}/checklist.md не содержит чек-пунктов"

    @pytest.mark.parametrize("discipline", KEY_DISCIPLINES)
    def test_finding_categories_not_empty(self, discipline, registry):
        d = _resolve_profile_dir(discipline, registry)
        fc = d / "finding_categories.md"
        assert fc.exists(), f"{discipline}/finding_categories.md не найден (dir={d.name})"
        content = fc.read_text(encoding="utf-8").strip()
        assert len(content) > 30, f"{discipline}/finding_categories.md слишком короткий"

    @pytest.mark.parametrize("discipline", KEY_DISCIPLINES)
    def test_all_profile_files_present(self, discipline, registry):
        d = _resolve_profile_dir(discipline, registry)
        for fname in PROFILE_FILES:
            assert (d / fname).exists(), f"{discipline}/{fname} не найден (dir={d.name})"
