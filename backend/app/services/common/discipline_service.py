"""
Сервис для работы с дисциплинами проекта.
Загрузка профилей, автодетекция, инъекция в шаблоны задач Claude.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backend.app.core.config import BASE_DIR, DISCIPLINES_DIR
REGISTRY_FILE = DISCIPLINES_DIR / "_registry.json"

logger = logging.getLogger(__name__)

def _safe_profile_segment(name: str) -> bool:
    """Годится ли строка как ОДИН сегмент пути внутри каталога дисциплин.

    Не «латиница и цифры»: пользователь вправе завести раздел с кириллическим
    кодом через `add_discipline`, и запрет сломал бы существующие установки.
    Запрещается ровно то, что выводит из каталога: разделители, `.`/`..`,
    управляющие символы и скрытые имена.
    """
    text = str(name or "")
    if not text or len(text) > 40:
        return False
    if text in (".", ".."):
        return False
    if text[0] in ".~-" or text != text.strip():
        return False
    if any(ch in text for ch in "/\\\0:*?\"<>|"):
        return False
    return not any(ord(ch) < 32 or ord(ch) == 127 for ch in text)


class DisciplineProfileMissing(FileNotFoundError):
    """Профиль известной дисциплины отсутствует, а строгий режим включён."""


class DisciplineProfileUnsafe(ValueError):
    """Имя каталога профиля не является безопасным сегментом пути."""

# Кэш загруженных профилей (дисциплины не меняются в рантайме)
_profile_cache: dict[str, "DisciplineProfile"] = {}
_registry_cache: Optional[dict] = None


@dataclass
class DisciplineProfile:
    """Загруженный профиль дисциплины."""
    code: str
    name: str
    short_name: str
    color: str
    role: str = ""
    checklist: str = ""
    triage_table: str = ""
    project_params: str = ""
    drawing_types: str = ""
    finding_categories: str = ""
    compact_strategy: str = ""
    norms_reference_path: str = ""


def _load_registry() -> dict:
    """Загрузить реестр дисциплин (_registry.json)."""
    global _registry_cache
    if _registry_cache is not None:
        return _registry_cache
    if not REGISTRY_FILE.exists():
        _registry_cache = {"disciplines": {}}
        return _registry_cache
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        _registry_cache = json.load(f)
    return _registry_cache


def _read_file(path: Path) -> str:
    """Прочитать файл, вернуть пустую строку если не существует."""
    if path.exists():
        return path.read_text(encoding="utf-8").strip()
    return ""


def _resolve_profile_dir(code: str, disc_info: dict) -> Path:
    """Resolve discipline profile directory using profile_dir from registry.

    Priority: profile_dir from registry > code as folder name.
    This ensures portability: Cyrillic codes map to ASCII folder names.

    Сегмент ПРОВЕРЯЕТСЯ, а не подставляется: `code` приходит из
    `project_info.section`, то есть из пользовательских метаданных, и
    непроверенное значение вида `../../..` выбирало бы файлы за пределами
    каталога дисциплин (CH-02).
    """
    profile_dir_name = str(disc_info.get("profile_dir") or code)
    if not _safe_profile_segment(profile_dir_name):
        raise DisciplineProfileUnsafe(
            f"Имя каталога профиля {profile_dir_name!r} не является безопасным "
            "сегментом пути — профиль не загружается"
        )
    return DISCIPLINES_DIR / profile_dir_name


def load_discipline(code: str) -> DisciplineProfile:
    """Загрузить профиль дисциплины по коду. Кэширует результат.

    **Код нормализуется ДО поиска каталога.** Каталоги профилей названы
    латиницей (`AR`), а `project_info.section` реальных проектов сплошь и рядом
    кириллический (`АР`) — совпадения по строке нет, каталога нет, и прежняя
    версия молча возвращала профиль EOM. Нормализация выполняется закрытым
    индексом алиасов реестра (`discipline_identity`), а не заменой символов.

    **Строгий режим** (`AUDIT_DISCIPLINE_PROFILE_STRICT=1`) превращает оба
    молчаливых исхода — «код не опознан» и «каталог профиля отсутствует» — в
    исключение. Он всегда включён в процессе удалённой ноги аудита: там
    подстановка EOM означала бы аудит чужим профилем на чужой машине, и увидеть
    это можно было бы только в логе, оставшемся на VPS.
    """
    from backend.app.services.common import discipline_identity as _identity

    strict = _identity.strict_profile_mode()
    canonical = _identity.normalize_discipline_code(code)
    if canonical is None:
        if strict:
            raise _identity.UnknownDisciplineError(code, _identity.known_codes())
        logger.warning(
            "Дисциплина %r не опознана — используется профиль EOM. "
            "Известные коды: %s",
            code, ", ".join(_identity.known_codes()),
        )
        canonical = "EOM"
    if canonical in _profile_cache:
        return _profile_cache[canonical]

    registry = _load_registry()
    disc_info = registry.get("disciplines", {}).get(canonical, {})

    disc_dir = _resolve_profile_dir(canonical, disc_info)
    if not disc_dir.exists():
        if strict:
            raise DisciplineProfileMissing(
                f"Профиль дисциплины {canonical!r} не найден: {disc_dir}. "
                "Строгий режим запрещает подстановку профиля EOM."
            )
        # Fallback на EOM если профиль не найден
        if canonical != "EOM":
            logger.warning(
                "Профиль дисциплины %r не найден (%s) — fallback на EOM. "
                "Раздел будет аудирован профилем EOM, а не своим профилем.",
                canonical, disc_dir,
            )
            return load_discipline("EOM")
        # Если даже EOM нет — возвращаем пустой профиль
        logger.warning(
            "Профиль EOM не найден (%s) — возвращён ПУСТОЙ профиль для %r "
            "(аудит без ролевого профиля дисциплины).",
            disc_dir, canonical,
        )
        return DisciplineProfile(
            code=canonical,
            name=disc_info.get("name", canonical),
            short_name=disc_info.get("short_name", canonical),
            color=disc_info.get("color", "#666"),
        )
    code = canonical

    # Прочитать config.json дисциплины
    config_path = disc_dir / "config.json"
    config = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)

    # Загрузить все файлы профиля
    norms_ref_file = config.get("norms_reference_file", "norms_reference.md")
    norms_path = disc_dir / norms_ref_file

    profile = DisciplineProfile(
        code=code,
        name=disc_info.get("name", config.get("name", code)),
        short_name=disc_info.get("short_name", config.get("short_name", code)),
        color=disc_info.get("color", "#666"),
        role=_read_file(disc_dir / config.get("role_file", "role.md")),
        checklist=_read_file(disc_dir / config.get("checklist_file", "checklist.md")),
        triage_table=_read_file(disc_dir / config.get("triage_table_file", "triage_table.md")),
        project_params=_read_file(disc_dir / config.get("project_params_file", "project_params.md")),
        drawing_types=_read_file(disc_dir / config.get("drawing_types_file", "drawing_types.md")),
        finding_categories=_read_file(disc_dir / config.get("finding_categories_file", "finding_categories.md")),
        compact_strategy=_read_file(disc_dir / config.get("compact_strategy_file", "compact_strategy.md")),
        norms_reference_path=str(norms_path) if norms_path.exists() else str(BASE_DIR / "norms_reference.md"),
    )

    _profile_cache[code] = profile
    return profile


def detect_discipline(folder_name: str, text_sample: str = "") -> str:
    """
    Автодетекция дисциплины по имени папки и/или тексту.

    Приоритет:
    1. Имя папки → поиск folder_patterns из _registry.json
    2. Текст → подсчёт text_keywords, порог >= 2 совпадения
    3. Fallback → "EOM"
    """
    registry = _load_registry()
    disciplines = registry.get("disciplines", {})

    # Шаг 1: по имени папки
    folder_upper = folder_name.upper()
    for code, disc in disciplines.items():
        for pattern in disc.get("folder_patterns", []):
            if pattern.upper() in folder_upper:
                return code

    # Шаг 2: по тексту
    if text_sample:
        text_lower = text_sample.lower()
        scores: dict[str, int] = {}
        for code, disc in disciplines.items():
            scores[code] = sum(
                1 for kw in disc.get("text_keywords", [])
                if kw.lower() in text_lower
            )
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] >= 2:
                return best

    # Fallback
    return "EOM"


def detect_discipline_detailed(folder_name: str = "", pdf_name: str = "",
                               doc_text: str = "") -> dict:
    """Как detect_discipline, но возвращает источник детекции.

    Приоритет: имя папки → имя PDF → текст document.md (порог keywords >= 2) →
    fallback EOM. Возвращает {code, source, reason}, где source ∈
    {folder_name, pdf_name, document_text, fallback}.
    """
    registry = _load_registry()
    disciplines = registry.get("disciplines", {})

    def _match_name(name: str):
        up = (name or "").upper()
        for code, disc in disciplines.items():
            for pattern in disc.get("folder_patterns", []):
                if pattern and pattern.upper() in up:
                    return code, pattern
        return None, None

    code, pat = _match_name(folder_name)
    if code:
        return {"code": code, "source": "folder_name", "reason": f"паттерн «{pat}» в имени папки"}
    code, pat = _match_name(pdf_name)
    if code:
        return {"code": code, "source": "pdf_name", "reason": f"паттерн «{pat}» в имени PDF"}
    if doc_text:
        tl = doc_text.lower()
        scores = {c: sum(1 for kw in d.get("text_keywords", []) if kw.lower() in tl)
                  for c, d in disciplines.items()}
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] >= 2:
                return {"code": best, "source": "document_text",
                        "reason": f"{scores[best]} ключевых слов в тексте"}
    return {"code": "EOM", "source": "fallback", "reason": "не определено — по умолчанию EOM"}


def get_supported_disciplines() -> list[dict]:
    """Список поддерживаемых дисциплин для UI, отсортированный по order."""
    registry = _load_registry()
    result = []
    for code, disc in registry.get("disciplines", {}).items():
        try:
            disc_dir = _resolve_profile_dir(code, disc)
        except DisciplineProfileUnsafe:
            # Небезопасное имя каталога не должно ронять список разделов в UI:
            # у такой дисциплины просто нет профиля.
            disc_dir = DISCIPLINES_DIR / "__unsafe__"
        result.append({
            "code": code,
            "name": disc.get("name", code),
            "short_name": disc.get("short_name", code),
            "color": disc.get("color", "#666"),
            "order": disc.get("order", 999),
            "has_profile": disc_dir.exists(),
        })
    result.sort(key=lambda d: d["order"])
    return result


def get_supported_codes() -> list[str]:
    """Список кодов поддерживаемых дисциплин."""
    registry = _load_registry()
    return list(registry.get("disciplines", {}).keys())


def inject_discipline(template: str, profile: DisciplineProfile) -> str:
    """Заменить плейсхолдеры в шаблоне на содержимое профиля дисциплины."""
    replacements = {
        "{DISCIPLINE_ROLE}": profile.role,
        "{DISCIPLINE_CHECKLIST}": profile.checklist,
        "{DISCIPLINE_TRIAGE_TABLE}": profile.triage_table,
        "{DISCIPLINE_PROJECT_PARAMS}": profile.project_params,
        "{DISCIPLINE_TEXT_ANALYSIS}": _extract_text_analysis(profile.project_params),
        "{DISCIPLINE_DRAWING_TYPES}": profile.drawing_types,
        "{DISCIPLINE_FINDING_CATEGORIES}": profile.finding_categories,
        "{DISCIPLINE_COMPACT_STRATEGY}": profile.compact_strategy,
        "{DISCIPLINE_NORMS_FILE}": profile.norms_reference_path,
    }

    # Также заменяем JSON-шаблон project_params в контексте JSON-блоков
    params_json = _extract_params_json(profile.project_params)
    if params_json:
        replacements["{DISCIPLINE_PROJECT_PARAMS_JSON}"] = params_json

    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)

    return template


def _extract_text_analysis(project_params_md: str) -> str:
    """Извлечь секцию 'Что искать в тексте' из project_params.md."""
    lines = project_params_md.split("\n")
    result = []
    capture = False
    for line in lines:
        if "что искать в тексте" in line.lower() or "что искать" in line.lower():
            capture = True
            continue
        if capture:
            if line.startswith("## ") or line.startswith("```"):
                break
            if line.strip():
                result.append(line)
    return "\n".join(result) if result else project_params_md.split("##")[0].strip()


def _extract_params_json(project_params_md: str) -> str:
    """Извлечь JSON-блок из project_params.md."""
    match = re.search(r"```json\s*\n(.*?)\n```", project_params_md, re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""


def add_discipline(code: str, name: str, color: str = "#666") -> dict:
    """Добавить пользовательский раздел в _registry.json."""
    registry = _load_registry()
    disciplines = registry.setdefault("disciplines", {})
    if code in disciplines:
        raise ValueError(f"Раздел с кодом '{code}' уже существует")
    disciplines[code] = {
        "name": name,
        "short_name": name,
        "color": color,
        "folder_patterns": [code],
        "text_keywords": [],
    }
    # Сохранить обновлённый реестр
    DISCIPLINES_DIR.mkdir(parents=True, exist_ok=True)
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
    # Сбросить кэш
    invalidate_cache()
    return disciplines[code]


def update_discipline(code: str, name: str = None, color: str = None) -> dict:
    """Обновить параметры раздела в _registry.json."""
    registry = _load_registry()
    disciplines = registry.get("disciplines", {})
    if code not in disciplines:
        raise ValueError(f"Раздел с кодом '{code}' не найден")
    if name is not None:
        disciplines[code]["name"] = name
        disciplines[code]["short_name"] = name
    if color is not None:
        disciplines[code]["color"] = color
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
    invalidate_cache()
    return disciplines[code]


def delete_discipline(code: str):
    """Удалить раздел из _registry.json."""
    registry = _load_registry()
    disciplines = registry.get("disciplines", {})
    if code not in disciplines:
        raise ValueError(f"Раздел с кодом '{code}' не найден")
    del disciplines[code]
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
    invalidate_cache()


def reorder_disciplines(ordered_codes: list[str]):
    """Переупорядочить дисциплины. ordered_codes — коды в нужном порядке."""
    registry = _load_registry()
    disciplines = registry.get("disciplines", {})
    for i, code in enumerate(ordered_codes):
        if code in disciplines:
            disciplines[code]["order"] = i
    # Дисциплины не в списке получают order после всех
    max_order = len(ordered_codes)
    for code in disciplines:
        if code not in ordered_codes:
            disciplines[code]["order"] = max_order
            max_order += 1
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
    invalidate_cache()


def invalidate_cache():
    """Сбросить кэш (для тестирования или горячей перезагрузки)."""
    global _profile_cache, _registry_cache
    _profile_cache.clear()
    _registry_cache = None
    # Индекс алиасов строится из ТОГО ЖЕ реестра: оставить его горячим значило
    # бы нормализовать код по составу дисциплин, которого больше нет.
    from backend.app.services.common import discipline_identity as _identity

    _identity.invalidate_cache()
