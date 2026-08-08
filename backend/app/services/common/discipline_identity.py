"""Авторитетный идентификатор дисциплины проекта.

**Зачем отдельный модуль, когда есть `discipline_service`.** До этого этапа
дисциплина была СТРОКОЙ, которую каждый вызывающий читал сам и по-своему, а
`load_discipline` превращал любое непонятное значение в `EOM` с одной строчкой
`logger.warning`. На центре это давало «раздел АР аудирован профилем ЭОМ», и
увидеть факт можно было только в логе; на воркере — то же самое, но лог
оставался на чужой машине.

Механизм ровно один и он воспроизводится в одну строку:

    >>> load_discipline("АР").code
    'EOM'

Каталог профиля называется `AR` ЛАТИНИЦЕЙ, а `project_info.section` реальных
проектов сплошь и рядом кириллический («АР», «ВК», «ЭОМ») — потому что
`projects/<КОД_ДИСЦИПЛИНЫ>/<имя>/` именуется по-русски, а
`project_service` берёт `section` в том числе из имени физического каталога
раздела. Совпадения по строке нет, каталога нет, дальше — молчаливый EOM.

Поэтому здесь:

* **закрытая нормализованная форма** — код всегда один из ключей реестра;
* **алиасы выводятся из реестра и профилей**, а не пишутся руками: список
  дисциплин меняется из UI, и жёсткая таблица отстала бы в первый же день;
* **неоднозначный алиас удаляется**, а не «достаётся первому»;
* **неизвестное значение — структурированная ошибка**, а не EOM;
* **имя каталога профиля не строится из пользовательской строки** — только из
  проверенного `profile_dir` реестра (иначе `section` вида `../../etc`
  выбирал бы файлы за пределами каталога дисциплин).

Модуль намеренно не читает файлы профиля: он отвечает на вопрос «какая это
дисциплина», а не «что в её профиле».
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from backend.app.core.config import DISCIPLINES_DIR

#: Значение `section`, которое исторически означает «не определили».
#: Отдельная константа: сравнение со строкой в пяти местах рано или поздно
#: разъезжается.
FALLBACK_DISCIPLINE = "EOM"

#: Максимальная длина сырого значения. Дисциплина — код, а не текст.
MAX_RAW_LENGTH = 64

def safe_profile_segment(name: Any) -> bool:
    """Годится ли строка как ОДИН сегмент пути внутри каталога дисциплин.

    Не «латиница и цифры»: раздел с кириллическим кодом заводится штатно через
    `add_discipline`, и запрет сломал бы существующие установки. Запрещается
    ровно то, что выводит из каталога.
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


class DisciplineError(ValueError):
    """Базовая ошибка идентификации дисциплины."""


class UnknownDisciplineError(DisciplineError):
    """Значение не сопоставляется ни с одной известной дисциплиной.

    Отдельный класс, потому что обработка разная: неизвестная дисциплина на
    центре — повод показать оператору выбор, на воркере — повод не запускать
    конвейер вовсе.
    """

    def __init__(self, raw: Any, known: Iterable[str]) -> None:
        self.raw = raw
        self.known = sorted(known)
        super().__init__(
            f"Дисциплина {raw!r} не опознана. Известные коды: "
            + ", ".join(self.known)
        )


class DisciplineProfileError(DisciplineError):
    """Профиль известной дисциплины отсутствует или повреждён."""


@dataclass(frozen=True)
class DisciplineId:
    """Нормализованный идентификатор дисциплины.

    `code` — ключ реестра (латиница, верхний регистр). `profile_dir` —
    ПРОВЕРЕННОЕ имя каталога профиля. `source` — откуда значение взялось; оно
    едет в манифест и в evidence, потому что «дисциплина определена по
    метаданным версии» и «дисциплина угадана по имени папки» — разные утверждения
    с разной ценой ошибки.
    """

    code: str
    profile_dir: str
    display_name: str = ""
    source: str = "unknown"
    raw: str = ""

    def __str__(self) -> str:                      # pragma: no cover — диагностика
        return self.code

    def as_dict(self) -> dict[str, Any]:
        return {
            "discipline_id": self.code,
            "profile_dir": self.profile_dir,
            "display_name": self.display_name,
            "source": self.source,
            "raw": self.raw,
        }


# ─── Реестр и алиасы ─────────────────────────────────────────────────────────
_CACHE: dict[str, Any] = {}


def _registry() -> dict[str, dict[str, Any]]:
    """Реестр дисциплин. Читается тем же файлом, что и `discipline_service`."""
    cached = _CACHE.get("registry")
    if cached is not None:
        return cached
    path = Path(DISCIPLINES_DIR) / "_registry.json"
    data: dict[str, Any] = {}
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                data = raw.get("disciplines") or {}
        except (OSError, ValueError):
            data = {}
    result = {
        str(code).strip().upper(): dict(info)
        for code, info in data.items()
        if str(code).strip()
    }
    _CACHE["registry"] = result
    return result


def _normalize_token(value: Any) -> str:
    """Каноническая форма СРАВНЕНИЯ.

    Регистр, пробелы и разделители снимаются: `«ЭОМ»`, `эом`, ` ЭОМ ` и `ЭОМ-`
    — одно и то же значение, а различать их значило бы делать вид, что
    оператор обязан вводить код побайтово точно.
    """
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    text = text.replace(" ", " ")
    text = text.strip(" \t\r\n«»\"'")
    text = text.strip(" ._-/\\")
    return text.upper()


def _alias_candidates(code: str, info: Mapping[str, Any]) -> set[str]:
    """Все написания, которыми эта дисциплина встречается в данных.

    Источники — только машинные: ключ реестра, `profile_dir`, `config.json`
    профиля и те `folder_patterns`, которые являются самостоятельным словом.
    Шаблоны вида `-ЭС` и `ЭС-` в алиасы не идут: это правила поиска ПОДСТРОКИ в
    имени каталога, а не коды.
    """
    out: set[str] = set()
    for value in (code, info.get("profile_dir"), info.get("code")):
        token = _normalize_token(value)
        if token:
            out.add(token)
    for pattern in info.get("folder_patterns") or []:
        raw = str(pattern or "")
        if raw.startswith("-") or raw.endswith("-"):
            continue
        token = _normalize_token(raw)
        if len(token) >= 2 and token.replace(" ", "").isalnum():
            out.add(token)
    profile_dir = str(info.get("profile_dir") or code)
    if safe_profile_segment(profile_dir):
        config_path = Path(DISCIPLINES_DIR) / profile_dir / "config.json"
        if config_path.is_file():
            try:
                config = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                config = {}
            if isinstance(config, dict):
                for key in ("code", "short_name"):
                    for part in str(config.get(key) or "").split("/"):
                        token = _normalize_token(part)
                        # `short_name` профиля бывает фразой («Электроснабжение
                        # и электрооборудование»): фразы кодами не являются.
                        if 2 <= len(token) <= 8 and " " not in token:
                            out.add(token)
    return out


def _alias_index() -> dict[str, str]:
    """Алиас → код. Неоднозначные алиасы ИСКЛЮЧАЮТСЯ.

    Отдать неоднозначный алиас «первому по порядку словаря» значит поставить
    выбор профиля в зависимость от порядка ключей в JSON. Лучше честное «не
    опознано».
    """
    cached = _CACHE.get("aliases")
    if cached is not None:
        return cached
    owners: dict[str, set[str]] = {}
    for code, info in _registry().items():
        for alias in _alias_candidates(code, info):
            owners.setdefault(alias, set()).add(code)
    index = {alias: next(iter(codes)) for alias, codes in owners.items()
             if len(codes) == 1}
    # Ключ реестра всегда побеждает: он и есть каноническое имя.
    for code in _registry():
        index[code] = code
    _CACHE["aliases"] = index
    return index


def invalidate_cache() -> None:
    """Сбросить кэш реестра и алиасов (реестр редактируется из UI)."""
    _CACHE.clear()


def known_codes() -> list[str]:
    """Отсортированный список канонических кодов дисциплин."""
    return sorted(_registry())


def ambiguous_aliases() -> dict[str, list[str]]:
    """Алиасы, претендующие на две и более дисциплины. Диагностика реестра."""
    owners: dict[str, set[str]] = {}
    for code, info in _registry().items():
        for alias in _alias_candidates(code, info):
            owners.setdefault(alias, set()).add(code)
    return {
        alias: sorted(codes)
        for alias, codes in sorted(owners.items())
        if len(codes) > 1 and alias not in _registry()
    }


# ─── Нормализация ────────────────────────────────────────────────────────────
def normalize_discipline_code(raw: Any) -> Optional[str]:
    """Канонический код дисциплины либо None.

    None означает ровно «не опознано» и НИКОГДА не означает EOM: подстановка
    умолчания на этом уровне и есть тот дефект, ради которого написан модуль.
    """
    text = str(raw if raw is not None else "").strip()
    if not text or len(text) > MAX_RAW_LENGTH:
        return None
    token = _normalize_token(text)
    if not token:
        return None
    return _alias_index().get(token)


def profile_dir_name(code: str) -> str:
    """Проверенное имя каталога профиля для КАНОНИЧЕСКОГО кода.

    Путь никогда не строится из пользовательской строки: сюда приходит только
    результат `normalize_discipline_code`, а само имя каталога дополнительно
    проверяется на форму сегмента.
    """
    info = _registry().get(code)
    if info is None:
        raise UnknownDisciplineError(code, known_codes())
    name = str(info.get("profile_dir") or code)
    if not safe_profile_segment(name):
        raise DisciplineProfileError(
            f"profile_dir={name!r} дисциплины {code!r} не является безопасным "
            "сегментом пути"
        )
    return name


def discipline_id(raw: Any, *, source: str = "explicit") -> DisciplineId:
    """Собрать `DisciplineId` из сырого значения. Неизвестное — ошибка."""
    code = normalize_discipline_code(raw)
    if code is None:
        raise UnknownDisciplineError(raw, known_codes())
    info = _registry().get(code) or {}
    return DisciplineId(
        code=code,
        profile_dir=profile_dir_name(code),
        display_name=str(info.get("name") or code),
        source=source,
        raw=str(raw if raw is not None else ""),
    )


# ─── Авторитетный источник ───────────────────────────────────────────────────
#: Порядок источников. Первый выигравший и есть ответ; порядок отражает
#: убывание авторитетности, а не удобство.
#:
#: `project_info.section` — то, что оператор/импортёр записал ЯВНО.
#: `version.json → project_info.section` — то же значение, но зафиксированное
#: на конкретную версию (именно оно уезжает в пакет).
#: `document.json → discipline` — метаданные документа v2.
#:
#: Имени каталога, внешнего кода проекта и отображаемого имени в списке НЕТ
#: намеренно (CH-02): они приходят от пользователя и могут содержать что
#: угодно, включая сегменты пути.
AUTHORITATIVE_SOURCES: tuple[str, ...] = (
    "project_info.section",
    "version.project_info.section",
    "version.section",
    "document.discipline",
)


def resolve_project_discipline(
    *,
    project_info: Optional[Mapping[str, Any]] = None,
    version_meta: Optional[Mapping[str, Any]] = None,
    document_meta: Optional[Mapping[str, Any]] = None,
) -> DisciplineId:
    """Определить дисциплину проекта по АВТОРИТЕТНЫМ метаданным.

    Возвращает первый источник, который дал ОПОЗНАННЫЙ код. Источник, который
    дал неопознанное значение, не «пропускается молча»: он запоминается и
    попадает в текст ошибки, если не опознан ни один — иначе разбор инцидента
    сводится к угадыванию, что именно лежало в метаданных.
    """
    raw_values: list[tuple[str, Any]] = [
        ("project_info.section", (project_info or {}).get("section")),
        (
            "version.project_info.section",
            ((version_meta or {}).get("project_info") or {}).get("section")
            if isinstance((version_meta or {}).get("project_info"), Mapping)
            else None,
        ),
        ("version.section", (version_meta or {}).get("section")),
        ("document.discipline", (document_meta or {}).get("discipline")),
    ]
    seen: list[str] = []
    for source, value in raw_values:
        if value in (None, ""):
            continue
        seen.append(f"{source}={value!r}")
        code = normalize_discipline_code(value)
        if code is not None:
            return discipline_id(value, source=source)
    raise UnknownDisciplineError(
        "; ".join(seen) if seen else "метаданные не содержат дисциплины",
        known_codes(),
    )


def resolve_from_version_dir(version_dir: Path) -> DisciplineId:
    """Определить дисциплину по файлам каталога версии.

    Читает `01_input/project_info.json`, `version.json` и `document.json`
    документа — то есть ровно те файлы, которые уезжают в пакет. Имя
    физического каталога `disciplines/<Д>` НЕ используется: оно совпадает с
    дисциплиной только по соглашению, а пакет обязан оставаться верным и когда
    соглашение нарушено.
    """
    version_dir = Path(version_dir)
    project_info = _read_json(version_dir / "01_input" / "project_info.json")
    version_meta = _read_json(version_dir / "version.json")
    document_meta = _read_json(version_dir.parent.parent / "document.json")
    return resolve_project_discipline(
        project_info=project_info,
        version_meta=version_meta,
        document_meta=document_meta,
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


# ─── Строгий режим ───────────────────────────────────────────────────────────
#: Процессный переключатель: «отсутствующий профиль — отказ, а не EOM».
#: Выставляется удалённой ногой аудита (`remote_audit_runner.harden_process_env`)
#: и может быть включён на центре администратором. Единица — процесс, потому
#: что процесс удалённой ноги целиком принадлежит одному аудиту.
STRICT_PROFILE_ENV = "AUDIT_DISCIPLINE_PROFILE_STRICT"


def strict_profile_mode() -> bool:
    raw = os.environ.get(STRICT_PROFILE_ENV)
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}
