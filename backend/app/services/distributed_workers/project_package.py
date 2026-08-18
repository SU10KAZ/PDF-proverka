"""Сборка переносимого пакета РЕАЛЬНОГО проекта для удалённого аудита.

Три решения, каждое подтверждено первым аудитом архитектуры:

**TAR, а не ZIP.** 18 % файлов корпуса (36 673 из 199 016) — хардлинки, из них
34 932 — кропы блоков после дедупликации. ZIP не имеет типа записи «жёсткая
ссылка» вовсе, и пакет раздувается на 40 %. Здесь хардлинки сохраняются явной
картой инодов, а не надеждой на библиотеку.

**Сканирование дерева, а не список путей.** Раскладка версий НЕОДНОРОДНА: у
одной версии `pipeline_log.json` лежит в `99_service/`, у другой каталога
`99_service/` нет вовсе и всё в `03_analysis/latest/`. Фиксированный список
путей на таком корпусе молча теряет артефакты, а resume-детектор на воркере
после этого начинает конвейер не с того этапа.

**Снимок конфигурации внутри пакета.** `prompts/` редактируются из UI, а
`stage_models.json` вообще вне git: воркер, взявший их из своего клона,
прогонит аудит другими моделями и другими промптами — молча и дороже. Снимок
делается на КОНКРЕТНУЮ попытку, его хэш едет в манифесте, и изменение
конфигурации центра после старта на текущую попытку уже не влияет.

Чего в пакете нет и быть не может: `.env`, секретов, токенов, авторизации
Claude/Codex, PID-файлов, WAL, каталога `.git`, исходного кода приложения,
нормативной базы, чужих проектов.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import tarfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from backend.app.services.distributed_workers import package_service

#: Версия раскладки проекта внутри пакета. Растёт при несовместимом изменении.
#:
#: 1 — плоская раскладка `payload/project/<содержимое каталога версии>`. Она
#:     несовместима с обоими режимами резолва проекта (`resolve_project_dir` и
#:     `resolve_v2_job_paths`) и удалённый прогон на ней падал ДО первого этапа
#:     (блокер Б-3 отчёта 07).
#: 2 — переносимый корень `projects_v2` целиком:
#:     `payload/projects_v2/objects/<obj>/disciplines/<Д>/documents/<код>/versions/<vid>/…`
#:     плюс `object.json`, `document.json` и `current_version.txt` — то есть
#:     ровно та форма дерева, которую перебирает `ProjectsV2Adapter`.
PROJECT_LAYOUT_VERSION = 2

#: Раскладки, которые распаковщик воркера соглашается исполнять. Единица, а не
#: диапазон: плоский пакет не «хуже поддерживается», он не работает вовсе.
SUPPORTED_PROJECT_LAYOUT_VERSIONS: frozenset[int] = frozenset({2})

#: Корень ПЕРЕНОСИМОГО дерева проектов внутри архива (под общим `payload/`).
#: Значение станет `AUDIT_PROJECTS_V2_DIR` процесса конвейера на воркере.
PROJECTS_ROOT = "projects_v2/"

#: Историческая раскладка версии 1. Оставлена ИМЕНЕМ, чтобы распаковщик мог
#: назвать причину отказа, а не молча не найти проект.
LEGACY_FLAT_PROJECT_ROOT = "project/"

#: Каталог снимков конфигурации внутри архива.
SNAPSHOT_ROOT = "snapshot/"

#: Каталог снимка runtime-конфигурации внутри архива.
RUNTIME_ROOT = "runtime/"

#: Каталог снимка профиля дисциплины внутри архива. Раздел отдельный именно
#: потому, что у него собственный хэш и собственный контракт проверки.
DISCIPLINE_PROFILE_ROOT = "discipline_profile/"

#: Что НИКОГДА не попадает в пакет. Проверяется по каждому сегменту пути.
FORBIDDEN_NAMES: frozenset[str] = frozenset(
    {
        ".git", ".env", ".venv", "venv", "__pycache__", "node_modules",
        ".claude", ".codex", ".ssh", ".aws", ".config",
    }
)

#: Расширения и имена, которые не переносятся никогда.
FORBIDDEN_SUFFIXES: tuple[str, ...] = (
    ".pid", ".lock", ".sock", "-wal", "-shm", ".db-wal", ".db-shm",
    ".pem", ".key", ".crt", ".p12", ".pfx",
)

FORBIDDEN_FILENAMES: frozenset[str] = frozenset(
    {
        ".env", ".env.local", "token", "claim_secret", "credentials",
        ".credentials.json", "auth.json", "workers.db", "worker.db",
        "batch_queue.json", "paid_cost.json", "paid_cost_events.jsonl",
        "usage_data.json", "decisions_log.json", "norms_paragraphs.json",
        # Центральные артефакты. Отправлять их воркеру нельзя не из-за
        # секретности, а из-за АСИММЕТРИИ: сборщик пакета результата возвращает
        # всё дерево `03_analysis/`, а импортёр отклоняет ВЕСЬ пакет, увидев
        # центральный артефакт. На любом повторном аудите версии, где нормы уже
        # проходили, многочасовой прогон выбрасывался целиком.
        "norm_checks.json", "norm_checks_llm.json", "03a_norms_verified.json",
        "decision_carryover_report.json", "migrated_findings_report.json",
        # Разметка эксперта. У неё есть вторая каноническая точка ВНУТРИ
        # разрешённого воркеру префикса (`03_analysis/latest/expert_review.json`),
        # и вернувшаяся устаревшая копия «воскресила» бы снятые вердикты.
        "expert_review.json",
    }
)

#: Восстановимое: кропы блоков ре-рендерятся из `02_work/document.pdf` офлайн.
#: Исключаются ТОЛЬКО если PDF в пакете есть — иначе воркер молча ушёл бы в
#: сеть на портал, а 15 % ссылок `crop_url` в корпусе мертвы.
REGENERABLE_DIR_PATTERNS: tuple[str, ...] = (
    "_stage02_paid_response_cache",
    ".evicted",
)

#: Максимальный размер одного файла в пакете. Больше — почти наверняка мусор.
MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024


class ProjectPackageError(RuntimeError):
    """Пакет собрать нельзя. Сообщение показывается оператору."""


@dataclass
class PackageLimits:
    max_total_bytes: int = 8 * 1024 * 1024 * 1024
    max_files: int = 200_000


@dataclass
class ScanResult:
    """Что нашлось в дереве версии."""

    files: list[tuple[Path, str]] = field(default_factory=list)   # (абсолютный, относительный)
    excluded: list[str] = field(default_factory=list)
    total_bytes: int = 0


# ─── Безопасные физические сегменты пути ─────────────────────────────────────
#: Символы, которые не могут быть частью имени каталога внутри пакета. Список
#: намеренно шире POSIX: пакет распаковывается на ЧУЖОЙ машине, и «у нас это
#: работает» не является аргументом.
_UNSAFE_SEGMENT_CHARS = set('/\\:*?"<>|\0')

#: Зарезервированные имена Windows. Пакет туда не поедет, но проверка стоит
#: одну строку, а отсутствие проверки однажды стоит дороже.
_RESERVED_SEGMENTS = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{i}" for i in range(1, 10)}
    | {f"lpt{i}" for i in range(1, 10)}
)

MAX_SEGMENT_LEN = 120


def safe_path_segment(value: Any, *, field: str) -> str:
    """Проверить значение как ФИЗИЧЕСКИЙ сегмент пути внутри пакета.

    Ключевое требование этапа: внешний код проекта («ТЕСТ/РД-АР1 — корпус 1»)
    содержит «/» и обязан остаться МЕТАДАННЫМИ, а не превратиться в подкаталог.
    Поэтому сегмент не «санируется» (санация молча склеила бы два разных
    проекта в один каталог), а ОТВЕРГАЕТСЯ — сборка падает до записи архива.
    """
    text = str(value if value is not None else "").strip()
    if not text:
        raise ProjectPackageError(f"{field}: пустой сегмент пути")
    if len(text) > MAX_SEGMENT_LEN:
        raise ProjectPackageError(
            f"{field}: сегмент длиннее {MAX_SEGMENT_LEN} символов ({len(text)})"
        )
    if text in (".", ".."):
        raise ProjectPackageError(f"{field}: сегмент {text!r} недопустим")
    if text.startswith("~"):
        raise ProjectPackageError(f"{field}: сегмент не может начинаться с '~'")
    bad = sorted(_UNSAFE_SEGMENT_CHARS & set(text))
    if bad:
        raise ProjectPackageError(
            f"{field}: недопустимые символы {bad!r} в сегменте {text!r} — "
            "внешний код проекта путём не является"
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in text):
        raise ProjectPackageError(f"{field}: управляющие символы в сегменте")
    if text.split(".")[0].lower() in _RESERVED_SEGMENTS:
        raise ProjectPackageError(f"{field}: зарезервированное имя {text!r}")
    if text != text.strip(" ."):
        raise ProjectPackageError(
            f"{field}: сегмент не может начинаться/заканчиваться пробелом или точкой"
        )
    return text


def safe_relative_path(value: Any, *, field: str) -> str:
    """Проверить многосегментный относительный путь внутри пакета."""
    text = str(value if value is not None else "").strip().replace("\\", "/")
    if not text:
        raise ProjectPackageError(f"{field}: пустой относительный путь")
    if text.startswith("/"):
        raise ProjectPackageError(f"{field}: абсолютный путь {text!r} недопустим")
    parts = [p for p in text.split("/") if p]
    if not parts:
        raise ProjectPackageError(f"{field}: путь не содержит сегментов")
    for part in parts:
        if part in (".", ".."):
            raise ProjectPackageError(f"{field}: обход каталога в {text!r}")
    return "/".join(parts)


# ─── Идентичность переносимого проекта ───────────────────────────────────────
#: Ключи метаданных, которые несут АБСОЛЮТНЫЙ путь центрального хоста. Они
#: очищаются при упаковке: их значение на воркере не просто бесполезно — оно
#: рассказывает чужой машине о раскладке нашей.
_HOST_PATH_KEYS = frozenset(
    {"legacy_path", "legacy_folder_path", "legacy_project_path", "source_path",
     "abs_path", "root_dir", "project_dir", "output_dir", "artifacts_dir"}
)


@dataclass(frozen=True)
class PortableProjectIdentity:
    """Физические сегменты переносимого дерева + внешние идентификаторы.

    Физические сегменты — то, из чего строится путь В ПАКЕТЕ. Внешние
    идентификаторы (`project_external_id`, отображаемое имя) путём не
    становятся никогда и живут только в метаданных.
    """

    object_folder: str
    discipline: str
    document_code: str
    version_id: str
    object_id: Optional[str] = None
    project_external_id: Optional[str] = None

    @property
    def project_relative_path(self) -> str:
        return (
            f"objects/{self.object_folder}/disciplines/{self.discipline}"
            f"/documents/{self.document_code}"
        )

    @property
    def version_relative_path(self) -> str:
        return f"{self.project_relative_path}/versions/{self.version_id}"

    def as_manifest(self) -> dict[str, Any]:
        return {
            "portable_projects_root": package_service.PAYLOAD_ROOT + PROJECTS_ROOT,
            "object_folder": self.object_folder,
            "object_id": self.object_id,
            "discipline": self.discipline,
            "document_id": self.document_code,
            "document_code": self.document_code,
            "version_id": self.version_id,
            "project_relative_path": self.project_relative_path,
            "version_relative_path": self.version_relative_path,
            "project_external_id": self.project_external_id,
        }


def resolve_portable_identity(
    version_dir: Path, *, project_external_id: Optional[str] = None
) -> PortableProjectIdentity:
    """Вывести идентичность из ФАКТИЧЕСКОГО положения каталога версии.

    Сегменты не придумываются и не берутся из задания: они читаются с диска
    центра. Это и есть гарантия «дерево в пакете совпадает с настоящим» —
    придуманный `object_id` дал бы форму, которой у центра нет, и резолвер на
    воркере снова не нашёл бы проект.

    Ожидаемая форма (её же перебирает `ProjectsV2Adapter.list_documents`):
    `objects/<obj>/disciplines/<Д>/documents/<код>/versions/<vid>`.
    """
    version_dir = Path(version_dir).resolve()
    doc_dir = version_dir.parent.parent           # documents/<код>
    versions_dir = version_dir.parent             # versions
    if versions_dir.name != "versions":
        raise ProjectPackageError(
            f"Каталог версии не в раскладке projects_v2: ожидался .../versions/<vid>, "
            f"получено {version_dir}"
        )
    documents_dir = doc_dir.parent                # documents
    discipline_dir = documents_dir.parent         # <Д>
    disciplines_dir = discipline_dir.parent       # disciplines
    object_dir = disciplines_dir.parent           # <obj>
    if documents_dir.name != "documents" or disciplines_dir.name != "disciplines":
        raise ProjectPackageError(
            "Каталог версии не в раскладке projects_v2: ожидалось "
            f".../objects/<obj>/disciplines/<Д>/documents/<код>/versions/<vid>, "
            f"получено {version_dir}"
        )

    document_json = doc_dir / "document.json"
    if not document_json.is_file():
        # Без него `ProjectsV2Adapter.list_documents` пропускает документ молча,
        # и воркер получит пакет, в котором проекта «нет».
        raise ProjectPackageError(
            f"В {doc_dir} нет document.json — переносимое дерево собрать нельзя"
        )
    try:
        doc_meta = json.loads(document_json.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ProjectPackageError(f"document.json нечитаем: {exc}") from exc
    if not isinstance(doc_meta, dict):
        raise ProjectPackageError("document.json не является объектом JSON")

    return PortableProjectIdentity(
        object_folder=safe_path_segment(object_dir.name, field="object_folder"),
        discipline=safe_path_segment(discipline_dir.name, field="discipline"),
        document_code=safe_path_segment(doc_dir.name, field="document_code"),
        version_id=safe_path_segment(version_dir.name, field="version_id"),
        object_id=(str(doc_meta.get("object_id")) if doc_meta.get("object_id") else None),
        project_external_id=(
            project_external_id
            if project_external_id is not None
            else doc_meta.get("external_id")
        ),
    )


def sanitize_metadata_blob(raw: bytes, *, source: str) -> tuple[bytes, list[str]]:
    """Убрать из метаданных абсолютные пути центрального хоста.

    Возвращает `(очищенный блоб, список очищенных json-путей)`. Не-JSON и
    неожиданная структура возвращаются как есть: молча ломать метаданные хуже,
    чем оставить в них лишнее поле, а рубеж «в пакете нет абсолютных путей»
    держится отдельной проверкой при сборке.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return raw, []
    cleared: list[str] = []

    def _is_host_path(value: Any) -> bool:
        return isinstance(value, str) and value.startswith("/") and len(value) > 1

    def walk(node: Any, path: str, *, key_name: str = "") -> Any:
        """Проверяется КАЖДЫЙ строковый узел, где бы он ни лежал.

        Прежняя версия смотрела только на ПРЯМЫЕ значения словаря, поэтому
        `{"sources": ["/home/coder/…"]}` и вложенные структуры проходили
        насквозь — а манифест при этом честно сообщал `cleared_absolute_paths: []`,
        то есть измерением не был.
        """
        if isinstance(node, dict):
            out: dict[str, Any] = {}
            for key, value in node.items():
                where = f"{path}.{key}" if path else key
                if key in _HOST_PATH_KEYS and isinstance(value, str) and value:
                    cleared.append(f"{source}:{where}")
                    out[key] = None
                    continue
                out[key] = walk(value, where, key_name=str(key))
            return out
        if isinstance(node, list):
            return [walk(item, f"{path}[{i}]") for i, item in enumerate(node)]
        if _is_host_path(node):
            cleared.append(f"{source}:{path}")
            return None
        return node

    if not cleared and not isinstance(data, (dict, list)):
        return raw, []
    cleaned = walk(data, "")
    if not cleared:
        return raw, []
    return (
        json.dumps(cleaned, ensure_ascii=False, indent=2).encode("utf-8"),
        cleared,
    )


# ─── Классификация путей ─────────────────────────────────────────────────────
def _is_forbidden(rel_parts: tuple[str, ...], name: str) -> Optional[str]:
    for part in rel_parts:
        if part in FORBIDDEN_NAMES:
            return f"запрещённый каталог {part!r}"
    if name in FORBIDDEN_FILENAMES:
        return f"запрещённое имя {name!r}"
    lowered = name.lower()
    for suffix in FORBIDDEN_SUFFIXES:
        if lowered.endswith(suffix):
            return f"запрещённое расширение {suffix!r}"
    if lowered.startswith(".env"):
        return "файл окружения"
    return None


def _is_regenerable(rel_parts: tuple[str, ...]) -> bool:
    return any(part in REGENERABLE_DIR_PATTERNS for part in rel_parts)


def scan_version_tree(
    version_dir: Path, *, limits: Optional[PackageLimits] = None
) -> ScanResult:
    """Обойти дерево версии и отобрать то, что едет на воркер.

    Симлинки не переносятся и не разыменовываются: пакет должен быть
    самодостаточным, а ссылка наружу дерева — это либо ошибка, либо попытка
    вынести чужие данные.
    """
    lim = limits or PackageLimits()
    version_dir = Path(version_dir).resolve()
    if not version_dir.is_dir():
        raise ProjectPackageError(f"Каталог версии не найден: {version_dir}")

    result = ScanResult()
    for root, dirnames, filenames in os.walk(version_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(version_dir)
        rel_parts = tuple(p for p in rel_root.parts if p not in (".",))
        # Обрезаем ветки целиком: так дешевле и так виднее в отчёте.
        keep_dirs = []
        for dirname in dirnames:
            if dirname in FORBIDDEN_NAMES:
                result.excluded.append(str(rel_root / dirname) + "/ (запрещённый каталог)")
                continue
            if dirname in REGENERABLE_DIR_PATTERNS:
                result.excluded.append(str(rel_root / dirname) + "/ (восстановимо)")
                continue
            keep_dirs.append(dirname)
        dirnames[:] = sorted(keep_dirs)

        for filename in sorted(filenames):
            abs_path = root_path / filename
            # `lstrip("./")` снимал НАБОР символов, а не префикс: файл
            # `.gitkeep` в корне версии уезжал в пакет как `gitkeep`, то есть
            # переименовывался молча. Относительный путь строится явно.
            rel_path = (rel_root / filename).as_posix()
            if rel_path.startswith("./"):
                rel_path = rel_path[2:]
            if abs_path.is_symlink():
                result.excluded.append(f"{rel_path} (симлинк)")
                continue
            reason = _is_forbidden(rel_parts + (filename,), filename)
            if reason:
                result.excluded.append(f"{rel_path} ({reason})")
                continue
            if _is_regenerable(rel_parts):
                result.excluded.append(f"{rel_path} (восстановимо)")
                continue
            try:
                size = abs_path.stat().st_size
            except OSError:
                result.excluded.append(f"{rel_path} (недоступен)")
                continue
            if size > MAX_FILE_BYTES:
                raise ProjectPackageError(
                    f"Файл {rel_path} больше потолка ({size} байт)"
                )
            result.files.append((abs_path, rel_path))
            result.total_bytes += size
            if len(result.files) > lim.max_files:
                raise ProjectPackageError(
                    f"В версии больше {lim.max_files} файлов — пакет не собирается"
                )
            if result.total_bytes > lim.max_total_bytes:
                raise ProjectPackageError(
                    f"Версия больше потолка пакета ({lim.max_total_bytes} байт)"
                )
    result.files.sort(key=lambda pair: pair[1])
    return result


# ─── Снимки конфигурации ─────────────────────────────────────────────────────
_SECRET_KEY_RE = re.compile(
    r"(secret|token|password|passwd|api[_-]?key|credential|cookie|bootstrap)",
    re.IGNORECASE,
)


#: Каталоги внутри `prompts/`, которые в ОБЩИЙ снимок промптов не входят.
#:
#: `disciplines/` — это четырнадцать профилей разных разделов. Их отправка
#: целиком означала три вещи сразу: на воркер уезжал в том числе профиль EOM
#: (то есть «аудит пошёл не тем профилем» нельзя отличить от «своим»),
#: `prompt_bundle_hash` менялся от правки постороннего раздела, и отдельного
#: хэша ПРИМЕНЁННОГО профиля не существовало. Нужный профиль едет отдельным
#: разделом пакета — `discipline_profile/` — со своим `tree_hash`.
#:
#: Исключение симметрично: и центр, и воркер считают хэш одной функцией,
#: поэтому порядок «разложить профиль ↔ сверить снимок» на результат не влияет.
#:
#: Исключаются именно КАТАЛОГИ профилей, а не весь `disciplines/`: сам
#: `_registry.json` — закрытый список дисциплин, по которому код нормализует
#: `section` и проверяет имя каталога профиля. Без него воркер не опознаёт
#: НИ ОДНУ дисциплину («известные коды: » пусто) и в строгом режиме отказывает
#: даже правильному профилю, который лежит рядом. Найдено живым прогоном.
PROMPT_SNAPSHOT_EXCLUDED_TOP_DIRS: frozenset[str] = frozenset({"disciplines"})


def collect_prompt_snapshot(prompts_dir: Path) -> dict[str, bytes]:
    """Снимок промптов. Только текстовые шаблоны, только относительные пути."""
    out: dict[str, bytes] = {}
    prompts_dir = Path(prompts_dir)
    if not prompts_dir.is_dir():
        return out
    for path in sorted(prompts_dir.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.suffix.lower() not in (".md", ".txt", ".json"):
            continue
        rel = path.relative_to(prompts_dir).as_posix()
        parts = rel.split("/")
        if any(part in FORBIDDEN_NAMES for part in parts):
            continue
        if parts[0] in PROMPT_SNAPSHOT_EXCLUDED_TOP_DIRS and len(parts) > 2:
            continue
        out[f"prompts/{rel}"] = path.read_bytes()
    return out


def collect_model_config_snapshot(stage_models_file: Path) -> dict[str, bytes]:
    """Снимок моделей этапов. Файл вне git, и без него прогон пойдёт не на тех."""
    path = Path(stage_models_file)
    if not path.is_file():
        return {}
    return {"stage_models.json": path.read_bytes()}


def collect_feature_flags_snapshot(
    env: Optional[dict[str, str]] = None,
    *,
    dropped_paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Профиль флагов БЕЗ секретов и БЕЗ путей центрального хоста.

    Берутся только переменные из известных префиксов, и каждая проходит два
    фильтра.

    Первый — по имени: ключ, похожий на секрет, не попадает в снимок ни при
    каких обстоятельствах (E-25).

    Второй — по ЗНАЧЕНИЮ: переменная, чьё значение является абсолютным путём,
    отбрасывается. Под префикс `AUDIT_` попадают не только флаги, но и корни
    данных центра (`AUDIT_DATA_DIR`, `AUDIT_APP_DATA_DIR`, `AUDIT_PROMPTS_DIR`,
    `AUDIT_CODEX_CLI_PATH` …), и они бессмысленны на чужой машине по
    построению: там все корни вычисляются от каталога попытки. Хуже того,
    `runtime_config.assert_no_secrets` такие значения ОТВЕРГАЕТ — то есть
    сборщик и валидатор противоречили друг другу, и на любом центре, где корни
    заданы через окружение (а в проде они заданы именно так), создание
    удалённого задания падало с «снимок содержит недопустимое». Дефект не
    видели тесты: они передают `feature_flags` явным словарём.

    Отброшенные имена возвращаются через `dropped_paths` — молча терять факт
    нельзя, он уезжает в манифест пакета.
    """
    source = env if env is not None else os.environ
    prefixes = (
        "AUDIT_", "PIPELINE_", "STAGE01_", "STAGE02_", "FINDINGS_", "BLOCK_",
        "BUDGET_", "PAID_API_", "CRITIC_", "NORMS_",
    )
    flags: dict[str, str] = {}
    for key, value in sorted(source.items()):
        if not key.startswith(prefixes):
            continue
        if _SECRET_KEY_RE.search(key):
            continue
        text = str(value)
        if text.startswith("/") and len(text) > 1:
            if dropped_paths is not None:
                dropped_paths.append(key)
            continue
        flags[key] = text
    return flags


def hash_files(files: dict[str, bytes]) -> str:
    """Хэш набора файлов: стабильный, зависит от имён и содержимого."""
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(files[name]).digest())
    return "sha256:" + digest.hexdigest()


def hash_json(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(blob).hexdigest()


# ─── Сборка архива ───────────────────────────────────────────────────────────
def _tar_add_bytes(tar: tarfile.TarFile, name: str, data: bytes, mtime: int) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mtime = mtime
    info.mode = 0o644
    tar.addfile(info, io.BytesIO(data))


def _collect_tree_metadata(
    version_dir: Path, identity: PortableProjectIdentity
) -> tuple[dict[str, bytes], list[str]]:
    """Метаданные выше каталога версии: object.json, document.json, current_version.

    Они лежат ВНЕ `version_dir`, поэтому сканер дерева версии их не видит, — а
    без `document.json` `ProjectsV2Adapter` документ пропускает молча. Именно
    поэтому переносимое дерево собирается отдельным шагом, а не «тем же
    обходом».
    """
    version_dir = Path(version_dir).resolve()
    doc_dir = version_dir.parent.parent
    # parents: [0]=documents, [1]=<дисциплина>, [2]=disciplines, [3]=<объект>.
    object_dir = doc_dir.parents[3]
    out: dict[str, bytes] = {}
    cleared: list[str] = []

    def take(path: Path, rel: str, *, required: bool) -> None:
        if not path.is_file() or path.is_symlink():
            if required:
                raise ProjectPackageError(f"Обязательный файл дерева отсутствует: {path}")
            return
        raw = path.read_bytes()
        if path.suffix.lower() == ".json":
            raw, dropped = sanitize_metadata_blob(raw, source=rel)
            cleared.extend(dropped)
        out[rel] = raw

    take(
        object_dir / "object.json",
        f"objects/{identity.object_folder}/object.json",
        required=False,
    )
    take(
        doc_dir / "document.json",
        f"{identity.project_relative_path}/document.json",
        required=True,
    )
    take(
        doc_dir / "current_version.txt",
        f"{identity.project_relative_path}/current_version.txt",
        required=False,
    )
    return out, cleared


def build_project_source_package(
    *,
    dest_path: Path,
    version_dir: Path,
    manifest_base: dict[str, Any],
    snapshot_files: dict[str, bytes],
    feature_flags: dict[str, Any],
    runtime_config: Optional[bytes] = None,
    discipline_profile_entries: Optional[dict[str, bytes]] = None,
    identity: Optional[PortableProjectIdentity] = None,
    compression: str = "gzip",
    limits: Optional[PackageLimits] = None,
) -> dict[str, Any]:
    """Собрать пакет из ФАКТИЧЕСКОГО дерева версии в ПЕРЕНОСИМОЙ раскладке.

    Внутри архива воспроизводится корень `projects_v2` целиком:

        payload/projects_v2/objects/<obj>/object.json
        payload/projects_v2/objects/<obj>/disciplines/<Д>/documents/<код>/document.json
        payload/projects_v2/objects/<obj>/disciplines/<Д>/documents/<код>/current_version.txt
        payload/projects_v2/objects/<obj>/…/documents/<код>/versions/<vid>/<файлы версии>

    Это не украшение раскладки. Плоский `payload/project/<содержимое версии>`
    не находил ни `resolve_project_dir`, ни `resolve_v2_job_paths`: первый
    доходил до fallback'а по суффиксу `.pdf` и возвращал ФАЙЛ вместо каталога,
    второй возвращал `None`. Удалённый прогон падал до первого этапа (Б-3).

    Хардлинки сохраняются: первый файл каждого инода кладётся как обычный, все
    последующие — записью типа `link`. Карта групп уезжает в манифест, чтобы
    приёмная сторона могла проверить, что связи не потерялись.
    """
    scan = scan_version_tree(version_dir, limits=limits)
    if not scan.files:
        raise ProjectPackageError(f"В версии {version_dir} нет ни одного файла")

    ident = identity or resolve_portable_identity(
        version_dir,
        project_external_id=manifest_base.get("project_external_id"),
    )
    tree_meta, cleared_paths = _collect_tree_metadata(version_dir, ident)
    version_prefix = ident.version_relative_path + "/"

    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    mtime = int(time.time())

    file_entries: list[dict[str, Any]] = []
    hardlink_groups: dict[str, list[str]] = {}
    inode_first: dict[tuple[int, int], str] = {}
    uncompressed = 0

    manifest = dict(manifest_base)
    manifest.update(
        {
            "manifest_version": manifest_base.get("manifest_version", 1),
            "package_type": "source",
            "project_layout_version": PROJECT_LAYOUT_VERSION,
            "path_root": package_service.PAYLOAD_ROOT,
            "project_root": PROJECTS_ROOT,
            "snapshot_root": SNAPSHOT_ROOT,
            "runtime_root": RUNTIME_ROOT,
            "discipline_profile_root": DISCIPLINE_PROFILE_ROOT,
            # Внешний идентификатор версии сохраняется отдельным полем: путь
            # строится ТОЛЬКО по физическому `version_id` из `ident`.
            "version_external_id": manifest_base.get("version_id"),
            **ident.as_manifest(),
            "cleared_absolute_paths": sorted(set(cleared_paths)),
            "compression": compression,
            "created_at": time.time(),
            # Потолки объявляются в манифесте ВСЕГДА: приёмная сторона обязана
            # знать, по каким границам пакет собирался, а не догадываться.
            "limits": dict(
                manifest_base.get("limits") or {},
                max_files=(limits or PackageLimits()).max_files,
                max_total_bytes=(limits or PackageLimits()).max_total_bytes,
                max_file_bytes=MAX_FILE_BYTES,
            ),
        }
    )

    tar = package_service._open_write(tmp_path, compression)   # noqa: SLF001
    try:
        # Метаданные дерева идут ПЕРВЫМИ: без `document.json` каталог версии на
        # воркере — просто набор файлов, который адаптер не видит.
        for rel_meta, blob in sorted(tree_meta.items()):
            arc_name = package_service.PAYLOAD_ROOT + PROJECTS_ROOT + safe_relative_path(
                rel_meta, field="tree_metadata"
            )
            _tar_add_bytes(tar, arc_name, blob, mtime)
            file_entries.append(
                {
                    "path": arc_name,
                    "bytes": len(blob),
                    "sha256": package_service.sha256_bytes(blob),
                }
            )
            uncompressed += len(blob)

        for abs_path, rel_path in scan.files:
            arc_name = (
                package_service.PAYLOAD_ROOT + PROJECTS_ROOT + version_prefix + rel_path
            )
            stat = abs_path.stat()
            key = (stat.st_dev, stat.st_ino)
            if stat.st_nlink > 1 and key in inode_first:
                info = tarfile.TarInfo(arc_name)
                info.type = tarfile.LNKTYPE
                info.linkname = inode_first[key]
                info.mtime = mtime
                info.mode = 0o644
                tar.addfile(info)
                hardlink_groups.setdefault(inode_first[key], []).append(arc_name)
                file_entries.append(
                    {"path": arc_name, "bytes": 0, "hardlink_to": inode_first[key]}
                )
                continue
            data_hash = package_service.sha256_file(abs_path)
            info = tar.gettarinfo(str(abs_path), arcname=arc_name)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with abs_path.open("rb") as fh:
                tar.addfile(info, fh)
            if stat.st_nlink > 1:
                inode_first[key] = arc_name
                hardlink_groups.setdefault(arc_name, [])
            file_entries.append(
                {"path": arc_name, "bytes": stat.st_size, "sha256": data_hash}
            )
            uncompressed += stat.st_size

        for name, data in sorted(snapshot_files.items()):
            arc_name = package_service.PAYLOAD_ROOT + SNAPSHOT_ROOT + name
            _tar_add_bytes(tar, arc_name, data, mtime)
            file_entries.append(
                {
                    "path": arc_name,
                    "bytes": len(data),
                    "sha256": package_service.sha256_bytes(data),
                }
            )
            uncompressed += len(data)

        flags_blob = json.dumps(feature_flags, ensure_ascii=False, indent=2).encode("utf-8")
        flags_name = package_service.PAYLOAD_ROOT + SNAPSHOT_ROOT + "feature_flags.json"
        _tar_add_bytes(tar, flags_name, flags_blob, mtime)
        file_entries.append(
            {
                "path": flags_name,
                "bytes": len(flags_blob),
                "sha256": package_service.sha256_bytes(flags_blob),
            }
        )
        uncompressed += len(flags_blob)

        # Профиль дисциплины. Пути уже проверены сборщиком снимка
        # (`discipline_profile._safe_relative`), здесь — второй рубеж: имя
        # записи в архиве строится только из проверенного относительного пути.
        for rel_profile, blob in sorted((discipline_profile_entries or {}).items()):
            arc_name = package_service.PAYLOAD_ROOT + safe_relative_path(
                rel_profile, field="discipline_profile"
            )
            if not arc_name.startswith(
                package_service.PAYLOAD_ROOT + DISCIPLINE_PROFILE_ROOT
            ):
                raise ProjectPackageError(
                    f"Запись профиля вне {DISCIPLINE_PROFILE_ROOT}: {rel_profile!r}"
                )
            _tar_add_bytes(tar, arc_name, blob, mtime)
            file_entries.append(
                {
                    "path": arc_name,
                    "bytes": len(blob),
                    "sha256": package_service.sha256_bytes(blob),
                }
            )
            uncompressed += len(blob)

        if runtime_config is not None:
            runtime_name = (
                package_service.PAYLOAD_ROOT + RUNTIME_ROOT + "runtime_config.json"
            )
            _tar_add_bytes(tar, runtime_name, runtime_config, mtime)
            file_entries.append(
                {
                    "path": runtime_name,
                    "bytes": len(runtime_config),
                    "sha256": package_service.sha256_bytes(runtime_config),
                }
            )
            uncompressed += len(runtime_config)

        # Рубеж RRG-07: ни одно имя внутри архива не является абсолютным и не
        # содержит обхода каталога. Проверяется по ФАКТИЧЕСКОМУ списку записей,
        # а не по намерению сборщика.
        bad_names = [
            e["path"]
            for e in file_entries
            if e["path"].startswith("/")
            or "\\" in e["path"]
            or ".." in e["path"].split("/")
            or not e["path"].startswith(package_service.PAYLOAD_ROOT)
        ]
        if bad_names:
            raise ProjectPackageError(
                "В пакете абсолютные или небезопасные пути: " + ", ".join(bad_names[:5])
            )

        tree_source = "\n".join(
            f"{e['path']}:{e.get('sha256') or e.get('hardlink_to')}"
            for e in file_entries
        )
        manifest.update(
            {
                "files": file_entries,
                "hardlinks": {k: sorted(v) for k, v in hardlink_groups.items() if v},
                "hardlink_groups": len([v for v in hardlink_groups.values() if v]),
                "excluded_regenerable_paths": scan.excluded[:2000],
                "excluded_count": len(scan.excluded),
                "total_size": scan.total_bytes,
                "uncompressed_size": uncompressed,
                "source_tree_hash": "sha256:"
                + hashlib.sha256(tree_source.encode("utf-8")).hexdigest(),
                "feature_flags_hash": hash_json(feature_flags),
            }
        )
        manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        _tar_add_bytes(tar, package_service.MANIFEST_NAME, manifest_bytes, mtime)
    except Exception:
        tar_streams = getattr(tar, "_dw_streams", None)
        try:
            tar.close()
        except Exception:                                # noqa: BLE001
            pass
        if tar_streams:
            for stream in tar_streams:
                try:
                    stream.close()
                except Exception:                        # noqa: BLE001
                    pass
        tmp_path.unlink(missing_ok=True)
        raise
    else:
        package_service._close_write(tar)                # noqa: SLF001

    os.replace(tmp_path, dest_path)
    manifest["archive"] = {
        "sha256": package_service.sha256_file(dest_path),
        "compressed_bytes": dest_path.stat().st_size,
        "uncompressed_bytes": uncompressed + len(manifest_bytes),
        "entries": len(file_entries) + 1,
        "hardlink_entries": sum(len(v) for v in hardlink_groups.values()),
    }
    sidecar = dest_path.parent / package_service.MANIFEST_NAME
    sidecar.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest


def find_secrets_in_files(files: Iterable[tuple[str, bytes]]) -> list[str]:
    """Грубый сканер секретов для проверки собранного пакета.

    Не «на всякий случай», а рубеж: если сюда что-то попало, дальше оно уедет
    на чужой VPS. Ищутся конкретные формы, встречающиеся в этом репозитории.
    """
    patterns = (
        (re.compile(rb"wtk_[A-Za-z0-9_\-]{20,}"), "worker token"),
        (re.compile(rb"etk_[A-Za-z0-9_\-]{20,}"), "execution token"),
        (re.compile(rb"clm_[A-Za-z0-9_\-]{20,}"), "claim secret"),
        # Дефис сразу после префикса — норма современных ключей
        # (`sk-ant-api03-…`, `sk-or-v1-…`, `sk-proj-…`), а прежний класс
        # символов её не допускал: ни один реальный ключ не ловился.
        (re.compile(rb"sk-[A-Za-z0-9_\-]{20,}"), "api key"),
        (re.compile(rb"AKIA[0-9A-Z]{16}"), "aws access key"),
        (re.compile(rb"AIza[0-9A-Za-z_\-]{30,}"), "google api key"),
        (re.compile(rb"gh[pous]_[A-Za-z0-9]{20,}"), "github token"),
        (re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "private key"),
        (re.compile(rb"eyJ[A-Za-z0-9_\-]{10,}\.eyJ[A-Za-z0-9_\-]{10,}\."), "jwt"),
        (re.compile(rb"[Aa]uthorization\s*[:=]\s*['\"]?Bearer\s"), "bearer header"),
        (re.compile(rb"(?i)(postgres|mysql|redis|amqp)://[^\s:@]+:[^\s@]+@"), "dsn with password"),
        (re.compile(rb"PORTAL_SESSION_SECRET\s*="), "portal session secret"),
        (re.compile(rb"PORTAL_AUTH_USERS\s*="), "portal users"),
        # Имена ключей провайдеров — и в env-форме (`KEY=`), и в JSON-форме
        # (`"KEY":`): снимок флагов приезжает именно JSON'ом.
        (
            re.compile(
                rb"(?i)(OPENROUTER|OPENAI|ANTHROPIC|GOOGLE|GEMINI|DEEPSEEK|QWEN)"
                rb"_API_KEY[\"']?\s*[:=]"
            ),
            "provider api key",
        ),
        (re.compile(rb"(?i)DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET[\"']?\s*[:=]"), "bootstrap secret"),
        (re.compile(rb"pbkdf2_sha256\$"), "password hash"),
    )
    hits: list[str] = []
    for name, blob in files:
        for pattern, label in patterns:
            if pattern.search(blob):
                hits.append(f"{name}: {label}")
    return hits
