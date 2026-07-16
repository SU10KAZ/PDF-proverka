"""
projects_v2 — общая библиотека для inventory / migrate / validate.

ЭТАП 1 (подготовка). Эта библиотека ТОЛЬКО:
  * читает старую структуру `projects/` (никогда не пишет в неё, не удаляет);
  * пишет исключительно в параллельную папку `projects_v2/`.

Backend/UI к этой структуре НЕ подключены. См.
`docs/projects_v2_storage_standard.md` и `docs/projects_v2_migration_plan.md`.

Чистые функции вынесены сюда, чтобы их могли импортировать и CLI-скрипты,
и тесты (`tests/test_projects_v2_*.py`).
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Константы раскладки
# ---------------------------------------------------------------------------

LAYOUT_VERSION = 1
CONTAINER_SUFFIX = "(main)"
VERSION_GROUP_FILENAME = "version_group.json"

# Папки фиксированного скелета версии
VERSION_SUBDIRS = (
    "01_input",
    "02_work",
    "03_analysis",
    "04_review",
    "05_export",
    "99_service",
)

# Входной комплект (по суффиксам имени файла). Базовое имя произвольно
# (document.* / <project>.* / <project> (от 04.02.2026).*), поэтому ищем
# по окончанию имени.
INPUT_QUAD = {
    "pdf": ".pdf",
    "document_md": "_document.md",
    "ocr_html": "_ocr.html",
    "result_json": "_result.json",
}

# Суффиксы-синонимы по ролям, в порядке приоритета. С 2026-07 портал отдаёт
# 3-файловый комплект: <имя>.pdf + <имя>_results.md + <имя>_results.html
# (без result.json). Старый 4-файловый метод (_document.md/_ocr.html/_result.json)
# принимаем до ~2026-08-14 (раздел ВК ещё распознаётся по-старому), после чего
# приём старых суффиксов можно удалить; ЧТЕНИЕ уже мигрированных версий от
# этого не зависит (в 02_work лежат нормализованные имена).
INPUT_SUFFIXES: dict[str, tuple[str, ...]] = {
    "pdf": (".pdf",),
    "document_md": ("_document.md", "_results.md"),
    "ocr_html": ("_ocr.html", "_results.html", "_results.htm"),
    "result_json": ("_result.json",),
}

# Нормализованные имена в 02_work (рабочая копия для backend).
WORK_NORMALIZED = {
    "pdf": "document.pdf",
    "document_md": "document.md",
    "ocr_html": "ocr.html",
    "result_json": "result.json",
}

# Ключевые артефакты анализа, которые обязаны попасть в 03_analysis/latest
LATEST_ANALYSIS_FILES = (
    "02_text_analysis.json",
    "01_blocks_analysis.json",
    "03_findings.json",
    "document_graph.json",
    "norm_checks.json",
    "03a_norms_verified.json",
    "optimization.json",
)

# Артефакты, которые нельзя потерять (validate проверяет их явно).
CRITICAL_ANALYSIS_FILES = (
    "02_text_analysis.json",
    "01_blocks_analysis.json",
    "03_findings.json",
)


def repo_root() -> Path:
    """scripts/projects_v2/v2lib.py -> repo root."""
    return Path(__file__).resolve().parents[2]


def projects_v2_root(root: Optional[Path] = None) -> Path:
    return (root or repo_root()) / "projects_v2"


def legacy_projects_root(root: Optional[Path] = None) -> Path:
    return (root or repo_root()) / "projects"


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def sha256_file(path: Path, _bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(_bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def safe_component(name: str) -> str:
    """Безопасное имя компонента пути в projects_v2.

    Не агрессивная: сохраняем кириллицу/пробелы/скобки (linux ext4 их держит),
    режем только разделители путей и управляющие символы.
    """
    name = name.replace("\x00", "")
    name = name.replace("/", "∕").replace("\\", "＼")
    name = name.strip()
    return name or "_unnamed_"


def load_objects_map(root: Optional[Path] = None) -> dict:
    """Читает backend/app/data/objects.json → {by_name, by_path, by_id}."""
    objects_file = (root or repo_root()) / "backend" / "app" / "data" / "objects.json"
    by_name: dict[str, str] = {}
    by_path: dict[str, str] = {}
    by_id: dict[str, str] = {}
    if objects_file.exists():
        try:
            data = json.loads(objects_file.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        for obj in data.get("objects", []):
            oid = str(obj.get("id") or "").strip()
            name = str(obj.get("name") or "").strip()
            pdir = str(obj.get("projects_dir") or "").strip()
            if oid:
                by_id[oid] = name
                if name:
                    by_name[name] = oid
                if pdir:
                    by_path[str(Path(pdir))] = oid
    return {"by_name": by_name, "by_path": by_path, "by_id": by_id}


def object_id_for(object_dir: Path, objects_map: dict) -> str:
    """Сопоставляет папку объекта с object_id из objects.json.

    Приоритет: точный путь → имя папки → детерминированный fallback-хэш имени.
    """
    p = str(object_dir.resolve())
    if p in objects_map.get("by_path", {}):
        return objects_map["by_path"][p]
    name = object_dir.name
    if name in objects_map.get("by_name", {}):
        return objects_map["by_name"][name]
    # fallback: стабильный короткий хэш (как в objects.json — 8 hex)
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]


# ---------------------------------------------------------------------------
# Распознавание проектов / контейнеров / версий
# ---------------------------------------------------------------------------


def is_version_container(path: Path) -> bool:
    return path.is_dir() and path.name.endswith(CONTAINER_SUFFIX)


def read_version_group(container_dir: Path) -> Optional[dict]:
    f = container_dir / VERSION_GROUP_FILENAME
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return None


def strip_pdf_suffix(name: str) -> str:
    """`13АВ-РД-ЭО-К3 V2.pdf` (это ПАПКА) -> `13АВ-РД-ЭО-К3 V2`.

    В legacy структуре часть папок названа с `.pdf` на конце (gotcha). В
    projects_v2 это просто папка версии vNNN, имя нормализуем.
    """
    return re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)


def document_code_for(path: Path) -> str:
    """Стабильный код документа.

    * контейнер `<base>(main)` -> logical_project_id (или base без суффикса);
    * обычный проект -> имя папки без хвостового `.pdf`.
    """
    if is_version_container(path):
        vg = read_version_group(path)
        if vg and vg.get("logical_project_id"):
            return str(vg["logical_project_id"]).strip()
        return path.name[: -len(CONTAINER_SUFFIX)].strip()
    return strip_pdf_suffix(path.name).strip()


@dataclass
class VersionRec:
    version_id: str          # v001, v002, ...
    version_no: int          # 1, 2, ...
    legacy_folder: Path      # папка версии в старой структуре
    legacy_name: str         # оригинальное имя папки (может оканчиваться на .pdf)
    label: str = ""          # V1, V2 из манифеста
    source: str = ""         # promoted / edit_projects_modal / plain ...
    status: str = ""


def enumerate_versions(path: Path) -> list[VersionRec]:
    """Возвращает версии проекта в порядке индексов v001, v002, ...

    Обычный проект -> одна версия v001 (сама папка).
    Контейнер `(main)` -> версии из version_group.json (version_no -> vNNN).
    """
    if is_version_container(path):
        vg = read_version_group(path) or {}
        versions = vg.get("versions") or []
        recs: list[VersionRec] = []
        # сортируем по version_no для строгого индекса
        for entry in sorted(versions, key=lambda e: int(e.get("version_no") or 0)):
            no = int(entry.get("version_no") or (len(recs) + 1))
            folder_name = str(entry.get("folder") or "").strip()
            legacy_folder = path / folder_name if folder_name else path
            recs.append(
                VersionRec(
                    version_id=f"v{no:03d}",
                    version_no=no,
                    legacy_folder=legacy_folder,
                    legacy_name=folder_name,
                    label=str(entry.get("label") or f"V{no}"),
                    source=str(entry.get("source") or ""),
                    status=str(entry.get("status") or ""),
                )
            )
        if recs:
            return recs
        # контейнер без валидного манифеста — деградируем до одной версии
    return [
        VersionRec(
            version_id="v001",
            version_no=1,
            legacy_folder=path,
            legacy_name=path.name,
            label="V1",
            source="plain",
            status="active",
        )
    ]


def find_input_quad(version_dir: Path) -> dict[str, Optional[Path]]:
    """Ищет входной комплект в папке версии по суффиксам имени.

    Возвращает {pdf, document_md, ocr_html, result_json} -> Path | None.
    `.pdf` ищем как файл (исключая директории с .pdf в имени).
    """
    found: dict[str, Optional[Path]] = {k: None for k in INPUT_QUAD}
    if not version_dir.is_dir():
        return found
    entries = sorted(version_dir.iterdir(), key=lambda p: p.name)
    # сначала специфичные суффиксы; для каждой роли перебираем синонимы
    # в порядке приоритета (старый суффикс выигрывает у нового при обоих в папке)
    for key in ("document_md", "ocr_html", "result_json"):
        for suffix in INPUT_SUFFIXES[key]:
            for e in entries:
                if e.is_file() and e.name.lower().endswith(suffix):
                    found[key] = e
                    break
            if found[key] is not None:
                break
    # .pdf — только файлы
    for e in entries:
        if e.is_file() and e.name.lower().endswith(".pdf"):
            found["pdf"] = e
            break
    return found


def classify_output_file(name: str) -> str:
    """Куда положить КОПИЮ файла из legacy `_output/` в новой раскладке.

    ВНИМАНИЕ: классификация эвристическая. Полная verbatim-копия всего
    `_output` лежит в `03_analysis/runs/<run_id>/` и является источником
    истины — здесь ничего не теряется. Эти buckets — только удобная
    раскладка часто используемых артефактов.
    """
    n = name
    if n in LATEST_ANALYSIS_FILES:
        return "03_analysis/latest"
    if "_review" in n or "_pre_review" in n or n.startswith("critic_v2"):
        return "04_review"
    if n.endswith(".xlsx") or n.endswith(".csv") or "report" in n:
        return "05_export"
    # всё остальное — служебное (логи, бэкапы, intermediate, summary)
    return "99_service"


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

INVENTORY_FIELDS = [
    "object",
    "object_id",
    "discipline",
    "project_name",
    "kind",                 # plain | container
    "versions",
    "version_count",
    "has_pdf",
    "has_document_md",
    "has_ocr_html",
    "has_result_json",
    "has_project_info",
    "has_output",
    "has_01_text_analysis",
    "has_02_blocks_analysis",
    "has_03_findings",
    "has_pipeline_log",
    "has_blocks",
    "has_version_group",
    "legacy_path",
    "warnings",
]


def _has_blocks(output_dir: Path) -> bool:
    if not output_dir.is_dir():
        return False
    for name in ("blocks_gemma_100", "blocks_stage02_100", "blocks"):
        d = output_dir / name
        if d.is_dir() and any(d.iterdir()):
            return True
    # legacy block batch files
    if any(output_dir.glob("block_batch_*.json")):
        return True
    return False


def inventory_one_project(object_dir: Path, discipline: str, project_path: Path,
                          objects_map: dict) -> dict:
    """Собирает строку inventory для одного проекта/контейнера (read-only)."""
    warnings: list[str] = []
    kind = "container" if is_version_container(project_path) else "plain"
    versions = enumerate_versions(project_path)

    # признаки берём по ПЕРВОЙ (primary) версии — это то, что показывает UI
    primary = versions[0]
    vdir = primary.legacy_folder

    quad = find_input_quad(vdir)
    output_dir = vdir / "_output"

    if kind == "plain" and project_path.name.lower().endswith(".pdf"):
        warnings.append("plain-folder-name-ends-with-.pdf")
    if kind == "container" and not (project_path / VERSION_GROUP_FILENAME).exists():
        warnings.append("container-without-version_group.json")
    for v in versions:
        if v.legacy_name.lower().endswith(".pdf"):
            warnings.append(f"version-folder-name-ends-with-.pdf:{v.version_id}")
        if not v.legacy_folder.exists():
            warnings.append(f"version-folder-missing:{v.version_id}")
    if quad["pdf"] is None:
        warnings.append("missing-pdf")
    if quad["document_md"] is None:
        warnings.append("missing-_document.md")
    if quad["result_json"] is None:
        warnings.append("missing-_result.json")
    if not output_dir.is_dir():
        warnings.append("no-_output")

    return {
        "object": object_dir.name,
        "object_id": object_id_for(object_dir, objects_map),
        "discipline": discipline,
        "project_name": project_path.name,
        "kind": kind,
        "versions": ";".join(f"{v.version_id}<-{v.legacy_name}" for v in versions),
        "version_count": len(versions),
        "has_pdf": quad["pdf"] is not None,
        "has_document_md": quad["document_md"] is not None,
        "has_ocr_html": quad["ocr_html"] is not None,
        "has_result_json": quad["result_json"] is not None,
        "has_project_info": (vdir / "project_info.json").exists(),
        "has_output": output_dir.is_dir(),
        "has_01_text_analysis": (output_dir / "02_text_analysis.json").exists(),
        "has_02_blocks_analysis": (output_dir / "01_blocks_analysis.json").exists(),
        "has_03_findings": (output_dir / "03_findings.json").exists(),
        "has_pipeline_log": (output_dir / "pipeline_log.json").exists(),
        "has_blocks": _has_blocks(output_dir),
        "has_version_group": (project_path / VERSION_GROUP_FILENAME).exists(),
        "legacy_path": str(project_path),
        "warnings": ";".join(warnings),
    }


def iter_legacy_projects(projects_root: Path):
    """Йелдит (object_dir, discipline_code, project_path) по старой структуре.

    Пропускает служебные папки с префиксом `_`/`.` и `__BATCH__`.
    Контейнер `(main)` отдаётся как один project_path (не его внутренности).
    """
    if not projects_root.is_dir():
        return
    for object_dir in sorted(p for p in projects_root.iterdir() if p.is_dir()):
        if object_dir.name.startswith((".", "_")):
            continue
        for disc_dir in sorted(p for p in object_dir.iterdir() if p.is_dir()):
            if disc_dir.name.startswith((".", "_")) or disc_dir.name == "__BATCH__":
                continue
            discipline = disc_dir.name
            # внутри дисциплины — проекты или контейнеры
            children = sorted(p for p in disc_dir.iterdir())
            for child in children:
                if not child.is_dir():
                    continue
                if child.name.startswith((".", "_")):
                    continue
                yield object_dir, discipline, child


def build_inventory(projects_root: Path, objects_map: dict) -> list[dict]:
    rows: list[dict] = []
    for object_dir, discipline, project_path in iter_legacy_projects(projects_root):
        rows.append(inventory_one_project(object_dir, discipline, project_path, objects_map))
    return rows


def write_inventory(rows: list[dict], json_path: Path, csv_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "layout_version": LAYOUT_VERSION,
        "generated_at": utc_now_iso(),
        "count": len(rows),
        "projects": rows,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=INVENTORY_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------------------------------------------------------
# old_to_new_map
# ---------------------------------------------------------------------------


def load_old_to_new_map(map_path: Path) -> dict:
    if map_path.exists():
        try:
            return json.loads(map_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"schema_version": 1, "generated_at": utc_now_iso(), "migrations": []}


def upsert_migration(map_obj: dict, record: dict) -> dict:
    """Заменяет запись миграции с тем же (object_id, document_code, version_id)."""
    key = (record["object_id"], record["document_code"], record["version_id"])
    migrations = [
        m for m in map_obj.get("migrations", [])
        if (m.get("object_id"), m.get("document_code"), m.get("version_id")) != key
    ]
    migrations.append(record)
    map_obj["migrations"] = migrations
    map_obj["generated_at"] = utc_now_iso()
    return map_obj


def save_old_to_new_map(map_obj: dict, map_path: Path) -> None:
    map_path.parent.mkdir(parents=True, exist_ok=True)
    map_path.write_text(json.dumps(map_obj, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Skeleton / schema
# ---------------------------------------------------------------------------


def schema_document() -> dict:
    """Машиночитаемое описание стандарта раскладки projects_v2."""
    return {
        "layout_version": LAYOUT_VERSION,
        "description": "projects_v2 storage standard (этап 1, не подключён к backend)",
        "tree": {
            "_system/": "служебные файлы: schema.json, migration_inventory.*, old_to_new_map.json",
            "objects/<readable_object_folder>/": "объект, человекочитаемая папка (напр. 214_Alia_ASTERUS); object_id хранится в object.json, не в имени папки",
            "objects/<readable_object_folder>/object.json": "метаданные объекта: object_id, display_name, folder_name, legacy_path",
            ".../disciplines/<discipline_code>/": "дисциплина (EOM, AR, OV, ...)",
            ".../documents/<document_code>/": "стабильная папка документа (раздела)",
            ".../documents/<document_code>/document.json": "метаданные документа + список версий",
            ".../documents/<document_code>/current_version.txt": "текущий version_id (vNNN)",
            ".../versions/vNNN/": "версия со строгим индексом v001, v002, ...",
            ".../versions/vNNN/version.json": "метаданные версии + legacy-имя папки",
            ".../versions/vNNN/01_input/": "НЕИЗМЕНЯЕМЫЙ входной комплект (.pdf/_document.md/_ocr.html/_result.json) + input_manifest.json",
            ".../versions/vNNN/02_work/": "нормализованные рабочие копии (document.pdf/document.md/ocr.html/result.json)",
            ".../versions/vNNN/03_analysis/": "runs/<run_id>/ (verbatim копия legacy _output) + latest/ (ключевые артефакты)",
            ".../versions/vNNN/04_review/": "critic/corrector/review артефакты",
            ".../versions/vNNN/05_export/": "отчёты, excel, csv",
            ".../versions/vNNN/99_service/": "логи, pipeline_log, бэкапы, intermediate",
            ".../comparisons/<vA>_vs_<vB>/comparison_link.json": "ссылка на сравнение версий (этап 2)",
        },
        "input_quad": INPUT_QUAD,
        "work_normalized": WORK_NORMALIZED,
        "version_indexing": {
            "format": "vNNN (zero-padded)",
            "rule": "plain-проект -> v001; контейнер (main) -> version_no из version_group.json -> vNNN",
            "pdf_in_folder_name": "legacy V2-папка с .pdf на конце становится vNNN; оригинал в version.json/input_manifest.json",
        },
        "invariants": [
            "01_input неизменяем (источник истины загрузки)",
            "03_analysis/runs/<run_id> — verbatim копия legacy _output (ничего не теряется)",
            "legacy projects/ и comparison/ только читаются, никогда не изменяются",
            "backend/UI к projects_v2 НЕ подключены на этом этапе",
        ],
    }


def ensure_v2_skeleton(v2_root: Path) -> None:
    (v2_root / "_system").mkdir(parents=True, exist_ok=True)
    (v2_root / "objects").mkdir(parents=True, exist_ok=True)
    schema_path = v2_root / "_system" / "schema.json"
    schema_path.write_text(
        json.dumps(schema_document(), ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

import shutil  # noqa: E402  (local to migration section)

# расширения, для которых считаем sha внутри verbatim run-копии _output
_SHA_TRACK_EXT = {".json", ".md", ".txt", ".html", ".csv", ".xlsx", ".jsonl"}


# --- человекочитаемые имена папок объектов ---

_TRANSLIT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}
# uppercase варианты
_TRANSLIT.update({k.upper(): (v.capitalize() if v else "") for k, v in list(_TRANSLIT.items())})


def _transliterate(s: str) -> str:
    return "".join(_TRANSLIT.get(ch, ch) for ch in s)


def make_object_folder_name(display_name: str, object_id: str = "") -> str:
    """Человекочитаемое имя папки объекта (стабильное, FS-safe, латиница).

    `214. Alia (ASTERUS)` -> `214_Alia_ASTERUS`
    `213. Мосфильмовская 31А "King&Sons"` -> `213_Mosfilmovskaya_31A_KingSons`

    Конфликт имён НЕ разрешается здесь (это делает вызывающий код, добавляя
    суффикс `_<object_id>` только при реальном конфликте).
    """
    s = _transliterate(display_name or "")
    s = s.replace("&", "").replace("'", "").replace("’", "")  # King&Sons -> KingSons
    s = re.sub(r"[^A-Za-z0-9]+", "_", s)   # всё прочее -> _
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = f"obj_{object_id}" if object_id else "object"
    return s


def _read_object_id(object_dir: Path) -> Optional[str]:
    oj = object_dir / "object.json"
    if not oj.exists():
        return None
    try:
        return str(json.loads(oj.read_text(encoding="utf-8")).get("object_id") or "") or None
    except Exception:
        return None


def resolve_object_folder(v2_root: Path, object_id: str,
                          display_name: Optional[str] = None) -> Path:
    """Возвращает путь к папке объекта в projects_v2.

    Приоритет:
    1. читаемая папка `make_object_folder_name(display_name)` если существует;
    2. любая папка objects/*, чей object.json совпадает по object_id;
    3. legacy `obj_<object_id>` если существует;
    4. (для новой миграции) читаемое имя, иначе `obj_<object_id>`.
    """
    objects_root = v2_root / "objects"
    if display_name:
        cand = objects_root / make_object_folder_name(display_name, object_id)
        if cand.exists():
            return cand
    if objects_root.is_dir():
        for d in sorted(objects_root.iterdir()):
            if d.is_dir() and _read_object_id(d) == object_id:
                return d
    legacy = objects_root / f"obj_{object_id}"
    if legacy.exists():
        return legacy
    if display_name:
        return objects_root / make_object_folder_name(display_name, object_id)
    return objects_root / f"obj_{object_id}"


def allocate_object_folder(v2_root: Path, object_id: str, display_name: str) -> Path:
    """Папка объекта для НОВОЙ миграции (с разрешением конфликта имён).

    Если читаемое имя занято другим object_id — добавляет суффикс `_<object_id>`.
    Если занято нашим object_id (повторная миграция) — переиспользует.
    """
    objects_root = v2_root / "objects"
    # уже существует папка этого объекта (читаемая/legacy) — переиспользуем
    existing = resolve_object_folder(v2_root, object_id, display_name)
    if existing.exists():
        return existing
    base = make_object_folder_name(display_name, object_id)
    target = objects_root / base
    if not target.exists():
        return target
    other = _read_object_id(target)
    if other == object_id:
        return target
    return objects_root / f"{base}_{object_id}"  # конфликт с другим объектом


def document_dir_in_v2(v2_root: Path, object_id: str, discipline: str,
                       document_code: str, *, display_name: Optional[str] = None) -> Path:
    obj_dir = resolve_object_folder(v2_root, object_id, display_name)
    return (
        obj_dir / "disciplines"
        / safe_component(discipline) / "documents" / safe_component(document_code)
    )


def _copy_file_tracked(src: Path, dst: Path, role: str, *, with_sha: bool = True) -> dict:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    sha = sha256_file(dst) if with_sha else None
    return {
        "old_path": str(src),
        "new_path": str(dst),
        "sha256": sha,
        "bytes": dst.stat().st_size,
        "role": role,
    }


def _run_id_for(output_dir: Path) -> str:
    """Детерминированный run_id из mtime pipeline_log.json / _output."""
    src = output_dir / "pipeline_log.json"
    if not src.exists():
        src = output_dir
    try:
        ts = datetime.fromtimestamp(src.stat().st_mtime)
        return "run_" + ts.strftime("%Y%m%dT%H%M%S")
    except Exception:
        return "run_legacy"


def _copytree_tracked(src_dir: Path, dst_dir: Path, role: str) -> list[dict]:
    """Verbatim-копия каталога. sha считаем только для артефактов-текстов."""
    files: list[dict] = []
    for src in sorted(src_dir.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(src_dir)
        dst = dst_dir / rel
        track_sha = src.suffix.lower() in _SHA_TRACK_EXT
        files.append(_copy_file_tracked(src, dst, role, with_sha=track_sha))
    return files


_CRITICAL_ANALYSIS_NAMES = ("02_text_analysis.json", "01_blocks_analysis.json", "03_findings.json")


def migrate_version(version: VersionRec, doc_dir: Path, *,
                    run_id: Optional[str] = None,
                    policy: Optional[dict] = None) -> dict:
    """Мигрирует одну версию в `documents/<code>/versions/<version_id>/`.

    Возвращает запись для old_to_new_map (files[], legacy-имена, run_id).
    Старые файлы только читаются и копируются (copy2), никогда не меняются.

    `policy` (необязательно) — переопределения для version.json
    (`analysis_status`, `analysis_generation`, `preserve_reason`), напр. для
    POLICY_READY_LEGACY_KB_PRESERVE.
    """
    vroot = doc_dir / "versions" / version.version_id
    for sub in VERSION_SUBDIRS:
        (vroot / sub).mkdir(parents=True, exist_ok=True)

    legacy_dir = version.legacy_folder
    files: list[dict] = []

    # --- 01_input: входной комплект (verbatim, оригинальные имена) ---
    quad = find_input_quad(legacy_dir)
    input_manifest_entries: list[dict] = []
    for key, src in quad.items():
        if src is None:
            input_manifest_entries.append({"role": key, "legacy_name": None, "present": False})
            continue
        dst = vroot / "01_input" / src.name
        rec = _copy_file_tracked(src, dst, role=f"input:{key}")
        files.append(rec)
        input_manifest_entries.append({
            "role": key,
            "legacy_name": src.name,
            "present": True,
            "sha256": rec["sha256"],
            "bytes": rec["bytes"],
        })

    # project_info.json — тоже вход (конфиг). Кладём в 01_input.
    pinfo = legacy_dir / "project_info.json"
    if pinfo.exists():
        rec = _copy_file_tracked(pinfo, vroot / "01_input" / "project_info.json",
                                 role="input:project_info")
        files.append(rec)

    # --- 02_work: нормализованные рабочие копии ---
    for key, src in quad.items():
        if src is None:
            continue
        rec = _copy_file_tracked(src, vroot / "02_work" / WORK_NORMALIZED[key],
                                 role=f"work:{key}")
        files.append(rec)

    # --- 03_analysis: verbatim run-копия + классифицированные копии ---
    output_dir = legacy_dir / "_output"
    rid = run_id or _run_id_for(output_dir)
    if output_dir.is_dir():
        run_dst = vroot / "03_analysis" / "runs" / rid
        files.extend(_copytree_tracked(output_dir, run_dst, role="run"))
        # классифицированные копии часто используемых артефактов (верхний уровень _output)
        for entry in sorted(output_dir.iterdir()):
            if not entry.is_file():
                continue
            bucket = classify_output_file(entry.name)
            rec = _copy_file_tracked(entry, vroot / bucket / entry.name,
                                     role=f"classified:{bucket}")
            files.append(rec)

    # analysis_status из реально перенесённых критичных артефактов (latest)
    latest_dir = vroot / "03_analysis" / "latest"
    present_crit = [c for c in _CRITICAL_ANALYSIS_NAMES if (latest_dir / c).exists()]
    if len(present_crit) == len(_CRITICAL_ANALYSIS_NAMES):
        analysis_status = "complete"
    elif present_crit:
        analysis_status = "partial"
    else:
        analysis_status = "none"
    missing_analysis = [c for c in _CRITICAL_ANALYSIS_NAMES if c not in present_crit]

    version_json = {
        "schema_version": 1,
        "version_id": version.version_id,
        "version_no": version.version_no,
        "label": version.label,
        "legacy_folder_name": version.legacy_name,
        "legacy_folder_path": str(legacy_dir),
        "source": version.source,
        "status": version.status,
        "analysis_run_id": rid if output_dir.is_dir() else None,
        "analysis_status": analysis_status,
        "missing_analysis_files": missing_analysis,
        "migrated_at": utc_now_iso(),
    }
    # policy-переопределения (напр. legacy_kb_preserve)
    if policy:
        for k in ("analysis_status", "analysis_generation", "preserve_reason"):
            if policy.get(k) is not None:
                version_json[k] = policy[k]
    (vroot / "version.json").write_text(
        json.dumps(version_json, ensure_ascii=False, indent=2), encoding="utf-8")

    # optional входные файлы, которых нет (ocr_html — опциональный)
    missing_optional = [k for k in ("ocr_html",) if quad.get(k) is None]
    input_manifest = {
        "schema_version": 1,
        "version_id": version.version_id,
        "legacy_folder_name": version.legacy_name,
        "input_quad": input_manifest_entries,
        "missing_optional_files": missing_optional,
        "note": "Файлы в 01_input неизменяемы. Рабочие копии — в 02_work/.",
    }
    (vroot / "01_input" / "input_manifest.json").write_text(
        json.dumps(input_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "version_id": version.version_id,
        "version_no": version.version_no,
        "legacy_folder_name": version.legacy_name,
        "legacy_folder_path": str(legacy_dir),
        "analysis_run_id": version_json["analysis_run_id"],
        "files": files,
    }


def migrate_project(project_path: Path, v2_root: Path, *,
                    objects_map: Optional[dict] = None,
                    run_id: Optional[str] = None,
                    policy: Optional[dict] = None) -> dict:
    """Мигрирует один legacy-проект или контейнер `(main)` в projects_v2.

    Read-only по отношению к `projects/`. Идемпотентно перезаписывает копии
    внутри projects_v2 (legacy не трогает). `policy` пробрасывается в
    `migrate_version` (version.json overrides, напр. legacy_kb_preserve).
    """
    project_path = project_path.resolve()
    discipline = project_path.parent.name
    object_dir = project_path.parent.parent
    objects_map = objects_map if objects_map is not None else load_objects_map()
    object_id = object_id_for(object_dir, objects_map)
    document_code = document_code_for(project_path)
    display_name = object_dir.name

    # читаемая папка объекта (obj_<hash> не используется как имя)
    obj_root = allocate_object_folder(v2_root, object_id, display_name)
    folder_name = obj_root.name
    doc_dir = obj_root / "disciplines" / safe_component(discipline) / "documents" / safe_component(document_code)
    doc_dir.mkdir(parents=True, exist_ok=True)

    # object.json (на уровне читаемой папки объекта)
    obj_json_path = obj_root / "object.json"
    if not obj_json_path.exists():
        obj_json_path.parent.mkdir(parents=True, exist_ok=True)
        obj_json_path.write_text(json.dumps({
            "schema_version": 1,
            "object_id": object_id,
            "display_name": display_name,
            "folder_name": folder_name,
            "legacy_name": display_name,
            "legacy_path": str(object_dir),
            "created_at": utc_now_iso(),
        }, ensure_ascii=False, indent=2), encoding="utf-8")

    versions = enumerate_versions(project_path)
    version_records = [migrate_version(v, doc_dir, run_id=run_id, policy=policy) for v in versions]

    # current_version: latest_version_id из манифеста, иначе max version_no
    current_version = versions[-1].version_id
    if is_version_container(project_path):
        vg = read_version_group(project_path) or {}
        latest = str(vg.get("latest_version_id") or "").strip()  # 'v2'
        if latest:
            m = re.match(r"v(\d+)$", latest)
            if m:
                current_version = f"v{int(m.group(1)):03d}"

    document_json = {
        "schema_version": 1,
        "document_code": document_code,
        "object_id": object_id,
        "discipline": discipline,
        "kind": "container" if is_version_container(project_path) else "plain",
        "legacy_project_name": project_path.name,
        "legacy_project_path": str(project_path),
        "versions": [
            {
                "version_id": v.version_id,
                "version_no": v.version_no,
                "label": v.label,
                "legacy_folder_name": v.legacy_name,
            }
            for v in versions
        ],
        "current_version": current_version,
        "migrated_at": utc_now_iso(),
    }
    (doc_dir / "document.json").write_text(
        json.dumps(document_json, ensure_ascii=False, indent=2), encoding="utf-8")
    (doc_dir / "current_version.txt").write_text(current_version + "\n", encoding="utf-8")

    return {
        "object_id": object_id,
        "object_name": object_dir.name,
        "discipline": discipline,
        "document_code": document_code,
        "kind": document_json["kind"],
        "legacy_project_path": str(project_path),
        "v2_document_dir": str(doc_dir),
        "current_version": current_version,
        "versions": version_records,
        "migrated_at": utc_now_iso(),
    }
