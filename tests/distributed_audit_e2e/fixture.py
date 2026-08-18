"""Синтетический проект в ФАКТИЧЕСКОЙ раскладке `projects_v2`.

Почему проект строится программно, а не копируется из корпуса:

  * проектная документация заказчика в Git не кладётся ни при каких условиях;
  * прогон должен быть детерминированным — реальный PDF даёт разное число
    блоков от версии PyMuPDF и разный OCR;
  * фикстура обязана быть маленькой: E2E гоняется в тестах, а не раз в месяц.

Что фикстура при этом НЕ упрощает: раскладка каталогов, имена файлов, схема
`result.json`, наличие `document.json`/`version.json` и `01_input`/`02_work` —
всё как в корпусе, потому что именно по ним резолвятся пути (`resolve_v2_target`,
`resolve_v2_source_files`) и именно на них падает удалённый прогон.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

#: Unicode-код проекта из задания. Кириллица, пробелы, тире и «/» — легальная
#: часть кода, и она обязана переживать весь путь до воркера и обратно.
DEFAULT_EXTERNAL_ID = "ТЕСТ/РД-АР1 — корпус 1"

#: Код документа = имя каталога `documents/<code>`. Он же `project_id` для
#: конвейера: `resolve_v2_target_by_id` берёт `basename(project_id)`.
DEFAULT_DOCUMENT_CODE = "ТЕСТ-РД-АР1-К1"

DEFAULT_OBJECT_FOLDER = "E2E_ОБЪЕКТ"
DEFAULT_OBJECT_ID = "obj-e2e-0001"
DEFAULT_DISCIPLINE = "АР"
DEFAULT_VERSION_ID = "v001"

#: Дисциплина в АВТОРИТЕТНЫХ метаданных (`project_info.section`). Она НЕ обязана
#: совпадать с именем физического каталога `disciplines/<Д>`: имя каталога
#: приходит из legacy-раскладки и совпадает с дисциплиной только по соглашению.
#: Фикстура разводит их намеренно — иначе «дисциплина взята из метаданных» и
#: «дисциплина угадана по имени папки» неразличимы.
DEFAULT_SECTION = "АР"


@dataclass(frozen=True)
class ProjectFixture:
    """Пути готовой фикстуры. Всё остальное выводится из них."""

    v2_root: Path
    doc_dir: Path
    version_dir: Path
    document_code: str
    external_id: str
    object_id: str
    object_folder: str
    discipline: str
    version_id: str
    section: str = ""

    @property
    def project_id(self) -> str:
        return self.document_code


# ─── Содержимое исходников ───────────────────────────────────────────────────
_MD_TEMPLATE = """# {code} — раздел {discipline}

## Лист 1. Общие данные

Проект выполнен на основании задания на проектирование.
Расчётная мощность щита ЩР-1 составляет 12,5 кВт.
Кабель питающей линии — ВВГнг(А)-LS 5х6.

[IMAGE] Схема электрическая принципиальная ЩР-1

## Лист 2. План расстановки оборудования

Отметка чистого пола +0,000.
Высота установки розеток — 0,3 м от уровня чистого пола.
Аппарат защиты вводной — автоматический выключатель 25 А.

[IMAGE] План расстановки оборудования, отм. +0,000

## Контрольный фрагмент 11C

Насос P-1 узла ввода: проектный расход 10 м3/ч, напор 32 м.
Спецификация оборудования, позиция 4: насос P-1, расход 12 м3/ч.
"""
#: Раздел «Контрольный фрагмент 11C» добавлен для этапа `provider_selfcheck` и
#: существует ровно ради него. Числа расходятся НАМЕРЕННО: модель обязана найти
#: противоречие 10 против 12, и это единственная содержательная работа, которую
#: синтетическое задание от неё требует. Фрагмент маленький сознательно —
#: расход подписки на проверку канала должен быть минимальным.


def _pdf_bytes(code: str, discipline: str) -> bytes:
    """Настоящий двухстраничный PDF с вектор-слоем.

    Вектор-слой существенен: без него `crop_blocks` и профили Вектографа
    работают по другой ветке, и прогон перестал бы быть похожим на боевой.
    """
    import fitz  # PyMuPDF

    doc = fitz.open()
    for page_no, title in (
        (1, "Лист 1. Общие данные"),
        (2, "Лист 2. План расстановки оборудования"),
    ):
        page = doc.new_page(width=842, height=595)          # A4 альбомный
        page.draw_rect(fitz.Rect(20, 20, 822, 575), width=1.5)
        page.insert_text((40, 60), f"{code} — {discipline}", fontsize=16)
        page.insert_text((40, 90), title, fontsize=12)
        # Рамка «графического блока» — то, что станет image-блоком.
        page.draw_rect(fitz.Rect(60, 130, 520, 430), width=1.0)
        page.insert_text((70, 160), "ЩР-1", fontsize=11)
        page.insert_text((70, 185), "QF1 25A", fontsize=9)
        page.insert_text((70, 205), "ВВГнг(А)-LS 5х6", fontsize=9)
        for i in range(6):
            y = 230 + i * 30
            page.draw_line(fitz.Point(70, y), fitz.Point(500, y), width=0.6)
        # Штамп.
        page.draw_rect(fitz.Rect(560, 430, 810, 560), width=1.0)
        page.insert_text((570, 455), f"Лист {page_no}", fontsize=10)
        page.insert_text((570, 480), code, fontsize=8)
    payload = doc.tobytes()
    doc.close()
    return payload


def _result_json(code: str) -> dict[str, Any]:
    """`result.json` в схеме, которую читает `build_document_graph_v2`."""
    pages = []
    for page_no, sheet_name in (
        (1, "Общие данные"),
        (2, "План расстановки оборудования"),
    ):
        blocks: list[dict[str, Any]] = [
            {
                "id": f"E2E-T{page_no}-01",
                "block_type": "text",
                "source": "pdf_text",
                "coords_px": [40, 40, 800, 120],
                "coords_norm": [0.047, 0.067, 0.950, 0.202],
                "ocr_text": (
                    f"{code} — лист {page_no}. {sheet_name}. "
                    "Расчётная мощность щита ЩР-1 составляет 12,5 кВт."
                ),
            },
            {
                "id": f"E2E-I{page_no}-01",
                "block_type": "image",
                "source": "pdf_vector",
                "coords_px": [60, 130, 520, 430],
                "coords_norm": [0.071, 0.218, 0.617, 0.723],
                "ocr_text": "Схема электрическая принципиальная ЩР-1",
            },
            {
                "id": f"E2E-S{page_no}-01",
                "block_type": "text",
                "source": "pdf_text",
                "coords_px": [560, 430, 810, 560],
                "coords_norm": [0.665, 0.723, 0.962, 0.941],
                "ocr_text": f"Лист {page_no}. {code}",
                "stamp_data": {
                    "sheet_no": str(page_no),
                    "sheet_name": sheet_name,
                    "confidence": 0.9,
                },
            },
        ]
        pages.append(
            {
                "page_number": page_no,
                "width": 842,
                "height": 595,
                "blocks": blocks,
            }
        )
    return {"document_id": code, "pages": pages}


def _blocks_json(code: str) -> dict[str, Any]:
    """Геометрия блоков нового трёхфайлового комплекта портала.

    `crop_url` намеренно указывает на несуществующий хост: кроп обязан идти по
    локальному PDF. Если однажды кто-то включит сетевой путь, прогон это
    заметит — тестовый сетевой guard прибьёт процесс (E2E-08).
    """
    blocks = []
    for page_no in (1, 2):
        blocks.append(
            {
                "block_id": f"E2E-I{page_no}-01",
                "page": page_no,
                "block_type": "image",
                "coords_norm": [0.071, 0.218, 0.617, 0.723],
                "crop_url": "https://invalid.e2e.local/never-fetched.png",
            }
        )
    return {"document_id": code, "blocks": blocks}


# ─── Сборка дерева ───────────────────────────────────────────────────────────
def _write(path: Path, data: bytes | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(data, encoding="utf-8")
    return path


def _write_json(path: Path, payload: Any) -> Path:
    return _write(path, json.dumps(payload, ensure_ascii=False, indent=2))


def build_project_fixture(
    v2_root: Path,
    *,
    document_code: str = DEFAULT_DOCUMENT_CODE,
    external_id: str = DEFAULT_EXTERNAL_ID,
    object_folder: str = DEFAULT_OBJECT_FOLDER,
    object_id: str = DEFAULT_OBJECT_ID,
    discipline: str = DEFAULT_DISCIPLINE,
    version_id: str = DEFAULT_VERSION_ID,
    section: Optional[str] = None,
) -> ProjectFixture:
    """Создать проект `projects_v2` целиком, без единого реального документа.

    `discipline` — имя ФИЗИЧЕСКОГО каталога раздела, `section` — дисциплина в
    метаданных. Разные параметры намеренно: авторитетным источником является
    второе, и тест обязан уметь их развести.
    """
    v2_root = Path(v2_root)
    section = section if section is not None else discipline
    doc_dir = (
        v2_root / "objects" / object_folder / "disciplines" / discipline
        / "documents" / document_code
    )
    version_dir = doc_dir / "versions" / version_id

    pdf = _pdf_bytes(document_code, section)
    md = _MD_TEMPLATE.format(code=document_code, discipline=section)
    result = _result_json(document_code)
    blocks = _blocks_json(document_code)

    # 01_input — неизменяемый исходник портала.
    _write(version_dir / "01_input" / f"{document_code}.pdf", pdf)
    _write(version_dir / "01_input" / f"{document_code}_document.md", md)
    _write_json(version_dir / "01_input" / f"{document_code}_result.json", result)
    _write_json(version_dir / "01_input" / f"{document_code}_blocks.json", blocks)
    _write_json(
        version_dir / "01_input" / "project_info.json",
        {
            "project_id": document_code,
            "name": document_code,
            "section": section,
            "description": "Синтетический проект E2E-стенда",
            "pdf_file": f"{document_code}.pdf",
            "md_file": f"{document_code}_document.md",
            "external_id": external_id,
        },
    )
    _write_json(
        version_dir / "01_input" / "input_manifest.json",
        {
            "schema_version": 1,
            "files": [
                {
                    "name": f"{document_code}.pdf",
                    "sha256": hashlib.sha256(pdf).hexdigest(),
                    "bytes": len(pdf),
                },
                {
                    "name": f"{document_code}_document.md",
                    "sha256": hashlib.sha256(md.encode("utf-8")).hexdigest(),
                    "bytes": len(md.encode("utf-8")),
                },
            ],
        },
    )

    # 02_work — нормализованные рабочие копии (их и читает конвейер).
    _write(version_dir / "02_work" / "document.pdf", pdf)
    _write(version_dir / "02_work" / "document.md", md)
    _write_json(version_dir / "02_work" / "result.json", result)
    _write_json(version_dir / "02_work" / "blocks.json", blocks)

    # Метаданные версии и документа.
    _write_json(
        version_dir / "version.json",
        {
            "schema_version": 1,
            "version_id": version_id,
            "version_no": 1,
            "document_code": document_code,
            "created_at": 0.0,
            "project_info": {
                "project_id": document_code,
                "section": section,
                "external_id": external_id,
            },
        },
    )
    (version_dir / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (version_dir / "03_analysis" / "runs").mkdir(parents=True, exist_ok=True)
    (version_dir / "99_service").mkdir(parents=True, exist_ok=True)
    (version_dir / "04_review").mkdir(parents=True, exist_ok=True)
    (version_dir / "05_export").mkdir(parents=True, exist_ok=True)

    _write_json(
        doc_dir / "document.json",
        {
            "schema_version": 1,
            "document_code": document_code,
            "object_id": object_id,
            "discipline": section,
            "external_id": external_id,
            "current_version": version_id,
            "versions": [{"version_id": version_id, "version_no": 1}],
        },
    )
    _write(doc_dir / "current_version.txt", version_id)

    _write_json(
        v2_root / "objects" / object_folder / "object.json",
        {"schema_version": 1, "object_id": object_id, "name": object_folder},
    )
    return ProjectFixture(
        v2_root=v2_root,
        doc_dir=doc_dir,
        version_dir=version_dir,
        document_code=document_code,
        external_id=external_id,
        object_id=object_id,
        object_folder=object_folder,
        discipline=discipline,
        version_id=version_id,
        section=section,
    )


def clone_fixture(source: ProjectFixture, target_root: Path) -> ProjectFixture:
    """Независимая копия фикстуры — для `local_case` и `remote_case`.

    Копия, а не повторная генерация: два случая обязаны стартовать с БАЙТОВО
    одинакового дерева, иначе семантическое сравнение сравнивает не то.
    """
    target_root = Path(target_root)
    target_root.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source.v2_root, target_root, dirs_exist_ok=True)
    return ProjectFixture(
        v2_root=target_root,
        doc_dir=target_root / source.doc_dir.relative_to(source.v2_root),
        version_dir=target_root / source.version_dir.relative_to(source.v2_root),
        document_code=source.document_code,
        external_id=source.external_id,
        object_id=source.object_id,
        object_folder=source.object_folder,
        discipline=source.discipline,
        version_id=source.version_id,
        section=source.section,
    )


# ─── Контроль неизменности исходников ────────────────────────────────────────
#: Что обязано остаться байтово тем же после аудита. Это и есть E2E-17.
SOURCE_GUARDED_DIRS: tuple[str, ...] = ("01_input", "02_work")
SOURCE_GUARDED_FILES: tuple[str, ...] = ("version.json",)


def source_tree_hash(version_dir: Path) -> str:
    """SHA-256 исходной части дерева версии.

    Считается по отсортированному списку `(относительный путь, sha256)` — то
    есть меняется и от содержимого, и от появления/исчезновения файла.
    """
    version_dir = Path(version_dir)
    digest = hashlib.sha256()
    entries: list[tuple[str, str]] = []
    for sub in SOURCE_GUARDED_DIRS:
        base = version_dir / sub
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(version_dir).as_posix()
            entries.append((rel, _sha256_file(path)))
    for name in SOURCE_GUARDED_FILES:
        path = version_dir / name
        if path.is_file():
            entries.append((name, _sha256_file(path)))
    for rel, sha in sorted(entries):
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update(sha.encode("ascii"))
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prompts_snapshot_dir(repo_root: Path, target: Path) -> Path:
    """Скопировать реальные промпты проекта в изолированный каталог.

    Промпты берутся настоящие: подменить их значило бы проверять не тот
    конвейер. Копия нужна, чтобы прогон не зависел от правок промптов из UI во
    время теста.
    """
    source = Path(repo_root) / "prompts"
    target = Path(target)
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        target.mkdir(parents=True, exist_ok=True)
    return target


def stage_models_snapshot(
    target: Path,
    *,
    claude_model: str = "claude-opus-5",
    stage01_model: str = "codex/gpt-5.4",
) -> Path:
    """Детерминированный `stage_models.json` для прогона.

    Файл вне git, и без него прогон уходит на дефолты кода — а дефолт этапа
    `block_batch` (`ensemble/gpt-codex`) содержит ногу, которая ходит в
    OpenRouter по HTTPS и подделкой CLI не закрывается вовсе.

    Поэтому Stage 01 ставится на `codex/…`: это CLI-путь, то есть ровно тот,
    который перекрывается поддельным бинарём. Значение обязано быть из набора,
    который Stage 01 считает совместимым с `findings_only`
    (`runner.py: findings_only_compatible`), иначе он молча вернётся к
    платному дефолту и напишет об этом только в лог.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "text_analysis": claude_model,
        "block_batch": stage01_model,
        "findings_merge": claude_model,
        "findings_critic": claude_model,
        "findings_corrector": claude_model,
        "norm_verify": claude_model,
        "norm_fix": claude_model,
        "norm_requote": claude_model,
        "optimization": claude_model,
        "optimization_critic": claude_model,
        "optimization_corrector": claude_model,
    }
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target
