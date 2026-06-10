"""
md_resolver.py
--------------
Version-aware, defensive разрешение MD-файла проекта (источник текста для
text_analysis). Чистый filesystem-слой без сети — полностью покрывается
unit-тестами на временных директориях.

Контекст бага:
* queue items имели `version_id=None`, при этом `get_version_dir(None)` отдаёт
  ПАПКУ АКТУАЛЬНОЙ версии (latest_version_id), а не V1. Для части V2-проектов
  MD лежит только в V1/root, а в папке актуальной версии его нет → «MD не найден».
* У АР1.2-К6 битый project_info: `md_file=None`, `pdf_file` указывает на ДРУГОЙ
  проект (АР1.1-К6). Старый поиск, опираясь на имя PDF, мог искать не тот файл.

Принципы резолвера:
* искать `*_document.md` в ПАПКЕ АКТУАЛЬНОЙ версии (передаётся вызывающим);
* НЕ выводить имя MD из `pdf_file` (он может указывать на соседний проект);
* `project_info.md_file` использовать ТОЛЬКО если такой файл реально существует;
* при нескольких кандидатах — не угадывать вслепую: попытаться сматчить по
  basename проекта, иначе вернуть `ambiguous_md_candidates`;
* при отсутствии — `md_not_found` с диагностикой (что искали, есть ли MD в
  V1/root, какой latest_version_id).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

STATUS_OK = "ok"
STATUS_AMBIGUOUS = "ambiguous_md_candidates"
STATUS_NOT_FOUND = "md_not_found"

_DOC_SUFFIX = "_document.md"
# Служебные .md, которые не являются исходным текстом проекта.
_EXCLUDE_PREFIXES = ("audit_", "readme", "claude", "_combined")


@dataclass
class MdResolution:
    status: str                                   # ok | ambiguous_md_candidates | md_not_found
    md_name: Optional[str] = None                 # выбранный файл (только при ok)
    md_path: Optional[Path] = None
    searched_dir: Optional[Path] = None
    candidates: list[str] = field(default_factory=list)
    root_candidates: list[str] = field(default_factory=list)  # MD в V1/root, если отличается
    project_info_md_file: Optional[str] = None
    project_info_md_file_exists: bool = False
    pdf_file: Optional[str] = None
    pdf_file_mismatch: bool = False               # pdf_file похоже указывает на другой проект
    latest_version_id: Optional[str] = None
    diagnostics: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == STATUS_OK

    def error_message(self, project_id: str) -> str:
        if self.status == STATUS_NOT_FOUND:
            hint = ""
            if self.root_candidates:
                hint = (
                    f" MD найден в корне/V1 ({', '.join(self.root_candidates)}), но НЕ в папке "
                    f"актуальной версии — данные версии неполные."
                )
            return (
                f"md_not_found: MD-файл (*_document.md) не найден для проекта {project_id} "
                f"в {self.searched_dir} (latest_version_id={self.latest_version_id})."
                f"{hint} Анализ без MD не поддерживается."
            )
        if self.status == STATUS_AMBIGUOUS:
            return (
                f"ambiguous_md_candidates: для проекта {project_id} в {self.searched_dir} "
                f"найдено несколько *_document.md ({', '.join(self.candidates)}), "
                f"однозначно сопоставить нельзя — укажите md_file явно."
            )
        return ""


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "").replace("ё", "е").replace("Ё", "Е")
    s = s.lower()
    s = re.sub(r"[^0-9a-zа-я]+", "", s)
    return s


def _is_doc_md(name: str) -> bool:
    low = name.lower()
    if not low.endswith(_DOC_SUFFIX):
        return False
    if low.startswith(_EXCLUDE_PREFIXES):
        return False
    return True


def _list_doc_md(d: Path) -> list[str]:
    if not d or not d.is_dir():
        return []
    out = []
    for f in sorted(d.iterdir()):
        if f.is_file() and _is_doc_md(f.name):
            out.append(f.name)
    return out


def _stem_no_doc(name: str) -> str:
    """Имя MD без хвоста `_document.md`."""
    if name.lower().endswith(_DOC_SUFFIX):
        return name[: -len(_DOC_SUFFIX)]
    return name


def _pdf_references_other_project(pdf_file: Optional[str], project_id: str) -> bool:
    """True, если pdf_file явно НЕ относится к этому проекту.

    Консервативно: считаем mismatch только когда у pdf_file есть осмысленный
    стем, и нормализованный basename проекта НЕ входит в нормализованный стем
    pdf_file (и наоборот). Пустой/`document.pdf` не считается mismatch.
    """
    if not pdf_file:
        return False
    stem = Path(pdf_file).stem
    if not stem or stem.lower() in ("document", ""):
        return False
    pid_base = Path(project_id).name
    a = _norm(stem)
    b = _norm(pid_base)
    if not a or not b:
        return False
    return (b not in a) and (a not in b)


def resolve_project_md(
    version_dir: Path,
    project_id: str,
    *,
    project_info: Optional[dict] = None,
    root_dir: Optional[Path] = None,
    latest_version_id: Optional[str] = None,
) -> MdResolution:
    """Найти MD-файл проекта в папке актуальной версии.

    Args:
        version_dir: папка АКТИВНОЙ версии (где должен лежать MD аудита).
        project_id: id проекта (для диагностики и disambiguation по basename).
        project_info: загруженный project_info.json (опц.) — md_file/pdf_file.
        root_dir: V1/root проекта (опц.) — для cross-version подсказки в диагностике.
        latest_version_id: для диагностики.
    """
    version_dir = Path(version_dir)
    info = project_info or {}
    pi_md = info.get("md_file")
    pdf_file = info.get("pdf_file")

    res = MdResolution(
        status=STATUS_NOT_FOUND,
        searched_dir=version_dir,
        project_info_md_file=pi_md,
        pdf_file=pdf_file,
        latest_version_id=latest_version_id,
    )
    res.pdf_file_mismatch = _pdf_references_other_project(pdf_file, project_id)

    candidates = _list_doc_md(version_dir)
    res.candidates = candidates

    # cross-version подсказка: что есть в V1/root
    if root_dir is not None and Path(root_dir) != version_dir:
        res.root_candidates = _list_doc_md(Path(root_dir))

    # 1) Явный md_file из project_info — только если реально существует в version_dir.
    if pi_md:
        pi_path = version_dir / pi_md
        exists = pi_path.is_file() and _is_doc_md(pi_md)
        res.project_info_md_file_exists = exists
        if exists:
            res.status = STATUS_OK
            res.md_name = pi_md
            res.md_path = pi_path
            res.diagnostics["selected_by"] = "project_info.md_file"
            return res
        else:
            res.diagnostics["project_info_md_file_stale"] = True

    # 2) Единственный кандидат в папке версии.
    if len(candidates) == 1:
        res.status = STATUS_OK
        res.md_name = candidates[0]
        res.md_path = version_dir / candidates[0]
        res.diagnostics["selected_by"] = "single_candidate"
        return res

    # 3) Несколько кандидатов — пытаемся сматчить по basename проекта.
    if len(candidates) > 1:
        pid_norm = _norm(Path(project_id).name)
        matched = [c for c in candidates if pid_norm and pid_norm in _norm(_stem_no_doc(c))]
        if len(matched) == 1:
            res.status = STATUS_OK
            res.md_name = matched[0]
            res.md_path = version_dir / matched[0]
            res.diagnostics["selected_by"] = "basename_match"
            return res
        # неоднозначно — не угадываем
        res.status = STATUS_AMBIGUOUS
        res.diagnostics["matched_by_basename"] = matched
        return res

    # 4) Кандидатов нет.
    res.status = STATUS_NOT_FOUND
    return res


@dataclass
class RepairPlan:
    """План безопасного ремонта project_info.json (только md_file; pdf_file —
    только репорт). Применяется ТОЛЬКО явно (--apply) с backup."""
    project_id: str
    version_dir: Path
    md_status: str
    needs_repair: bool = False
    set_md_file: Optional[str] = None             # предлагаемое значение md_file (или None)
    current_md_file: Optional[str] = None
    current_pdf_file: Optional[str] = None
    pdf_file_mismatch: bool = False
    local_pdfs: list[str] = field(default_factory=list)
    candidates: list[str] = field(default_factory=list)
    root_candidates: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    diagnostics: dict = field(default_factory=dict)


def plan_project_info_repair(
    version_dir: Path,
    project_id: str,
    project_info: Optional[dict] = None,
    *,
    root_dir: Optional[Path] = None,
    latest_version_id: Optional[str] = None,
) -> RepairPlan:
    """Построить безопасный план ремонта project_info.json.

    Что чинит (только при --apply, с backup):
      * `md_file` — если он пустой/устарел, а MD однозначно разрешается → проставить.
    Что только РЕПОРТит (не трогает автоматически):
      * `pdf_file`, указывающий на другой проект (нельзя угадать правильный PDF);
      * ambiguous_md_candidates / md_not_found.
    НИКОГДА не трогает артефакты 01/02/03/04.
    """
    version_dir = Path(version_dir)
    info = project_info or {}
    res = resolve_project_md(
        version_dir, project_id,
        project_info=info, root_dir=root_dir, latest_version_id=latest_version_id,
    )
    cur_md = info.get("md_file")
    cur_pdf = info.get("pdf_file")
    local_pdfs = (
        sorted(f.name for f in version_dir.glob("*.pdf")) if version_dir.is_dir() else []
    )

    plan = RepairPlan(
        project_id=project_id,
        version_dir=version_dir,
        md_status=res.status,
        current_md_file=cur_md,
        current_pdf_file=cur_pdf,
        pdf_file_mismatch=res.pdf_file_mismatch,
        local_pdfs=local_pdfs,
        candidates=res.candidates,
        root_candidates=res.root_candidates,
        diagnostics=dict(res.diagnostics),
    )

    if res.status == STATUS_OK:
        if cur_md != res.md_name:
            plan.set_md_file = res.md_name
            plan.actions.append(f"set md_file: {cur_md!r} → {res.md_name!r}")
    elif res.status == STATUS_AMBIGUOUS:
        plan.actions.append(
            f"AMBIGUOUS: несколько MD {res.candidates} — выбрать вручную (не угадываем)"
        )
    else:  # not_found
        msg = f"MD_NOT_FOUND в {version_dir}"
        if res.root_candidates:
            msg += f"; есть в корне/V1: {res.root_candidates} (данные версии неполные)"
        plan.actions.append(msg)

    if res.pdf_file_mismatch:
        suffix = f"; локальные PDF: {local_pdfs}" if local_pdfs else ""
        plan.actions.append(
            f"pdf_file {cur_pdf!r} похоже относится к ДРУГОМУ проекту{suffix} "
            f"(репорт; авто-фикс не выполняется)"
        )

    plan.needs_repair = bool(plan.actions)
    return plan
