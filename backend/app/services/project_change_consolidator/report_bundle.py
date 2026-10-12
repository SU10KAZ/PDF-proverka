"""«Классический отчёт» архивом: Excel + исходные документы обеих сторон пары.

Состав архива::

    <отчёт>.xlsx
    Было/<шифр документа>/<исходный PDF> + его MD-файлы
    Стало/<шифр документа>/...

Сторона пары — один документ или сборка из нескольких; у сборки в архив кладётся
каждый её исходный документ отдельной папкой (участники сборки резолвятся через
``project_assemblies.service._resolve_source``), а не склеенный PDF сборки.
Файлы берутся из неизменяемого ``01_input`` версии (исходные имена); если там их
нет — нормализованные копии ``02_work`` под именем шифра.
"""
from __future__ import annotations

import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any

SIDES = (("left", "Было"), ("right", "Стало"))
_UNSAFE = re.compile(r'[\\/:*?"<>|\x00-\x1f]+')


def _safe(name: str) -> str:
    cleaned = _UNSAFE.sub("_", str(name or "")).strip(" .")
    return cleaned or "документ"


def _version_files(version_dir: Path, code: str) -> list[tuple[Path, str]]:
    """(путь, имя в архиве): исходные PDF и MD версии."""
    inputs = version_dir / "01_input"
    files = sorted(
        p for p in inputs.glob("*")
        if p.is_file() and p.suffix.lower() in {".pdf", ".md"}
    ) if inputs.is_dir() else []
    if any(p.suffix.lower() == ".pdf" for p in files):
        return [(p, p.name) for p in files]
    fallback = []
    for name, suffix in (("document.pdf", ".pdf"), ("document.md", ".md")):
        path = version_dir / "02_work" / name
        if path.is_file():
            fallback.append((path, f"{_safe(code)}{suffix}"))
    return fallback


def side_documents(side: dict[str, Any]) -> list[dict[str, Any]]:
    """Документы стороны пары: [{code, version_dir}] — участники сборки или сам документ."""
    if side.get("assembly_ref"):
        origin_path = Path(str((side.get("assembly_ref") or {}).get("origin_path") or ""))
        try:
            origin = json.loads(origin_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            origin = {}
        from backend.app.services.project_assemblies import service as assemblies

        documents = []
        for source in origin.get("sources") or []:
            try:
                resolved = assemblies._resolve_source(str(source.get("source_ref")), str(source.get("object_id")))
            except Exception:  # noqa: BLE001 — a missing member is reported, not fatal
                documents.append({"code": str(source.get("document_code") or ""), "version_dir": None})
                continue
            documents.append({"code": str(resolved.get("document_code") or ""),
                              "version_dir": Path(resolved["version_dir"])})
        if documents:
            return documents
    pdf_path = Path(str(side.get("pdf_path") or ""))
    version_dir = pdf_path.parents[1] if len(pdf_path.parents) > 1 else None
    return [{"code": str(side.get("document_code") or pdf_path.stem), "version_dir": version_dir}]


def build_bundle(xlsx: bytes, xlsx_name: str, pair: dict[str, Any]) -> Path:
    """Собрать zip во временный файл и вернуть путь (удаляет вызывающий)."""
    handle = tempfile.NamedTemporaryFile(prefix="classic_report_", suffix=".zip", delete=False)
    handle.close()
    path = Path(handle.name)
    missing: list[str] = []
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(_safe(xlsx_name), xlsx, compress_type=zipfile.ZIP_DEFLATED)
        for key, folder in SIDES:
            used: set[str] = set()
            for document in side_documents(pair.get(key) or {}):
                name = _safe(document["code"])
                while name in used:
                    name += "_"
                used.add(name)
                version_dir = document["version_dir"]
                files = _version_files(version_dir, document["code"]) if version_dir and version_dir.is_dir() else []
                if not files:
                    missing.append(f"{folder}/{name}")
                for source, arcname in files:
                    # PDF уже сжат — храним как есть; MD сжимаем.
                    compress = zipfile.ZIP_STORED if source.suffix.lower() == ".pdf" else zipfile.ZIP_DEFLATED
                    archive.write(source, f"{folder}/{name}/{_safe(arcname)}", compress_type=compress)
        if missing:
            archive.writestr("НЕ_НАЙДЕНО.txt", "Не найдены файлы документов:\n" + "\n".join(missing) + "\n",
                             compress_type=zipfile.ZIP_DEFLATED)
    return path
