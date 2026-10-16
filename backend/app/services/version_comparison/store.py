"""Каталог сравнения версий объекта: проекты, версии, порядок, метки, загрузки.

Порядок версий задаёт пользователь. Номер в имени файла, дата выпуска и
«Изм.» из штампов показываются только как подсказки (план §3.1 п. 2).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import tempfile
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO

import fitz

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from . import paths, pdfio, stamp

CATALOG_SCHEMA = "version_comparison.catalog/v1"
MAX_ZIP_MEMBERS = 500


class CatalogError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return prefix + secrets.token_hex(5)


def _empty(object_id: str) -> dict[str, Any]:
    return {"schema": CATALOG_SCHEMA, "object_id": object_id, "projects": [], "versions": {}}


def load_catalog(object_id: str) -> dict[str, Any]:
    path = paths.catalog_path(object_id)
    if not path.exists():
        return _empty(object_id)
    return json.loads(path.read_text(encoding="utf-8"))


def _modify(object_id: str, fn) -> dict[str, Any]:
    return load_modify_save(paths.catalog_path(object_id), fn, default=_empty(object_id))


def project(catalog: dict[str, Any], project_id: str) -> dict[str, Any]:
    for p in catalog["projects"]:
        if p["project_id"] == project_id:
            return p
    raise CatalogError(f"проект {project_id} не найден")


def project_versions(catalog: dict[str, Any], project_id: str) -> list[dict[str, Any]]:
    return [catalog["versions"][vid] for vid in project(catalog, project_id)["versions"]]


# --------------------------------------------------------------- распознавание

_FILE_NUMBER = re.compile(r"\((\d{1,3})\)\s*$")
_IZM_IN_NAME = re.compile(r"изм\.?\s*(\d{1,2})", re.I)


def _name_hint(file_name: str) -> dict[str, Any]:
    stem = Path(file_name).stem
    m = _FILE_NUMBER.search(stem)
    izm = _IZM_IN_NAME.search(stem)
    base = _FILE_NUMBER.sub("", stem)
    base = re.sub(r"[\s_]*(изм\.?\s*\d+|v\d+|ред\.?\s*\d+)\s*$", "", base, flags=re.I).strip(" _-")
    return {"file_number": int(m.group(1)) if m else None, "izm_in_name": int(izm.group(1)) if izm else None,
            "name_base": base}


# Шифр основного комплекта должен стоять хотя бы на такой доле листов, чтобы
# листы приложений («….СО», «…-ОЛ», «….КЖ1») сводились к нему.
_PARENT_SHARE = 0.2


def document_designation(designations: Counter) -> str:
    """Полный шифр документа вместе с маркой («133/23-ГК-ИТП.ТМ») по шифрам штампов листов.

    Последний «.ХХ» не срезается вслепую: так ИТП.АК, ИТП.ТМ и ИТП.ЭМ слипались
    в один «133/23-ГК-ИТП». Лист приложения засчитывается основному комплекту,
    только если тот сам стоит в штампах (у ЭМ2 листов «ЭМ2.СО» больше, чем «ЭМ2»).
    """
    total = sum(designations.values())
    parents = [d for d, n in designations.items() if n >= total * _PARENT_SHARE]
    roots: Counter = Counter()
    for d, n in designations.items():
        roots[min((p for p in parents if d.startswith(p) and d[len(p):len(p) + 1] in (".", "-")),
                  key=len, default=d)] += n
    return roots.most_common(1)[0][0] if roots else ""


def detect(pdf_path: Path) -> dict[str, Any]:
    """Быстрые подсказки по файлу: обозначение, «Изм.» из штампов, даты выпуска."""
    doc = pdfio.open_memory(pdfio.read_bytes(pdf_path))
    designations: Counter = Counter()
    izm: list[int] = []
    dates: set[str] = set()
    for page in doc:
        rect = page.rect
        corner = fitz.Rect(rect.x1 - 205 * stamp.MM, rect.y1 - 70 * stamp.MM, rect.x1, rect.y1) * page.derotation_matrix
        words = []
        m = page.rotation_matrix
        for w in page.get_text("words", clip=corner):
            words.append(pdfio.to_display(page, w[:4], m) + [w[4]])
        st = stamp.parse(words, rect.width, rect.height)
        if st.get("designation"):
            designations[st["designation"]] += 1
        if st.get("izm_max") is not None:
            izm.append(st["izm_max"])
        for rel in stamp.release_blocks([list(b) for b in page.get_text("blocks")]):
            if rel["date"]:
                dates.add(rel["date"])
    return {
        "page_count": len(doc),
        "designation": document_designation(designations),
        "izm_max": max(izm) if izm else None,
        "release_dates": sorted(dates, key=lambda d: (d[6:], d[3:5], d[:2])),
    }


# --------------------------------------------------------------------- загрузка

def create_upload(object_id: str) -> str:
    uid = _new_id("u")
    d = paths.upload_dir(object_id, uid)
    (d / "files").mkdir(parents=True, exist_ok=True)
    atomic_write_json(d / "upload.json", {"upload_id": uid, "created_at": _now(), "files": []})
    return uid


def _register_file(object_id: str, uid: str, src: Path, file_name: str, relative_path: str) -> dict[str, Any]:
    d = paths.upload_dir(object_id, uid)
    h = hashlib.sha256()
    with open(src, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    sha = h.hexdigest()
    key = _new_id("f")
    dst = d / "files" / f"{key}.pdf"
    shutil.move(str(src), dst)
    entry: dict[str, Any] = {"file_key": key, "file_name": file_name, "relative_path": relative_path,
                             "sha256": sha, "size": dst.stat().st_size}
    try:
        entry["detected"] = detect(dst) | _name_hint(file_name)
    except Exception as exc:
        entry["error"] = f"PDF не читается: {exc}"
    def add(data):
        data["files"].append(entry)
        return data
    load_modify_save(d / "upload.json", add, default={"upload_id": uid, "files": []})
    return entry


def add_file(object_id: str, uid: str, file_name: str, relative_path: str, stream: BinaryIO) -> list[dict[str, Any]]:
    """Принять один файл загрузки: PDF или ZIP с PDF. Исходник не изменяется."""
    d = paths.upload_dir(object_id, uid)
    if not (d / "upload.json").exists():
        raise CatalogError("загрузка не найдена")
    safe_name = Path(file_name).name
    with tempfile.NamedTemporaryFile(dir=d, delete=False, suffix=".part") as tmp:
        shutil.copyfileobj(stream, tmp, 1 << 20)
        tmp_path = Path(tmp.name)
    if safe_name.lower().endswith(".zip"):
        out = []
        try:
            with zipfile.ZipFile(tmp_path) as zf:
                members = [m for m in zf.infolist() if not m.is_dir() and m.filename.lower().endswith(".pdf")]
                if len(members) > MAX_ZIP_MEMBERS:
                    raise CatalogError("слишком много файлов в архиве")
                for member in members:
                    with zf.open(member) as src, tempfile.NamedTemporaryFile(dir=d, delete=False, suffix=".part") as t2:
                        shutil.copyfileobj(src, t2, 1 << 20)
                    out.append(_register_file(object_id, uid, Path(t2.name), Path(member.filename).name,
                                              f"{relative_path or safe_name}/{member.filename}"))
        finally:
            tmp_path.unlink(missing_ok=True)
        return out
    if not safe_name.lower().endswith(".pdf"):
        tmp_path.unlink(missing_ok=True)
        raise CatalogError("принимаются только PDF и ZIP")
    return [_register_file(object_id, uid, tmp_path, safe_name, relative_path or safe_name)]


def _date_key(d: str) -> tuple:
    return (d[6:], d[3:5], d[:2]) if d else ("",)


def proposal(object_id: str, uid: str) -> dict[str, Any]:
    """Предложение группировки и порядка. Ничего не применяет."""
    up = json.loads((paths.upload_dir(object_id, uid) / "upload.json").read_text(encoding="utf-8"))
    catalog = load_catalog(object_id)
    groups: dict[str, list[dict]] = {}
    for f in up["files"]:
        det = f.get("detected") or {}
        key = det.get("designation") or det.get("name_base") or Path(f["file_name"]).stem
        groups.setdefault(key, []).append(f)
    out = []
    for key, files in groups.items():
        def order_key(f):
            det = f.get("detected") or {}
            last_date = (det.get("release_dates") or [""])[-1]
            num = det.get("file_number")
            return (det.get("izm_max") if det.get("izm_max") is not None else -1,
                    _date_key(last_date), num if num is not None else 10 ** 6, f["file_name"])
        files = sorted(files, key=order_key)
        warnings = []
        nums = [(f.get("detected") or {}).get("file_number") for f in files]
        if all(n is not None for n in nums) and nums != sorted(nums):
            warnings.append("порядок по «Изм.» и датам выпуска не совпадает с номерами в именах файлов — проверьте")
        for a, b in zip(files, files[1:]):
            ia, ib = (a.get("detected") or {}).get("izm_max"), (b.get("detected") or {}).get("izm_max")
            if ia is not None and ia == ib:
                warnings.append(f"«{b['file_name']}»: тот же Изм.{ib}, что у «{a['file_name']}» — проверьте порядок")
        existing = next((p for p in catalog["projects"] if p.get("code") and p["code"] == key), None)
        out.append({
            "key": key, "title": key, "code": key if (files[0].get("detected") or {}).get("designation") else "",
            "existing_project_id": existing["project_id"] if existing else None,
            "files": [{"file_key": f["file_key"], "file_name": f["file_name"], "sha256": f["sha256"],
                       "detected": f.get("detected"), "error": f.get("error"),
                       "suggested_label": _suggest_label(f)} for f in files],
            "warnings": warnings,
        })
    return {"upload_id": uid, "groups": out,
            "note": "Номер в имени файла, «Изм.» и дата выпуска — подсказки; порядок версий задаёте вы."}


def _suggest_label(f: dict[str, Any]) -> str:
    det = f.get("detected") or {}
    number = det.get("file_number")
    parts = [f"V{int(number) + 1 if number is not None else 1}"]
    if det.get("izm_max") is not None:
        parts.append(f"Изм.{det['izm_max']}")
    if det.get("release_dates"):
        parts.append(det["release_dates"][-1])
    return " ".join(parts)


def confirm(object_id: str, uid: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Применить подтверждённую группировку: копии в sources/, версии в каталог."""
    d = paths.upload_dir(object_id, uid)
    up = json.loads((d / "upload.json").read_text(encoding="utf-8"))
    files = {f["file_key"]: f for f in up["files"]}
    groups = payload.get("projects") or []
    used = [x["file_key"] for g in groups for x in g.get("files", [])]
    if len(used) != len(set(used)):
        raise CatalogError("один файл указан дважды")
    for key in used:
        if key not in files:
            raise CatalogError(f"файл {key} не найден в загрузке")
        if files[key].get("error"):
            raise CatalogError(f"файл «{files[key]['file_name']}» не читается")
    paths.sources_dir(object_id).mkdir(parents=True, exist_ok=True)
    for key in used:
        f = files[key]
        dst = paths.source_pdf(object_id, f["sha256"])
        if not dst.exists():
            tmp = dst.with_suffix(".part")
            shutil.copyfile(d / "files" / f"{key}.pdf", tmp)
            os.chmod(tmp, 0o444)
            os.replace(tmp, dst)
    created: list[str] = []

    def apply(cat):
        for g in groups:
            pid = g.get("project_id")
            if pid:
                proj = project(cat, pid)
            else:
                proj = {"project_id": _new_id("p"), "title": (g.get("title") or "Проект").strip(),
                        "code": (g.get("code") or "").strip(), "versions": [], "created_at": _now()}
                cat["projects"].append(proj)
            for item in g.get("files", []):
                f = files[item["file_key"]]
                vid = _new_id("v")
                det = f.get("detected") or {}
                cat["versions"][vid] = {
                    "version_id": vid, "project_id": proj["project_id"], "file_name": f["file_name"],
                    "relative_path": f.get("relative_path"), "sha256": f["sha256"], "size": f["size"],
                    "page_count": det.get("page_count"), "uploaded_at": _now(),
                    "user_label": (item.get("label") or _suggest_label(f)).strip(),
                    "detected": {k: det.get(k) for k in ("designation", "izm_max", "release_dates", "file_number",
                                                         "izm_in_name")},
                }
                proj["versions"].append(vid)
                created.append(vid)
            proj["updated_at"] = _now()
        return cat

    catalog = _modify(object_id, apply)
    shutil.rmtree(d, ignore_errors=True)
    return {"created_versions": created, "catalog": catalog}


def reorder(object_id: str, project_id: str, version_ids: list[str]) -> dict[str, Any]:
    def apply(cat):
        proj = project(cat, project_id)
        if sorted(version_ids) != sorted(proj["versions"]):
            raise CatalogError("новый порядок должен содержать ровно те же версии")
        proj["versions"] = list(version_ids)
        proj["updated_at"] = _now()
        return cat
    return _modify(object_id, apply)


def update_version(object_id: str, version_id: str, *, label: str | None = None) -> dict[str, Any]:
    def apply(cat):
        v = cat["versions"].get(version_id)
        if not v:
            raise CatalogError("версия не найдена")
        if label is not None:
            v["user_label"] = label.strip()[:120]
        return cat
    return _modify(object_id, apply)


def update_project(object_id: str, project_id: str, *, title: str | None = None) -> dict[str, Any]:
    def apply(cat):
        proj = project(cat, project_id)
        if title is not None and title.strip():
            proj["title"] = title.strip()[:200]
        return cat
    return _modify(object_id, apply)


def remove_version(object_id: str, version_id: str) -> dict[str, Any]:
    """Исключить версию из каталога. Неизменяемая копия PDF остаётся в sources/."""
    def apply(cat):
        v = cat["versions"].pop(version_id, None)
        if not v:
            raise CatalogError("версия не найдена")
        proj = project(cat, v["project_id"])
        proj["versions"] = [x for x in proj["versions"] if x != version_id]
        return cat
    return _modify(object_id, apply)
