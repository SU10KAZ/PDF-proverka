"""
projects_v2_adapter.py — READ-ONLY адаптер чтения нового хранилища `projects_v2`.

Назначение (подготовительный этап, НЕ подключён к production):
  дать backend способ ЧИТАТЬ структуру `projects_v2` (объекты / дисциплины /
  документы / версии / analysis-артефакты), не используя её как основной
  источник и НЕ переключая поведение UI/pipeline.

ЖЁСТКИЕ ГАРАНТИИ:
  * адаптер ТОЛЬКО читает: не пишет, не создаёт, не удаляет файлы, не делает
    mkdir, не меняет metadata, не запускает анализ;
  * НЕ делает fallback в legacy `projects/` при чтении v2 (если чего-то нет в
    v2 — возвращает None/пусто, а не лезет в старое хранилище);
  * feature flag `AUDIT_STORAGE_BACKEND` по умолчанию `legacy` — пока никакой
    production-код этот адаптер не вызывает, поведение системы не меняется.

Раскладка `projects_v2` (источник истины, см. docs/projects_v2_storage_standard.md):

  objects/<folder>/object.json
  objects/<folder>/disciplines/<disc>/documents/<code>/document.json
  objects/<folder>/disciplines/<disc>/documents/<code>/current_version.txt
  objects/<folder>/disciplines/<disc>/documents/<code>/versions/<vid>/version.json
  .../versions/<vid>/01_input/ (+ legacy_bundle/ для legacy-снимков)
  .../versions/<vid>/03_analysis/latest/{01_text_analysis,02_blocks_analysis,03_findings,...}.json
  .../versions/<vid>/03_analysis/runs/<run_id>/...
  .../versions/<vid>/99_service/ (pipeline_log.json для обычных миграций)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# feature flag (подготовка; production пока всегда legacy)
# ---------------------------------------------------------------------------

STORAGE_BACKEND_LEGACY = "legacy"
STORAGE_BACKEND_V2 = "projects_v2"
_STORAGE_BACKEND_ENV = "AUDIT_STORAGE_BACKEND"
_V2_DIR_ENV = "AUDIT_PROJECTS_V2_DIR"


def get_storage_backend() -> str:
    """Текущий backend хранилища. Default `legacy` (production не меняется).

    Значение читается из env `AUDIT_STORAGE_BACKEND`. Любое значение, кроме
    `projects_v2`, трактуется как `legacy`.
    """
    val = (os.environ.get(_STORAGE_BACKEND_ENV) or "").strip().lower()
    return STORAGE_BACKEND_V2 if val == STORAGE_BACKEND_V2 else STORAGE_BACKEND_LEGACY


def is_v2_backend_enabled() -> bool:
    """True только если оператор ЯВНО выставил AUDIT_STORAGE_BACKEND=projects_v2."""
    return get_storage_backend() == STORAGE_BACKEND_V2


def _default_v2_root() -> Path:
    env = os.environ.get(_V2_DIR_ENV)
    if env:
        return Path(env).resolve()
    # backend/app/services/storage/projects_v2_adapter.py -> repo root = parents[4]
    return Path(__file__).resolve().parents[4] / "projects_v2"


# приоритет файла замечаний (как в findings_service._get_findings_path)
_FINDINGS_PRIORITY = ("03a_norms_verified.json", "03_findings.json",
                      "03_findings_pre_merge.json")
_CRITICAL = ("01_text_analysis.json", "02_blocks_analysis.json", "03_findings.json")
# где может лежать pipeline_log в v2 (обычные миграции vs legacy-снимки)
_PIPELINE_LOG_REL = ("03_analysis/latest/pipeline_log.json",
                     "99_service/pipeline_log.json")


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


class ProjectsV2Adapter:
    """Read-only доступ к `projects_v2`. Не трогает legacy и ничего не пишет."""

    def __init__(self, v2_root: Optional[Path] = None):
        self.v2_root = Path(v2_root).resolve() if v2_root else _default_v2_root()
        self.objects_root = self.v2_root / "objects"

    # -- existence --------------------------------------------------------
    def is_available(self) -> bool:
        return self.objects_root.is_dir()

    # -- objects ----------------------------------------------------------
    def list_objects(self) -> list[dict]:
        out: list[dict] = []
        if not self.objects_root.is_dir():
            return out
        for d in sorted(self.objects_root.iterdir()):
            if not d.is_dir():
                continue
            oj = _read_json(d / "object.json") or {}
            out.append({
                "folder_name": d.name,
                "object_id": oj.get("object_id"),
                "display_name": oj.get("display_name") or oj.get("legacy_name"),
                "legacy_path": oj.get("legacy_path"),
            })
        return out

    def object_dir(self, object_folder: str) -> Path:
        return self.objects_root / object_folder

    # -- disciplines ------------------------------------------------------
    def list_disciplines(self, object_folder: str) -> list[str]:
        disc_root = self.object_dir(object_folder) / "disciplines"
        if not disc_root.is_dir():
            return []
        return sorted(d.name for d in disc_root.iterdir() if d.is_dir())

    # -- documents --------------------------------------------------------
    def _doc_dir(self, object_folder: str, discipline: str, document_code: str) -> Path:
        return (self.object_dir(object_folder) / "disciplines" / discipline
                / "documents" / document_code)

    def list_documents(self, object_folder: Optional[str] = None,
                       discipline: Optional[str] = None) -> list[dict]:
        """Все документы (или внутри объекта/дисциплины). Каждый — summary dict."""
        out: list[dict] = []
        objects = ([object_folder] if object_folder
                   else [o["folder_name"] for o in self.list_objects()])
        for of in objects:
            discs = ([discipline] if discipline else self.list_disciplines(of))
            for dc in discs:
                docs_root = self.object_dir(of) / "disciplines" / dc / "documents"
                if not docs_root.is_dir():
                    continue
                for dd in sorted(docs_root.iterdir()):
                    dj = _read_json(dd / "document.json")
                    if dj is None:
                        continue
                    out.append(self._doc_summary(of, dc, dd, dj))
        return out

    def _doc_summary(self, object_folder: str, discipline: str, doc_dir: Path,
                     dj: dict) -> dict:
        versions = dj.get("versions", [])
        return {
            "object_folder": object_folder,
            "object_id": dj.get("object_id"),
            "discipline": discipline,
            "document_code": dj.get("document_code") or doc_dir.name,
            "kind": dj.get("kind"),
            "migration_kind": dj.get("migration_kind"),
            "current_version": self.current_version_id(doc_dir, dj),
            "version_ids": [v.get("version_id") for v in versions],
            "version_count": len(versions),
            "doc_dir": str(doc_dir),
        }

    def get_document(self, object_folder: str, discipline: str,
                     document_code: str) -> Optional[dict]:
        doc_dir = self._doc_dir(object_folder, discipline, document_code)
        dj = _read_json(doc_dir / "document.json")
        if dj is None:
            return None
        return {**self._doc_summary(object_folder, discipline, doc_dir, dj),
                "document_json": dj}

    def find_document(self, document_code: str,
                      object_id: Optional[str] = None) -> Optional[dict]:
        """Находит документ по коду (опц. в рамках object_id). Первое совпадение."""
        for d in self.list_documents():
            if d["document_code"] != document_code:
                continue
            if object_id and d["object_id"] != object_id:
                continue
            return d
        return None

    # -- versions ---------------------------------------------------------
    def read_document_json(self, doc_dir: Path) -> Optional[dict]:
        return _read_json(Path(doc_dir) / "document.json")

    def current_version_id(self, doc_dir: Path, dj: Optional[dict] = None) -> Optional[str]:
        doc_dir = Path(doc_dir)
        cv = doc_dir / "current_version.txt"
        if cv.exists():
            try:
                v = cv.read_text(encoding="utf-8").strip()
                if v:
                    return v
            except Exception:
                pass
        dj = dj if dj is not None else self.read_document_json(doc_dir)
        if dj:
            if dj.get("current_version"):
                return dj["current_version"]
            vers = dj.get("versions", [])
            if vers:
                return vers[-1].get("version_id")
        return None

    def list_versions(self, doc_dir: Path) -> list[dict]:
        dj = self.read_document_json(Path(doc_dir)) or {}
        return dj.get("versions", [])

    def version_dir(self, doc_dir: Path, version_id: str) -> Path:
        return Path(doc_dir) / "versions" / version_id

    def read_version_json(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        return _read_json(self.version_dir(doc_dir, version_id) / "version.json")

    def version_metadata(self, doc_dir: Path, version_id: str) -> dict:
        """analysis_status + legacy-preserve/source_only metadata (read-only)."""
        vj = self.read_version_json(doc_dir, version_id) or {}
        status = vj.get("analysis_status")
        return {
            "version_id": vj.get("version_id") or version_id,
            "version_no": vj.get("version_no"),
            "label": vj.get("label"),
            "analysis_status": status,
            "missing_analysis_files": vj.get("missing_analysis_files"),
            "analysis_generation": vj.get("analysis_generation"),
            "analysis_run_id": vj.get("analysis_run_id"),
            "migration_kind": vj.get("migration_kind"),
            "preserve_reason": vj.get("preserve_reason"),
            "source_files_strategy": vj.get("source_files_strategy"),
            "is_legacy_preserve": (vj.get("migration_kind") == "legacy_findings_preserve"),
            "is_source_only": (status == "source_only"),
            "is_legacy_partial": (status == "legacy_partial"),
        }

    def analysis_status(self, doc_dir: Path, version_id: str) -> Optional[str]:
        vj = self.read_version_json(doc_dir, version_id) or {}
        return vj.get("analysis_status")

    # -- inputs -----------------------------------------------------------
    def input_files(self, doc_dir: Path, version_id: str) -> list[str]:
        """Имена входных файлов версии (01_input, включая legacy_bundle), отн. пути."""
        inp = self.version_dir(doc_dir, version_id) / "01_input"
        if not inp.is_dir():
            return []
        return sorted(str(p.relative_to(inp)) for p in inp.rglob("*") if p.is_file())

    # -- analysis (03_analysis/latest) -----------------------------------
    def latest_dir(self, doc_dir: Path, version_id: str) -> Path:
        return self.version_dir(doc_dir, version_id) / "03_analysis" / "latest"

    def latest_analysis_files(self, doc_dir: Path, version_id: str) -> dict:
        latest = self.latest_dir(doc_dir, version_id)
        present = sorted(p.name for p in latest.glob("*")) if latest.is_dir() else []
        return {
            "present": present,
            "has_01_text_analysis": "01_text_analysis.json" in present,
            "has_02_blocks_analysis": "02_blocks_analysis.json" in present,
            "has_03_findings": "03_findings.json" in present,
        }

    def _latest_file(self, doc_dir: Path, version_id: str, name: str) -> Optional[Path]:
        p = self.latest_dir(doc_dir, version_id) / name
        return p if p.is_file() else None

    def read_text_analysis(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self._latest_file(doc_dir, version_id, "01_text_analysis.json")
        return _read_json(p) if p else None

    def read_blocks_analysis(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self._latest_file(doc_dir, version_id, "02_blocks_analysis.json")
        return _read_json(p) if p else None

    def findings_path(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """Лучший файл замечаний в latest (приоритет как в findings_service)."""
        latest = self.latest_dir(doc_dir, version_id)
        for name in _FINDINGS_PRIORITY:
            p = latest / name
            if p.is_file():
                return p
        return None

    def read_findings(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self.findings_path(doc_dir, version_id)
        return _read_json(p) if p else None

    def findings_list(self, doc_dir: Path, version_id: str) -> list:
        data = self.read_findings(doc_dir, version_id) or {}
        if isinstance(data, list):
            return data
        return data.get("findings", data.get("items", [])) or []

    def findings_count(self, doc_dir: Path, version_id: str) -> int:
        return len(self.findings_list(doc_dir, version_id))

    def findings_by_severity(self, doc_dir: Path, version_id: str) -> dict:
        out: dict = {}
        for f in self.findings_list(doc_dir, version_id):
            if isinstance(f, dict):
                sev = str(f.get("severity") or f.get("category") or "unknown")
                out[sev] = out.get(sev, 0) + 1
        return out

    # -- pipeline_log -----------------------------------------------------
    def pipeline_log_path(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        vdir = self.version_dir(doc_dir, version_id)
        for rel in _PIPELINE_LOG_REL:
            p = vdir / rel
            if p.is_file():
                return p
        runs = vdir / "03_analysis" / "runs"
        if runs.is_dir():
            hits = sorted(runs.rglob("pipeline_log.json"))
            if hits:
                return hits[0]
        return None

    def has_pipeline_log(self, doc_dir: Path, version_id: str) -> bool:
        return self.pipeline_log_path(doc_dir, version_id) is not None

    def read_pipeline_log(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self.pipeline_log_path(doc_dir, version_id)
        return _read_json(p) if p else None

    # -- convenience: полный снимок документа (для parity/диагностики) -----
    def document_snapshot(self, object_folder: str, discipline: str,
                          document_code: str) -> Optional[dict]:
        doc = self.get_document(object_folder, discipline, document_code)
        if doc is None:
            return None
        doc_dir = Path(doc["doc_dir"])
        cur = doc["current_version"]
        snap = {**doc, "versions": []}
        for v in self.list_versions(doc_dir):
            vid = v.get("version_id")
            meta = self.version_metadata(doc_dir, vid)
            files = self.latest_analysis_files(doc_dir, vid)
            snap["versions"].append({
                "version_id": vid,
                "is_current": vid == cur,
                "analysis_status": meta["analysis_status"],
                "migration_kind": meta["migration_kind"],
                "is_legacy_preserve": meta["is_legacy_preserve"],
                **files,
                "findings_count": self.findings_count(doc_dir, vid),
                "has_pipeline_log": self.has_pipeline_log(doc_dir, vid),
                "input_file_count": len(self.input_files(doc_dir, vid)),
            })
        return snap
