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
    # Должно совпадать с РАЗМЕЩЕНИЕМ ДАННЫХ, а не кода. В production code и data
    # разнесены (код в …-deploy, данные в основном репо через AUDIT_DATA_DIR),
    # поэтому берём config.DATA_DIR (= AUDIT_DATA_DIR или авто-root), а не путь к
    # файлу модуля. Fallback на code-relative parents[4] — если config недоступен.
    try:
        from backend.app.core.config import DATA_DIR
        return Path(DATA_DIR) / "projects_v2"
    except Exception:
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

    def find_document_by_project_id(self, project_id: str,
                                    object_id: Optional[str] = None) -> Optional[dict]:
        """Resolve legacy-like project_id to a v2 document without touching legacy."""
        raw = str(project_id or "").strip().strip("/")
        candidates: list[str] = []
        for value in (raw, os.path.basename(raw)):
            if value and value not in candidates:
                candidates.append(value)
            if value.lower().endswith(".pdf"):
                stem = value[:-4].strip()
                if stem and stem not in candidates:
                    candidates.append(stem)
        for code in candidates:
            doc = self.find_document(code, object_id=object_id)
            if doc is not None:
                return doc
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

    def resolve_version_id(self, doc: dict, version_id: Optional[str] = None) -> Optional[str]:
        doc_dir = Path(doc["doc_dir"])
        wanted = (version_id or "").strip()
        if not wanted:
            return self.current_version_id(doc_dir)
        candidates = [wanted]
        if wanted.startswith("v") and wanted[1:].isdigit():
            candidates.append(f"v{int(wanted[1:]):03d}")
        ids = {v for v in (doc.get("version_ids") or []) if v}
        for candidate in candidates:
            if not ids or candidate in ids:
                return candidate
        return None

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
        present = (
            {p.name for p in latest.iterdir() if p.is_file()}
            if latest.is_dir()
            else set()
        )
        run_dir = self._fallback_run_dir(doc_dir, version_id)
        if run_dir is not None:
            present.update(p.name for p in run_dir.iterdir() if p.is_file())
        return {
            "present": sorted(present),
            "has_01_text_analysis": self._latest_file(
                doc_dir, version_id, "01_text_analysis.json"
            ) is not None,
            "has_02_blocks_analysis": self._latest_file(
                doc_dir, version_id, "02_blocks_analysis.json"
            ) is not None,
            "has_03_findings": self.findings_path(doc_dir, version_id) is not None,
        }

    def _runs_dir(self, doc_dir: Path, version_id: str) -> Path:
        return self.version_dir(doc_dir, version_id) / "03_analysis" / "runs"

    def _fallback_run_dir(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        runs = self._runs_dir(doc_dir, version_id)
        if not runs.is_dir():
            return None
        candidates = [p for p in runs.iterdir() if p.is_dir()]
        if not candidates:
            return None
        return max(candidates, key=lambda p: (p.stat().st_mtime_ns, p.name))

    def _runs_file(self, doc_dir: Path, version_id: str, name: str) -> Optional[Path]:
        safe = os.path.basename((name or "").strip())
        if not safe or safe != name:
            return None
        runs = self._runs_dir(doc_dir, version_id)
        if not runs.is_dir():
            return None
        hits = [p for p in runs.glob(f"*/{safe}") if p.is_file()]
        if not hits:
            return None
        return max(hits, key=lambda p: (p.stat().st_mtime_ns, p.parent.name))

    def _latest_file(self, doc_dir: Path, version_id: str, name: str) -> Optional[Path]:
        p = self.latest_dir(doc_dir, version_id) / name
        if p.is_file():
            return p
        return self._runs_file(doc_dir, version_id, name)

    def read_text_analysis(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self._latest_file(doc_dir, version_id, "01_text_analysis.json")
        return _read_json(p) if p else None

    def read_blocks_analysis(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        p = self._latest_file(doc_dir, version_id, "02_blocks_analysis.json")
        return _read_json(p) if p else None

    def blocks_dir(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """Папка кропнутых блоков версии (read-only).

        В projects_v2 кропы лежат под `03_analysis/latest/blocks/` либо (чаще)
        под последним `03_analysis/runs/<run>/blocks/`. Возвращает первую папку,
        где есть `index.json`, иначе None.
        """
        vdir = self.version_dir(doc_dir, version_id)
        analysis = vdir / "03_analysis"
        cand = analysis / "latest" / "blocks"
        if (cand / "index.json").is_file():
            return cand
        runs = analysis / "runs"
        if runs.is_dir():
            for run in sorted((p for p in runs.iterdir() if p.is_dir()), reverse=True):
                bd = run / "blocks"
                if (bd / "index.json").is_file():
                    return bd
        # King&Sons legacy_findings_preserve: блоки лежат в сохранённом legacy-бандле
        # `99_service/legacy_output/<...>/_output/blocks/` (read-only).
        legacy_out = vdir / "99_service" / "legacy_output"
        if legacy_out.is_dir():
            for idx in sorted(legacy_out.glob("*/_output/blocks/index.json")):
                return idx.parent
        return None

    def read_blocks_index(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        bd = self.blocks_dir(doc_dir, version_id)
        return _read_json(bd / "index.json") if bd else None

    def read_document_graph(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        """document_graph.json версии (read-only): latest, иначе King&Sons-бандл."""
        p = self._latest_file(doc_dir, version_id, "document_graph.json")
        if p:
            return _read_json(p)
        legacy_out = self.version_dir(doc_dir, version_id) / "99_service" / "legacy_output"
        if legacy_out.is_dir():
            for g in sorted(legacy_out.glob("*/_output/document_graph.json")):
                return _read_json(g)
        return None

    def read_block_batches(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        """block_batches.json версии (read-only) для классификации merged_into блоков.

        Приоритет: тот же run, что и blocks index (чтобы merged-карта совпадала с
        index.json) → 99_service → 03_analysis/latest → King&Sons legacy-бандл.
        Возвращает None, если файла нет (тогда merged_into просто не строится).
        """
        candidates: list[Path] = []
        bd = self.blocks_dir(doc_dir, version_id)
        if bd is not None:
            candidates.append(bd.parent / "block_batches.json")  # run папки index'а
        vdir = self.version_dir(doc_dir, version_id)
        candidates += [
            vdir / "99_service" / "block_batches.json",
            vdir / "03_analysis" / "latest" / "block_batches.json",
        ]
        legacy_out = vdir / "99_service" / "legacy_output"
        if legacy_out.is_dir():
            candidates += sorted(legacy_out.glob("*/_output/block_batches.json"))
        for p in candidates:
            if p and p.is_file():
                data = _read_json(p)
                if data is not None:
                    return data
        return None

    def read_findings_03(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        """Именно 03_findings.json из latest (read-only).

        В отличие от read_findings (priority chain c 03a_norms_verified), здесь
        нужен конкретно 03_findings.json — так legacy get_blocks_analysis строит
        blocks_in_findings. King&Sons-бандл — fallback.
        """
        p = self._latest_file(doc_dir, version_id, "03_findings.json")
        if p:
            return _read_json(p)
        legacy_out = self.version_dir(doc_dir, version_id) / "99_service" / "legacy_output"
        if legacy_out.is_dir():
            for g in sorted(legacy_out.glob("*/_output/03_findings.json")):
                return _read_json(g)
        return None

    # -- gap-closure readers (optimization / review / batches / inputs) -------
    def _legacy_bundle_output(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """King&Sons legacy_findings_preserve: единый `_output`-снимок
        `99_service/legacy_output/<code>/_output` (там лежат ВСЕ артефакты)."""
        legacy_out = self.version_dir(doc_dir, version_id) / "99_service" / "legacy_output"
        if legacy_out.is_dir():
            for o in sorted(legacy_out.glob("*/_output")):
                if o.is_dir():
                    return o
        return None

    def review_dir(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """Папка 04_review версии (expert_review/optimization_review/findings_review)."""
        d = self.version_dir(doc_dir, version_id) / "04_review"
        return d if d.is_dir() else None

    def read_optimization(self, doc_dir: Path, version_id: str) -> Optional[dict]:
        """optimization.json: latest → King&Sons-бандл (read-only)."""
        p = self._latest_file(doc_dir, version_id, "optimization.json")
        if p:
            return _read_json(p)
        b = self._legacy_bundle_output(doc_dir, version_id)
        if b and (b / "optimization.json").is_file():
            return _read_json(b / "optimization.json")
        return None

    def read_review(self, doc_dir: Path, version_id: str, name: str) -> Optional[dict]:
        """Файл ревью по имени (expert_review.json / optimization_review.json /
        03_findings_review.json): 04_review → latest → King&Sons-бандл (read-only)."""
        rd = self.review_dir(doc_dir, version_id)
        if rd and (rd / name).is_file():
            return _read_json(rd / name)
        p = self._latest_file(doc_dir, version_id, name)
        if p:
            return _read_json(p)
        b = self._legacy_bundle_output(doc_dir, version_id)
        if b and (b / name).is_file():
            return _read_json(b / name)
        return None

    def block_batches_dir(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """Папка с block_batches.json + block_batch_*.json (для подсчёта батчей):
        99_service → King&Sons-бандл → latest → последний run. None, если нет."""
        vdir = self.version_dir(doc_dir, version_id)
        for cand in (vdir / "99_service", vdir / "03_analysis" / "latest"):
            if (cand / "block_batches.json").is_file():
                return cand
        b = self._legacy_bundle_output(doc_dir, version_id)
        if b and (b / "block_batches.json").is_file():
            return b
        runs = vdir / "03_analysis" / "runs"
        if runs.is_dir():
            for r in sorted((p for p in runs.iterdir() if p.is_dir()), reverse=True):
                if (r / "block_batches.json").is_file():
                    return r
        return None

    def input_pdf_files(self, doc_dir: Path, version_id: str) -> list[tuple[str, int]]:
        """[(имя, размер)] исходных PDF в 01_input (read-only)."""
        inp = self.version_dir(doc_dir, version_id) / "01_input"
        out: list[tuple[str, int]] = []
        if inp.is_dir():
            for p in sorted(inp.glob("*.pdf")):
                if p.is_file():
                    try:
                        out.append((p.name, p.stat().st_size))
                    except Exception:
                        out.append((p.name, 0))
        return out

    def input_md_files(self, doc_dir: Path, version_id: str) -> list[tuple[str, int]]:
        """[(имя, размер)] MD-файлов версии: 01_input/*.md или 02_work/document.md."""
        vdir = self.version_dir(doc_dir, version_id)
        out: list[tuple[str, int]] = []
        inp = vdir / "01_input"
        if inp.is_dir():
            seen = set()
            for pat in ("*_document.md", "*.md"):
                for p in sorted(inp.glob(pat)):
                    if p.is_file() and p.name not in seen:
                        seen.add(p.name)
                        try:
                            out.append((p.name, p.stat().st_size))
                        except Exception:
                            out.append((p.name, 0))
        w = vdir / "02_work" / "document.md"
        if not out and w.is_file():
            try:
                out.append((w.name, w.stat().st_size))
            except Exception:
                out.append((w.name, 0))
        return out

    def input_dir(self, doc_dir: Path, version_id: str) -> Path:
        """Папка 01_input версии (там же лежит *_ocr.html для text_evidence)."""
        return self.version_dir(doc_dir, version_id) / "01_input"

    def md_text(self, doc_dir: Path, version_id: str) -> tuple[Optional[str], Optional[str]]:
        """(текст MD, имя MD-файла) для версии (read-only).

        Приоритет: 02_work/document.md → 01_input/*_document.md → 01_input/*.md.
        Возвращает (None, None), если MD не найден/нечитаем.
        """
        vdir = self.version_dir(doc_dir, version_id)
        candidates = [vdir / "02_work" / "document.md"]
        inp = vdir / "01_input"
        if inp.is_dir():
            candidates += sorted(inp.glob("*_document.md")) + sorted(inp.glob("*.md"))
        for p in candidates:
            if p.is_file():
                try:
                    return p.read_text(encoding="utf-8"), p.name
                except Exception:
                    continue
        return None, None

    def read_analysis_artifact(self, doc_dir: Path, version_id: str, name: str) -> Optional[dict]:
        p = self._latest_file(doc_dir, version_id, name)
        return _read_json(p) if p else None

    def analysis_artifact_path(self, doc_dir: Path, version_id: str, name: str) -> Optional[Path]:
        return self._latest_file(doc_dir, version_id, name)

    def findings_path(self, doc_dir: Path, version_id: str) -> Optional[Path]:
        """Лучший файл замечаний в latest/runs (приоритет как в findings_service)."""
        latest = self.latest_dir(doc_dir, version_id)
        for name in _FINDINGS_PRIORITY:
            p = latest / name
            if p.is_file():
                return p
        for name in _FINDINGS_PRIORITY:
            p = self._runs_file(doc_dir, version_id, name)
            if p is not None:
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
        run_log = self._runs_file(doc_dir, version_id, "pipeline_log.json")
        if run_log is not None:
            return run_log
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
