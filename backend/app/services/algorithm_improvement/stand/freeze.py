"""Архивные копии для стенда: код релиза, рантайм норм, промпты, app data, флаги, входы.

Всё копируется ОДИН раз в `<exp>/frozen/` и `<exp>/inputs/`, после чего повторы
читают только эти копии. Источники (боевой релиз, `shared/norms`, данные
`projects_v2`) только читаются. Каждая копия получает sha256-дерево в
`<exp>/frozen/FROZEN.json`; повтор перед стартом сверяет его.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

RELEASE_BACKEND_EXCLUDE = {
    "backend/app/data/project_change_preview_snapshot",
    "backend/app/data/project_change_preview_snapshot_v3",
    "backend/app/data/human_mapping_fixtures",
    "backend/app/data/section_optimization_pipeline",
    "backend/tests",
}
APP_DATA_ALLOW = (
    "stage_models.json", "stage_batch_modes.json", "codex_model_substitution.json",
    "astra_parallelism.json", "objects.json", "hidden_projects.json", "project_groups.json",
    "model_prices.json", "missing_norms_vault.json", "missing_norms_review_rules.json",
    "discipline_checklists", "discipline_checklists_metadata",
)
FLAG_PREFIXES = (
    "PIPELINE_", "STAGE01_", "CRITIC_V2_", "NORM_", "FINDINGS_", "AUDIT_CODEX_", "AUDIT_RESUME_",
    "BLOCK_VALUE_", "SINGLELINE_", "OPTIMIZATION_", "AUDIT_STAGE02_", "AUDIT_PROJECTS_V2_",
    "AUDIT_STORAGE_BACKEND",
)
SECRET_RX = re.compile(r"KEY|SECRET|TOKEN|PASSW|USERS|AUTH|COOKIE|CREDENTIAL", re.IGNORECASE)
# Входы версии, произведённые ДО прохода docnorm. Всё остальное в каталоге прогона —
# результаты docnorm и последующих этапов, они в песочницу не копируются.
DISCIPLINE_PASSES = ("alia_ar_masonry_audit", "alia_eom_protection_audit", "alia_ss_lowcurrent_audit",
                     "alia_km_steel_audit")
PRE_DOCNORM_RUN_FILES = (
    "01_blocks_analysis.json", "01_blocks_for_text.json", "01_text_prescan.json",
    "02_text_analysis.json", "03_findings_codex_base.json",
    *(f"03_findings_targeted_{p}.json" for p in DISCIPLINE_PASSES), "block_batches.runtime.json",
    "block_context_summary.json", "block_grounding_summary.json", "document_graph.json",
    "blocks_stage02_100/index.json",
)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def tree_sha256(root: Path, *, skip_dirs: Iterable[str] = ("__pycache__",)) -> dict:
    """sha256 дерева: по отсортированным относительным путям и содержимому файлов."""
    root = Path(root)
    h = hashlib.sha256()
    files = 0
    size = 0
    for path in sorted(p for p in root.rglob("*") if p.is_file() and not p.is_symlink()
                       and not any(part in skip_dirs for part in p.parts)):
        rel = path.relative_to(root).as_posix()
        h.update(rel.encode("utf-8") + b"\0" + sha256_file(path).encode() + b"\n")
        files += 1
        size += path.stat().st_size
    return {"sha256": h.hexdigest(), "files": files, "bytes": size}


def make_read_only(root: Path) -> None:
    for path in Path(root).rglob("*"):
        if path.is_symlink():
            continue
        mode = path.stat().st_mode
        path.chmod(mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))


def _copytree(src: Path, dst: Path, *, exclude_rel: set[str] = frozenset(), root: Path | None = None) -> None:
    root = root or src
    dst.mkdir(parents=True, exist_ok=True)
    for entry in sorted(src.iterdir()):
        rel = entry.relative_to(root).as_posix()
        if entry.name == "__pycache__" or rel in exclude_rel:
            continue
        target = dst / entry.name
        if entry.is_symlink():
            os.symlink(os.readlink(entry), target)
        elif entry.is_dir():
            _copytree(entry, target, exclude_rel=exclude_rel, root=root)
        else:
            shutil.copy2(entry, target)


def freeze_code(release: Path, dest: Path, overlay_src: Path) -> dict:
    """Код релиза (backend без тяжёлых данных, norms) + оверлей модуля исследования (только новые файлы)."""
    app = Path(release).resolve() / "app"
    if dest.exists():
        raise FileExistsError(f"{dest} уже существует — заморозка повторно не делается")
    _copytree(app / "backend", dest / "backend", exclude_rel=RELEASE_BACKEND_EXCLUDE, root=app)
    _copytree(app / "norms", dest / "norms", root=app)
    overlay_dst = dest / "backend" / "app" / "services" / "algorithm_improvement"
    if overlay_dst.exists():
        raise FileExistsError("в релизе уже есть algorithm_improvement — оверлей не должен затирать боевые файлы")
    _copytree(Path(overlay_src), overlay_dst)
    return {
        "release": Path(release).resolve().name,
        "code": tree_sha256(dest),
        "overlay": tree_sha256(overlay_dst),
        "targeted_module_sha256": sha256_file(
            dest / "backend/app/pipeline/stages/prepare/codex_targeted_findings.py"),
        "norms_db_sha256": sha256_file(dest / "norms" / "norms_db.json"),
    }


def freeze_norms_runtime(shared_norms: Path, dest: Path) -> dict:
    """Копия рантайма норм (status_index, vault, эмбеддинги, venv MCP, кеш цитат)."""
    if dest.exists():
        raise FileExistsError(f"{dest} уже существует")
    shared_norms = Path(shared_norms)
    dest.mkdir(parents=True)
    for name in ("tools", "vault"):
        subprocess.run(["cp", "-a", str(shared_norms / name), str(dest / name)], check=True)
    for name in ("norms_paragraphs.json", "runtime-manifest.json"):
        shutil.copy2(shared_norms / name, dest / name)
    manifest = json.loads((dest / "runtime-manifest.json").read_text(encoding="utf-8"))
    checks = {
        "status_index": (sha256_file(dest / "tools" / "status_index.json"), manifest.get("status_index_sha256")),
        "paragraphs_embeddings": (sha256_file(dest / "tools" / "paragraphs_embeddings.npz"),
                                  manifest.get("paragraphs_embeddings_sha256")),
    }
    info = {
        "source": str(shared_norms),
        "manifest_checks": {k: {"copy": a, "runtime_manifest": b, "equal": a == b} for k, (a, b) in checks.items()},
        "vault": tree_sha256(dest / "vault"),
        "norms_paragraphs_sha256": sha256_file(dest / "norms_paragraphs.json"),
        "tools_data": {name: sha256_file(dest / "tools" / name) for name in
                       ("status_index.json", "paragraphs.jsonl", "embeddings.npz", "active_norms.json",
                        "refs_graph.json", "semantic_neighbors.json")},
    }
    make_read_only(dest)
    return info


def freeze_prompts(src: Path, dest: Path) -> dict:
    if dest.exists():
        raise FileExistsError(f"{dest} уже существует")
    _copytree(Path(src), dest)
    data_root = Path(src).resolve().parent
    head = subprocess.run(["git", "-C", str(data_root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(data_root), "status", "--porcelain", "--", "prompts"],
                           capture_output=True, text=True).stdout.strip()
    info = {"source": str(src), "source_git_head": head, "source_prompts_dirty": bool(dirty), "tree": tree_sha256(dest)}
    make_read_only(dest)
    return info


def freeze_app_data(src: Path, dest: Path) -> dict:
    if dest.exists():
        raise FileExistsError(f"{dest} уже существует")
    dest.mkdir(parents=True)
    copied = []
    for name in APP_DATA_ALLOW:
        path = Path(src) / name
        if path.is_dir():
            _copytree(path, dest / name)
            copied.append(name)
        elif path.is_file():
            shutil.copy2(path, dest / name)
            copied.append(name)
    info = {"source": str(src), "copied": copied, "tree": tree_sha256(dest)}
    make_read_only(dest)
    return info


def service_flags(pid: int) -> dict:
    """Флаги поведения боевой службы по белому списку префиксов; секреты отбрасываются."""
    raw = Path(f"/proc/{int(pid)}/environ").read_bytes().split(b"\0")
    env = {}
    for item in raw:
        if b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        key = key.decode("utf-8", "replace")
        if SECRET_RX.search(key) or not key.startswith(FLAG_PREFIXES):
            continue
        env[key] = value.decode("utf-8", "replace")
    return env


def find_reviewed_run(vdir: Path, decided: dict[str, str]) -> tuple[Path | None, float]:
    """Прогон с базой свода и выходом docnorm, чьи замечания совпадают с решёнными (item_id + текст)."""
    best, best_score = None, -1.0
    for run in sorted(p for p in (Path(vdir) / "03_analysis" / "runs").glob("*") if p.is_dir()):
        if not all((run / n).is_file() for n in ("03_findings_codex_base.json",
                                                  "03_findings_targeted_alia_docnorm_audit.json")):
            continue
        try:
            data = json.loads((run / "03_findings.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        items = {str(x.get("id")): x for x in (data.get("findings", []) if isinstance(data, dict) else [])
                 if isinstance(x, dict)}
        hits = sum(1 for iid, problem in decided.items()
                   if str((items.get(iid) or {}).get("problem") or "").strip()[:200] == problem[:200])
        score = hits / max(1, len(decided))
        if score > best_score:
            best, best_score = run, score
    return best, round(best_score, 3)


def _pick_md_rel(version_dir: Path, md_rel_from_objects: str, objects_root: Path) -> Path:
    md_abs = objects_root / md_rel_from_objects
    return md_abs.relative_to(version_dir)


def freeze_inputs(entry: dict, objects_root: Path, dest: Path) -> dict:
    """Входы одной версии до docnorm: структура projects_v2 в миниатюре, с хэшами."""
    if dest.exists():
        raise FileExistsError(f"{dest} уже существует")
    objects_root = Path(objects_root)
    vdir = objects_root / entry["version_path"]
    doc_dir = vdir.parent.parent
    obj_dir = objects_root / entry["version_path"].split("/")[0]
    run_dir = vdir / "03_analysis" / "runs" / entry["reviewed_run"]
    rel_v = Path(entry["version_path"])
    out_v = dest / "projects_v2" / "objects" / rel_v
    files: dict[str, str] = {}

    def put(src: Path, rel_dst: Path) -> None:
        target = dest / "projects_v2" / "objects" / rel_dst
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        files[rel_dst.as_posix()] = sha256_file(target)

    put(obj_dir / "object.json", Path(obj_dir.name) / "object.json")
    put(doc_dir / "document.json", doc_dir.relative_to(objects_root) / "document.json")
    if (doc_dir / "current_version.txt").is_file():
        put(doc_dir / "current_version.txt", doc_dir.relative_to(objects_root) / "current_version.txt")
    for name in ("version.json",):
        if (vdir / name).is_file():
            put(vdir / name, rel_v / name)
    put(vdir / "01_input" / "project_info.json", rel_v / "01_input" / "project_info.json")
    md_rel = _pick_md_rel(vdir, entry["md"]["path"], objects_root)
    put(vdir / md_rel, rel_v / md_rel)
    pdf = vdir / "02_work" / "document.pdf"
    if pdf.is_file():
        put(pdf, rel_v / "02_work" / "document.pdf")
    for name in PRE_DOCNORM_RUN_FILES:
        src = run_dir / name
        if src.is_file():
            put(src, rel_v / "03_analysis" / "latest" / name)
    (out_v / "04_review").mkdir(parents=True, exist_ok=True)

    pinned = {
        "md": (rel_v / md_rel).as_posix(),
        "project_info": (rel_v / "01_input" / "project_info.json").as_posix(),
        **{name: (rel_v / "03_analysis" / "latest" / name).as_posix()
           for name in entry["shared_inputs"] if entry["shared_inputs"][name]["sha256"]},
    }
    mismatches = []
    if files.get(pinned["md"]) != entry["md"]["sha256"]:
        mismatches.append("md")
    if files.get(pinned["project_info"]) != entry["project_info_sha256"]:
        mismatches.append("project_info")
    for name, info in entry["shared_inputs"].items():
        if info["sha256"] and files.get(pinned[name]) != info["sha256"]:
            mismatches.append(name)
    make_read_only(dest)
    return {"version_path": entry["version_path"], "reviewed_run": entry["reviewed_run"],
            "files": files, "pinned_mismatches": mismatches,
            "frozen_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
