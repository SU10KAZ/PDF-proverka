#!/usr/bin/env python3
"""Срез данных для нагрузочного стенда портала (план доступа, этап 8).

ТОЛЬКО ЧТЕНИЕ источника: файлы копируются в ``--dest``, абсолютные пути
внутри JSON (их пишет боевая служба как ``/home/coder/projects/PDF-proverka/…``)
переписываются на корень стенда. Источник не изменяется.

Что берётся:
* projects_v2: ``--docs`` документов (текущая версия: 01_input, 02_work,
  03_analysis/latest) из каждого объекта ``--v2-objects``;
* сравнение версий: каталог объекта ``--vc-object`` и один проект с готовым
  прогоном (источники, профили, линия, прогоны и кэш их листов);
* сравнение стадий: ``session.json`` выбранной сессии и ``--sc-pairs`` пар с
  их PDF.

  python scripts/load/build_stand_slice.py --dest /path/to/stand
"""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

SRC = Path("/srv/auditmanager/data")
SERVICE_ROOT = "/home/coder/projects/PDF-proverka"   # так пути видит боевая служба


def _rewrite(text: str, dest: Path) -> str:
    return text.replace(SERVICE_ROOT, str(dest / "data"))


def _copy_file(src: Path, dst: Path, dest_root: Path, *, rewrite: bool = False) -> int:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if rewrite and src.suffix == ".json":
        dst.write_text(_rewrite(src.read_text(encoding="utf-8"), dest_root), encoding="utf-8")
    else:
        shutil.copy2(src, dst)
    return dst.stat().st_size


def _copy_tree(src: Path, dst: Path, dest_root: Path, *, max_bytes: int | None = None) -> int:
    total = 0
    if not src.exists():
        return 0
    for f in sorted(src.rglob("*")):
        if not f.is_file() or f.suffix == ".lock":
            continue
        if max_bytes is not None and total + f.stat().st_size > max_bytes:
            break
        total += _copy_file(f, dst / f.relative_to(src), dest_root, rewrite=True)
    return total


def slice_projects_v2(dest: Path, objects: list[str], docs: int) -> dict:
    out = {}
    src_root = SRC / "projects_v2" / "objects"
    dst_root = dest / "data" / "projects_v2" / "objects"
    for folder in objects:
        src_obj = src_root / folder
        _copy_file(src_obj / "object.json", dst_root / folder / "object.json", dest, rewrite=True)
        picked = []
        for doc_json in sorted(src_obj.glob("disciplines/*/documents/*/document.json")):
            doc_dir = doc_json.parent
            meta = json.loads(doc_json.read_text(encoding="utf-8"))
            vid = (doc_dir / "current_version.txt").read_text().strip() if (doc_dir / "current_version.txt").exists() \
                else meta.get("current_version")
            latest = doc_dir / "versions" / str(vid) / "03_analysis" / "latest" / "03_findings.json"
            if not vid or not latest.exists():
                continue
            rel = doc_dir.relative_to(src_obj)
            for name in ("document.json", "current_version.txt"):
                if (doc_dir / name).exists():
                    _copy_file(doc_dir / name, dst_root / folder / rel / name, dest, rewrite=True)
            ver = doc_dir / "versions" / str(vid)
            for sub in ("version.json",):
                if (ver / sub).exists():
                    _copy_file(ver / sub, dst_root / folder / rel / "versions" / str(vid) / sub, dest, rewrite=True)
            for sub in ("01_input", "02_work", "03_analysis/latest", "04_review"):
                _copy_tree(ver / sub, dst_root / folder / rel / "versions" / str(vid) / sub, dest)
            picked.append(f"{meta.get('discipline')}/{meta.get('document_code')}")
            if len(picked) >= docs:
                break
        out[folder] = picked
    return out


def slice_version_comparison(dest: Path, object_id: str) -> dict:
    src = SRC / "version_comparison" / "objects" / object_id
    dst = dest / "data" / "version_comparison" / "objects" / object_id
    catalog = json.loads((src / "catalog.json").read_text(encoding="utf-8"))
    runs = sorted((src / "runs").iterdir(), key=lambda p: p.name)
    run = project_id = None
    for rdir in reversed(runs):
        try:
            rj = json.loads((rdir / "run.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if rj.get("state") == "done" and len(rj.get("items") or []) >= 5:
            run, project_id = rdir, rj["project_id"]
            break
    assert run is not None, "не найден готовый прогон"
    project = next(p for p in catalog["projects"] if p["project_id"] == project_id)
    vids = list(project["versions"])
    sliced = {**catalog, "projects": [project],
              "versions": {v: catalog["versions"][v] for v in vids if v in catalog["versions"]}}
    dst.mkdir(parents=True, exist_ok=True)
    (dst / "catalog.json").write_text(json.dumps(sliced, ensure_ascii=False), encoding="utf-8")
    shas = [catalog["versions"][v]["sha256"] for v in vids if v in catalog["versions"]]
    for sha in shas:
        if (src / "sources" / f"{sha}.pdf").exists():
            _copy_file(src / "sources" / f"{sha}.pdf", dst / "sources" / f"{sha}.pdf", dest)
        _copy_tree(src / "profiles" / sha, dst / "profiles" / sha, dest)
    if (src / "lineage" / f"{project_id}.json").exists():
        _copy_file(src / "lineage" / f"{project_id}.json", dst / "lineage" / f"{project_id}.json", dest, rewrite=True)
    copied_runs = []
    for rdir in runs:
        try:
            rj = json.loads((rdir / "run.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if rj.get("project_id") != project_id:
            continue
        _copy_tree(rdir, dst / "runs" / rdir.name, dest)
        for item in rj.get("items") or []:
            rd = str(item.get("result_dir") or "")
            if rd.startswith(SERVICE_ROOT):
                real = Path(rd.replace(SERVICE_ROOT, str(SRC)))
                _copy_tree(real, Path(_rewrite(rd, dest)), dest)
        copied_runs.append(rdir.name)
    for sdir in (src / "series").iterdir() if (src / "series").is_dir() else []:
        sj = sdir / "series.json"
        if sj.exists() and json.loads(sj.read_text(encoding="utf-8")).get("project_id") == project_id:
            _copy_tree(sdir, dst / "series" / sdir.name, dest)
    return {"object_id": object_id, "project_id": project_id, "runs": copied_runs, "versions": len(shas)}


def slice_stage_comparison(dest: Path, session_id: str, pairs: int) -> dict:
    src = SRC / "comparison" / "sessions" / session_id
    dst = dest / "data" / "comparison" / "sessions" / session_id
    _copy_file(src / "session.json", dst / "session.json", dest, rewrite=True)
    index = json.loads((SRC / "comparison" / "index.json").read_text(encoding="utf-8"))
    entry = next(e for e in index["sessions"] if e["id"] == session_id)
    (dest / "data" / "comparison").mkdir(parents=True, exist_ok=True)
    (dest / "data" / "comparison" / "index.json").write_text(
        _rewrite(json.dumps({"sessions": [entry]}, ensure_ascii=False), dest), encoding="utf-8")
    copied = []
    for pdir in sorted((src / "pairs").iterdir())[:pairs]:
        pj = json.loads((pdir / "pair.json").read_text(encoding="utf-8"))
        _copy_file(pdir / "pair.json", dst / "pairs" / pdir.name / "pair.json", dest, rewrite=True)
        for side in ("left", "right"):
            pdf = str((pj.get(side) or {}).get("pdf_path") or "")
            if pdf.startswith(SERVICE_ROOT):
                real = Path(pdf.replace(SERVICE_ROOT, str(SRC)))
                if real.exists():
                    _copy_file(real, Path(_rewrite(pdf, dest)), dest)
        copied.append(pdir.name)
    return {"session_id": session_id, "pairs": copied}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest", required=True)
    ap.add_argument("--v2-objects", default="256_Primavera_K14_Spartak,213_Mosfilmovskaya_31A_KingSons")
    ap.add_argument("--docs", type=int, default=12)
    ap.add_argument("--vc-object", default="0b540226")
    ap.add_argument("--sc-session", default="6d9e1deb31674269")
    ap.add_argument("--sc-pairs", type=int, default=3)
    args = ap.parse_args()
    dest = Path(args.dest).resolve()
    assert not str(dest).startswith(str(SRC)), "назначение не может быть внутри боевых данных"
    dest.mkdir(parents=True, exist_ok=True)
    manifest = {
        "projects_v2": slice_projects_v2(dest, args.v2_objects.split(","), args.docs),
        "version_comparison": slice_version_comparison(dest, args.vc_object),
        "stage_comparison": slice_stage_comparison(dest, args.sc_session, args.sc_pairs),
    }
    registry = json.loads((SRC / "backend" / "app" / "data" / "objects.json").read_text(encoding="utf-8"))
    # projects_dir реестра указывает на чекаут боевой службы — переводим на
    # корень стенда, чтобы стенд ни при каком пути не обращался к боевым папкам.
    for obj in registry.get("objects") or []:
        obj["projects_dir"] = str(dest / "data" / "projects" / Path(str(obj.get("projects_dir") or obj["id"])).name)
    (dest / "objects.json").write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
    (dest / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=1)[:3000])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
