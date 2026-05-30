#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cleanup_version_containers.py — нормализация имён папок-контейнеров версий
`<dirty_base>(main)/` без отвязки замечаний.

Контейнеры исключались из rename_project.py (там трогать их нельзя — это
сменило бы project_id primary-версии и сломало version_group.json). Этот скрипт
делает version-aware переименование:

  * для каждого контейнера, у которого `transform(primary) != primary`:
    - переименовать папки версий: `<dirty>` -> `<clean>`, `<dirty> V{N}` ->
      `<clean> V{N}`;
    - переименовать сам контейнер `<dirty>(main)` -> `<clean>(main)`;
    - переписать `version_group.json` (logical_project_id, container,
      versions[].folder);
    - переписать `project_info.json` в каждой версии (project_id/name -> clean,
      version_id у V2+ сохранить);
  * собрать id-map: любой source_project/project_id/group-pid, нормализующийся в
    clean base контейнера -> clean base (decisions_log не version-aware: все
    версии под базовым id);
  * переписать decisions_log (по object_id), usage_data (по id),
    project_groups (по object_id, dedup);
  * reverse-log, --dry-run / --apply, атомарная запись JSON (os.replace).

Пути из backend.app.core.config (уважает AUDIT_DATA_DIR / AUDIT_APP_DATA_DIR).

Usage:
  python backend/scripts/cleanup_version_containers.py --object "214. Alia (ASTERUS)" --object-id 73a0e59a
  python backend/scripts/cleanup_version_containers.py --object "214. Alia (ASTERUS)" --object-id 73a0e59a --apply
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.core import config  # noqa: E402
from backend.app.services.common import project_service as ps  # noqa: E402

CONTAINER_SUFFIX = "(main)"

RE_NUM_PAREN   = re.compile(r"\s*\((\d+)\)")
RE_IZM         = re.compile(r"\s*\(Изм\.?\s*\d+\)", re.IGNORECASE)
RE_SOGL_PAREN  = re.compile(r"\s*\((?:согл[^)]*|от\s[^)]*)\)", re.IGNORECASE)
RE_SOGL_BARE   = re.compile(r"\s+(?:согл\.?\s*)?от\s+\d{1,2}\.\d{1,2}\.\d{2,4}", re.IGNORECASE)
RE_IZM_BARE    = re.compile(r"[ _]изм\.?\s*\d+$", re.IGNORECASE)
RE_VER_SUFFIX  = re.compile(r"[ _-]в\d+$", re.IGNORECASE)
RE_DATE_PREFIX = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{2,4}_")


def transform(name: str) -> str:
    has_pdf = name.lower().endswith(".pdf")
    body = name[:-4] if has_pdf else name
    body = RE_DATE_PREFIX.sub("", body)
    body = RE_VER_SUFFIX.sub("", body)
    body = RE_IZM.sub("", body)
    body = RE_IZM_BARE.sub("", body)
    body = RE_SOGL_PAREN.sub("", body)
    body = RE_SOGL_BARE.sub("", body)
    body = RE_NUM_PAREN.sub("", body)
    return body.strip()


def _basename(raw: str) -> str:
    return raw.rsplit("/", 1)[-1] if "/" in raw else raw


def clean_base(raw: str) -> str:
    return transform(_basename(raw))


def is_container(name: str) -> bool:
    return name.endswith(CONTAINER_SUFFIX)


def _atomic_write_json(path: Path, data) -> None:
    tmp = path.with_name(path.name + ".tmp_cln")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--object", required=True)
    ap.add_argument("--object-id", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reverse-log", default=None)
    args = ap.parse_args()

    obj_dir = config.PROJECTS_DIR / args.object
    if not obj_dir.is_dir():
        print(f"ОШИБКА: объект не найден: {obj_dir}", file=sys.stderr)
        sys.exit(2)

    decisions_file = config.DECISIONS_LOG_FILE
    usage_file = config.APP_DATA_DIR / "usage_data.json"
    groups_file = config.APP_DATA_DIR / "project_groups.json"

    # ── план по контейнерам ──
    plans = []   # dict per dirty container
    clean_bases = set()
    for disc in sorted(d for d in obj_dir.iterdir() if d.is_dir()):
        for child in sorted(disc.iterdir()):
            if not (child.is_dir() and is_container(child.name)):
                continue
            gm = child / "version_group.json"
            manifest = json.loads(gm.read_text(encoding="utf-8")) if gm.exists() else {}
            versions = manifest.get("versions", [])
            v1 = next((v for v in versions if v.get("version_id") == "v1"), None)
            primary = (v1 or {}).get("folder") or child.name[: -len(CONTAINER_SUFFIX)]
            base = transform(primary)
            if base == primary:
                continue  # уже чистый — пропускаем
            # collision
            if (disc / base).exists() or (disc / f"{base}{CONTAINER_SUFFIX}").exists():
                print(f"!! КОЛЛИЗИЯ: {disc.name}/{base} уже существует — пропуск {child.name}")
                continue
            # folder renames внутри контейнера
            folder_renames = []
            for v in versions:
                old_f = v["folder"]
                new_f = base if v.get("version_id") == "v1" else transform(old_f.replace(primary, base))
                # для V{N}: имя = "<base> V{N}" (берём суффикс после primary)
                if v.get("version_id") != "v1":
                    suffix = old_f[len(primary):] if old_f.startswith(primary) else f" {v.get('label','V?')}"
                    new_f = f"{base}{suffix}"
                folder_renames.append((v.get("version_id"), old_f, new_f))
            plans.append({
                "disc": disc, "container": child, "manifest": manifest,
                "primary": primary, "base": base, "folder_renames": folder_renames,
            })
            clean_bases.add(base)

    if not plans:
        print("Нет грязных контейнеров для очистки.")
        return

    # ── id-map для сторов: всё, что нормализуется в clean base ──
    dec = json.loads(decisions_file.read_text(encoding="utf-8"))
    dec_entries = dec["entries"] if isinstance(dec, dict) else dec
    groups = json.loads(groups_file.read_text(encoding="utf-8"))
    obj_groups = groups.get(args.object_id, {})

    universe = set()
    for e in dec_entries:
        if e.get("object_id") == args.object_id and e.get("source_project"):
            universe.add(e["source_project"])
    for sec, glist in obj_groups.items():
        for g in glist:
            universe.update(g.get("project_ids", []))
    for p in plans:
        for _, old_f, _ in p["folder_renames"]:
            universe.add(old_f)

    id_map = {}
    for x in universe:
        b = clean_base(x)   # path-форма (ДИСЦ/имя) + junk нормализуются
        if b in clean_bases and x != b:
            id_map[x] = b

    # ── вывод плана ──
    print("== КОНТЕЙНЕРЫ К ОЧИСТКЕ ==")
    for p in plans:
        print(f"  [{p['disc'].name}] {p['container'].name}  ->  {p['base']}{CONTAINER_SUFFIX}")
        for vid, old_f, new_f in p["folder_renames"]:
            print(f"       {vid}: {old_f}  ->  {new_f}")
    print(f"\n== id-map для сторов ({len(id_map)}) ==")
    for k in sorted(id_map):
        print(f"   {k!r} -> {id_map[k]!r}")

    if not args.apply:
        print("\n(dry-run — ничего не изменено)")
        return

    # ── APPLY ──
    reverse = {"containers": [], "stores": []}
    for p in plans:
        disc, container, manifest, base = p["disc"], p["container"], p["manifest"], p["base"]
        # 1. переименовать папки версий (внутри контейнера, two-phase)
        for vid, old_f, new_f in p["folder_renames"]:
            src = container / old_f
            tmp = container / f".__cln__{new_f}"
            if src.exists():
                shutil.move(str(src), str(tmp))
        for vid, old_f, new_f in p["folder_renames"]:
            tmp = container / f".__cln__{new_f}"
            dst = container / new_f
            if tmp.exists():
                shutil.move(str(tmp), str(dst))
                # project_info
                info = dst / "project_info.json"
                if info.exists():
                    try:
                        d = json.loads(info.read_text(encoding="utf-8"))
                        d["project_id"] = base
                        d["name"] = base
                        _atomic_write_json(info, d)
                    except Exception as ex:
                        print(f"  warn project_info {dst}: {ex}")
        # 2. манифест
        manifest["logical_project_id"] = base
        manifest["container"] = f"{base}{CONTAINER_SUFFIX}"
        fr = {old: new for _, old, new in p["folder_renames"]}
        for v in manifest.get("versions", []):
            if v.get("folder") in fr:
                v["folder"] = fr[v["folder"]]
        _atomic_write_json(container / "version_group.json", manifest)
        # 3. переименовать сам контейнер
        new_container = disc / f"{base}{CONTAINER_SUFFIX}"
        shutil.move(str(container), str(new_container))
        reverse["containers"].append([str(new_container), str(container)])

    # 4. decisions_log + дедуп (source_project,item_id)
    changed = 0
    for e in dec_entries:
        if e.get("object_id") == args.object_id and e.get("source_project") in id_map:
            e["source_project"] = id_map[e["source_project"]]
            changed += 1

    def _rank(e):
        return (2 if e.get("expert_decision") else 0) + (1 if e.get("customer_confirmed") else 0)
    seen, deduped, removed_dups = {}, [], 0
    for e in dec_entries:
        if e.get("object_id") != args.object_id:
            deduped.append(e); continue
        key = (e.get("source_project"), e.get("item_id"))
        if key in seen:
            removed_dups += 1
            idx = seen[key]
            if _rank(e) > _rank(deduped[idx]):
                deduped[idx] = e
            continue
        seen[key] = len(deduped); deduped.append(e)
    dec_entries = deduped
    if isinstance(dec, dict):
        dec["entries"] = dec_entries
    _atomic_write_json(decisions_file, dec if isinstance(dec, dict) else {"entries": dec_entries})

    # 5. usage
    usage = json.loads(usage_file.read_text(encoding="utf-8"))
    ucount = 0
    for r in usage.get("records", []):
        if r.get("project_id") in id_map:
            r["project_id"] = id_map[r["project_id"]]
            ucount += 1
    _atomic_write_json(usage_file, usage)

    # 6. groups + dedup
    gcount = 0
    for sec, glist in obj_groups.items():
        for g in glist:
            new_ids, seen = [], set()
            for pid in g.get("project_ids", []):
                npid = id_map.get(pid, pid)
                if npid not in seen:
                    seen.add(npid); new_ids.append(npid)
                    if npid != pid:
                        gcount += 1
            g["project_ids"] = new_ids
    _atomic_write_json(groups_file, groups)

    reverse["stores"] = {"decisions": changed, "usage": ucount, "groups": gcount, "dups_removed": removed_dups}
    rl = Path(args.reverse_log) if args.reverse_log else (config.APP_DATA_DIR / "cleanup_containers.reverse.json")
    _atomic_write_json(rl, reverse)
    ps.invalidate_project_cache()

    print("\n== ПРИМЕНЕНО ==")
    print(f"  контейнеров очищено: {len(plans)}")
    print(f"  decisions переписано: {changed}")
    print(f"  usage переписано:     {ucount}")
    print(f"  group-ссылок:         {gcount}")
    print(f"  reverse-log:          {rl}")


if __name__ == "__main__":
    main()
