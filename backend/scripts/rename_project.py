#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rename_project.py — нормализация имён папок проектов внутри одного объекта
без отвязки замечаний в базе знаний.

Что делает:
  * вычисляет old→new для каждой папки проекта объекта по правилам очистки
    (срезать .pdf / (Изм.N) / (согл …)/(от …) / (N) / _вN / дату-префикс;
     сохранить (Книга N) / (врем. сети));
  * собирает единый id-mapping (папки + source_project из decisions_log +
    project_ids из project_groups), чтобы переписать ВСЕ ссылки и заодно
    «вылечить» исторические orphan-id (`…​.pdf`, у которых уже нет папки);
  * для явно заданных VERSION_PAIRS делает контейнер `<база>(main)/` с V1 (без
    `(1)`) и V2 (папка `(1)`, перемещается целиком вместе с _output);
  * переписывает decisions_log.json (по object_id), usage_data.json (по id),
    project_groups.json (по object_id), project_info.json в папках;
  * пишет reverse-log для отката, поддерживает --dry-run.

Пути берутся из backend.app.core.config, поэтому скрипт уважает env-оверрайды
AUDIT_DATA_DIR / AUDIT_APP_DATA_DIR — это и есть механизм blue-green: запуск с
указанием на staging-копию правит только её.

Usage:
  python backend/scripts/rename_project.py --object "214. Alia (ASTERUS)" \
         --object-id 73a0e59a --dry-run
  python backend/scripts/rename_project.py --object "214. Alia (ASTERUS)" \
         --object-id 73a0e59a --apply
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
from backend.app.services.common import version_service as vs  # noqa: E402

CONTAINER_SUFFIX = "(main)"

# ── Правила очистки имени ───────────────────────────────────────────────────
RE_NUM_PAREN   = re.compile(r"\s*\((\d+)\)")
RE_IZM         = re.compile(r"\s*\(Изм\.?\s*\d+\)", re.IGNORECASE)
RE_SOGL_PAREN  = re.compile(r"\s*\((?:согл[^)]*|от\s[^)]*)\)", re.IGNORECASE)
RE_SOGL_BARE   = re.compile(r"\s+(?:согл\.?\s*)?от\s+\d{1,2}\.\d{1,2}\.\d{2,4}", re.IGNORECASE)
RE_IZM_BARE    = re.compile(r"[ _]изм\.?\s*\d+$", re.IGNORECASE)  # _изм10 / _изм.8 (без скобок)
RE_VER_SUFFIX  = re.compile(r"[ _-]в\d+$", re.IGNORECASE)
RE_DATE_PREFIX = re.compile(r"^\d{1,2}\.\d{1,2}\.\d{2,4}_")
# СОХРАНЯЕМ: (Книга N), (врем. сети)


def transform(name: str) -> str:
    """Очистить имя по правилам (без учёта VERSION_PAIRS)."""
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
    """Часть после последнего '/' — лечит path-форму source_project (ДИСЦ/имя)."""
    return raw.rsplit("/", 1)[-1] if "/" in raw else raw


def clean_base(raw: str) -> str:
    """Финальное чистое имя для произвольного id (path-форма + junk)."""
    return transform(_basename(raw))


def is_container(name: str) -> bool:
    return name.endswith(CONTAINER_SUFFIX)


# ── Объект-специфичная конфигурация версий ──────────────────────────────────
# base → {"v1_dir": <текущее имя папки V1>, "v2_dir": <текущее имя папки (1)>}
# Все source_project/project_id, нормализующиеся в base и содержащие "(1)",
# отправляются в "<base> V2"; остальные нормализующиеся в base → base (V1).
VERSION_PAIRS_ALIA = {
    "13АВ-РД-АР1.2-К3К4": {
        "section": "AR",
        "v1_dir": "13АВ-РД-АР1.2-К3К4",
        "v2_dir": "13АВ-РД-АР1.2-К3К4 (1)",
    },
    "13АВ-РД-АР1.2-К5К6": {
        "section": "AR",
        "v1_dir": "13АВ-РД-АР1.2-К5К6",
        "v2_dir": "13АВ-РД-АР1.2-К5К6 (1)",
    },
}


def map_id(raw: str, version_pairs: dict) -> str:
    """old id → new id с учётом VERSION_PAIRS (path-форма нормализуется)."""
    name = _basename(raw)
    base = transform(name)
    if base in version_pairs:
        # это один из version-base. (1) → V2, иначе V1(base).
        if RE_NUM_PAREN.search(name):  # содержит "(N)" → это V2-кандидат
            return f"{base} V2"
        return base
    return base


def _atomic_write_json(path: Path, data) -> None:
    tmp = path.with_name(path.name + ".tmp_rename")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--object", required=True, help="имя папки объекта в projects/")
    ap.add_argument("--object-id", required=True, help="object_id (hash) для decisions_log/groups")
    ap.add_argument("--apply", action="store_true", help="применить (иначе dry-run)")
    ap.add_argument("--reverse-log", default=None, help="куда писать лог отката")
    args = ap.parse_args()

    # VERSION_PAIRS специфичны для объекта; для не-Алии пусто (нет collision-дублей).
    version_pairs = VERSION_PAIRS_ALIA if args.object.startswith("214. Alia") else {}

    projects_dir = config.PROJECTS_DIR
    obj_dir = projects_dir / args.object
    if not obj_dir.is_dir():
        print(f"ОШИБКА: объект не найден: {obj_dir}", file=sys.stderr)
        sys.exit(2)

    decisions_file = config.DECISIONS_LOG_FILE
    usage_file = config.APP_DATA_DIR / "usage_data.json"
    groups_file = config.APP_DATA_DIR / "project_groups.json"

    print(f"== Данные ==")
    print(f"  projects: {obj_dir}")
    print(f"  decisions_log: {decisions_file}")
    print(f"  usage: {usage_file}")
    print(f"  groups: {groups_file}")
    print(f"  apply={args.apply}")

    # ── 1. Папки: old basename → new basename (плоские переименования) ──
    ignore_disc = {"M31A", "__BATCH__", "1_DOC"}
    version_v1 = {v["v1_dir"] for v in version_pairs.values()}
    version_v2 = {v["v2_dir"] for v in version_pairs.values()}

    # Существующие контейнеры `(main)` НЕ трогаем в этом проходе. Любой id,
    # нормализующийся в base существующего контейнера, исключаем из id-mapping,
    # иначе перепишем source_project контейнерных findings и отвяжем их.
    from backend.app.services.common import project_service as _ps
    container_bases = set()
    container_primary_ids = set()
    for disc in sorted(d for d in obj_dir.iterdir() if d.is_dir() and d.name not in ignore_disc):
        for child in sorted(disc.iterdir()):
            if child.is_dir() and is_container(child.name):
                prim = _ps._container_primary(child)
                if prim:
                    container_primary_ids.add(prim[0])
                    container_bases.add(transform(prim[0]))

    folder_renames = []   # (section, parent, old_name, new_name)
    for disc in sorted(d for d in obj_dir.iterdir() if d.is_dir() and d.name not in ignore_disc):
        for proj in sorted(disc.iterdir()):
            if not proj.is_dir() or is_container(proj.name):
                continue
            if proj.name in version_v1 or proj.name in version_v2:
                continue  # версии — отдельно
            new = transform(proj.name)
            if new != proj.name:
                folder_renames.append((disc.name, disc, proj.name, new))

    # collision check
    by_target = {}
    for sec, parent, old, new in folder_renames:
        by_target.setdefault((parent, new), []).append(old)
    collisions = {k: v for k, v in by_target.items() if len(v) > 1 or (k[0] / k[1]).exists()}
    # exclude self (target equals an old being renamed away in same parent is fine via temp)
    real_coll = {}
    renamed_away = {(parent, old) for _, parent, old, _ in folder_renames}
    for (parent, new), olds in collisions.items():
        if len(olds) > 1:
            real_coll[(parent, new)] = olds
        elif (parent / new).exists() and (parent, new) not in renamed_away:
            real_coll[(parent, new)] = olds + ["<существующая папка>"]
    if real_coll:
        print("\n!! КОЛЛИЗИИ — прерываю:")
        for (parent, new), olds in real_coll.items():
            print(f"   {parent.name}/{new}  <-  {olds}")
        sys.exit(3)

    # ── 2. Полный id-mapping для сторов (папки + orphan ids) ──
    # universe: имена папок + source_project(object) + group project_ids(object)
    universe = set()
    for sec, parent, old, new in folder_renames:
        universe.add(old)
    universe |= version_v1 | version_v2

    dec = json.loads(decisions_file.read_text(encoding="utf-8"))
    dec_entries = dec["entries"] if isinstance(dec, dict) else dec
    for e in dec_entries:
        if e.get("object_id") == args.object_id and e.get("source_project"):
            universe.add(e["source_project"])

    groups = json.loads(groups_file.read_text(encoding="utf-8"))
    obj_groups = groups.get(args.object_id, {})
    for sec, glist in obj_groups.items():
        for g in glist:
            for pid in g.get("project_ids", []):
                universe.add(pid)

    # Исключаем всё, что относится к существующим контейнерам `(main)`.
    excluded = sorted(x for x in universe
                      if clean_base(x) in container_bases or x in container_primary_ids)
    universe = {x for x in universe
                if clean_base(x) not in container_bases and x not in container_primary_ids}

    id_map = {old: map_id(old, version_pairs) for old in universe}
    id_map = {k: v for k, v in id_map.items() if k != v}  # только изменившиеся

    # bad-merge detection: разные РЕАЛЬНЫЕ папки → один target (кроме version V2)
    target_to_olds = {}
    for old, new in id_map.items():
        target_to_olds.setdefault(new, []).append(old)

    print(f"\n== id-mapping ({len(id_map)} изменений) ==")
    for old in sorted(id_map):
        print(f"   {old!r}\n     -> {id_map[old]!r}")

    print(f"\n== Папки ({len(folder_renames)}) ==")
    for sec, parent, old, new in folder_renames:
        print(f"   [{sec}] {old}  ->  {new}")

    print(f"\n== Исключено (существующие контейнеры (main), не трогаем) ({len(excluded)}) ==")
    for x in excluded:
        print(f"   {x}")

    print(f"\n== Версии ({len(version_pairs)}) ==")
    for base, cfg in version_pairs.items():
        print(f"   [{cfg['section']}] контейнер {base}{CONTAINER_SUFFIX}/  V1={cfg['v1_dir']}  V2(<-(1))={cfg['v2_dir']}")

    if not args.apply:
        print("\n(dry-run — ничего не изменено)")
        return

    # ── 3. APPLY ──
    reverse = {"folders": [], "versions": [], "stores": []}

    # 3a. плоские переименования папок: two-phase через .tmp_rn
    for sec, parent, old, new in folder_renames:
        src = parent / old
        tmp = parent / (f".__rn__{new}")
        shutil.move(str(src), str(tmp))
        reverse["folders"].append([str(parent / new), str(src)])
    for sec, parent, old, new in folder_renames:
        tmp = parent / (f".__rn__{new}")
        dst = parent / new
        shutil.move(str(tmp), str(dst))
        # обновить project_info.json
        info = dst / "project_info.json"
        if info.exists():
            try:
                d = json.loads(info.read_text(encoding="utf-8"))
                d["project_id"] = new
                d["name"] = new
                _atomic_write_json(info, d)
            except Exception as ex:
                print(f"  warn: project_info {dst}: {ex}")

    # 3b. версии: промоут V1 в контейнер + перемещение (1) как V2
    for base, cfg in version_pairs.items():
        disc = obj_dir / cfg["section"]
        v1_dir = disc / cfg["v1_dir"]
        v2_src = disc / cfg["v2_dir"]
        if not v1_dir.is_dir():
            print(f"  warn: V1 не найден: {v1_dir}")
            continue
        container, primary_dir, manifest = vs.promote_to_container(v1_dir, base)
        reverse["versions"].append(["promote", str(container), str(v1_dir)])
        # переместить (1) целиком в контейнер как "<base> V2"
        v2_folder = f"{base} V2"
        v2_dst = container / v2_folder
        if v2_src.is_dir():
            shutil.move(str(v2_src), str(v2_dst))
            reverse["versions"].append(["v2move", str(v2_dst), str(v2_src)])
            # манифест: добавить v2
            versions = list(manifest.get("versions", []))
            next_no = max((v["version_no"] for v in versions), default=1) + 1
            versions.append({
                "version_id": f"v{next_no}", "version_no": next_no,
                "label": f"V{next_no}", "folder": v2_folder,
                "created_at": vs._now_iso(), "status": "migrated",
                "source": "rename_project_migration",
            })
            manifest["versions"] = versions
            manifest["latest_version_id"] = f"v{next_no}"
            vs._write_group_manifest(container, manifest)
            # project_info V2 → project_id=base, version_id=v2
            info = v2_dst / "project_info.json"
            d = {}
            if info.exists():
                try:
                    d = json.loads(info.read_text(encoding="utf-8"))
                except Exception:
                    d = {}
            d["project_id"] = base
            d["name"] = base
            d["section"] = cfg["section"]
            d["version_id"] = f"v{next_no}"
            d["version_label"] = f"V{next_no}"
            _atomic_write_json(info, d)
            # project_info V1 → project_id=base
            info1 = primary_dir / "project_info.json"
            if info1.exists():
                try:
                    d1 = json.loads(info1.read_text(encoding="utf-8"))
                    d1["project_id"] = base
                    d1["name"] = base
                    _atomic_write_json(info1, d1)
                except Exception:
                    pass

    # 3c. decisions_log (по object_id) + дедуп (source_project,item_id) после merge
    changed = 0
    for e in dec_entries:
        if e.get("object_id") == args.object_id:
            sp = e.get("source_project")
            if sp in id_map:
                e["source_project"] = id_map[sp]
                changed += 1

    def _entry_rank(e: dict) -> int:
        # при дедупе оставляем более «полную» запись
        r = 0
        if e.get("expert_decision"): r += 2
        if e.get("customer_confirmed"): r += 1
        return r

    seen: dict = {}          # (source_project,item_id) -> index в deduped
    deduped: list = []
    removed_dups = 0
    for e in dec_entries:
        if e.get("object_id") != args.object_id:
            deduped.append(e)
            continue
        key = (e.get("source_project"), e.get("item_id"))
        if key in seen:
            removed_dups += 1
            idx = seen[key]
            if _entry_rank(e) > _entry_rank(deduped[idx]):
                deduped[idx] = e   # оставляем более полную запись
            continue
        seen[key] = len(deduped)
        deduped.append(e)
    dec_entries = deduped
    if isinstance(dec, dict):
        dec["entries"] = dec_entries
    _atomic_write_json(decisions_file, dec if isinstance(dec, dict) else {"entries": dec_entries})
    reverse["stores"].append(["decisions_log", changed, f"dups_removed={removed_dups}"])

    # 3d. usage_data (по id — без object_id, скоупим по id_map ключам)
    usage = json.loads(usage_file.read_text(encoding="utf-8"))
    ucount = 0
    for r in usage.get("records", []):
        pid = r.get("project_id")
        if pid in id_map:
            r["project_id"] = id_map[pid]
            ucount += 1
    _atomic_write_json(usage_file, usage)
    reverse["stores"].append(["usage", ucount])

    # 3e. project_groups (по object_id) + dedup
    gcount = 0
    for sec, glist in obj_groups.items():
        for g in glist:
            new_ids = []
            seen = set()
            for pid in g.get("project_ids", []):
                npid = id_map.get(pid, pid)
                if npid not in seen:
                    seen.add(npid)
                    new_ids.append(npid)
                    if npid != pid:
                        gcount += 1
            g["project_ids"] = new_ids
    _atomic_write_json(groups_file, groups)
    reverse["stores"].append(["groups", gcount])

    # reverse-log
    rl = Path(args.reverse_log) if args.reverse_log else (config.APP_DATA_DIR / "rename_project.reverse.json")
    _atomic_write_json(rl, reverse)
    print(f"\n== ПРИМЕНЕНО ==")
    print(f"  папок переименовано: {len(folder_renames)}")
    print(f"  версий-контейнеров:  {len(version_pairs)}")
    print(f"  decisions переписано: {changed}")
    print(f"  usage переписано:     {ucount}")
    print(f"  group-ссылок:         {gcount}")
    print(f"  reverse-log:          {rl}")


if __name__ == "__main__":
    main()
