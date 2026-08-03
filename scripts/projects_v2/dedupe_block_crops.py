#!/usr/bin/env python3
"""Дедупликация одинаковых PNG-кропов блоков жёсткими ссылками.

Зачем
-----
Один и тот же кроп лежит на диске несколько раз:

* ``blocks_stage02_100`` и ``blocks_gemma_100`` рендерятся ОДНОЙ политикой
  (dpi=100, min_long_side=800, compact=False, skip_small=False — см.
  ``gemma_enrichment_contract.STAGE02_CROP_POLICY`` / ``GEMMA_BASE_CROP_POLICY``),
  различается только строка ``profile``. Замер на живом дереве 2026-08-03:
  **16564 из 16564 файлов байт-идентичны — 2.55 ГБ**.
* ``blocks/`` — read-canary-алиас, который ``crop_blocks/runner.py`` до сих пор
  делал через ``shutil.copytree``. Замер: 79 папок идентичны соседнему
  ``blocks_stage02_100`` (**0.71 ГБ**), 87 папок — единственные (алиас там
  ПЕРВИЧЕН, трогать нельзя), 9 расходятся.

Скрипт НИЧЕГО не удаляет: он заменяет доказанно одинаковые файлы жёсткими
ссылками на общий inode. Полностью обратимо обычным копированием.

Безопасность
------------
* Связываются только файлы ``block_*.png``. ``index.json`` НИКОГДА не
  связывается — он различается между папками (``profile``, ``output_dir_name``).
* Идентичность доказывается размером И sha256, а не именем.
* Файлы с уже общим inode пропускаются.
* Запись кропов сделана атомарной (``blocks._save_pixmap_atomic``): пере-кроп
  подставляет новый inode и рвёт связь только у переписываемого файла, не
  задевая соседнюю папку.
* Пропускаются версии с активным аудитом (``pipeline_log.json`` со стадией
  в ``running``) и папки, изменённые за последний час.

Использование::

    python scripts/projects_v2/dedupe_block_crops.py scan --json /tmp/dedupe.json
    python scripts/projects_v2/dedupe_block_crops.py apply --confirm DEDUPE_BLOCK_CROPS
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

CONFIRM = "DEDUPE_BLOCK_CROPS"
CHUNK = 1024 * 1024

#: Папки кропов, участвующие в дедупликации. Порядок задаёт приоритет
#: «канонического» файла: первый существующий становится целью ссылки.
CROP_DIRNAMES = (
    "blocks_stage02_100",
    "blocks_gemma_100",
    "blocks_gemma_300",
    "blocks",
)

#: Не связываем файлы, изменённые совсем недавно: возможен идущий кроп.
MIN_AGE_S = 3600


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def _version_dirs(root: Path, object_filter: str | None) -> list[Path]:
    base = root / "objects"
    if not base.is_dir():
        return []
    out: list[Path] = []
    for obj in sorted(base.iterdir()):
        if not obj.is_dir():
            continue
        if object_filter and object_filter not in obj.name:
            continue
        out.extend(
            p for p in sorted(obj.glob("disciplines/*/documents/*/versions/*")) if p.is_dir()
        )
    return out


def _has_running_stage(version_dir: Path) -> bool:
    """Идёт ли по версии аудит прямо сейчас (по pipeline_log.json)."""
    for rel in ("03_analysis/latest/pipeline_log.json", "99_service/pipeline_log.json"):
        log_path = version_dir / rel
        if not log_path.is_file():
            continue
        try:
            data = json.loads(log_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return True  # нечитаемый лог — считаем занятым, это безопасная сторона
        stages = data.get("stages") or {}
        if any((st or {}).get("status") == "running" for st in stages.values()):
            return True
    return False


def _crop_dir_groups(version_dir: Path) -> list[list[Path]]:
    """Со-расположенные папки кропов: latest/* и каждая runs/<run>/*."""
    groups: list[list[Path]] = []
    analysis = version_dir / "03_analysis"
    bases = [analysis / "latest"]
    runs = analysis / "runs"
    if runs.is_dir():
        bases.extend(p for p in sorted(runs.iterdir()) if p.is_dir())
    for base in bases:
        dirs = [base / name for name in CROP_DIRNAMES if (base / name).is_dir()]
        if len(dirs) > 1:
            groups.append(dirs)
    return groups


def _recently_touched(dirs: list[Path], now: float) -> bool:
    for d in dirs:
        try:
            if now - d.stat().st_mtime < MIN_AGE_S:
                return True
        except OSError:
            return True
    return False


def _plan_group(dirs: list[Path]) -> tuple[list[dict], dict]:
    """Спланировать связывание внутри одной группы со-расположенных папок.

    Возвращает (действия, счётчики). Действие — {'canonical', 'duplicate', 'bytes'}.
    """
    by_name: dict[str, list[Path]] = defaultdict(list)
    for d in dirs:
        for f in d.glob("block_*.png"):
            if f.is_file():
                by_name[f.name].append(f)

    actions: list[dict] = []
    stats = {"compared": 0, "identical": 0, "differing": 0, "already_linked": 0, "bytes": 0}

    for name, paths in sorted(by_name.items()):
        if len(paths) < 2:
            continue
        # Канонический — первый по приоритету CROP_DIRNAMES.
        order = {n: i for i, n in enumerate(CROP_DIRNAMES)}
        paths.sort(key=lambda p: order.get(p.parent.name, len(CROP_DIRNAMES)))
        canonical = paths[0]
        try:
            c_stat = canonical.stat()
        except OSError:
            continue
        c_hash: str | None = None

        for dup in paths[1:]:
            stats["compared"] += 1
            try:
                d_stat = dup.stat()
            except OSError:
                continue
            if d_stat.st_ino == c_stat.st_ino and d_stat.st_dev == c_stat.st_dev:
                stats["already_linked"] += 1
                continue
            if d_stat.st_size != c_stat.st_size:
                stats["differing"] += 1
                continue
            if d_stat.st_dev != c_stat.st_dev:
                stats["differing"] += 1  # разные ФС — hardlink невозможен
                continue
            try:
                if c_hash is None:
                    c_hash = _sha256(canonical)
                if _sha256(dup) != c_hash:
                    stats["differing"] += 1
                    continue
            except OSError:
                stats["differing"] += 1
                continue
            stats["identical"] += 1
            stats["bytes"] += d_stat.st_size
            actions.append(
                {
                    "canonical": str(canonical),
                    "duplicate": str(dup),
                    "bytes": d_stat.st_size,
                }
            )
    return actions, stats


def _link_one(canonical: Path, duplicate: Path) -> None:
    """Заменить duplicate жёсткой ссылкой на canonical (атомарно)."""
    tmp = duplicate.with_name(f".{duplicate.name}.dedupe.{os.getpid()}.tmp")
    if tmp.exists():
        tmp.unlink()
    os.link(canonical, tmp)
    os.replace(tmp, duplicate)


def cmd_scan(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    now = time.time()
    versions = _version_dirs(root, args.object)
    report = {
        "schema": 1,
        "generated_at": _utc_now(),
        "root": str(root),
        "versions_scanned": 0,
        "versions_skipped_busy": 0,
        "versions_skipped_fresh": 0,
        "totals": {"compared": 0, "identical": 0, "differing": 0, "already_linked": 0, "bytes": 0},
        "actions": [],
    }
    for version_dir in versions:
        groups = _crop_dir_groups(version_dir)
        if not groups:
            continue
        if _has_running_stage(version_dir):
            report["versions_skipped_busy"] += 1
            continue
        report["versions_scanned"] += 1
        for dirs in groups:
            if _recently_touched(dirs, now):
                report["versions_skipped_fresh"] += 1
                continue
            actions, stats = _plan_group(dirs)
            for key, val in stats.items():
                report["totals"][key] += val
            report["actions"].extend(actions)

    t = report["totals"]
    print(f"Версий просмотрено:      {report['versions_scanned']}")
    print(f"Пропущено (идёт аудит):  {report['versions_skipped_busy']}")
    print(f"Пропущено (свежие):      {report['versions_skipped_fresh']}")
    print(f"Файлов сопоставлено:     {t['compared']}")
    print(f"  идентичны:             {t['identical']}")
    print(f"  уже связаны:           {t['already_linked']}")
    print(f"  различаются:           {t['differing']}")
    print(f"Освободится:             {t['bytes'] / 1024 ** 3:.2f} ГБ")
    if args.json:
        Path(args.json).write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"Отчёт: {args.json}")
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    if args.confirm != CONFIRM:
        print(f"[ОТКАЗ] Нужна точная фраза --confirm {CONFIRM}", file=sys.stderr)
        return 2

    if args.report:
        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
        actions = report["actions"]
        print(f"План из отчёта: {len(actions)} файлов")
    else:
        scan_args = argparse.Namespace(root=args.root, object=args.object, json=None)
        root = Path(args.root).resolve()
        now = time.time()
        actions = []
        for version_dir in _version_dirs(root, args.object):
            groups = _crop_dir_groups(version_dir)
            if not groups or _has_running_stage(version_dir):
                continue
            for dirs in groups:
                if _recently_touched(dirs, now):
                    continue
                group_actions, _ = _plan_group(dirs)
                actions.extend(group_actions)
        del scan_args
        print(f"План построен на месте: {len(actions)} файлов")

    linked = 0
    freed = 0
    failed: list[dict] = []
    budget = args.max_bytes
    for act in actions:
        canonical = Path(act["canonical"])
        duplicate = Path(act["duplicate"])
        if budget is not None and freed >= budget:
            print(f"Достигнут лимит --max-bytes ({budget}), остановка")
            break
        # Пере-проверяем идентичность НЕПОСРЕДСТВЕННО перед связыванием:
        # между scan и apply файл мог быть пере-кропнут.
        try:
            c_stat = canonical.stat()
            d_stat = duplicate.stat()
            if c_stat.st_ino == d_stat.st_ino:
                continue
            if c_stat.st_size != d_stat.st_size or c_stat.st_dev != d_stat.st_dev:
                failed.append({"duplicate": str(duplicate), "reason": "size_or_dev_changed"})
                continue
            if _sha256(canonical) != _sha256(duplicate):
                failed.append({"duplicate": str(duplicate), "reason": "content_changed"})
                continue
            _link_one(canonical, duplicate)
        except OSError as exc:
            failed.append({"duplicate": str(duplicate), "reason": str(exc)[:200]})
            continue
        linked += 1
        freed += act["bytes"]

    print(f"Связано файлов:  {linked}")
    print(f"Освобождено:     {freed / 1024 ** 3:.2f} ГБ")
    print(f"Не удалось:      {len(failed)}")
    for item in failed[:10]:
        print(f"  {item['reason']}: {item['duplicate']}")

    receipt = {
        "schema": 1,
        "applied_at": _utc_now(),
        "linked": linked,
        "freed_bytes": freed,
        "failed": failed,
    }
    receipt_path = Path(args.root).resolve() / "_system" / "dedupe_block_crops.jsonl"
    try:
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with receipt_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(receipt, ensure_ascii=False) + "\n")
        print(f"Журнал: {receipt_path}")
    except OSError as exc:
        print(f"[WARN] Журнал не записан: {exc}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    default_root = _repo_root() / "projects_v2"
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", default=str(default_root), help="корень projects_v2")
    parser.add_argument("--object", default=None, help="фильтр по имени объекта")
    sub = parser.add_subparsers(dest="command", required=True)

    p_scan = sub.add_parser("scan", help="только отчёт, ничего не меняет")
    p_scan.add_argument("--json", default=None, help="куда записать отчёт")
    p_scan.set_defaults(func=cmd_scan)

    p_apply = sub.add_parser("apply", help="заменить дубли жёсткими ссылками")
    p_apply.add_argument("--confirm", default="", help=f"точная фраза {CONFIRM}")
    p_apply.add_argument("--report", default=None, help="использовать план из scan --json")
    p_apply.add_argument("--max-bytes", type=int, default=None, help="лимит за один запуск")
    p_apply.set_defaults(func=cmd_apply)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
