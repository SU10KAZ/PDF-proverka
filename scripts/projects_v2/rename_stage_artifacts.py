#!/usr/bin/env python3
"""Структурная миграция имён артефактов ранних этапов на диске (закалённая версия).

Свап номеров (порядок исполнения: блоки первыми, текст вторым):
    02_blocks_analysis.json  -> 01_blocks_analysis.json
    02_blocks_for_text.json  -> 01_blocks_for_text.json
    01_text_analysis.json    -> 02_text_analysis.json

Скрипт не только переименовывает файл, но и приводит содержимое в порядок:
  * верхнеуровневое поле `stage` (по таблице ниже);
  * ключ метаданных блоков `stage02_meta` -> `stage01_meta`;
  * в `version.json` список `missing_analysis_files` (старые имена -> новые).

Гарантии безопасности (запускать при ОСТАНОВЛЕННОМ бэкенде):
  * DRY-RUN по умолчанию; `--apply` выполняет.
  * **Атомарность**: rename делается как `os.rename(src->dst)` (атомарный move в том же
    каталоге) + правка содержимого через временный файл и `os.replace()`. В любой момент
    существует РОВНО одно имя (кроме заранее существовавших дублей) — нет полу-записанных
    файлов и нет both-exist конфликта из-за краша.
  * **Durable journal**: `journal.jsonl` в backup-каталоге. Перед КАЖДОЙ мутацией пишется
    строка `phase:"pre"` (со `sha_old` и путём бэкапа) с flush+fsync, после — `phase:"post"`
    (со `sha_new`). Падение посередине полностью откатывается `--rollback` по journal.
  * **Single-run lock**: backup-каталог создаётся с `exist_ok=False` (+ `.lock`), второй
    параллельный/повторный запуск в тот же каталог отвергается.
  * **Per-file backup** (copy2) до мутации; **`--rollback <backup_dir>`** восстанавливает
    оригиналы, СВЕРЯЯ sha текущего canonical со `sha_new` из journal — если отличается
    (данные созданы после миграции), НЕ удаляет, а предупреждает.
  * Идемпотентность: уже мигрированные файлы пропускаются; повторный `--apply` = 0 операций.
  * НЕ трогает `_system/destructive_backups`, `_trash`, `.git`.

Пути покрытия:
  projects_v2/**/03_analysis/latest/  и  .../03_analysis/runs/*/
  projects/**/_output/                (legacy V1)
  projects_v2/**/version.json         (missing_analysis_files)

Запуск (из корня репозитория):
    python scripts/projects_v2/rename_stage_artifacts.py                 # dry-run
    python scripts/projects_v2/rename_stage_artifacts.py --apply         # выполнить
    python scripts/projects_v2/rename_stage_artifacts.py --rollback DIR  # откат по journal
    python scripts/projects_v2/rename_stage_artifacts.py --root PATH ...  # иной корень
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

# old filename -> new filename
RENAME_MAP = {
    "02_blocks_analysis.json": "01_blocks_analysis.json",
    "02_blocks_for_text.json": "01_blocks_for_text.json",
    "01_text_analysis.json": "02_text_analysis.json",
}
# old top-level `stage` value -> new
STAGE_VALUE_MAP = {
    "02_blocks_analysis": "01_blocks_analysis",
    "02_blocks_for_text": "01_blocks_for_text",
    "01_text_analysis": "02_text_analysis",
}
META_KEY_OLD = "stage02_meta"
META_KEY_NEW = "stage01_meta"

SKIP_DIR_MARKERS = ("_system/destructive_backups", "_trash", "/.git/")


# ─────────────────────────── helpers ───────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _skip(path: Path) -> bool:
    s = str(path).replace("\\", "/")
    return any(m in s for m in SKIP_DIR_MARKERS)


def _iter_artifact_dirs(root: Path):
    """Каталоги, где могут лежать переименовываемые артефакты."""
    v2 = root / "projects_v2"
    if v2.is_dir():
        for latest in v2.rglob("03_analysis/latest"):
            if latest.is_dir() and not _skip(latest):
                yield latest
        for runs in v2.rglob("03_analysis/runs"):
            if _skip(runs):
                continue
            for run in runs.iterdir():
                if run.is_dir() and not _skip(run):
                    yield run
    v1 = root / "projects"
    if v1.is_dir():
        for out in v1.rglob("_output"):
            if out.is_dir() and not _skip(out):
                yield out


def _transform_content(new_name: str, data):
    """Привести содержимое JSON к новому контракту. Возвращает (data, changed)."""
    if not isinstance(data, dict):
        return data, False
    changed = False
    stage = data.get("stage")
    if isinstance(stage, str) and stage in STAGE_VALUE_MAP:
        data["stage"] = STAGE_VALUE_MAP[stage]
        changed = True
    if new_name == "01_blocks_analysis.json" and META_KEY_OLD in data:
        if META_KEY_NEW not in data:  # конфликт ключей не сливаем молча
            data[META_KEY_NEW] = data.pop(META_KEY_OLD)
            changed = True
    return data, changed


def _atomic_write_json(path: Path, data) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


class Journal:
    """Append-only journal c fsync после каждой записи."""

    def __init__(self, path: Path):
        self._fp = open(path, "a", encoding="utf-8")

    def write(self, obj: dict) -> None:
        self._fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._fp.flush()
        os.fsync(self._fp.fileno())

    def close(self) -> None:
        try:
            self._fp.close()
        except OSError:
            pass


# ─────────────────────────── planning ───────────────────────────

def _plan_dir(art_dir: Path):
    ops = []
    for old_name, new_name in RENAME_MAP.items():
        src = art_dir / old_name
        dst = art_dir / new_name
        if not src.is_file():
            continue  # нечего мигрировать (идемпотентно)
        if dst.exists():
            if dst.is_file() and _sha256(dst) == _sha256(src):
                ops.append({"type": "dup_cleanup", "src": src, "dst": dst})
            else:
                ops.append({"type": "conflict", "src": src, "dst": dst})
            continue
        ops.append({"type": "rename", "src": src, "dst": dst, "new_name": new_name})
    return ops


def _plan_version_json(root: Path):
    ops = []
    v2 = root / "projects_v2"
    if not v2.is_dir():
        return ops
    for vj in v2.rglob("version.json"):
        if _skip(vj):
            continue
        try:
            data = json.loads(vj.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue  # невалидный — залогируем на apply
        miss = data.get("missing_analysis_files")
        if isinstance(miss, list) and any(m in RENAME_MAP for m in miss):
            ops.append({"type": "version_json", "path": vj})
    return ops


def _collect(root: Path):
    renames, dups, conflicts = [], [], []
    for art_dir in _iter_artifact_dirs(root):
        for op in _plan_dir(art_dir):
            {"rename": renames, "dup_cleanup": dups, "conflict": conflicts}[op["type"]].append(op)
    vjs = _plan_version_json(root)
    return renames, dups, conflicts, vjs


# ─────────────────────────── apply ───────────────────────────

def do_apply(root: Path, ts: str) -> int:
    renames, dups, conflicts, vjs = _collect(root)
    print(f"[apply] renames={len(renames)} dup_cleanup={len(dups)} "
          f"version_json={len(vjs)} conflicts={len(conflicts)}")
    for c in conflicts[:20]:
        print(f"  ! CONFLICT (пропуск): {c['src']} -> цель существует и отличается")

    backup_root = (root / "projects_v2" / "_system" / "destructive_backups"
                   / f"rename_stage_artifacts_{ts}")
    try:
        backup_root.mkdir(parents=True, exist_ok=False)  # single-run lock
    except FileExistsError:
        print(f"[apply] ОТКАЗ: backup-каталог уже существует: {backup_root}")
        return 2
    (backup_root / ".lock").write_text(str(os.getpid()), encoding="utf-8")
    journal = Journal(backup_root / "journal.jsonl")
    journal.write({"t": "meta", "ts": ts, "root": str(root)})

    def _backup(path: Path) -> tuple[str, str]:
        rel = str(path.resolve().relative_to(root.resolve()))
        dest = backup_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        return rel, _sha256(dest)

    n_ren = n_dup = n_vj = 0
    try:
        # 1) Переименования: атомарный move + правка содержимого на месте
        for op in renames:
            src, dst, new_name = op["src"], op["dst"], op["new_name"]
            rel, sha_old = _backup(src)
            journal.write({"t": "rename", "phase": "pre", "old": str(src),
                           "new": str(dst), "backup": rel, "sha_old": sha_old})
            os.rename(src, dst)  # атомарно; теперь существует только dst (старое содержимое)
            try:
                data = json.loads(dst.read_text(encoding="utf-8"))
                data, changed = _transform_content(new_name, data)
                if changed:
                    _atomic_write_json(dst, data)
            except (json.JSONDecodeError, OSError):
                pass  # неразбираемый JSON — оставляем как есть (имя уже правильное)
            journal.write({"t": "rename", "phase": "post", "old": str(src),
                           "new": str(dst), "sha_new": _sha256(dst)})
            n_ren += 1
            print(f"  ✓ {src.name} -> {dst.name}  ({src.parent})")

        # 2) Дубль-очистка (цель идентична исходнику): удалить старое имя
        for op in dups:
            src = op["src"]
            rel, sha_old = _backup(src)
            journal.write({"t": "dup", "phase": "pre", "removed": str(src),
                           "backup": rel, "sha_old": sha_old})
            src.unlink()
            journal.write({"t": "dup", "phase": "post", "removed": str(src)})
            n_dup += 1
            print(f"  ✓ дубль удалён: {src}")

        # 3) version.json: missing_analysis_files (атомарно)
        for op in vjs:
            vj = op["path"]
            try:
                data = json.loads(vj.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as e:
                print(f"  ! version.json невалиден, пропуск: {vj} ({e})")
                continue
            rel, sha_old = _backup(vj)
            journal.write({"t": "vj", "phase": "pre", "path": str(vj),
                           "backup": rel, "sha_old": sha_old})
            miss = data.get("missing_analysis_files") or []
            data["missing_analysis_files"] = [RENAME_MAP.get(m, m) for m in miss]
            _atomic_write_json(vj, data)
            journal.write({"t": "vj", "phase": "post", "path": str(vj),
                           "sha_new": _sha256(vj)})
            n_vj += 1
    finally:
        journal.write({"t": "done", "renames": n_ren, "dup": n_dup, "version_json": n_vj})
        journal.close()

    print(f"\n[apply] backup+journal: {backup_root}")
    print(f"[apply] Итого: renames={n_ren} dup_cleanup={n_dup} version_json={n_vj} "
          f"conflicts={len(conflicts)}")
    return 1 if conflicts else 0


# ─────────────────────────── rollback ───────────────────────────

def do_rollback(backup_dir: Path) -> int:
    jpath = backup_dir / "journal.jsonl"
    if not jpath.is_file():
        print(f"[rollback] ОТКАЗ: нет journal.jsonl в {backup_dir}")
        return 2
    entries = []
    for line in jpath.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    # sha_new по (new/path) из post-записей — для hash-проверки
    sha_new = {}
    for e in entries:
        if e.get("phase") == "post":
            key = e.get("new") or e.get("path")
            if key and "sha_new" in e:
                sha_new[key] = e["sha_new"]

    restored = removed = skipped = 0
    # Идём в обратном порядке pre-записей
    for e in reversed([e for e in entries if e.get("phase") == "pre"]):
        t = e.get("t")
        backup_rel = e.get("backup")
        backup_file = backup_dir / backup_rel if backup_rel else None

        if t == "rename":
            old_p, new_p = Path(e["old"]), Path(e["new"])
            # Удалить canonical (new), только если он == тому, что произвела миграция
            if new_p.exists():
                expected = sha_new.get(str(new_p))
                if expected is not None and _sha256(new_p) != expected:
                    print(f"  ! пропуск удаления {new_p}: содержимое изменилось после миграции")
                    skipped += 1
                    continue
                new_p.unlink()
                removed += 1
            if backup_file and backup_file.is_file():
                old_p.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(backup_file, old_p)
                restored += 1
        elif t == "dup":
            removed_p = Path(e["removed"])
            if backup_file and backup_file.is_file() and not removed_p.exists():
                shutil.copy2(backup_file, removed_p)
                restored += 1
        elif t == "vj":
            vj = Path(e["path"])
            expected = sha_new.get(str(vj))
            if vj.exists() and expected is not None and _sha256(vj) != expected:
                print(f"  ! пропуск восстановления {vj}: изменён после миграции")
                skipped += 1
                continue
            if backup_file and backup_file.is_file():
                shutil.copy2(backup_file, vj)
                restored += 1

    print(f"[rollback] восстановлено={restored} удалено_canonical={removed} пропущено={skipped}")
    return 0


# ─────────────────────────── dry-run / main ───────────────────────────

def do_dry_run(root: Path) -> int:
    renames, dups, conflicts, vjs = _collect(root)
    print(f"[rename_stage_artifacts] root={root} apply=False")
    print(f"  переименований: {len(renames)}")
    print(f"  дубль-очисток (цель==исходник): {len(dups)}")
    print(f"  version.json к правке: {len(vjs)}")
    print(f"  КОНФЛИКТОВ (цель!=исходник, пропуск): {len(conflicts)}")
    for c in conflicts[:20]:
        print(f"    ! {c['src']}  ->  цель уже существует и отличается")
    print("\nDRY-RUN: изменений не внесено. Повторите с --apply для выполнения.")
    return 1 if conflicts else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Rename stage artifacts on disk (dry-run by default).")
    ap.add_argument("--root", default=".", help="Корень репозитория (по умолчанию текущий).")
    ap.add_argument("--apply", action="store_true", help="Выполнить (иначе dry-run).")
    ap.add_argument("--rollback", metavar="BACKUP_DIR", default=None,
                    help="Откатить миграцию по journal из указанного backup-каталога.")
    ap.add_argument("--ts", default=None, help="Штамп времени backup-папки (иначе UTC now).")
    args = ap.parse_args()

    if args.rollback:
        return do_rollback(Path(args.rollback).resolve())

    root = Path(args.root).resolve()
    if not args.apply:
        return do_dry_run(root)

    ts = args.ts or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return do_apply(root, ts)


if __name__ == "__main__":
    sys.exit(main())
