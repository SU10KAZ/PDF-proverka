#!/usr/bin/env python3
"""Эвакуация локальных PNG-кропов блоков из ИСТОРИЧЕСКИХ прогонов.

Кропы занимают ~12 ГБ и полностью воспроизводимы из ``02_work/document.pdf``
по координатам блока (замер 2026-08-03: локальный ре-рендер даёт ровно
``render_size`` из index.json). Скрипт удаляет только то, что доказанно
восстановимо, оставляя ``index.json`` и sidecar ``crops_evicted.json``.

Порядок команд намеренно ступенчатый::

    python scripts/projects_v2/evict_block_crops.py scan   --json /tmp/evict.json
    python scripts/projects_v2/evict_block_crops.py plan   --report /tmp/evict.json
    python scripts/projects_v2/evict_block_crops.py verify --report /tmp/evict.json --sample 3
    python scripts/projects_v2/evict_block_crops.py apply  --report /tmp/evict.json \
        --confirm EVICT_BLOCK_CROPS_RUNS_ONLY --max-bytes 1G

Что защищено безусловно
-----------------------
* **Живой путь чтения.** Кандидат отбрасывается, если попадает в
  ``ProjectsV2Adapter.resolved_blocks_dirs()`` — набор папок, до которых
  способно дотянуться чтение. Пересечение прерывает ВЕСЬ запуск, а не пропускает
  версию. Считать «runs/* — это история» нельзя: у 183 из 440 версий index
  блоков есть ТОЛЬКО в runs/, то есть именно run-папка обслуживает UI.
* **``03_analysis/latest``** — никогда, без флага-обхода.
* **Версии с человеческими вердиктами** в ``04_review``: их доказательная база
  должна оставаться пиксельно той же.
* **Идущие аудиты**: стадия в ``running`` или свежая (< 1 ч) папка кропов.
* **Невоспроизводимые блоки**: ``promoted_to_full``, ``compact``, наличие
  ``_full.png``, отсутствие ``crop_px``/размеров страницы/локального PDF.
  Перед удалением каждый блок проходит КОНТРОЛЬНЫЙ ре-рендер — это превращает
  «мы считаем, что восстановимо» в «мы это только что проверили».

Удаление делается переносом в ``.evicted/`` внутри той же папки, а не unlink:
ошибки первой недели обратимы обычным ``mv``.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

CONFIRM = "EVICT_BLOCK_CROPS_RUNS_ONLY"
MIN_AGE_S = 3600


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


sys.path.insert(0, str(_repo_root()))

from backend.app.services.common import block_crop_store  # noqa: E402
from backend.app.services.storage.projects_v2_adapter import (  # noqa: E402
    ProjectsV2Adapter,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_size(raw: str | None) -> int | None:
    if not raw:
        return None
    text = str(raw).strip().upper()
    mult = 1
    if text.endswith("G"):
        mult, text = 1024 ** 3, text[:-1]
    elif text.endswith("M"):
        mult, text = 1024 ** 2, text[:-1]
    elif text.endswith("K"):
        mult, text = 1024, text[:-1]
    return int(float(text) * mult)


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
    for rel in ("03_analysis/latest/pipeline_log.json", "99_service/pipeline_log.json"):
        path = version_dir / rel
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return True
        stages = data.get("stages") or {}
        if any((st or {}).get("status") == "running" for st in stages.values()):
            return True
    return False


def _has_human_verdicts(version_dir: Path) -> bool:
    review = version_dir / "04_review"
    if not review.is_dir():
        return False
    return any(p.is_file() and p.stat().st_size > 2 for p in review.rglob("*.json"))


def _protected_dirs(version_dir: Path) -> set[Path]:
    """Папки кропов, до которых способно дотянуться чтение."""
    try:
        doc_dir = version_dir.parent.parent
        version_id = version_dir.name
        return ProjectsV2Adapter().resolved_blocks_dirs(doc_dir, version_id)
    except Exception:  # noqa: BLE001 — не смогли посчитать → защищаем всё
        return {p.resolve() for p in version_dir.rglob("blocks*") if p.is_dir()}


def _candidate_dirs(version_dir: Path) -> list[Path]:
    """Папки кропов ТОЛЬКО в historical runs (latest исключён безусловно)."""
    runs = version_dir / "03_analysis" / "runs"
    if not runs.is_dir():
        return []
    out: list[Path] = []
    for run in sorted(p for p in runs.iterdir() if p.is_dir()):
        for name in ("blocks_stage02_100", "blocks_gemma_100", "blocks_gemma_300", "blocks"):
            bd = run / name
            if (bd / "index.json").is_file():
                out.append(bd)
    return out


def _scan(
    root: Path, object_filter: str | None, *, allow_versions_with_verdicts: bool = False
) -> dict:
    now = time.time()
    report = {
        "schema": 1,
        "generated_at": _utc_now(),
        "root": str(root),
        "versions_total": 0,
        "skipped": {"busy": 0, "human_verdicts": 0, "fresh": 0, "protected": 0},
        "overlap_abort": [],
        "candidates": [],
        "totals": {"dirs": 0, "evictable": 0, "kept": 0, "bytes": 0},
    }
    for version_dir in _version_dirs(root, object_filter):
        report["versions_total"] += 1
        if _has_running_stage(version_dir):
            report["skipped"]["busy"] += 1
            continue
        if not allow_versions_with_verdicts and _has_human_verdicts(version_dir):
            # По умолчанию версии с вердиктами эксперта не трогаем совсем.
            # На живом дереве это 346 из 440 версий, поэтому есть осознанный
            # опт-аут: вердикты ссылаются на findings из latest, а кропы
            # ИСТОРИЧЕСКИХ прогонов их доказательной базой не являются.
            report["skipped"]["human_verdicts"] += 1
            continue
        protected = _protected_dirs(version_dir)
        for bd in _candidate_dirs(version_dir):
            resolved = bd.resolve()
            if "latest" in resolved.parts:
                report["skipped"]["protected"] += 1
                continue
            if resolved in protected:
                # Живой путь чтения — в план не берём. Это ШТАТНЫЙ пропуск
                # (на живом дереве таких папок 628), а не аварийная ситуация:
                # прерывать запуск нужно, если пересечение всплывёт у кандидата
                # УЖЕ В ПЛАНЕ — это проверяется заново в apply.
                report["skipped"]["protected"] += 1
                continue
            # Свежесть считаем по index.json, а не по mtime папки: индекс пишет
            # САМ этап кропа в конце прохода, поэтому «index новее часа» —
            # прямой признак недавнего кропа. mtime папки же сдвигает любой
            # временный файл, включая наши собственные проверки.
            try:
                if now - (bd / "index.json").stat().st_mtime < MIN_AGE_S:
                    report["skipped"]["fresh"] += 1
                    continue
            except OSError:
                continue
            r = block_crop_store.evict_blocks_dir(bd, dry_run=True, verify_render=False)
            if not r.evicted:
                continue
            report["candidates"].append(
                {
                    "blocks_dir": str(bd),
                    "version_dir": str(version_dir),
                    "evictable": r.evicted,
                    "kept": r.kept,
                    "bytes": r.freed_bytes,
                }
            )
            report["totals"]["dirs"] += 1
            report["totals"]["evictable"] += r.evicted
            report["totals"]["kept"] += r.kept
            report["totals"]["bytes"] += r.freed_bytes
    return report


def cmd_scan(args: argparse.Namespace) -> int:
    report = _scan(
        Path(args.root).resolve(),
        args.object,
        allow_versions_with_verdicts=args.allow_versions_with_verdicts,
    )
    t = report["totals"]
    print(f"Версий просмотрено:        {report['versions_total']}")
    print(f"Пропущено (идёт аудит):    {report['skipped']['busy']}")
    print(f"Пропущено (есть вердикты): {report['skipped']['human_verdicts']}")
    print(f"Пропущено (свежие):        {report['skipped']['fresh']}")
    print(f"Пропущено (живой путь):    {report['skipped']['protected']}")
    print(f"Папок-кандидатов:          {t['dirs']}")
    print(f"Блоков к эвакуации:        {t['evictable']} (оставить {t['kept']})")
    print(f"Освободится:               {t['bytes'] / 1024 ** 3:.2f} ГБ")
    if args.json:
        Path(args.json).write_text(
            json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8"
        )
        print(f"Отчёт: {args.json}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    rows = sorted(report["candidates"], key=lambda c: -c["bytes"])
    print(f"{'МБ':>8}  {'блоков':>7}  {'оставить':>8}  папка")
    for c in rows[: args.limit]:
        print(
            f"{c['bytes'] / 1024 ** 2:8.1f}  {c['evictable']:7d}  {c['kept']:8d}  "
            f"{Path(c['blocks_dir']).relative_to(report['root'])}"
        )
    if len(rows) > args.limit:
        print(f"... и ещё {len(rows) - args.limit} папок")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Контрольный ре-рендер выборки блоков из каждой папки-кандидата."""
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))
    failed: list[str] = []
    checked = 0
    for c in report["candidates"]:
        bd = Path(c["blocks_dir"])
        sidecar = block_crop_store.build_sidecar(bd)
        policy = sidecar["policy"]
        kept = set(sidecar.get("kept_block_ids") or [])
        sample = [b for b in sidecar["blocks"] if b not in kept][: args.sample]
        for block_id in sample:
            checked += 1
            ok = block_crop_store._verify_restorable(bd, sidecar["blocks"][block_id], policy)
            if not ok:
                failed.append(f"{bd}::{block_id}")
    print(f"Проверено блоков: {checked}, не восстановились: {len(failed)}")
    for item in failed[:20]:
        print(f"  FAIL {item}")
    verdict = {"checked": checked, "failed": failed, "verified_at": _utc_now()}
    report["verification"] = verdict
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    return 1 if failed else 0


def cmd_apply(args: argparse.Namespace) -> int:
    if args.confirm != CONFIRM:
        print(f"[ОТКАЗ] Нужна точная фраза --confirm {CONFIRM}", file=sys.stderr)
        return 2
    report = json.loads(Path(args.report).read_text(encoding="utf-8"))

    # Zero-overlap guard ПЕРЕСЧИТЫВАЕТСЯ здесь, а не берётся из отчёта: между
    # scan и apply мог пройти новый прогон, и папка-кандидат могла стать живым
    # путём чтения. Любое пересечение прерывает ВЕСЬ запуск, а не пропускает
    # версию — иначе легко не заметить, что защита сработала.
    overlaps: list[str] = []
    protected_by_version: dict[str, set] = {}
    for c in report["candidates"]:
        vdir = Path(c["version_dir"])
        key = str(vdir)
        if key not in protected_by_version:
            protected_by_version[key] = _protected_dirs(vdir)
        if Path(c["blocks_dir"]).resolve() in protected_by_version[key]:
            overlaps.append(c["blocks_dir"])
    if overlaps:
        print(
            f"[ОТКАЗ] {len(overlaps)} папок из плана оказались живым путём чтения "
            f"— запуск прерван целиком",
            file=sys.stderr,
        )
        for p in overlaps[:5]:
            print(f"  {p}", file=sys.stderr)
        return 3

    verification = report.get("verification")
    if not verification:
        print("[ОТКАЗ] Сначала выполните verify", file=sys.stderr)
        return 4
    failed_dirs = {item.split("::")[0] for item in verification.get("failed", [])}
    if failed_dirs:
        print(f"[ВНИМАНИЕ] {len(failed_dirs)} папок не прошли verify — они будут пропущены")

    budget = _parse_size(args.max_bytes)
    freed = 0
    evicted = 0
    kept = 0
    skipped = 0
    for c in report["candidates"]:
        bd = Path(c["blocks_dir"])
        if str(bd) in failed_dirs:
            skipped += 1
            continue
        if budget is not None and freed >= budget:
            print(f"Достигнут лимит --max-bytes, остановка")
            break
        if not (bd / "index.json").is_file():
            skipped += 1
            continue
        r = block_crop_store.evict_blocks_dir(
            bd,
            dry_run=False,
            evicted_by="scripts/projects_v2/evict_block_crops.py",
            verify_render=True,
        )
        evicted += r.evicted
        kept += r.kept
        freed += r.freed_bytes
        if r.skipped_reason:
            skipped += 1

    print(f"Эвакуировано блоков: {evicted}")
    print(f"Оставлено:           {kept}")
    print(f"Пропущено папок:     {skipped}")
    print(f"Освобождено:         {freed / 1024 ** 3:.2f} ГБ")
    print("Файлы перенесены в <папка>/.evicted/ — окончательное удаление отдельным шагом")

    receipt_path = Path(args.root).resolve() / "_system" / "destructive_confirmations.jsonl"
    try:
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with receipt_path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "op": "evict_block_crops",
                        "at": _utc_now(),
                        "evicted": evicted,
                        "kept": kept,
                        "freed_bytes": freed,
                        "confirm": CONFIRM,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        print(f"Журнал: {receipt_path}")
    except OSError as exc:
        print(f"[WARN] Журнал не записан: {exc}", file=sys.stderr)
    return 0


def cmd_purge(args: argparse.Namespace) -> int:
    """Окончательно удалить содержимое .evicted/ старше N дней."""
    root = Path(args.root).resolve()
    cutoff = time.time() - args.older_than_days * 86400
    freed = 0
    dirs = 0
    for trash in root.rglob(".evicted"):
        if not trash.is_dir():
            continue
        try:
            if trash.stat().st_mtime > cutoff:
                continue
        except OSError:
            continue
        size = sum(f.stat().st_size for f in trash.rglob("*") if f.is_file())
        if not args.apply:
            freed += size
            dirs += 1
            continue
        shutil.rmtree(trash, ignore_errors=True)
        freed += size
        dirs += 1
    mode = "УДАЛЕНО" if args.apply else "БУДЕТ УДАЛЕНО (dry-run)"
    print(f"{mode}: {dirs} папок .evicted, {freed / 1024 ** 3:.2f} ГБ")
    return 0


def main(argv: list[str] | None = None) -> int:
    default_root = _repo_root() / "projects_v2"
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", default=str(default_root))
    parser.add_argument("--object", default=None, help="фильтр по имени объекта")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("scan", help="отчёт о кандидатах, ничего не меняет")
    p.add_argument("--json", default=None)
    p.add_argument(
        "--allow-versions-with-verdicts",
        action="store_true",
        help="не пропускать версии с вердиктами эксперта (на живом дереве это "
             "разница между 0.10 ГБ и 3.43 ГБ; защита живого пути чтения остаётся)",
    )
    p.set_defaults(func=cmd_scan)

    p = sub.add_parser("plan", help="показать план из отчёта")
    p.add_argument("--report", required=True)
    p.add_argument("--limit", type=int, default=40)
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("verify", help="контрольный ре-рендер выборки")
    p.add_argument("--report", required=True)
    p.add_argument("--sample", type=int, default=3)
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("apply", help="эвакуировать (требует verify и --confirm)")
    p.add_argument("--report", required=True)
    p.add_argument("--confirm", default="")
    p.add_argument("--max-bytes", default=None, help="например 1G")
    p.set_defaults(func=cmd_apply)

    p = sub.add_parser("purge", help="окончательно удалить .evicted/ старше N дней")
    p.add_argument("--older-than-days", type=int, default=14)
    p.add_argument("--apply", action="store_true")
    p.set_defaults(func=cmd_purge)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
