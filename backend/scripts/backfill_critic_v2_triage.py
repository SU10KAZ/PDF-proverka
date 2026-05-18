#!/usr/bin/env python3
"""
backfill_critic_v2_triage.py
----------------------------
Offline backfill: пробежать по проектам с готовым 03_findings.json и записать
project-local critic_v2/* artifacts (см. critic_v2_triage stage runner).

Безопасность:
    * НЕ модифицирует 03_findings.json / 03_findings_review.json /
      expert_review.json / production artifacts.
    * НЕ запускает LLM (флаг --no-llm установлен по умолчанию).
    * Пишет ТОЛЬКО в <project>/_output/<output-subdir>/ (default critic_v2).
    * Пропускает проекты без 03_findings.json или без чего-то нужного.

CLI:
    python backend/scripts/backfill_critic_v2_triage.py \\
        --project /path/to/project1 [--project /path/to/project2 ...]

    python backend/scripts/backfill_critic_v2_triage.py \\
        --projects-root /path/to/projects [--section EOM] \\
        --profile conservative --no-llm

Exit codes:
    0 — все процессы прошли (в т.ч. dry-run).
    1 — нет проектов с 03_findings.json / нет валидного input.
    2 — runner ошибся хотя бы на одном проекте (без --force).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Iterable

# Поддержка запуска как `python backend/scripts/...` из корня репозитория.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.pipeline.stages.critic_v2_triage import (  # noqa: E402
    CriticV2TriageStageResult,
    run_critic_v2_triage,
)
from backend.app.pipeline.stages.critic_v2_triage.runner import ARTIFACT_STAGE_SUMMARY  # noqa: E402

logger = logging.getLogger("backfill_critic_v2_triage")


def _iter_projects_from_root(
    projects_root: Path, section: str | None = None
) -> Iterable[Path]:
    """Найти все проекты с _output/03_findings.json под projects_root.

    Поддерживаемые layouts:
      1. projects_root/<project_name>                         (legacy flat)
      2. projects_root/<SECTION>/<project_name>               (section-grouped)
      3. projects_root/<OBJECT>/<SECTION>/<project_name>      (multi-object)

    Фильтр section применяется к самому нижнему контейнеру (имя папки-родителя
    проекта). Не зависит от layout.
    """
    if not projects_root.exists():
        return []
    sec_filter = section.strip().lower() if section else None
    seen: set[Path] = set()

    def _emit(p: Path) -> Iterable[Path]:
        findings = p / "_output" / "03_findings.json"
        if not findings.exists():
            return
        rp = p.resolve()
        if rp in seen:
            return
        if sec_filter and p.parent.name.lower() != sec_filter:
            return
        seen.add(rp)
        yield p

    # Layout 1: projects_root/<project>/_output/...
    for p in projects_root.iterdir():
        if not p.is_dir() or p.name.startswith("_"):
            continue
        yield from _emit(p)

        # Layout 2: projects_root/<SECTION_or_OBJECT>/<project>/_output/...
        try:
            children = list(p.iterdir())
        except OSError:
            continue
        for sub in children:
            if not sub.is_dir() or sub.name.startswith("_"):
                continue
            yield from _emit(sub)

            # Layout 3: projects_root/<OBJECT>/<SECTION>/<project>/_output/...
            try:
                gchildren = list(sub.iterdir())
            except OSError:
                continue
            for sub2 in gchildren:
                if not sub2.is_dir() or sub2.name.startswith("_"):
                    continue
                yield from _emit(sub2)


def _check_writeable(project_dir: Path, output_subdir: str) -> str | None:
    """Sanity check — можно ли вообще писать в _output/<subdir>/."""
    target = project_dir / "_output" / output_subdir
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return f"cannot create artifacts dir {target}: {exc}"
    if not target.is_dir():
        return f"artifacts dir {target} is not a directory"
    return None


def _has_existing_artifacts(project_dir: Path, output_subdir: str) -> bool:
    """Есть ли уже stage_summary.json (значит, backfill уже отработал)?"""
    return (project_dir / "_output" / output_subdir / ARTIFACT_STAGE_SUMMARY).exists()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Offline backfill для critic_v2_triage stage. "
                    "Запускается per-project, не трогает production artifacts."
    )
    p.add_argument(
        "--project", action="append", default=[],
        help="Путь к проекту (можно указать несколько раз)."
    )
    p.add_argument(
        "--projects-root", type=Path, default=None,
        help="Корневая папка проектов (например projects/). "
             "Скрипт найдёт все проекты с _output/03_findings.json."
    )
    p.add_argument(
        "--section", default=None,
        help="Фильтр по разделу (например EOM, AR). Применяется только "
             "при --projects-root."
    )
    p.add_argument(
        "--profile", default="conservative",
        help="Triage profile: conservative|assisted|aggressive|assisted_round1|"
             "assisted_round2_candidate."
    )
    p.add_argument(
        "--no-llm", action="store_true", default=True,
        help="Не вызывать LLM (default). Сохранено для явности."
    )
    p.add_argument(
        "--llm", dest="no_llm", action="store_false",
        help="Разрешить LLM gate (в текущей версии stage runner всё равно "
             "пропускает LLM — оставлено как явный contract на будущее)."
    )
    p.add_argument(
        "--force", action="store_true",
        help="Перезаписать существующие artifacts (по умолчанию пропускаем "
             "проекты, где critic_v2_stage_summary.json уже есть)."
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Только показать, что будет сделано — ничего не писать."
    )
    p.add_argument(
        "--output-subdir", default="critic_v2",
        help="Подпапка внутри _output/ для artifacts (default: critic_v2)."
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="Включить debug-логирование."
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    # Сбор списка проектов
    projects: list[Path] = []
    for p_arg in args.project:
        path = Path(p_arg).expanduser().resolve()
        if not path.exists():
            logger.error("project not found: %s", path)
            continue
        if not (path / "_output" / "03_findings.json").exists():
            logger.warning("skip %s: no _output/03_findings.json", path)
            continue
        projects.append(path)

    if args.projects_root:
        root = args.projects_root.expanduser().resolve()
        if not root.exists():
            logger.error("projects-root not found: %s", root)
            return 1
        projects.extend(_iter_projects_from_root(root, args.section))

    # Dedup, preserving order
    seen: set[Path] = set()
    unique_projects: list[Path] = []
    for p in projects:
        rp = p.resolve()
        if rp not in seen:
            seen.add(rp)
            unique_projects.append(rp)

    if not unique_projects:
        logger.error(
            "Нет проектов с _output/03_findings.json. "
            "Укажите --project <path> или --projects-root <dir>."
        )
        return 1

    logger.info("Найдено проектов с findings: %d", len(unique_projects))
    logger.info(
        "Параметры: profile=%s llm=%s dry_run=%s force=%s output_subdir=%s",
        args.profile, "off" if args.no_llm else "on (ignored in this runner)",
        args.dry_run, args.force, args.output_subdir,
    )

    results: list[dict] = []
    n_ok = n_skipped = n_failed = 0

    for project_dir in unique_projects:
        rec: dict = {"project_dir": str(project_dir), "status": "?"}

        # Уже есть artifacts → skip без --force
        if _has_existing_artifacts(project_dir, args.output_subdir) and not args.force:
            logger.info("[skip] %s: уже есть %s (используйте --force)",
                        project_dir.name, ARTIFACT_STAGE_SUMMARY)
            rec["status"] = "skipped_existing"
            results.append(rec)
            n_skipped += 1
            continue

        if args.dry_run:
            logger.info("[dry-run] would run for %s", project_dir)
            rec["status"] = "dry_run"
            results.append(rec)
            continue

        # Проверка writeability
        err = _check_writeable(project_dir, args.output_subdir)
        if err:
            logger.error("[fail] %s: %s", project_dir.name, err)
            rec["status"] = "fail_writeability"
            rec["error"] = err
            results.append(rec)
            n_failed += 1
            if not args.force:
                continue
            else:
                continue

        try:
            result: CriticV2TriageStageResult = run_critic_v2_triage(
                project_dir=project_dir,
                output_subdir=args.output_subdir,
                profile=args.profile,
                llm_enabled=not args.no_llm,
                project_id=project_dir.name,
            )
        except Exception as exc:  # noqa: BLE001 — backfill never crashes
            logger.exception("[fail] %s: runner crashed: %s", project_dir.name, exc)
            rec["status"] = "fail_crash"
            rec["error"] = str(exc)
            results.append(rec)
            n_failed += 1
            continue

        if not result.success:
            logger.warning("[fail] %s: %s", project_dir.name, result.error)
            rec["status"] = "fail_runner"
            rec["error"] = result.error
            results.append(rec)
            n_failed += 1
            continue

        rec["status"] = "ok"
        rec["profile"] = result.profile
        rec["findings_total"] = result.findings_total
        rec["triage_total"] = result.triage_total
        rec["artifacts_dir"] = str(result.artifacts_dir) if result.artifacts_dir else None
        results.append(rec)
        n_ok += 1
        logger.info(
            "[ok] %s: %d findings → %d triage decisions (%s)",
            project_dir.name, result.findings_total, result.triage_total, result.profile,
        )

    # Summary
    print("\n=== backfill summary ===")
    print(json.dumps({
        "total": len(unique_projects),
        "ok": n_ok,
        "skipped": n_skipped,
        "failed": n_failed,
        "dry_run": args.dry_run,
        "profile": args.profile,
        "llm_enabled": not args.no_llm,
        "output_subdir": args.output_subdir,
        "results": results,
    }, ensure_ascii=False, indent=2))

    if n_failed > 0 and not args.force:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
