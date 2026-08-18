#!/usr/bin/env python3
"""Страж происхождения прод-кода: выкатывать можно только опубликованный коммит.

Зачем это существует
────────────────────
18.08.2026 подряд случилось два одинаковых инцидента. Каноническая ветка была
сведена и запушена, после чего параллельная сессия закоммитила правки локально,
собрала из них релиз центра и переключила `current` — и боевой код стал жить в
коммите, которого нет ни на origin, ни в рабочем дереве, а только в клоне под
`/tmp`. Первый раз это был `78199ef7`, второй — `08666e4d`. Оба раза
восстановление стоило отдельной сессии, и оба раза потеря `/tmp` уничтожила бы
исходник работающего прода.

Дисциплина «сначала запушь» не работает: она держится на внимательности
оператора, а оператор в этот момент занят инцидентом. Поэтому здесь запрет
технический — до сборки и до переключения релиза.

Что именно требуется
────────────────────
1. Удалённая ветка проверяется СЕЙЧАС (`git fetch`), а не по устаревшему
   remote-tracking ref. Иначе страж подтвердит достижимость по снимку,
   сделанному вчера, и снова пропустит неопубликованный коммит.
2. Коммит-источник обязан быть ДОСТИЖИМ из канонической удалённой ветки.
   Проверка именно на достижимость, а не на равенство: прод часто отстаёт от
   головы ветки на несколько коммитов, и это нормально.
3. Дерево-источник обязано быть чистым по ОТСЛЕЖИВАЕМЫМ файлам. Это не
   формальность: `scripts/deploy_audit_worker.py` собирает bundle из РАБОЧЕГО
   ДЕРЕВА (по allowlist'у отслеживаемых путей), поэтому изменённый, но не
   закоммиченный файл уехал бы на воркер, не оставив следа ни в одном коммите.
4. Неотслеживаемые файлы внутри кодовых каталогов — тоже отказ. Сами по себе в
   релиз они не попадают (`git archive` и `tracked_only=True` их не берут),
   но их присутствие означает несохранённую работу в сборочном дереве, а
   значит — расхождение между тем, что проверено, и тем, что человек считает
   собранным. Артефакты аудита, отчёты и кэши инструментов в кодовые каталоги
   не лезут и разрешены явным списком.

Чего страж НЕ делает
────────────────────
Не мешает грязной РАЗРАБОТКЕ. Рабочий корень
`/home/coder/projects/PDF-proverka` может быть сколь угодно грязным — страж
срабатывает только на пути production build/switch/restart, где дерево
обязано быть коммитом.

Не подменяет замок выкатки (`scripts/deploy_lock.py`). Порядок обязателен и
именно такой:

    fetch → страж происхождения → страж чистоты → тесты/проверки релиза
    → замок выкатки → гейт живой работы → мутация прода

Сначала дешёвые и безопасные отказы, и только потом захват общего ресурса:
падать после взятия замка значит держать чужую выкатку в отказе на время
собственной диагностики.

Использование
─────────────
    # из дерева-источника
    python scripts/production_source_guard.py
    python scripts/production_source_guard.py --commit <sha> --json
    python scripts/production_source_guard.py --repo /path/to/tree

Код возврата: 0 — можно выкатывать; 4 — PRODUCTION_SOURCE_NOT_CANONICAL.
Четвёрка выбрана намеренно: у установщика шлюза 4 уже означает
PRECHECK_FAILED — «живой прод не тронут».
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence

#: Каноническая ветка прод-истины и remote, на котором она обязана лежать.
#: Значения переопределяются окружением ради тестов и ради второй инсталляции,
#: но НЕ ради обхода: подмена ветки видна в квитанции стража.
CANONICAL_REMOTE = os.environ.get("AUDITMANAGER_CANONICAL_PRODUCTION_REMOTE", "origin")
CANONICAL_BRANCH = os.environ.get(
    "AUDITMANAGER_CANONICAL_PRODUCTION_BRANCH", "feature/block-vector-graphs"
)

#: Каталоги и файлы, попадающие в прод-артефакты (дерево релиза центра =
#: `git archive`, bundle воркера = allowlist отслеживаемых путей). Появление
#: здесь неотслеживаемого файла — отказ.
BUILD_RELEVANT_PREFIXES: tuple[str, ...] = (
    "audit_worker/",
    "backend/",
    "contracts/",
    "deploy/",
    "disciplines/",
    "frontend/",
    "norms/",
    "scripts/",
    "tests/",
    "tools/",
)

#: Файлы в корне, которые тоже едут в прод.
BUILD_RELEVANT_ROOT_FILES: tuple[str, ...] = (
    "blocks.py",
    "norms.py",
    "process_project.py",
    "query_project.py",
    "generate_excel_report.py",
    "pytest.ini",
    "requirements.txt",
    "requirements-gateway.txt",
    ".env.example",
)

#: Неотслеживаемое, что заведомо не влияет ни на сборку, ни на рантайм:
#: рабочие материалы аудита, отчёты, кэши инструментов. Список узкий и
#: пополняется осознанно — «разрешить всё в docs/» здесь было бы дырой.
HARMLESS_UNTRACKED_PREFIXES: tuple[str, ...] = (
    ".claude/",
    ".pytest_cache/",
    ".ruff_cache/",
    "deliverables/",
    "docs/",
    "experiments/",
    "logs/",
    "node_modules/",
    "projects/",
    "projects_v2/",
    "knowledge_base/",
    "comparison_sources/",
)

#: Расширения безобидных одиночных файлов в корне (рабочие заметки, трекеры).
HARMLESS_UNTRACKED_SUFFIXES: tuple[str, ...] = (".md", ".xlsx", ".log", ".patch")


class ProductionSourceNotCanonical(RuntimeError):
    """Источник прод-сборки не опубликован в канонической ветке.

    Текст всегда начинается с `PRODUCTION_SOURCE_NOT_CANONICAL:` — по этому
    префиксу отказ узнаётся и в логах выкатки, и в тестах.
    """

    def __init__(self, reason: str, detail: str) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"PRODUCTION_SOURCE_NOT_CANONICAL [{reason}]: {detail}")


def _git(repo: Path, *args: str, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=str(repo), capture_output=True, text=True
    )
    if check and proc.returncode != 0:
        raise ProductionSourceNotCanonical(
            "git_failed",
            f"git {' '.join(args)} → код {proc.returncode}: "
            f"{(proc.stderr or proc.stdout).strip()[:400]}",
        )
    return proc.stdout.strip()


def _is_build_relevant(rel: str) -> bool:
    """Влияет ли путь на прод-артефакт."""
    if rel in BUILD_RELEVANT_ROOT_FILES:
        return True
    return any(rel.startswith(p) for p in BUILD_RELEVANT_PREFIXES)


def _is_harmless_untracked(rel: str, *, allow: Sequence[str] = ()) -> bool:
    for pattern in allow:
        if rel == pattern or rel.startswith(pattern.rstrip("/") + "/"):
            return True
    if any(rel.startswith(p) for p in HARMLESS_UNTRACKED_PREFIXES):
        # Кодовый каталог внутри разрешённого префикса всё равно опасен:
        # `docs/` безобиден, а `scripts/` внутри него не бывает.
        return True
    if "/" not in rel and rel.endswith(HARMLESS_UNTRACKED_SUFFIXES):
        return True
    if "/" not in rel and rel.startswith(".tmp"):
        return True
    return False


def _untracked(repo: Path) -> list[str]:
    raw = subprocess.run(
        ["git", "status", "--porcelain", "-z", "--untracked-files=normal"],
        cwd=str(repo), capture_output=True,
    ).stdout
    out: list[str] = []
    for rec in raw.split(b"\0"):
        if len(rec) < 4:
            continue
        status, path = rec[:2].decode(), rec[3:].decode("utf-8", "replace")
        if status.strip() == "??":
            out.append(path)
    return out


def verify_clean_source_tree(
    repo: Path, *, allow_untracked: Sequence[str] = ()
) -> dict[str, object]:
    """Дерево-источник обязано быть коммитом, а не «коммит плюс правки»."""
    if subprocess.run(["git", "diff", "--quiet"], cwd=str(repo)).returncode != 0:
        changed = _git(repo, "diff", "--name-only").splitlines()
        raise ProductionSourceNotCanonical(
            "dirty_worktree",
            "в дереве-источнике есть НЕзакоммиченные правки отслеживаемых "
            f"файлов ({len(changed)}): {', '.join(changed[:10])}. "
            "Сборка воркера читает рабочее дерево — эти байты уехали бы в прод "
            "мимо любого коммита.",
        )
    if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(repo)).returncode != 0:
        staged = _git(repo, "diff", "--cached", "--name-only").splitlines()
        raise ProductionSourceNotCanonical(
            "staged_changes",
            f"в индексе дерева-источника есть незакоммиченное ({len(staged)}): "
            f"{', '.join(staged[:10])}",
        )
    untracked = _untracked(repo)
    blocking = [
        p for p in untracked
        if _is_build_relevant(p) and not _is_harmless_untracked(p, allow=allow_untracked)
    ]
    if blocking:
        raise ProductionSourceNotCanonical(
            "untracked_build_files",
            "в кодовых каталогах дерева-источника лежат неотслеживаемые файлы "
            f"({len(blocking)}): {', '.join(blocking[:10])}. Либо закоммитьте "
            "их, либо уберите из сборочного дерева.",
        )
    return {"untracked_total": len(untracked), "untracked_blocking": 0}


def verify_production_source(
    repo: Path | str = ".",
    *,
    commit: Optional[str] = None,
    remote: str = "",
    branch: str = "",
    fetch: bool = True,
    require_clean: bool = True,
    allow_untracked: Sequence[str] = (),
) -> dict[str, object]:
    """Разрешить прод-сборку/выкатку или отказать ДО любой мутации.

    Возвращает квитанцию для release-manifest / deploy-receipt. Бросает
    `ProductionSourceNotCanonical` при любом сомнении: страж обязан падать
    закрыто, потому что цена ложного разрешения — снова прод из `/tmp`.
    """
    repo = Path(repo).resolve()
    remote = remote or CANONICAL_REMOTE
    branch = branch or CANONICAL_BRANCH
    ref = f"refs/remotes/{remote}/{branch}"

    source = _git(repo, "rev-parse", commit or "HEAD")
    tree = _git(repo, "rev-parse", f"{source}^{{tree}}")

    clean_report: dict[str, object] = {"checked": False}
    if require_clean:
        clean_report = verify_clean_source_tree(repo, allow_untracked=allow_untracked)
        clean_report["checked"] = True

    if fetch:
        # Свежесть обязательна: без неё страж подтвердит достижимость по
        # позавчерашнему снимку. Недоступность remote — отказ, а не «ну ладно»:
        # доказать публикацию нечем.
        proc = subprocess.run(
            ["git", "fetch", "--quiet", remote,
             f"+refs/heads/{branch}:refs/remotes/{remote}/{branch}"],
            cwd=str(repo), capture_output=True, text=True,
        )
        if proc.returncode != 0:
            raise ProductionSourceNotCanonical(
                "remote_unreachable",
                f"не удалось обновить {remote}/{branch}: "
                f"{(proc.stderr or proc.stdout).strip()[:300]}. Публикацию "
                "коммита подтвердить нечем — выкатка запрещена.",
            )

    remote_head = _git(repo, "rev-parse", "--verify", "--quiet", ref, check=False)
    if not remote_head:
        raise ProductionSourceNotCanonical(
            "canonical_ref_missing",
            f"нет ссылки {ref}: каноническая ветка «{branch}» на remote "
            f"«{remote}» не найдена.",
        )

    reachable = subprocess.run(
        ["git", "merge-base", "--is-ancestor", source, remote_head],
        cwd=str(repo), capture_output=True,
    ).returncode == 0
    if not reachable:
        subject = _git(repo, "log", "-1", "--format=%s", source, check=False)
        raise ProductionSourceNotCanonical(
            "commit_not_published",
            f"коммит {source[:12]} («{subject[:80]}») НЕ достижим из "
            f"{remote}/{branch} ({remote_head[:12]}). Порядок обязателен: "
            "COMMIT → TEST → PUSH в каноническую ветку → BUILD → DEPLOY. "
            "Соберите коммит в каноническую ветку и запушьте, затем повторите.",
        )

    # Не отказ, а сведение: локальная ветка может отставать от remote, и это
    # само по себе прод не ломает — но в квитанции это должно быть видно.
    local_head = _git(repo, "rev-parse", "HEAD")
    behind = _git(
        repo, "rev-list", "--count", f"{local_head}..{remote_head}", check=False
    ) or "0"

    return {
        "guard": "auditmanager.production_source_guard.v1",
        "source_repo": str(repo),
        "source_commit": source,
        "source_tree": tree,
        "source_branch": _git(repo, "rev-parse", "--abbrev-ref", "HEAD", check=False),
        "canonical_remote": remote,
        "canonical_branch": branch,
        "canonical_remote_head": remote_head,
        "reachable_from_canonical_remote": True,
        "local_behind_remote": int(behind),
        "remote_freshly_fetched": bool(fetch),
        "clean_tree": clean_report,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Страж происхождения прод-кода: коммит обязан быть в "
                    "канонической удалённой ветке, дерево — чистым.",
    )
    parser.add_argument("--repo", default=".", help="дерево-источник сборки")
    parser.add_argument(
        "--component", default="", choices=["", "center", "gateway", "worker"],
        help="компонент выкатки — попадает в квитанцию. Обязателен для шлюза: "
             "репозиторного установщика у него ещё нет, и этот CLI — "
             "предписанная точка вызова стража из внешнего (sudo) установщика.",
    )
    parser.add_argument("--commit", default="", help="коммит-источник (умолчание HEAD)")
    parser.add_argument("--remote", default="")
    parser.add_argument("--branch", default="")
    parser.add_argument("--no-fetch", action="store_true",
                        help="ТОЛЬКО для тестов: не обновлять remote")
    parser.add_argument("--skip-clean-check", action="store_true",
                        help="ТОЛЬКО для проверки происхождения без дерева")
    parser.add_argument("--allow-untracked", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        receipt = verify_production_source(
            args.repo,
            commit=args.commit or None,
            remote=args.remote,
            branch=args.branch,
            fetch=not args.no_fetch,
            require_clean=not args.skip_clean_check,
            allow_untracked=args.allow_untracked,
        )
        if args.component:
            receipt["component"] = args.component
    except ProductionSourceNotCanonical as exc:
        print(str(exc), file=sys.stderr)
        return 4
    if args.json:
        print(json.dumps(receipt, ensure_ascii=False, indent=1))
    else:
        print(f"PRODUCTION_SOURCE_CANONICAL=YES "
              f"commit={receipt['source_commit'][:12]} "
              f"{receipt['canonical_remote']}/{receipt['canonical_branch']}="
              f"{str(receipt['canonical_remote_head'])[:12]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
