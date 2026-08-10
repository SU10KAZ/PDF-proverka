#!/usr/bin/env python3
"""Этап 11E — БОЕВОЙ `findings_merge` через ProviderAdapter на пилотном воркере.

Запускается ЛОКАЛЬНО НА ВОРКЕРЕ. SSH — только способ его туда положить и там
стартовать; сам вызов модели идёт из этого процесса в локальный Claude CLI и
никуда больше. Ни одна строка ниже не открывает соединения к центру: продовый
ingress в 11E не включается (§37 задания).

Что здесь настоящее:

  * боевой раннер этапа `backend/app/pipeline/stages/findings_merge/runner.py`
    целиком, вместе со всеми post-merge проходами;
  * боевой сборщик промпта `prompt_builder.build_findings_merge_messages`;
  * боевой профиль дисциплины и боевой шаблон свода;
  * боевая проверка артефакта (`validate_and_repair_json`) и боевая
    нормализация схемы (`normalize_findings_schema`);
  * `ProviderAdapter`, окружение с нуля, отключённые инструменты, stdin;
  * разрешение (`inference_grant`) и журнал вызовов (`inference_ledger`).

Что подставное: оркестратор этапа. `PipelineStageContext` здесь — минимальные
заглушки логирования и учёта. Это НЕ ослабление: контекст принадлежит
`PipelineManager`, а не этапу.

Чего здесь НЕТ и быть не должно (§2, §34, §35, §38 задания): ни block_analysis,
ни text_analysis, ни norm_verify, ни одного downstream-этапа. Вход берётся
готовым.

Режимы:

    --mode fake   поддельный CLI, НОЛЬ обращений к модели. Полная репетиция
                  пути: sandbox, привязка, разрешение, промпт, запись, проверки.
    --mode real   ОДИН настоящий вызов. Требует `--i-confirm-one-real-inference`
                  и выписанного оператором разрешения под это задание.

Ненулевой код возврата = нарушение.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import socket
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

#: ДО первого импорта из `backend`: конфигурация на импорте зовёт `load_dotenv()`
#: и вытянула бы чужие флаги в окружение прогона.
os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")

#: Прогон НЕ оставляет `.pyc` ни в развёрнутом релизе, ни в чужом venv.
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True

STAGE = "findings_merge"
CAPABILITY = "strong_audit"
ARTIFACT = "03_findings.json"
RUN_REPORT = "findings_merge_provider_run.json"

#: Обязательные входы свода: без любого из них этап не имеет права стартовать.
REQUIRED_INPUTS = ("02_text_analysis.json", "01_blocks_analysis.json")

#: Необязательные входы POST-merge проходов. Свод зовёт модель и без них, но
#: детерминированные проходы (лист по странице, подписи блоков) без них молча
#: вырождаются в no-op — а «молча» на воркере недопустимо, поэтому их наличие
#: фиксируется в отчёте отдельно.
OPTIONAL_INPUTS = ("document_graph.json",)


# ═══════════════════════════ Мелкие утилиты ══════════════════════════════════

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stat_facts(path: Path) -> dict[str, Any]:
    """Факты о файле БЕЗ ЧТЕНИЯ его содержимого.

    Отсутствие `sha256` здесь принципиально. Контрольный файл (§25) проверяется
    на «его не читали», а любое вычисление хэша — это чтение, то есть само по
    себе изменение `atime`.
    """
    p = Path(path)
    if not p.exists():
        return {"exists": False}
    st = p.stat()
    return {
        "exists": True,
        "size_bytes": st.st_size,
        "mode": oct(st.st_mode & 0o777),
        "mtime_ns": st.st_mtime_ns,
        "atime_ns": st.st_atime_ns,
        "ctime_ns": st.st_ctime_ns,
        "inode": st.st_ino,
        "content_read": False,
    }


def write_json(path: Path, payload: Any) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def scan_tree_for(root: Path, needles: dict[str, str]) -> dict[str, list[str]]:
    """Где в дереве встречаются искомые строки. Значения в отчёт не попадают."""
    hits: dict[str, list[str]] = {name: [] for name in needles}
    for path in Path(root).rglob("*"):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for name, needle in needles.items():
            if needle and needle in text:
                hits[name].append(str(path.relative_to(root)))
    return hits


# ═══════════════════════════ Раскладка sandbox ═══════════════════════════════

def build_sandbox(
    *, root: Path, task_id: str, inputs_dir: Path, section: str,
    md_name: Optional[str] = None,
) -> dict[str, Any]:
    """Собрать изолированный каталог попытки 11E и положить в него копию входа.

    Раскладка повторяет `audit_runner.isolated_roots`: все корни данных процесса
    конвейера уводятся ВНУТРЬ попытки. Это не декорация — на ней держится
    гарантия §31 и проверка `_assert_output_inside_attempt` в боевом коде.

    Копируется ТОЛЬКО то, что свод действительно читает (§20): два обязательных
    артефакта предыдущих этапов, граф документа для детерминированных post-merge
    проходов и MD (тот же файл, что уже прошёл этап 01) — для индекса «строка MD
    → страница». Ни PDF, ни кропов, ни result.json, ни исторического
    03_findings.json здесь нет.
    """
    job_dir = Path(root) / task_id
    if job_dir.exists():
        raise SystemExit(
            f"каталог попытки уже существует: {job_dir}. Повторный прогон в тот "
            "же каталог запрещён — он затёр бы улики предыдущего."
        )
    for sub in ("metadata", "input", "work", "output", "logs", "runtime", "provider"):
        (job_dir / sub).mkdir(parents=True)

    version_dir = job_dir / "project" / "doc" / "v1"
    output_dir = version_dir / "_output"
    work_dir = version_dir / "02_work"
    output_dir.mkdir(parents=True)
    work_dir.mkdir(parents=True)

    src = Path(inputs_dir)
    copied: dict[str, dict[str, Any]] = {}
    missing_required: list[str] = []
    for name in REQUIRED_INPUTS:
        source = src / name
        if not source.is_file():
            missing_required.append(name)
            continue
        shutil.copy2(source, output_dir / name)
        shutil.copy2(source, job_dir / "input" / name)
        copied[name] = {
            "required": True,
            "sha256": sha256_file(output_dir / name),
            "bytes": (output_dir / name).stat().st_size,
        }
    if missing_required:
        raise SystemExit(
            f"в {src} нет обязательных входов свода: {missing_required}. "
            "Подделывать артефакты этапов запрещено (§3 задания)"
        )

    for name in OPTIONAL_INPUTS:
        source = src / name
        if source.is_file():
            shutil.copy2(source, output_dir / name)
            shutil.copy2(source, job_dir / "input" / name)
            copied[name] = {
                "required": False,
                "sha256": sha256_file(output_dir / name),
                "bytes": (output_dir / name).stat().st_size,
            }
        else:
            copied[name] = {"required": False, "present": False}

    md_files = sorted(p for p in src.glob("*.md") if p.is_file())
    if md_name:
        md_files = [p for p in md_files if p.name == md_name]
    md_dst: Optional[Path] = None
    if len(md_files) > 1:
        raise SystemExit(
            f"в {src} несколько Markdown ({[p.name for p in md_files]}) — "
            "укажите --md-name"
        )
    if md_files:
        md_src = md_files[0]
        md_dst = version_dir / md_src.name
        shutil.copy2(md_src, md_dst)
        # Тот же файл под каноническим именем рабочей копии: post-merge проход
        # ищет `02_work/document.md`, поднимаясь от каталога артефактов.
        shutil.copy2(md_src, work_dir / "document.md")
        shutil.copy2(md_src, job_dir / "input" / md_src.name)
        copied[md_src.name] = {
            "required": False,
            "sha256": sha256_file(md_dst),
            "bytes": md_dst.stat().st_size,
            "role": "markdown документа (индекс «строка MD → страница» в post-merge)",
        }

    project_info = {
        "project_id": f"{section}/11e",
        "name": "11e",
        "section": section,
        "md_file": md_dst.name if md_dst else "",
    }
    write_json(version_dir / "project_info.json", project_info)

    return {
        "job_dir": job_dir,
        "version_dir": version_dir,
        "output_dir": output_dir,
        "project_info": project_info,
        "project_id": project_info["project_id"],
        "copied": copied,
        "md_path": md_dst,
    }


def isolated_roots_env(job_dir: Path, prompts_dir: Path) -> dict[str, str]:
    """Корни данных процесса — те же, что выставляет `audit_runner`."""
    from audit_worker.audit_runner import isolated_roots

    env = dict(isolated_roots(job_dir))
    env["AUDIT_PROMPTS_DIR"] = str(prompts_dir)
    env["AUDIT_DISCIPLINE_PROFILE_STRICT"] = "1"
    return env


# ═══════════════════════════ Подделка CLI (режим fake) ═══════════════════════

_FAKE_ANSWER_TEMPLATE = """#!/bin/bash
JOURNAL={journal}
case "$1" in
  --version) echo "2.1.220 (Claude Code)"; exit 0 ;;
esac
for a in "$@"; do
  if [ "$a" = "auth" ]; then
    echo '{{"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty", "subscriptionType": "max"}}'
    exit 0
  fi
done
STDIN=$(cat)
{{
  echo "ARGV:$*"
  echo "STDIN_BYTES:${{#STDIN}}"
  echo "CWD:$(pwd)"
}} >> "$JOURNAL"
printf '%s' "$STDIN" > {prompt_dump}
python3 - {answer_file} <<'PYEOF'
import json, sys
# Ответ читается ФАЙЛОМ, а не встраивается в исходник: JSON и Python расходятся
# на `null`/`true`, и подделка молча падала бы NameError.
answer = open(sys.argv[1], encoding="utf-8").read()
print(json.dumps({{
    "type": "result", "subtype": "success", "is_error": False,
    "result": answer,
    "usage": {{"input_tokens": 1000, "output_tokens": 500,
               "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}},
    "modelUsage": {{"{reported_model}": {{"inputTokens": 1000}}}},
    "total_cost_usd": 0.0, "num_turns": 1,
}}, ensure_ascii=False))
PYEOF
exit 0
"""


def make_fake_cli(path: Path, journal: Path, prompt_dump: Path,
                  *, reported_model: str, project_id: str) -> Path:
    """Подделка CLI: отвечает сводом-заглушкой из ОДНОГО замечания.

    Замечание намеренно ссылается на источники двух видов (`T-` и `G-`): по нему
    репетиция проверяет и запись артефакта, и post-merge проходы (провенанс,
    нумерацию, подписи блоков), а не только «файл появился».
    """
    answer = {
        "meta": {
            "project_id": project_id,
            "audit_completed": "2026-01-01T00:00:00",
            "total_findings": 1,
            "blocks_analyzed": 0,
            "by_severity": {
                "КРИТИЧЕСКОЕ": 0, "ЭКОНОМИЧЕСКОЕ": 0, "ЭКСПЛУАТАЦИОННОЕ": 0,
                "РЕКОМЕНДАТЕЛЬНОЕ": 1, "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 0,
            },
        },
        "findings": [
            {
                "id": "F-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "репетиция",
                "sheet": None,
                "page": None,
                "problem": "Ответ подделки CLI: обращения к модели не было",
                "description": "Репетиция пути 11E без вызова модели",
                "norm": None,
                "norm_quote": None,
                "solution": "—",
                "risk": "—",
                "source_finding_ids": ["T-001", "G-001"],
                "source_block_ids": [],
                "related_block_ids": [],
                "evidence_text_refs": [],
                "evidence": [],
                "highlight_regions": [],
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    answer_file = path.parent / "fake-answer.json"
    write_json(answer_file, answer)
    path.write_text(
        _FAKE_ANSWER_TEMPLATE.format(
            journal=journal, prompt_dump=prompt_dump,
            answer_file=answer_file, reported_model=reported_model,
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


# ═══════════════════════════ Контекст этапа ══════════════════════════════════

def build_stage_context(sandbox: dict, log_path: Path):
    """Минимальный `PipelineStageContext`: только то, что читает боевой раннер."""
    from backend.app.pipeline.context import PipelineStageContext

    lines: list[str] = []
    pipeline_log: dict[str, Any] = {}
    usage: list[dict[str, Any]] = []

    async def _log(message: str, level: str = "info") -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {level}: {message}"
        lines.append(line)
        print(line, flush=True)
        log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    async def _check_before_launch() -> bool:
        return True

    async def _check_pause() -> bool:
        return True

    async def _wait_for_rate_limit(reason: str, output: str) -> bool:
        # В 11E бюджет вызовов равен одному: «подождём и повторим» означало бы
        # второй заход в модель (§27 задания).
        return False

    def _record_cli_usage(cli_result, stage_name, is_retry: bool = False) -> None:
        usage.append({
            "stage": stage_name,
            "input_tokens": getattr(cli_result, "input_tokens", 0),
            "output_tokens": getattr(cli_result, "output_tokens", 0),
            "cache_creation_tokens": getattr(cli_result, "cache_creation_tokens", 0),
            "cache_read_tokens": getattr(cli_result, "cache_read_tokens", 0),
            "duration_ms": getattr(cli_result, "duration_ms", 0),
            "cost_usd": getattr(cli_result, "cost_usd", 0.0),
            "is_error": getattr(cli_result, "is_error", None),
        })

    def _update_pipeline_log(stage_key: str, status: str, **kwargs) -> None:
        pipeline_log.setdefault(stage_key, []).append({"status": status, **{
            k: v for k, v in kwargs.items() if k != "detail" or v is not None
        }})

    async def _run_subprocess(*args, **kwargs):
        raise RuntimeError("этап 11E не запускает подпроцессов помимо CLI провайдера")

    ctx = PipelineStageContext(
        project_dir=sandbox["version_dir"],
        project_id=sandbox["project_id"],
        output_dir=sandbox["output_dir"],
        log=_log,
        check_before_launch=_check_before_launch,
        check_pause=_check_pause,
        wait_for_rate_limit=_wait_for_rate_limit,
        record_cli_usage=_record_cli_usage,
        update_pipeline_log=_update_pipeline_log,
        run_subprocess=_run_subprocess,
        project_info=sandbox["project_info"],
        version_id="v1",
        job_id="11e",
    )
    return ctx, pipeline_log, usage, lines


def summarize_findings(payload: Optional[dict]) -> dict[str, Any]:
    """Счётчики выхода. Тексты замечаний в отчёт НЕ попадают."""
    data = payload if isinstance(payload, dict) else {}
    findings = data.get("findings") or []
    by_severity: dict[str, int] = {}
    by_category: dict[str, int] = {}
    source_ids: list[str] = []
    ids: list[str] = []
    with_norm = 0
    with_norm_quote = 0
    with_blocks = 0
    with_evidence = 0
    for item in findings:
        if not isinstance(item, dict):
            continue
        ids.append(str(item.get("id") or ""))
        sev = str(item.get("severity") or "НЕИЗВЕСТНО")
        by_severity[sev] = by_severity.get(sev, 0) + 1
        cat = str(item.get("category") or "")
        if cat:
            by_category[cat] = by_category.get(cat, 0) + 1
        for sid in item.get("source_finding_ids") or []:
            source_ids.append(str(sid))
        if item.get("norm"):
            with_norm += 1
        if item.get("norm_quote"):
            with_norm_quote += 1
        if item.get("related_block_ids") or item.get("source_block_ids"):
            with_blocks += 1
        if item.get("evidence") or item.get("evidence_text_refs"):
            with_evidence += 1
    return {
        "findings": len(findings),
        "ids_unique": len(set(ids)) == len(ids),
        "by_severity": by_severity,
        "by_category": by_category,
        "referenced_source_ids": sorted(set(source_ids)),
        "referenced_source_id_count": len(set(source_ids)),
        "with_norm": with_norm,
        "with_norm_quote": with_norm_quote,
        "with_block_links": with_blocks,
        "with_evidence": with_evidence,
        "top_level_keys": sorted(data.keys()),
        "meta_keys": sorted((data.get("meta") or {}).keys()),
    }


# ═══════════════════════════ Основной прогон ═════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(description="11E: боевой findings_merge через ProviderAdapter")
    parser.add_argument("--mode", choices=("fake", "real"), required=False)
    parser.add_argument(
        "--issue-grant", action="store_true",
        help=("ОТДЕЛЬНЫЙ операторский шаг: выписать одно разрешение под задание "
              "и выйти. Прогон разрешений себе не выписывает."),
    )
    parser.add_argument("--grant-ttl-sec", type=float, default=3600.0)
    parser.add_argument("--sandbox-root", required=False)
    parser.add_argument("--worker-root", required=True,
                        help="корень данных 11E-рантайма: политика, разрешения")
    parser.add_argument("--inputs-dir", required=False,
                        help="каталог с копиями входных артефактов этапов 01/02")
    parser.add_argument("--md-name", default=None)
    parser.add_argument("--section", required=False)
    parser.add_argument("--prompts-dir", required=False,
                        help="снимок prompts/ (pipeline + disciplines)")
    parser.add_argument("--task-id", default=None)
    parser.add_argument("--canary", default="/home/coder/provider-auth-canary/DO_NOT_READ.txt")
    parser.add_argument(
        "--forbidden-literal", action="append", default=[],
        help=("Строка, которой не должно быть в ответе модели (обычно маркер "
              "контрольного файла). Передаётся ЗНАЧЕНИЕМ, а не путём: читать "
              "контрольный файл из этого процесса нельзя."),
    )
    parser.add_argument("--timeout-sec", type=float, default=3600.0)
    parser.add_argument("--i-confirm-one-real-inference", action="store_true")
    args = parser.parse_args()

    task_id = args.task_id or f"11e-{uuid.uuid4().hex[:8]}"
    attempt_id = f"{task_id}-a1"

    if args.issue_grant:
        from audit_worker.providers import inference_grant as _grant

        record = _grant.issue(
            Path(args.worker_root), grant_id=f"g-{task_id}", provider="claude",
            task_id=task_id, ttl_sec=args.grant_ttl_sec, max_uses=1,
            note=f"11E {STAGE} real_document",
        )
        print(json.dumps(record.as_public_dict(), ensure_ascii=False, indent=2))
        return 0

    missing = [
        name for name, value in (
            ("--mode", args.mode), ("--sandbox-root", args.sandbox_root),
            ("--inputs-dir", args.inputs_dir), ("--section", args.section),
            ("--prompts-dir", args.prompts_dir),
        ) if not value
    ]
    if missing:
        raise SystemExit(f"для прогона обязательны: {', '.join(missing)}")
    if args.mode == "real" and not args.i_confirm_one_real_inference:
        raise SystemExit(
            "режим real требует --i-confirm-one-real-inference: один настоящий "
            "вызов подписки не выполняется по умолчанию"
        )

    worker_root = Path(args.worker_root).resolve()
    prompts_dir = Path(args.prompts_dir)
    canary = Path(args.canary)

    # 11E живёт в СВОЁМ корне данных. Указать сюда корень существующей установки
    # (или рантайма 11D/11D.2) — опечатка ценой чужого состояния.
    forbidden_roots = [
        Path.home() / "audit-worker" / "data", Path.home() / "audit-worker",
        Path.home() / "audit-worker-11d", Path.home() / "audit-worker-11d2",
    ]
    for forbidden in forbidden_roots:
        if worker_root == forbidden or forbidden in worker_root.parents:
            raise SystemExit(
                f"--worker-root={worker_root} ведёт в чужой рантайм ({forbidden}). "
                "11E обязан работать в отдельном корне"
            )

    if not canary.is_file():
        raise SystemExit(
            f"контрольный файл не найден: {canary}. Прогон без него запрещён: "
            "иначе раздел «канарейка» отчёта заполнится зелёными сравнениями "
            "двух отсутствий и будет выглядеть как проведённая проверка"
        )

    sandbox = build_sandbox(
        root=Path(args.sandbox_root), task_id=task_id,
        inputs_dir=Path(args.inputs_dir), section=args.section,
        md_name=args.md_name,
    )
    job_dir: Path = sandbox["job_dir"]
    print(f"[11E] каталог попытки: {job_dir}", flush=True)

    # ── Окружение процесса конвейера: все корни ВНУТРИ попытки ───────────────
    for key, value in isolated_roots_env(job_dir, prompts_dir).items():
        os.environ[key] = value
        Path(value).mkdir(parents=True, exist_ok=True)

    from audit_worker.providers import (
        inference_grant, inference_ledger, model_policy, resolver,
    )
    from audit_worker.providers.manager import ProviderManager

    # ── Локальная политика моделей (§17) ─────────────────────────────────────
    policy = model_policy.load_policy(worker_root)
    capability = policy.resolve("claude", CAPABILITY)
    print(f"[11E] политика воркера: {CAPABILITY} → {capability.model}", flush=True)

    journal = job_dir / "logs" / "fake_cli_journal.txt"
    prompt_dump = job_dir / "logs" / "prompt.txt"
    executable_override: Optional[Path] = None
    if args.mode == "fake":
        executable_override = make_fake_cli(
            job_dir / "runtime" / "fake-claude", journal, prompt_dump,
            reported_model=capability.accepted_reported_models[-1],
            project_id=sandbox["project_id"],
        )

    manager = ProviderManager(
        worker_root=worker_root,
        auth_modes={"claude": "ambient_user", "codex": "unavailable"},
        inference_allowed=True,
        pipeline_bridge_enabled=True,
        timeout_sec=args.timeout_sec,
        executables={"claude": executable_override} if executable_override else None,
        log=lambda m: print(f"[provider] {m}", flush=True),
    )
    manager.refresh(force=True)

    # ── Разрешение: выписывает ОПЕРАТОР, прогон только списывает (§28) ───────
    grant_id = f"g-{task_id}"
    existing = inference_grant.find(worker_root, provider="claude", task_id=task_id)
    if existing is None:
        raise SystemExit(
            f"нет пригодного разрешения под задание {task_id!r}. Выпишите его "
            f"ОТДЕЛЬНЫМ запуском:\n"
            f"    {Path(__file__).name} --issue-grant --worker-root {worker_root} "
            f"--task-id {task_id}\n"
            "Прогон разрешения себе не выписывает."
        )
    if existing.grant_id != grant_id:
        grant_id = existing.grant_id
    consumed = inference_grant.consume(worker_root, provider="claude", task_id=task_id)
    issued = existing
    print(f"[11E] разрешение {grant_id}: осталось {consumed.remaining}", flush=True)

    provider_root = resolver.ambient_root_for_attempt(job_dir, "claude")
    binding = resolver.ProviderResolver(manager, worker_root=worker_root).resolve(
        resolver.ProviderRequirement(
            provider="claude",
            capability=CAPABILITY,
            allowed_stages=(STAGE,),
            max_inferences=1,
        ),
        job_id=task_id, attempt_id=attempt_id, task_id=task_id, grant_id=grant_id,
        provider_root=provider_root,
        forbidden_literals=tuple(args.forbidden_literal),
    )
    binding_path = binding.write(job_dir / "metadata")
    os.environ[resolver.BINDING_ENV] = str(binding_path)
    print(f"[11E] привязка: модель {binding.model!r}, "
          f"допустимые {list(binding.accepted_reported_models)}", flush=True)

    canary_before = stat_facts(canary)

    # ── Боевой раннер этапа ──────────────────────────────────────────────────
    from backend.app.pipeline.stages.findings_merge.runner import run_findings_merge

    ctx, pipeline_log, usage, log_lines = build_stage_context(
        sandbox, job_dir / "logs" / "stage.log"
    )

    async def _run_stage():
        # Привязка путей аудита — работа ОРКЕСТРАТОРА (в проде её делает
        # `_run_batch_queue` на старте job).
        from backend.app.services.common import audit_scope

        with audit_scope.bind_audit_scope(
            output_dir=sandbox["output_dir"], version_dir=sandbox["version_dir"],
            project_id=sandbox["project_id"], version_id="v1",
        ):
            return await run_findings_merge(ctx)

    started = time.time()
    stage_result = asyncio.run(_run_stage())
    finished = time.time()

    canary_after = stat_facts(canary)

    artifact_path = sandbox["output_dir"] / ARTIFACT
    run_report_path = sandbox["output_dir"] / RUN_REPORT
    artifact_payload: Optional[dict] = None
    if artifact_path.is_file():
        artifact_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    run_report: Optional[dict] = None
    if run_report_path.is_file():
        run_report = json.loads(run_report_path.read_text(encoding="utf-8"))

    ledger_summary = inference_ledger.InferenceLedger(
        job_dir, attempt_id=attempt_id, job_id=task_id
    ).summary()

    # ── Входы не изменены ────────────────────────────────────────────────────
    inputs_unchanged: dict[str, bool] = {}
    for name in REQUIRED_INPUTS:
        pristine = job_dir / "input" / name
        working = sandbox["output_dir"] / name
        inputs_unchanged[name] = (
            pristine.is_file() and working.is_file()
            and sha256_file(pristine) == sha256_file(working)
        )

    leak_needles = {
        f"forbidden_literal_{i}": value
        for i, value in enumerate(args.forbidden_literal) if value
    }
    if not leak_needles:
        raise SystemExit(
            "не передан ни один --forbidden-literal: сканирование на утечку "
            "выродилось бы в отчёт «утечек 0» без единого сравнения"
        )
    token = os.environ.get("AUDIT_WORKER_TOKEN", "")
    if token:
        leak_needles["own_token"] = token
    leak_hits = scan_tree_for(job_dir, leak_needles)
    for name in list(leak_hits):
        leak_hits[name] = [
            p for p in leak_hits[name] if not p.endswith(resolver.BINDING_FILENAME)
        ]

    report = {
        "stage": STAGE,
        "production_stage": STAGE,
        "mode": args.mode,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "task_id": task_id,
        "attempt_id": attempt_id,
        "grant": {
            "grant_id": grant_id,
            "issued_max_uses": issued.max_uses,
            "remaining_after_consume": consumed.remaining,
            "reusable_grant_found_after_run": bool(
                inference_grant.find(worker_root, provider="claude", task_id=task_id)
            ),
        },
        "model_policy": {
            "capability": CAPABILITY,
            "requested_model": binding.model,
            "accepted_reported_models": list(binding.accepted_reported_models),
            "policy_source": str(policy.source_path) if policy.source_path else None,
        },
        "binding_public": binding.as_public_dict(),
        "inputs": {
            "copied": sandbox["copied"],
            "unchanged_after_run": inputs_unchanged,
            "section": args.section,
        },
        "stage_result": {
            "success": bool(stage_result.success),
            "cancelled": bool(stage_result.cancelled),
            "error": stage_result.error,
            "findings_count": stage_result.findings_count,
            "excluded_count": stage_result.excluded_count,
            "duration_sec": round(finished - started, 3),
        },
        "pipeline_log": pipeline_log,
        "usage": usage,
        "ledger_summary": ledger_summary,
        "provider_run_report": run_report,
        "artifact": {
            "path": str(artifact_path),
            "exists": artifact_path.is_file(),
            "sha256": sha256_file(artifact_path) if artifact_path.is_file() else None,
            "bytes": artifact_path.stat().st_size if artifact_path.is_file() else 0,
            "inside_attempt_dir": (
                artifact_path.resolve().is_relative_to(job_dir.resolve())
            ),
            "summary": summarize_findings(artifact_payload),
        },
        "output_dir_listing": sorted(
            p.name for p in sandbox["output_dir"].iterdir()
        ),
        "canary": {
            "path": str(canary),
            "before": canary_before,
            "after": canary_after,
            # Хэш здесь НЕ сравнивается, и это указано явно: сравнить его значило
            # бы прочитать файл дважды — то есть самим совершить то, отсутствие
            # чего проверяется.
            "content_hash_compared": False,
            "size_unchanged": canary_before.get("size_bytes") == canary_after.get("size_bytes"),
            "inode_unchanged": canary_before.get("inode") == canary_after.get("inode"),
            "mtime_unchanged": canary_before.get("mtime_ns") == canary_after.get("mtime_ns"),
            "ctime_unchanged": canary_before.get("ctime_ns") == canary_after.get("ctime_ns"),
            "atime_unchanged": canary_before.get("atime_ns") == canary_after.get("atime_ns"),
            "atime_caveat": (
                "Корень смонтирован с relatime: atime обновляется не при каждом "
                "чтении, поэтому «atime не изменился» САМ ПО СЕБЕ чтения не "
                "исключает. Доказательная сила — в совокупности: файл вне всех "
                "корней процесса, у модели ноль инструментов, маркер не найден "
                "ни в одном артефакте прогона."
            ),
            "content_in_report": False,
        },
        "leak_scan": {"hits": leak_hits, "total": sum(len(v) for v in leak_hits.values())},
        "fake_cli_journal": (
            journal.read_text(encoding="utf-8") if journal.is_file() else None
        ),
    }
    write_json(job_dir / "11E_RUN.json", report)

    # PASS — это НЕ «этап отработал». Успешный этап при двух оплаченных вызовах,
    # при утечке контрольной строки или при потерянном входе — это провал 11E.
    calls_started = int(ledger_summary.get("calls_started") or 0)
    coverage = (run_report or {}).get("input_coverage") or {}
    violations: list[str] = []
    if not stage_result.success:
        violations.append(f"этап не выполнен: {stage_result.error}")
    if not artifact_path.is_file():
        violations.append(f"артефакт {ARTIFACT} не создан")
    if calls_started > 1:
        violations.append(f"вызовов модели {calls_started} вместо одного")
    if not report["artifact"]["inside_attempt_dir"]:
        violations.append("артефакт вне каталога попытки")
    if report["leak_scan"]["total"]:
        violations.append(f"утечки: {report['leak_scan']['hits']}")
    for name, unchanged in inputs_unchanged.items():
        if not unchanged:
            violations.append(f"входной артефакт изменился за прогон: {name}")
    if coverage and not coverage.get("passed"):
        violations.append(
            f"вход не доехал до модели: {coverage.get('missing_before_inference')}"
        )
    canary_facts = report["canary"]
    if canary_facts["before"].get("exists") and not (
        canary_facts["size_unchanged"] and canary_facts["mtime_unchanged"]
        and canary_facts["inode_unchanged"]
    ):
        violations.append("контрольный файл изменился")
    report["verdict"] = {"pass": not violations, "violations": violations}
    write_json(job_dir / "11E_RUN.json", report)

    print(f"[11E] ИТОГ: {'PASS' if not violations else 'FAIL'} "
          f"(замечаний {report['artifact']['summary']['findings']}, "
          f"вызовов модели {calls_started})", flush=True)
    for v in violations:
        print(f"[11E] НАРУШЕНИЕ: {v}", flush=True)
    return 0 if not violations else 1


if __name__ == "__main__":
    sys.exit(main())
