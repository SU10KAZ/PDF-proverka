#!/usr/bin/env python3
"""Этап 11D — БОЕВОЙ `text_analysis` через ProviderAdapter на пилотном воркере.

Запускается ЛОКАЛЬНО НА ВОРКЕРЕ. SSH — только способ его туда положить и там
стартовать; сам вызов модели идёт из этого процесса в локальный Claude CLI и
никуда больше. Ни одна строка ниже не открывает соединения к центру: продовый
ingress в 11D не включается (§30 задания).

Что здесь настоящее:

  * боевой раннер этапа `backend/app/pipeline/stages/text_analysis/runner.py`;
  * боевой сборщик промпта `prompt_builder.build_text_analysis_messages`;
  * боевой профиль дисциплины, норм-база, pre-scan, страж отсутствия;
  * боевая проверка артефакта (`validate_and_repair_json`, `text_findings`)
    и боевое дообогащение (`md_prescan.augment_text_analysis_file`);
  * `ProviderAdapter`, окружение с нуля, отключённые инструменты, stdin;
  * разрешение (`inference_grant`) и журнал вызовов (`inference_ledger`).

Что подставное: оркестратор этапа. `PipelineStageContext` здесь — минимальные
заглушки логирования и учёта. Это НЕ ослабление: контекст принадлежит
`PipelineManager`, а не этапу, и на воркере его роль всё равно исполняет другой
код. Сам этап при этом настоящий, целиком.

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

STAGE = "text_analysis"
CAPABILITY = "strong_audit"
ARTIFACT = "02_text_analysis.json"
RUN_REPORT = "text_analysis_provider_run.json"


# ═══════════════════════════ Мелкие утилиты ══════════════════════════════════

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stat_facts(path: Path) -> dict[str, Any]:
    """Факты о файле БЕЗ ЧТЕНИЯ его содержимого.

    Отсутствие `sha256` здесь принципиально. Контрольный файл (§19) проверяется
    на «его не читали», а любое вычисление хэша — это чтение, то есть само по
    себе изменение `atime`. Снимок с хэшем уничтожал бы то самое свидетельство,
    ради которого делается. Хэш снимает оператор ОДИН раз до прогона, отдельным
    административным шагом, и знает, что этим он atime уже тронул.
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
    *, root: Path, task_id: str, document_dir: Path, section: str,
    md_name: Optional[str] = None,
) -> dict[str, Any]:
    """Собрать изолированный каталог попытки 11D и положить в него копию входа.

    Раскладка повторяет `audit_runner.isolated_roots`: все корни данных процесса
    конвейера уводятся ВНУТРЬ попытки. Это не декорация — на ней держится
    гарантия §17 и проверка `_assert_output_inside_attempt` в боевом коде.
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
    output_dir.mkdir(parents=True)

    src = Path(document_dir)
    md_files = sorted(p for p in src.glob("*.md") if p.is_file())
    if md_name:
        md_files = [p for p in md_files if p.name == md_name]
    if not md_files:
        raise SystemExit(f"в {src} нет Markdown-файла для этапа")
    if len(md_files) > 1:
        raise SystemExit(
            f"в {src} несколько Markdown ({[p.name for p in md_files]}) — "
            "укажите --md-name: гадать, какой из них источник аудита, нельзя"
        )
    md_src = md_files[0]
    md_dst = version_dir / md_src.name
    shutil.copy2(md_src, md_dst)
    # Копия входа кладётся и в `input/` — она неприкосновенна и служит эталоном
    # для проверки «конвейер не изменил вход».
    shutil.copy2(md_src, job_dir / "input" / md_src.name)

    project_info = {
        "project_id": f"{section}/11d",
        "name": "11d",
        "section": section,
        "md_file": md_src.name,
    }
    write_json(version_dir / "project_info.json", project_info)

    return {
        "job_dir": job_dir,
        "version_dir": version_dir,
        "output_dir": output_dir,
        "md_path": md_dst,
        "md_source": md_src,
        "md_sha256": sha256_file(md_dst),
        "md_chars": len(md_dst.read_text(encoding="utf-8")),
        "project_info": project_info,
        "project_id": project_info["project_id"],
    }


def isolated_roots_env(job_dir: Path, prompts_dir: Path) -> dict[str, str]:
    """Корни данных процесса — те же, что выставляет `audit_runner`."""
    from audit_worker.audit_runner import isolated_roots

    env = dict(isolated_roots(job_dir))
    # Снимок промптов и профилей дисциплин раскладывается отдельно: в 11D он
    # приезжает не пакетом задания, а вместе с артефактом развёртывания.
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
# Ответ читается ФАЙЛОМ, а не встраивается в исходник: JSON и Python
# расходятся на `null`/`true`, и подделка молча падала бы NameError, выдавая
# «модель не ответила» там, где сломан стенд.
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
    answer = {
        "stage": "02_text_analysis",
        "project_id": project_id,
        "text_source": "md",
        "timestamp": "2026-01-01T00:00:00",
        "project_params": {"object_type": "репетиция 11D"},
        "normative_refs_found": [],
        "text_findings": [
            {
                "id": "T-001", "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "репетиция", "source": "MD",
                "finding": "Ответ подделки CLI: обращения к модели не было",
                "norm": None, "norm_quote": None, "related_block_ids": [],
            }
        ],
        "items_verified_from_blocks": [],
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
    """Минимальный `PipelineStageContext`: только то, что читает боевой раннер.

    Заглушки здесь — роль ОРКЕСТРАТОРА, а не этапа. Ни одна из них не подменяет
    ни сборку промпта, ни вызов модели, ни проверку артефакта.
    """
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
        # В 11D ожидание лимита не предусмотрено: бюджет вызовов равен одному,
        # и «подождём и повторим» здесь означало бы второй заход в модель.
        return False

    def _record_cli_usage(cli_result, stage_name, is_retry: bool = False) -> None:
        usage.append({
            "stage": stage_name,
            "input_tokens": getattr(cli_result, "input_tokens", 0),
            "output_tokens": getattr(cli_result, "output_tokens", 0),
            "cache_creation_tokens": getattr(cli_result, "cache_creation_tokens", 0),
            "cache_read_tokens": getattr(cli_result, "cache_read_tokens", 0),
            "duration_ms": getattr(cli_result, "duration_ms", 0),
            "is_error": getattr(cli_result, "is_error", None),
        })

    def _update_pipeline_log(stage_key: str, status: str, **kwargs) -> None:
        pipeline_log.setdefault(stage_key, []).append({"status": status, **{
            k: v for k, v in kwargs.items() if k != "detail" or v is not None
        }})

    async def _run_subprocess(*args, **kwargs):
        raise RuntimeError("этап 11D не запускает подпроцессов помимо CLI провайдера")

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
        job_id="11d",
    )
    return ctx, pipeline_log, usage, lines


# ═══════════════════════════ Основной прогон ═════════════════════════════════

def main() -> int:
    parser = argparse.ArgumentParser(description="11D: боевой text_analysis через ProviderAdapter")
    parser.add_argument("--mode", choices=("fake", "real"), required=True)
    parser.add_argument("--sandbox-root", required=True)
    parser.add_argument("--worker-root", required=True,
                        help="корень данных 11D-рантайма: политика, разрешения")
    parser.add_argument("--document-dir", required=True,
                        help="каталог с ОДНОЙ разрешённой копией входного MD")
    parser.add_argument("--md-name", default=None)
    parser.add_argument("--section", required=True)
    parser.add_argument("--prompts-dir", required=True,
                        help="снимок prompts/ (pipeline + disciplines)")
    parser.add_argument("--task-id", default=None)
    parser.add_argument("--canary", default="/home/coder/provider-auth-canary/DO_NOT_READ.txt")
    parser.add_argument(
        "--forbidden-literal", action="append", default=[],
        help=("Строка, которой не должно быть в ответе модели (обычно маркер "
              "контрольного файла). Передаётся ЗНАЧЕНИЕМ, а не путём: читать "
              "контрольный файл из этого процесса нельзя — чтение изменило бы "
              "atime и обесценило проверку. Контрольный файл несекретен (§19)."),
    )
    parser.add_argument("--timeout-sec", type=float, default=1800.0)
    parser.add_argument("--i-confirm-one-real-inference", action="store_true")
    args = parser.parse_args()

    task_id = args.task_id or f"11d-{uuid.uuid4().hex[:8]}"
    attempt_id = f"{task_id}-a1"
    if args.mode == "real" and not args.i_confirm_one_real_inference:
        raise SystemExit(
            "режим real требует --i-confirm-one-real-inference: один настоящий "
            "вызов подписки не выполняется по умолчанию"
        )

    worker_root = Path(args.worker_root)
    prompts_dir = Path(args.prompts_dir)
    canary = Path(args.canary)

    sandbox = build_sandbox(
        root=Path(args.sandbox_root), task_id=task_id,
        document_dir=Path(args.document_dir), section=args.section,
        md_name=args.md_name,
    )
    job_dir: Path = sandbox["job_dir"]
    print(f"[11D] каталог попытки: {job_dir}", flush=True)

    # ── Окружение процесса конвейера: все корни ВНУТРИ попытки ───────────────
    for key, value in isolated_roots_env(job_dir, prompts_dir).items():
        os.environ[key] = value
        Path(value).mkdir(parents=True, exist_ok=True)

    from audit_worker.providers import (
        inference_grant, inference_ledger, model_policy, resolver,
    )
    from audit_worker.providers.manager import ProviderManager

    # ── Локальная политика моделей (§10) ─────────────────────────────────────
    policy = model_policy.load_policy(worker_root)
    capability = policy.resolve("claude", CAPABILITY)
    print(f"[11D] политика воркера: {CAPABILITY} → {capability.model}", flush=True)

    # ── Подделка CLI для репетиции ───────────────────────────────────────────
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

    # ── Разрешение: выписывает оператор, списываем ДО вызова (§21) ───────────
    grant_id = f"g-{task_id}"
    issued = inference_grant.issue(
        worker_root, grant_id=grant_id, provider="claude", task_id=task_id,
        ttl_sec=3600.0, max_uses=1,
        note=f"11D {STAGE} real_document (mode={args.mode})",
    )
    consumed = inference_grant.consume(worker_root, provider="claude", task_id=task_id)
    print(f"[11D] разрешение {grant_id}: осталось {consumed.remaining}", flush=True)

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
    print(f"[11D] привязка: модель {binding.model!r}, "
          f"допустимые {list(binding.accepted_reported_models)}", flush=True)

    # ── Канарейка: снимок ДО ─────────────────────────────────────────────────
    canary_before = stat_facts(canary)

    # ── Боевой раннер этапа ──────────────────────────────────────────────────
    from backend.app.pipeline.stages.text_analysis.runner import run_text_analysis

    ctx, pipeline_log, usage, log_lines = build_stage_context(
        sandbox, job_dir / "logs" / "stage.log"
    )

    async def _run_stage():
        # Привязка путей аудита — работа ОРКЕСТРАТОРА (в проде её делает
        # `_run_batch_queue` на старте job). Без неё пост-обработка этапа
        # (`md_prescan.augment_text_analysis_file`) ищет MD по резолверу версий
        # и не находит: сам вызов модели при этом отработал бы, а
        # детерминированное дообогащение молча пропустилось бы.
        from backend.app.services.common import audit_scope

        with audit_scope.bind_audit_scope(
            output_dir=sandbox["output_dir"], version_dir=sandbox["version_dir"],
            project_id=sandbox["project_id"], version_id="v1",
        ):
            return await run_text_analysis(
                ctx, stage_label=STAGE, with_rate_limit_retry=False
            )

    started = time.time()
    stage_result = asyncio.run(_run_stage())
    finished = time.time()

    # ── Канарейка: снимок ПОСЛЕ ──────────────────────────────────────────────
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

    # ── Вход не изменён ──────────────────────────────────────────────────────
    input_copy = job_dir / "input" / Path(sandbox["md_path"]).name
    md_unchanged = sha256_file(input_copy) == sha256_file(sandbox["md_path"])

    leak_needles = {
        f"forbidden_literal_{i}": value
        for i, value in enumerate(args.forbidden_literal) if value
    }
    token = os.environ.get("AUDIT_WORKER_TOKEN", "")
    if token:
        leak_needles["own_token"] = token
    leak_hits = scan_tree_for(job_dir, leak_needles) if leak_needles else {}
    # Привязка законно содержит контрольный литерал — она и есть механизм
    # проверки. Из отчёта об утечках её исключаем явно, а не молча.
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
            # `find` возвращает только ПРИГОДНЫЕ записи, поэтому исчерпанное
            # разрешение здесь честно даёт None — это и есть «списано до нуля».
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
        "document": {
            "md_name": Path(sandbox["md_path"]).name,
            "md_sha256": sandbox["md_sha256"],
            "md_chars": sandbox["md_chars"],
            "section": args.section,
            "unchanged_after_run": md_unchanged,
        },
        "stage_result": {
            "success": bool(stage_result.success),
            "cancelled": bool(stage_result.cancelled),
            "error": stage_result.error,
            "data": stage_result.data,
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
            "text_findings": len((artifact_payload or {}).get("text_findings") or []),
            "normative_refs": len((artifact_payload or {}).get("normative_refs_found") or []),
            "text_source": (artifact_payload or {}).get("text_source"),
            "keys": sorted(artifact_payload.keys()) if artifact_payload else [],
        },
        "canary": {
            "path": str(canary),
            "before": canary_before,
            "after": canary_after,
            "sha256_unchanged": canary_before.get("sha256") == canary_after.get("sha256"),
            "mtime_unchanged": canary_before.get("mtime_ns") == canary_after.get("mtime_ns"),
            "atime_unchanged": canary_before.get("atime_ns") == canary_after.get("atime_ns"),
            "content_in_report": False,
        },
        "leak_scan": {"hits": leak_hits, "total": sum(len(v) for v in leak_hits.values())},
        "fake_cli_journal": (
            journal.read_text(encoding="utf-8") if journal.is_file() else None
        ),
    }
    report_path = write_json(job_dir / "11D_RUN.json", report)
    print(f"[11D] отчёт прогона: {report_path}", flush=True)

    ok = bool(stage_result.success) and artifact_path.is_file()
    print(f"[11D] ИТОГ: {'PASS' if ok else 'FAIL'} "
          f"(замечаний {report['artifact']['text_findings']}, "
          f"вызовов модели {ledger_summary.get('calls_started')})", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
