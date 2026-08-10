#!/usr/bin/env python3
"""Этап 11F — ВЕСЬ worker-участок классического конвейера на пилотном воркере.

Чем отличается от скриптов 11D/11E. Те гоняли ОДИН этап: строили минимальный
контекст стадии и звали её production-раннер напрямую. Здесь этапа нет вовсе —
запускается штатная точка входа воркера `backend.app.pipeline.remote_audit_runner`,
которая зовёт тот же `PipelineManager._dispatch_action`, что и центр. То есть
порядок стадий, засевы, resume, промоуты артефактов и запрет центральных этапов
исполняются production-кодом, а не переписаны здесь.

Скрипт делает ровно то, что в бою делает исполнитель воркера, и ничего сверх:

    раскладка каталога попытки (audit_runner.prepare_job_dir)
      → распаковка пакета проекта (package_io.verify_and_unpack)
      → снимки центра: промпты, модели, флаги, профиль дисциплины
      → списание разрешения оператора (inference_grant)
      → привязка провайдера (ProviderBinding)
      → запуск remote_audit_runner ОТДЕЛЬНЫМ процессом с изолированным env
      → сборка пакета результата (package_io.build_result_package)

Режимы:

    --mode fake   подделка CLI, ноль обращений к модели. Проверяет ВСЮ цепочку:
                  пути, схемы, порядок, checkpoint/resume, границу центра,
                  сборку пакета. Обязателен перед боевым прогоном (§8).
    --mode real   настоящая модель через ProviderAdapter.

SSH здесь нет. Скрипт запускается ЛОКАЛЬНО НА ВОРКЕРЕ; SSH — только способ его
туда положить и там стартовать.

Пример:

    python scripts/run_11f_worker_slice.py \\
        --mode fake --task-id worker_slice_11f_fake1 \\
        --source-package /home/coder/audit-worker-11f/incoming/source.tar.gz \\
        --sandbox-root /home/coder/audit-worker-11f/sandbox
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

#: Этапы, которым привязка разрешает обращаться к модели. Список закрытый: этап,
#: которого здесь нет, получит отказ моста, а не тихий обход.
ALLOWED_STAGES: tuple[str, ...] = (
    "block_analysis",
    "text_analysis",
    "findings_merge",
    "findings_review",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
)

#: Модели этапов для worker-профиля. Это КОНФИГУРАЦИЯ прогона, а не правка кода.
#:
#: Отклонение от центральных дефолтов и его причина. `block_batch` в проде —
#: `ensemble/gpt-codex` (нога GPT в OpenRouter + нога Codex), `optimization` —
#: `ensemble/claude-codex-opt`. На воркере с единственной Claude-подпиской обе
#: недостижимы: Codex запрещён заданием 11F, OpenRouter требует платного ключа,
#: которого в окружении процесса конвейера нет и не должно быть. При активном
#: мосте строка модели в argv вообще не попадает — её задаёт локальная политика
#: воркера по способности `strong_audit`. Значения ниже нужны лишь чтобы этапы
#: не ушли в чужой транспорт ДО того, как мост их перехватит.
WORKER_STAGE_MODELS: dict[str, str] = {
    "text_analysis": "claude-opus-5",
    "block_batch": "claude-opus-5",
    "findings_merge": "claude-opus-5",
    "findings_critic": "claude-opus-5",
    "findings_corrector": "claude-opus-5",
    "norm_verify": "claude-opus-5",
    "norm_fix": "claude-opus-5",
    "norm_requote": "claude-opus-5",
    "optimization": "claude-opus-5",
    "optimization_critic": "claude-opus-5",
    "optimization_corrector": "claude-opus-5",
}

PIPELINE_ENTRYPOINT = "backend.app.pipeline.remote_audit_runner"


# ═══════════════════════════ мелкие утилиты ═════════════════════════════════

def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ═══════════════════════════ поддельный CLI ═════════════════════════════════

_FAKE_CLI = r'''#!/usr/bin/env python3
"""Подделка Claude CLI для репетиции этапа 11F. Сети не касается.

Воспроизводит ДВА фактических контракта настоящего бинаря:

  * текстовый вызов  — `--output-format json`        → один JSON-конверт;
  * вызов с картинкой — `--output-format stream-json` → NDJSON, последним
    объект `{"type":"result", …}`.

Полезная нагрузка выбирается по содержимому промпта: у этапов разные схемы
ответа, и один payload на всех дал бы «успех», который следующий этап прочитать
не может.
"""
import json, os, sys, time

JOURNAL = os.environ.get("AUDIT_11F_FAKE_JOURNAL", "")
DUMP_DIR = os.environ.get("AUDIT_11F_FAKE_DUMP_DIR", "")
REPORTED_MODEL = os.environ.get("AUDIT_11F_FAKE_MODEL", "claude-opus-5")

argv = sys.argv[1:]
stream = "stream-json" in argv
prompt = sys.stdin.read()

# Модальность определяется не догадкой, а форматом ввода: картинка приходит
# ТОЛЬКО строкой stream-json с content-блоком type=image.
images = 0
text = prompt
if stream:
    try:
        msg = json.loads(prompt.strip().splitlines()[0])
        content = (msg.get("message") or {}).get("content") or []
        images = sum(1 for c in content if c.get("type") == "image")
        text = "\n".join(c.get("text", "") for c in content if c.get("type") == "text")
    except Exception:
        pass


def payload_for(body: str) -> dict:
    """Схема ответа выбирается по МАРКЕРУ ЭТАПА, а не по угадыванию.

    Порядок проверок содержателен: с этапа 11F в промпт текстового анализа
    вкладывается блочный контекст, и он содержит слово "findings". Подделка,
    решавшая по подстроке, отдавала бы текстовому этапу схему блока — то есть
    ломала бы репетицию там, где конвейер исправен.
    """
    # Текстовый анализ: единственный, кто вкладывает исходный документ.
    if "SOURCE DOCUMENT (inlined by the pipeline)" in body:
        return {
            "stage": "text_analysis",
            "text_source": "md",
            "project_params": {},
            "text_findings": [{
                "id": "T-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "репетиция",
                "finding": "Ответ подделки CLI: обращения к модели не было",
                "sheet": None, "page": None,
            }],
            "normative_refs_found": [],
            "items_verified_from_blocks": [],
        }
    # Свод замечаний: у него свой корневой ключ и своя мета.
    if "findings_merge" in body or "СВОД ЗАМЕЧАНИЙ" in body.upper() or "MERGE" in body.upper():
        return {
            "meta": {"total_findings": 1},
            "findings": [{
                "id": "F-001",
                "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
                "category": "репетиция",
                "sheet": None, "page": None,
                "problem": "Ответ подделки CLI: обращения к модели не было",
                "description": "Репетиция пути 11F",
                "norm": None, "norm_quote": None,
                "solution": "—", "risk": "—",
                "source_finding_ids": ["T-001", "G-001"],
                "source_block_ids": [], "related_block_ids": [],
                "evidence_text_refs": [], "evidence": [], "highlight_regions": [],
            }],
        }
    if '"optimizations"' in body or "optimization" in body.lower():
        return {"optimizations": [{
            "id": "OPT-001",
            "title": "Ответ подделки CLI: обращения к модели не было",
            "description": "Репетиция пути 11F",
            "category": "репетиция",
            "savings_estimate": "",
            "verdict": "accepted",
        }]}
    return {"findings": []}


answer = payload_for(text)
if images:
    # Блочный вызов обязан вернуть схему блока, а не что угодно.
    answer = {"findings": [{
        "id": "G-001",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "category": "репетиция",
        "finding": "Ответ подделки CLI по приложенному изображению (%d шт.)" % images,
        "value_found": "",
        "norm_quote": None,
    }]}
answer_text = json.dumps(answer, ensure_ascii=False)

if JOURNAL:
    try:
        with open(JOURNAL, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "ts": time.time(), "stream": stream, "images": images,
                "prompt_chars": len(prompt), "argv": argv,
            }, ensure_ascii=False) + "\n")
    except OSError:
        pass
if DUMP_DIR:
    try:
        os.makedirs(DUMP_DIR, exist_ok=True)
        name = "%d_%s.txt" % (int(time.time() * 1000), "img" if images else "txt")
        with open(os.path.join(DUMP_DIR, name), "w", encoding="utf-8") as fh:
            fh.write(prompt)
    except OSError:
        pass

usage = {
    "input_tokens": 1000, "output_tokens": 16,
    "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0,
}
if stream:
    sys.stdout.write(json.dumps({
        "type": "assistant",
        "message": {"model": REPORTED_MODEL, "content": [{"type": "text", "text": answer_text}],
                    "usage": usage},
    }, ensure_ascii=False) + "\n")
    sys.stdout.write(json.dumps({
        "type": "result", "subtype": "success", "is_error": False,
        "result": answer_text, "usage": usage, "total_cost_usd": 0.0,
        "modelUsage": {REPORTED_MODEL: {"outputTokens": 16}},
    }, ensure_ascii=False) + "\n")
else:
    sys.stdout.write(json.dumps({
        "type": "result", "subtype": "success", "is_error": False,
        "result": answer_text, "usage": usage, "total_cost_usd": 0.0,
        "modelUsage": {REPORTED_MODEL: {"outputTokens": 16}},
    }, ensure_ascii=False))
sys.exit(0)
'''


def make_fake_cli(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_FAKE_CLI, encoding="utf-8")
    path.chmod(0o755)
    return path


# ═══════════════════════════ раскладка попытки ══════════════════════════════

def build_attempt(
    *,
    sandbox_root: Path,
    task_id: str,
    source_package: Path,
) -> dict[str, Any]:
    """Каталог попытки боевой раскладки + распакованный пакет проекта."""
    from audit_worker import audit_runner, package_io

    job_dir = sandbox_root / task_id
    if job_dir.exists():
        raise SystemExit(
            f"каталог попытки уже существует: {job_dir}. Повтор в тот же каталог "
            "запрещён — новая попытка требует нового task_id"
        )
    layout = audit_runner.prepare_job_dir(job_dir)

    # Распаковка идёт ТЕМ ЖЕ путём, что в бою (`agent._download_and_verify`):
    # сначала в `unpack_staging`, затем секции переносятся в каталог попытки.
    # Распаковывать прямо в `job_dir` нельзя — `verify_and_unpack` очищает
    # каталог назначения и снёс бы logs/, metadata/ и раскладку.
    import os as _os
    import shutil as _shutil

    unpack_target = job_dir / "unpack_staging"
    info = package_io.verify_and_unpack(
        archive=source_package,
        expected_sha256=sha256_file(source_package),
        work_dir=unpack_target,
    )
    manifest = info["manifest"]
    package_io.require_portable_layout(manifest, unpack_target)
    for source_name, dest_name in package_io.AUDIT_PACKAGE_SECTIONS:
        source = unpack_target / source_name
        if not source.is_dir():
            continue
        destination = job_dir / dest_name
        _shutil.rmtree(destination, ignore_errors=True)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _os.replace(source, destination)
    return {
        "job_dir": job_dir,
        "layout": layout,
        "source_manifest": manifest,
        "unpacked": {"files": info["files"], "bytes": info["bytes"]},
    }


def write_model_snapshot(job_dir: Path) -> Path:
    """Снимок моделей этапов. Едет в `snapshot/stage_models.json`, как из центра."""
    target = job_dir / "snapshot" / "stage_models.json"
    return write_json(target, WORKER_STAGE_MODELS)


def issue_grant_and_binding(
    *,
    job_dir: Path,
    task_id: str,
    attempt_id: str,
    job_id: str,
    max_inferences: int,
    fake_cli: Optional[Path],
    forbidden_literals: tuple[str, ...],
    policy_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Разрешение оператора + привязка провайдера. Ровно то, что делает исполнитель."""
    from audit_worker.providers import inference_grant as grant_mod
    from audit_worker.providers import model_policy
    from audit_worker.providers.paths import PROVIDER_CLAUDE
    from audit_worker.providers.auth_mode import AUTH_MODE_AMBIENT_USER
    from audit_worker.providers.resolver import BINDING_FILENAME, ProviderBinding

    worker_root = job_dir / "worker_root"
    worker_root.mkdir(parents=True, exist_ok=True)
    grant_id = f"g-{task_id}"
    grant = grant_mod.issue(
        worker_root,
        grant_id=grant_id,
        provider=PROVIDER_CLAUDE,
        task_id=task_id,
        ttl_sec=6 * 3600,
        max_uses=max_inferences,
        note="11F worker slice",
    )
    grant_path = grant_mod.grant_path(worker_root)

    policy = model_policy.load_policy(policy_root)
    resolution = policy.resolve(PROVIDER_CLAUDE, model_policy.CAPABILITY_STRONG_AUDIT)
    binding = ProviderBinding(
        schema_version=1,
        provider=PROVIDER_CLAUDE,
        auth_mode=AUTH_MODE_AMBIENT_USER,
        provider_root=str(job_dir / "providers" / PROVIDER_CLAUDE),
        executable=str(fake_cli) if fake_cli else None,
        timeout_sec=1800.0,
        job_id=job_id,
        attempt_id=attempt_id,
        task_id=task_id,
        grant_id=grant.grant_id,
        max_inferences=max_inferences,
        allowed_stages=ALLOWED_STAGES,
        model=resolution.model,
        forbidden_literals=forbidden_literals,
        capability=resolution.capability,
        accepted_reported_models=tuple(resolution.accepted_reported_models),
    )
    binding_path = job_dir / "metadata" / BINDING_FILENAME
    write_json(binding_path, binding.as_dict())
    return {
        "grant_path": grant_path,
        "grant_id": grant.grant_id,
        "binding_path": binding_path,
        "binding": binding,
        "model": resolution.model,
        "capability": resolution.capability,
    }


def build_run_spec(
    *,
    job_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    action: str,
    provider_mode: str,
    max_inferences: int,
    runtime_snapshot_hash: Optional[str],
    prompt_bundle_hash: Optional[str],
    model_config_hash: Optional[str],
    discipline_id: Optional[str],
    discipline_profile_hash: Optional[str],
) -> Path:
    """Спека прогона. Пишет её ВОРКЕР, читает — код платформы."""
    spec = {
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": project_id,
        "version_id": version_id,
        "profile": "remote_audit_pilot_v1",
        "action": action,
        "retry_stage": None,
        "provider_mode": provider_mode,
        # Снимок объявляет provider_mode="real", и `assert_compatible` требует
        # для него явного разрешения. Разрешает его ВОРКЕР (это свойство его
        # конфигурации, а не поля задания) — здесь это делает оператор,
        # запускающий диагностический прогон.
        "allow_real_llm": True,
        "runtime_snapshot_hash": runtime_snapshot_hash,
        "prompt_bundle_hash": prompt_bundle_hash,
        "model_config_hash": model_config_hash,
        "discipline_id": discipline_id,
        "discipline_profile_hash": discipline_profile_hash,
        "pipeline_revision": "11f",
        "provider_requirement": {
            "provider": "claude",
            "allowed_stages": list(ALLOWED_STAGES),
            "max_inferences": max_inferences,
        },
        "paths": {
            "project": str(job_dir / "project"),
            "snapshot": str(job_dir / "snapshot"),
            "work": str(job_dir / "work"),
            "result": str(job_dir / "result"),
            "logs": str(job_dir / "logs"),
            "usage": str(job_dir / "usage"),
            "metadata": str(job_dir / "metadata"),
            "runtime": str(job_dir / "runtime"),
            "discipline_profile": str(job_dir / "discipline_profile"),
            "comparison": str(job_dir / "comparison"),
        },
    }
    return write_json(job_dir / "metadata" / "run_spec.json", spec)


def run_pipeline(
    *,
    job_dir: Path,
    spec_path: Path,
    binding_path: Path,
    fake_cli: Optional[Path],
    python_bin: str,
    log_path: Path,
) -> dict[str, Any]:
    """Запустить `remote_audit_runner` отдельным процессом с изолированным env."""
    from audit_worker import audit_runner

    env = {
        k: v for k, v in os.environ.items()
        if k in ("PATH", "LANG", "LC_ALL", "TZ", "LD_LIBRARY_PATH",
                 "SSL_CERT_FILE", "SSL_CERT_DIR")
    }
    env.update(audit_runner.isolated_roots(job_dir))
    env["AUDIT_WORKER_PROVIDER_BINDING"] = str(binding_path)
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PYTHONUNBUFFERED"] = "1"
    if fake_cli is not None:
        env["AUDIT_11F_FAKE_JOURNAL"] = str(job_dir / "logs" / "fake_provider_calls.jsonl")
        env["AUDIT_11F_FAKE_DUMP_DIR"] = str(job_dir / "logs" / "prompt_dumps")
    # Каталоги корней данных создаются заранее: часть библиотек падает на
    # несуществующем HOME/TMPDIR. Создаются ТОЛЬКО корни из `isolated_roots`,
    # а не всё подряд из env — иначе путь к файлу привязки превратился бы в
    # каталог с тем же именем.
    for path in audit_runner.isolated_roots(job_dir).values():
        Path(path).mkdir(parents=True, exist_ok=True)

    argv = [python_bin, "-u", "-m", PIPELINE_ENTRYPOINT, str(spec_path)]
    started = time.time()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.run(
            argv, env=env, cwd=str(REPO_ROOT),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        log.write(proc.stdout or "")
    return {
        "argv": argv,
        "exit_code": proc.returncode,
        "duration_sec": round(time.time() - started, 2),
        "log": str(log_path),
        "stdout_tail": (proc.stdout or "").splitlines()[-40:],
    }


def collect_ledger(job_dir: Path) -> dict[str, Any]:
    """Разобрать журнал вызовов попытки: сколько, каких, какой моделью.

    Содержимого ответов здесь нет и быть не может — это замечания по документу
    заказчика. Только счётчики, статусы, отпечатки и расход.
    """
    from audit_worker.providers.inference_ledger import ledger_dir

    directory = ledger_dir(job_dir)
    calls: list[dict[str, Any]] = []
    if directory.is_dir():
        for path in sorted(directory.glob("*.result.json")):
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            result = entry.get("provider_result") or {}
            usage = result.get("usage") or {}
            calls.append({
                "key": entry.get("key"),
                "purpose": path.name.split("-")[0],
                "performed_by_pid": entry.get("performed_by_pid"),
                "provider": result.get("provider"),
                "model": result.get("model"),
                "status": result.get("status"),
                "error_code": result.get("error_code"),
                "duration_ms": result.get("duration_ms"),
                "exit_code": result.get("exit_code"),
                "raw_sha256": result.get("raw_sha256"),
                "input_tokens": usage.get("input_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "cache_creation_input_tokens": usage.get("cache_creation_input_tokens"),
                "cache_read_input_tokens": usage.get("cache_read_input_tokens"),
                "total_cost_usd": usage.get("total_cost_usd"),
            })
    claims = len(list(directory.glob("*.claim.json"))) if directory.is_dir() else 0
    by_stage: dict[str, int] = {}
    for call in calls:
        stage = str(call["key"] or "").split("-")[0].split(":")[0]
        by_stage[stage] = by_stage.get(stage, 0) + 1
    return {
        "total": len(calls),
        "claims": claims,
        "indeterminate": max(0, claims - len(calls)),
        "by_stage": by_stage,
        "models": sorted({str(c["model"]) for c in calls if c.get("model")}),
        "failed": [c["key"] for c in calls if c.get("status") != "success"],
        "tokens_out": sum(int(c.get("output_tokens") or 0) for c in calls),
        "cost_usd": round(sum(float(c.get("total_cost_usd") or 0.0) for c in calls), 6),
        "calls": calls,
    }


def build_result(
    *,
    job_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    source_package_hash: str,
    exit_code: int,
) -> dict[str, Any]:
    """Собрать пакет результата ТЕМ ЖЕ кодом, что и боевой исполнитель."""
    from audit_worker import audit_runner, package_io
    from audit_worker.__init__ import __version__ as worker_version

    audit_manifest_path = job_dir / "result" / "audit_manifest.json"
    audit_manifest: dict[str, Any] = {}
    if audit_manifest_path.is_file():
        audit_manifest = json.loads(audit_manifest_path.read_text(encoding="utf-8"))

    archive = job_dir / "package_output" / f"{attempt_id}.tar.gz"
    manifest = package_io.build_result_package(
        dest_path=archive,
        job_dir=job_dir,
        job_id=job_id,
        attempt_id=attempt_id,
        project_id=project_id,
        version_id=version_id,
        worker_id="11f-diagnostic",
        worker_version=str(worker_version),
        protocol_version=1,
        manifest_version=1,
        source_package_hash=source_package_hash,
        exit_code=exit_code,
        job_type="audit_pipeline_v1",
        required_artifacts=list(
            audit_runner.required_artifacts_for(str(audit_manifest.get("action") or ""))
        ),
        pipeline_revision=audit_manifest.get("pipeline_revision"),
        stage_completion=audit_manifest.get("stage_completion"),
        resume_hint=audit_manifest.get("resume_hint"),
        project_version_rel=audit_manifest.get("project_version_rel"),
        runtime_snapshot_hash=(
            (audit_manifest.get("applied_runtime_config") or {}).get("runtime_snapshot_hash")
        ),
        applied_write_mode=(
            (audit_manifest.get("applied_runtime_config") or {}).get("applied_write_mode")
        ),
        execution_profile=audit_manifest.get("profile"),
        worker_stage_plan=audit_manifest.get("worker_stage_plan"),
        completed_stages=audit_manifest.get("completed_stages"),
        forbidden_stages_not_run=audit_manifest.get("forbidden_stages_not_run"),
        provider_mode=audit_manifest.get("provider_mode"),
        discipline_id=audit_manifest.get("discipline_id"),
        discipline_profile_hash=audit_manifest.get("discipline_profile_hash"),
    )
    return {
        "archive": str(archive),
        "sha256": sha256_file(archive),
        "bytes": archive.stat().st_size,
        "files": len(manifest.get("files") or []),
        "tree_hash": manifest.get("tree_hash"),
        "stage_completion": manifest.get("stage_completion"),
        "resume_hint": manifest.get("resume_hint"),
        "forbidden_stages_not_run": manifest.get("forbidden_stages_not_run"),
    }


# ═══════════════════════════════ main ═══════════════════════════════════════

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — полный worker-участок конвейера")
    parser.add_argument("--mode", choices=("fake", "real"), required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--source-package", required=True, type=Path)
    parser.add_argument("--sandbox-root", required=True, type=Path)
    parser.add_argument("--action", default="full")
    parser.add_argument("--max-inferences", type=int, default=16)
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument(
        "--policy-root", type=Path, default=None,
        help="корень данных воркера с provider_policy.json (локальная политика моделей)",
    )
    parser.add_argument(
        "--forbidden-literal", action="append", default=[],
        help="значение, которого не должно быть в ответе модели (канарейка)",
    )
    args = parser.parse_args(argv)

    report: dict[str, Any] = {
        "stage": "11F",
        "mode": args.mode,
        "task_id": args.task_id,
        "host": socket.gethostname(),
        "started_at": utc_now(),
    }

    attempt = build_attempt(
        sandbox_root=args.sandbox_root,
        task_id=args.task_id,
        source_package=args.source_package,
    )
    job_dir = attempt["job_dir"]
    manifest = attempt["source_manifest"]
    project_id = str(manifest.get("project_id") or "")
    version_id = manifest.get("version_id")
    report["source_package"] = {
        "path": str(args.source_package),
        "sha256": sha256_file(args.source_package),
        "project_id": project_id,
        "version_id": version_id,
    }

    fake_cli = None
    if args.mode == "fake":
        fake_cli = make_fake_cli(job_dir / "providers" / "fake" / "claude")

    job_id = f"{args.task_id}-job"
    attempt_id = f"{args.task_id}-a1"
    issued = issue_grant_and_binding(
        job_dir=job_dir,
        task_id=args.task_id,
        attempt_id=attempt_id,
        job_id=job_id,
        max_inferences=args.max_inferences,
        fake_cli=fake_cli,
        forbidden_literals=tuple(args.forbidden_literal),
        policy_root=args.policy_root,
    )
    report["provider"] = {
        "grant_id": issued["grant_id"],
        "model": issued["model"],
        "capability": issued["capability"],
        "allowed_stages": list(ALLOWED_STAGES),
        "max_inferences": args.max_inferences,
        "executable": str(fake_cli) if fake_cli else "штатный путь установщика",
    }

    spec_path = build_run_spec(
        job_dir=job_dir, job_id=job_id, attempt_id=attempt_id,
        project_id=project_id, version_id=version_id, action=args.action,
        provider_mode="real",
        max_inferences=args.max_inferences,
        runtime_snapshot_hash=manifest.get("runtime_snapshot_hash"),
        prompt_bundle_hash=manifest.get("prompt_bundle_hash"),
        model_config_hash=manifest.get("model_config_hash"),
        discipline_id=manifest.get("discipline_id"),
        discipline_profile_hash=manifest.get("discipline_profile_hash"),
    )
    # provider_mode=fake несовместим с привязкой (bind_providers это проверяет):
    # в режиме подделок мост к настоящему CLI недопустим. Здесь подделка
    # ПРИВЯЗАНА к адаптеру, то есть путь остаётся провайдерским, а бинарь —
    # поддельным. Поэтому спека объявляет режим real в обоих случаях, а факт
    # подделки фиксируется отдельным полем отчёта.
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["provider_mode"] = "real"
    spec["_11f_fake_cli"] = bool(fake_cli)
    write_json(spec_path, spec)

    run = run_pipeline(
        job_dir=job_dir,
        spec_path=spec_path,
        binding_path=issued["binding_path"],
        fake_cli=fake_cli,
        python_bin=args.python,
        log_path=job_dir / "logs" / "pipeline.log",
    )
    report["run"] = run
    report["finished_at"] = utc_now()

    audit_manifest = job_dir / "result" / "audit_manifest.json"
    if audit_manifest.is_file():
        report["audit_manifest"] = json.loads(audit_manifest.read_text(encoding="utf-8"))

    ledger_dir = job_dir / "work" / "provider_ledger"
    if not ledger_dir.is_dir():
        ledger_dir = job_dir / "providers"
    report["ledger_files"] = sorted(
        str(p.relative_to(job_dir)) for p in job_dir.rglob("*.json")
        if "ledger" in str(p)
    )
    fake_journal = job_dir / "logs" / "fake_provider_calls.jsonl"
    if fake_journal.is_file():
        lines = [json.loads(x) for x in fake_journal.read_text(encoding="utf-8").splitlines() if x.strip()]
        report["fake_calls"] = {
            "total": len(lines),
            "with_images": sum(1 for x in lines if x.get("images")),
            "stream": sum(1 for x in lines if x.get("stream")),
        }

    # ─── Журнал вызовов модели: считается по ЖУРНАЛУ ПОПЫТКИ ────────────────
    # Не по счётчикам этапов и не по журналу подделки: подделка запускается
    # адаптером с окружением, собранным с нуля, и переменных диагностики не
    # видит (I-P6). Единственный источник истины о числе вызовов — ledger.
    report["model_calls"] = collect_ledger(job_dir)

    # ─── Пакет результата (§38) ─────────────────────────────────────────────
    try:
        report["result_package"] = build_result(
            job_dir=job_dir, job_id=job_id, attempt_id=attempt_id,
            project_id=project_id, version_id=version_id,
            source_package_hash=report["source_package"]["sha256"],
            exit_code=int(run["exit_code"]),
        )
    except Exception as exc:                            # noqa: BLE001
        report["result_package"] = {"error": f"{type(exc).__name__}: {exc}"}

    write_json(job_dir / "metadata" / "11F_RUN.json", report)
    print(json.dumps(report, ensure_ascii=False, indent=2)[:4000])
    return 0 if run["exit_code"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
