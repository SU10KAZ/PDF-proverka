"""Изолированный headless-драйвер пилота PIPELINE_NORMS_AFTER_MERGE_ENABLED.

Гонит ПОЛНЫЙ аудит на песочнице-копии AR7 в LEGACY storage-режиме
(без v2-регистрации/shadow-mirror), с флагом norms-after-merge = ON.
Env выставляется ДО импортов (load_dotenv override=False → наши значения победят).
"""
import os

# ── env ДО любых импортов приложения (load_dotenv override=False → наши победят) ──
os.environ["PIPELINE_NORMS_AFTER_MERGE_ENABLED"] = "true"
os.environ["AUDIT_STORAGE_BACKEND"] = "legacy"
os.environ["AUDIT_PROJECTS_V2_WRITE_MODE"] = "legacy"
os.environ["AUDIT_PROJECTS_V2_SHADOW_API_ENABLED"] = "false"
os.environ["AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED"] = "false"
os.environ["AUDIT_PROJECTS_V2_READ_CANARY_ENABLED"] = "false"
os.environ.setdefault("PAID_API_ENABLED", "true")

# Профиль обработки old_ngrok (vibe лежит; ngrok жив, держит gemma-4-26b-a4b + qwen3.6-35b).
# Подмена ТОЛЬКО в этом процессе — live .env не трогаем. Значения из server_profiles.PROFILES.
os.environ["CHANDRA_BASE_URL"] = "https://louvred-madie-gigglier.ngrok-free.dev"
os.environ["CHANDRA_AUTH_MODE"] = "basic"
os.environ["CHANDRA_CHAT_TRANSPORT"] = "native"
os.environ["GEMMA_ADAPTIVE_RELOAD_ENABLED"] = "true"
os.environ["LMSTUDIO_BASE_URL"] = "https://louvred-madie-gigglier.ngrok-free.dev/v1"
os.environ["STAGE_COMPARISON_GRAPHIC_LLM_BASE_URL"] = "https://louvred-madie-gigglier.ngrok-free.dev"
os.environ["STAGE_COMPARISON_GRAPHIC_LLM_AUTH"] = "basic"
os.environ["STAGE_COMPARISON_GRAPHIC_LLM_ENABLE_MODEL_LOAD"] = "true"

import sys
import asyncio
from datetime import datetime
from uuid import uuid4

PID = "AR/AR7_norms_pilot"
# Жёсткая привязка объекта 213 Мосфильмовская (0b540226): без неё _get_projects_dir
# берёт current_id из objects.json (=214 Alia) → split-brain путей (write→214, read→213).
# ContextVar наследуется в параллельные asyncio-задачи пост-findings.
OBJ = "0b540226"


def dry_check():
    from backend.app.core import config as cfg
    from backend.app.services.storage import storage_write_facade as swf
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.services.common.project_service import bind_object
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus  # noqa

    bind_object(OBJ)
    print("bound object:", OBJ)
    print("flag PIPELINE_NORMS_AFTER_MERGE_ENABLED:", cfg.PIPELINE_NORMS_AFTER_MERGE_ENABLED)
    print("helper _norms_after_merge_enabled():", pipeline_manager._norms_after_merge_enabled())
    print("v2_is_primary():", swf.v2_is_primary())
    print("PROJECTS_DIR:", cfg.PROJECTS_DIR)

    job = _make_job()
    root, vdir, out = pipeline_manager._resolve_job_paths(job)
    print("root_dir:", root)
    print("version_dir:", vdir)
    print("output_dir:", out)
    print("md exists:", (vdir / "133-23-ГК-АР7 (2)_document.md").exists())
    return job


def _make_job():
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    return AuditJob(
        job_id=str(uuid4()),
        object_id=OBJ,
        project_id=PID,
        version_id=None,
        stage=AuditStage.PREPARE,
        status=JobStatus.RUNNING,
        started_at=datetime.now().isoformat(),
    )


async def run_full():
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.services.common.project_service import bind_object
    from backend.app.models.audit import JobStatus
    bind_object(OBJ)
    job = _make_job()
    pipeline_manager.active_jobs[PID] = job
    pipeline_manager._tasks[PID] = asyncio.current_task()
    print(f"[{datetime.now().isoformat()}] ▶ START full audit {PID}")
    try:
        await pipeline_manager._run_ocr_pipeline(job, include_optimization=True)
    except Exception as e:
        import traceback
        print(f"[{datetime.now().isoformat()}] ✗ EXCEPTION: {e}")
        traceback.print_exc()
    print(f"[{datetime.now().isoformat()}] ■ END status={job.status} err={job.error_message}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "dry"
    if mode == "dry":
        dry_check()
    elif mode == "run":
        dry_check()
        asyncio.run(run_full())
