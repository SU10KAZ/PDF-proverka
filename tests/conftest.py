"""Общая тест-конфигурация.

Тесты не должны зависеть от production `.env`. В частности, портальная
аутентификация (`PORTAL_AUTH_ENABLED=true` в prod) ломает TestClient-тесты,
которые ходят в API без логина. Жёстко выключаем её ДО импорта приложения.

`backend/app/main.py` грузит `.env` через `os.environ.setdefault(...)`, поэтому
переменная, выставленная здесь раньше импорта, не перезаписывается значением из
`.env`.
"""
import os
import tempfile
from pathlib import Path

import pytest

os.environ["PORTAL_AUTH_ENABLED"] = "false"

# Keep both storage generations and the object registry away from live data for
# the entire pytest process.  Per-test monkeypatches are insufficient here:
# TestClient can leave a pipeline task alive after a fixture is torn down, at
# which point that task would see the restored production paths and recreate
# ``projects/``.  A process-lifetime sandbox remains valid until Python exits.
_STORAGE_SANDBOX = tempfile.TemporaryDirectory(prefix="pdf-proverka-pytest-storage-")
_STORAGE_SANDBOX_ROOT = Path(_STORAGE_SANDBOX.name)
os.environ["AUDIT_PROJECTS_DIR"] = str(_STORAGE_SANDBOX_ROOT / "projects")
os.environ["AUDIT_OBJECTS_FILE"] = str(_STORAGE_SANDBOX_ROOT / "objects.json")

# Storage cutover flags are production-controlled and may be v2-primary in the
# developer shell. Tests must start from a deterministic legacy baseline and
# opt into projects_v2 explicitly via monkeypatch inside the test.
_DEFAULT_STORAGE_ENV = {
    "AUDIT_STORAGE_BACKEND": "legacy",
    "AUDIT_PROJECTS_V2_WRITE_MODE": "dual_write_shadow",
    "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED": "false",
}
for _name, _value in _DEFAULT_STORAGE_ENV.items():
    os.environ[_name] = _value

# Rollout-флаги stage_comparison (могут быть включены в prod `.env` для
# контролируемого прогона) нейтрализуем по умолчанию — тесты должны проверять
# поведение при ДЕФОЛТНЫХ значениях, а не подхватывать `.env`. Тест, которому
# нужен флаг ON, выставляет его сам через monkeypatch.setenv. Выставляем ДО
# импорта приложения, чтобы `os.environ.setdefault` в main.py не перезаписал.
for _rollout_flag in (
    "STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED",
    "STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED",
    "STAGE_COMPARISON_BLOCK_PDF_SOURCE_ENABLED",
    "STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED",
    "STAGE_COMPARISON_BLOCK_EQUIVALENCE_SKIP_QWEN",
):
    os.environ[_rollout_flag] = "false"


@pytest.fixture(autouse=True)
def _isolate_batch_queue_file(tmp_path, monkeypatch):
    """НИ ОДИН тест не должен писать в реальный backend/app/data/batch_queue.json.

    Тесты, дёргающие реальный PipelineManager (start_batch / add_to_batch /
    resume_interrupted_batch / _persist_queue), без изоляции перезаписывают
    прод-файл очереди в общей data-папке — инцидент: фантомная M31A-очередь
    попала в production batch_queue.json. Перенаправляем module-global
    BATCH_QUEUE_FILE в per-test tmp для КАЖДОГО теста (autouse, future-proof).

    Тест, которому нужен собственный путь (напр. restart-recovery), может
    переопределить значение повторным monkeypatch.setattr — его значение
    победит, оба корректно откатятся (LIFO).

    Только тестовая изоляция: runtime-логика и API не меняются.
    """
    try:
        import backend.app.pipeline.manager as _mgr
    except Exception:
        # Тесты, не импортирующие backend (если такие есть) — изоляция не нужна.
        return
    monkeypatch.setattr(
        _mgr, "BATCH_QUEUE_FILE", tmp_path / "batch_queue.json", raising=False
    )


@pytest.fixture(autouse=True)
def _isolate_storage_cutover_env(tmp_path, monkeypatch):
    """Keep every test away from the live ``projects_v2`` store.

    The suite intentionally starts in ``dual_write_shadow`` so a number of
    integration tests exercise the mirror hooks.  Without an explicit v2 root
    those hooks resolve ``config.DATA_DIR / projects_v2`` and write synthetic
    pytest objects plus ledger entries into production.  Each test therefore
    gets a private v2 root by default; tests that need another root can still
    override or delete the environment variable with ``monkeypatch``.
    """
    for name, value in _DEFAULT_STORAGE_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "projects_v2"))


@pytest.fixture(autouse=True)
def _isolate_schedule_completion_file(tmp_path, monkeypatch):
    """НИ ОДИН тест не должен писать в реальный knowledge_base/schedule_completion.json.

    save_expert_review теперь штампует «день завершения» проекта через
    schedule_service.set_completion_once. Тесты, дёргающие save_expert_review
    (expert-review, external_register), без изоляции пишут в живой стор графика
    (инцидент: фейковые проекты DOC-REVIEW/1232-ЧМ-КМ-1 в проде). Перенаправляем
    module-global SCHEDULE_COMPLETION_FILE в per-test tmp для КАЖДОГО теста.

    Тест, которому нужен собственный путь, переопределяет повторным
    monkeypatch.setattr (его значение победит, оба откатятся LIFO).
    """
    try:
        import backend.app.services.common.schedule_service as _sched
    except Exception:
        return
    monkeypatch.setattr(
        _sched, "SCHEDULE_COMPLETION_FILE",
        tmp_path / "schedule_completion.json", raising=False,
    )
