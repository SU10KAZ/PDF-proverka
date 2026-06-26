"""Общая тест-конфигурация.

Тесты не должны зависеть от production `.env`. В частности, портальная
аутентификация (`PORTAL_AUTH_ENABLED=true` в prod) ломает TestClient-тесты,
которые ходят в API без логина. Жёстко выключаем её ДО импорта приложения.

`backend/app/main.py` грузит `.env` через `os.environ.setdefault(...)`, поэтому
переменная, выставленная здесь раньше импорта, не перезаписывается значением из
`.env`.
"""
import os

import pytest

os.environ["PORTAL_AUTH_ENABLED"] = "false"

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
def _isolate_storage_cutover_env(monkeypatch):
    """Every test starts from legacy storage unless it opts into v2 explicitly."""
    for name, value in _DEFAULT_STORAGE_ENV.items():
        monkeypatch.setenv(name, value)


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
