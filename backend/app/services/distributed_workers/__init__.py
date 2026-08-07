"""Распределённые audit-worker — центральная часть (этап 0).

Вертикальный срез инфраструктуры: регистрация воркера → одобрение оператором →
heartbeat → ручная выдача БЕЗОПАСНОГО тестового задания `test_pipeline_v1` →
передача пакета → приём событий и логов → возврат и проверка результата.

Чего здесь НЕТ и не должно появиться на этом этапе:
  * запуска реального аудита и любых точек врезки в PipelineManager;
  * вызовов Claude Code / Codex;
  * нормативного этапа и записи в decisions_log.json / norms_paragraphs.json;
  * произвольных shell-команд от центра (см. models/distributed_workers.py:
    command_type и job_type — закрытые enum).

Вся подсистема выключена флагом DISTRIBUTED_WORKERS_ENABLED (default false):
при выключенном флаге роутеры не регистрируются, база не создаётся, фоновых
задач нет.

Архитектурная основа: docs/distributed_audit_workers/02_technical_design.md
"""
