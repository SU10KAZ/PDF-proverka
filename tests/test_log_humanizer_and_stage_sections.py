"""Тесты вкладки «Лог»: гуманизация строк + жизненный цикл audit_log.jsonl.

Покрывают:
  1. log_humanizer.humanize_log_line — подавление сырого JSON/diff-мусора,
     очеловечивание событий Codex CLI и JSON-тел ошибок API, сохранение
     легитимных строк (включая начинающиеся с кавычки);
  2. лог переписывается ТОЛЬКО при полном перезапуске пайплайна
     (reset_audit_log архивирует файл); retry/resume этапов ДОПИСЫВАЮТ —
     ни чужие секции, ни собственная история ошибок этапа не удаляются;
  3. reset_audit_log — архив файла + WS log_reset;
  4. WSMessage.log_reset — форма сообщения.
"""
import asyncio
import json

import pytest

from backend.app.models.audit import AuditJob, AuditStage
from backend.app.models.websocket import WSMessage
from backend.app.services.common import audit_logger
from backend.app.services.common.log_humanizer import (
    humanize_log_line,
    split_known_prefix,
)


# ─── 1. Гуманизатор ─────────────────────────────────────────────────


@pytest.mark.parametrize("raw", [
    '{"type":"thread.started","thread_id":"019f68cb"}',
    '{"type":"turn.started"}',
    '{"type":"item.started","item":{"id":"item_0"}}',
    '{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"{}"}}',
])
def test_codex_stream_events_are_suppressed(raw):
    assert humanize_log_line(raw).text is None


def test_codex_turn_completed_is_humanized():
    raw = ('{"type":"turn.completed","usage":{"input_tokens":43696,'
           '"cached_input_tokens":8064,"output_tokens":10637,'
           '"reasoning_output_tokens":7748}}')
    result = humanize_log_line(raw)
    assert result.text is not None
    assert "Codex" in result.text
    assert "43 696" in result.text
    assert "10 637" in result.text
    assert "{" not in result.text  # никакого сырого JSON


def test_codex_turn_failed_becomes_error_line():
    raw = '{"type":"turn.failed","error":{"message":"rate limit"}}'
    result = humanize_log_line(raw)
    assert result.text == "Codex: ошибка — rate limit"
    assert result.level == "error"


def test_api_error_body_is_humanized_not_suppressed():
    """Однострочное JSON-тело ошибки API (codex печатает в stderr) — это
    диагностика, её нельзя молча глотать."""
    for raw, expected_part in [
        ('{"error":{"message":"usage limit reached"}}', "usage limit reached"),
        ('[OPT codex] {"error":{"message":"usage limit reached"}}', "usage limit reached"),
        ('[ERR] {"detail":"Internal Server Error"}', "Internal Server Error"),
    ]:
        result = humanize_log_line(raw)
        assert result.text is not None, raw
        assert expected_part in result.text
        assert result.level == "error"


def test_full_artifact_json_line_is_suppressed():
    raw = '{"stage": "02_text_analysis", "project_id": "X", "text_source": "md"}'
    assert humanize_log_line(raw).text is None


@pytest.mark.parametrize("raw", [
    '[OPT codex] +        "id": "OPT-1",',
    '[OPT codex]       ],',
    '[OPT codex]     },',
    '"F-001",',
    '      "source_url": null,',
    '{',
    '}',
    '],',
    '  5,',
    '  true,',
    '*** Begin Patch',
    '*** Update File: _output/optimization.json',
    '@@ -1,3 +1,3 @@',
    '[OPT codex] OpenAI Codex v0.144.2',
    '[OPT codex] workdir: /home/coder/projects',
    '[OPT codex] --------',
    'exec',
    'thinking',
    'tokens used: 12345',
])
def test_json_diff_and_banner_noise_is_suppressed(raw):
    assert humanize_log_line(raw).text is None


@pytest.mark.parametrize("raw", [
    '[ 1/10] OK blk_eb4d79 p=5 t=23.5s findings=2',
    '[TIMEOUT] Процесс превысил таймаут 300 сек.',
    '[ERR] Traceback (most recent call last):',
    '═══ ЭТАП 2: Текстовый анализ MD (Claude) ═══',
    'Шаг 1: Извлечение нормативных ссылок из замечаний...',
    '- **Проблема:** Цитата пункта не подтверждена',
    '### F-012',
    '[OPT claude] Claude запущен',
    'Найдено 19 уникальных нормативных ссылок',
    # Строки с кавычками — человеческие, не JSON-фрагменты:
    '"03_findings.json" создан',
    '- "СП 256.1325800.2016" статус: заменён',
    '- "ВВГнг 3х2.5" не найден в спецификации',
])
def test_human_lines_pass_through_unchanged(raw):
    result = humanize_log_line(raw)
    assert result.text == raw


def test_original_level_is_preserved_for_plain_lines():
    assert humanize_log_line("Ошибка оптимизации (код 1)", "error").level == "error"


def test_split_known_prefix():
    assert split_known_prefix('[OPT claude] {"type":"result"}') == (
        "[OPT claude] ", '{"type":"result"}'
    )
    assert split_known_prefix("обычная строка") == ("", "обычная строка")


# ─── 2. Retry дописывает, ничего не удаляя ──────────────────────────


def _seed_log(tmp_path, entries):
    log_path = tmp_path / "audit_log.jsonl"
    with open(log_path, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    return log_path


def _stages_and_messages(log_path):
    out = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            e = json.loads(line)
            out.append((e["stage"], e["message"]))
    return out


@pytest.fixture
def project_log_dir(tmp_path, monkeypatch):
    """audit_logger пишет в tmp_path."""
    monkeypatch.setattr(
        audit_logger, "_project_output_dir", lambda project_id: tmp_path,
    )
    return tmp_path


@pytest.fixture
def ws_broadcasts(monkeypatch):
    """Перехват WS-broadcast'ов audit_logger (sync и async путь)."""
    calls = []

    def _capture_sync(project_id, message):
        calls.append((project_id, message))

    async def _capture_async(project_id, message):
        calls.append((project_id, message))

    monkeypatch.setattr(
        audit_logger.ws_manager, "schedule_broadcast_to_project", _capture_sync,
    )
    monkeypatch.setattr(
        audit_logger.ws_manager, "broadcast_to_project", _capture_async,
    )
    return calls


def test_stage_retry_appends_and_keeps_history(project_log_dir, ws_broadcasts):
    """Перезапуск этапа: старые записи — и чужих секций, и СВОЕЙ (включая
    ошибки прошлых попыток) — остаются; новые дописываются в конец.
    Лог переписывается только при полном перезапуске пайплайна."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "crop_blocks", "message": "кроп прошлого прогона"},
        {"timestamp": "t2", "level": "error", "stage": "optimization", "message": "прошлая попытка упала"},
    ])
    audit_logger.persist_log("P1", "▶ новая оптимизация", "info", "optimization")
    audit_logger.persist_log("P1", "шаг 2", "info", "optimization")

    assert _stages_and_messages(log_path) == [
        ("crop_blocks", "кроп прошлого прогона"),
        ("optimization", "прошлая попытка упала"),
        ("optimization", "▶ новая оптимизация"),
        ("optimization", "шаг 2"),
    ]
    # Посекционных сбросов больше не существует
    assert not [m for _, m in ws_broadcasts if m.type == "log_stage_reset"]


def test_log_to_project_appends_and_broadcasts_only_log(project_log_dir, ws_broadcasts):
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "findings_merge", "message": "старая"},
    ])
    job = AuditJob(job_id="j1", project_id="P1", stage=AuditStage.FINDINGS_MERGE)
    asyncio.run(audit_logger.log_to_project(job, "═══ Свод замечаний ═══"))

    assert _stages_and_messages(log_path) == [
        ("findings_merge", "старая"),
        ("findings_merge", "═══ Свод замечаний ═══"),
    ]
    types = [m.type for _, m in ws_broadcasts]
    assert types == ["log"]
    assert ws_broadcasts[0][1].data["stage"] == "findings_merge"


def test_stage_override_wins_over_racing_job_stage(project_log_dir, ws_broadcasts):
    """Параллельная группа: norm_verify мутирует ОБЩИЙ job.stage, но строки
    верификатора с явным stage_override атрибуцируются своей секции."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "norm_verify", "message": "старые нормы"},
    ])
    job = AuditJob(job_id="j1", project_id="P1", stage=AuditStage.FINDINGS_REVIEW)
    # Гонка: параллельная задача норм переставила stage на общем job
    job.stage = AuditStage.NORM_VERIFY
    asyncio.run(audit_logger.log_to_project(
        job, "структурные проверки: ок", stage_override="findings_review",
    ))

    assert _stages_and_messages(log_path) == [
        ("norm_verify", "старые нормы"),
        ("findings_review", "структурные проверки: ок"),
    ]
    log_msg = [m for _, m in ws_broadcasts if m.type == "log"][0]
    assert log_msg.data["stage"] == "findings_review"


def test_update_pipeline_log_running_does_not_touch_audit_log(project_log_dir, ws_broadcasts):
    """running не трогает audit_log.jsonl: заголовки, уже записанные
    оркестратором, и история прошлых попыток не теряются."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "optimization", "message": "строка"},
    ])
    audit_logger.update_pipeline_log("P1", "optimization", "running")
    assert _stages_and_messages(log_path) == [("optimization", "строка")]


def test_reset_audit_log_broadcasts_log_reset(project_log_dir, ws_broadcasts):
    _seed_log(project_log_dir, [
        {"timestamp": "2026-07-16T10:00:00", "level": "info", "stage": "excel", "message": "a"},
    ])
    audit_logger.reset_audit_log("P1")

    assert not (project_log_dir / "audit_log.jsonl").exists()  # заархивирован
    assert [m.type for _, m in ws_broadcasts] == ["log_reset"]


# ─── 4. WSMessage-фабрика ───────────────────────────────────────────


def test_ws_log_reset_shape():
    msg = WSMessage.log_reset("P1")
    assert msg.type == "log_reset"
    assert msg.project == "P1"
    assert msg.data == {}
