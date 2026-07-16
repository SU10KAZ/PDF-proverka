"""Тесты переработки вкладки «Лог»: гуманизация строк + per-stage сброс.

Покрывают:
  1. log_humanizer.humanize_log_line — подавление сырого JSON/diff-мусора,
     очеловечивание событий Codex CLI и JSON-тел ошибок API, сохранение
     легитимных строк (включая начинающиеся с кавычки);
  2. «сброс при первой записи»: begin_log_run + persist_log/log_to_project —
     первая запись секции в рамках action переписывает файл без её старых
     записей и шлёт WS log_stage_reset, дальнейшие записи дописываются;
  3. заголовок этапа, записанный оркестратором ДО running, не теряется;
  4. reset_audit_log — WS log_reset при свежем прогоне;
  5. WSMessage.log_reset / log_stage_reset — форма сообщений.
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


# ─── 2-4. «Сброс при первой записи» ─────────────────────────────────


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
    """audit_logger пишет в tmp_path; freshness-состояние изолировано."""
    monkeypatch.setattr(
        audit_logger, "_project_output_dir", lambda project_id: tmp_path,
    )
    monkeypatch.setattr(audit_logger, "_FRESH_LOG_STAGES", {})
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


def test_first_write_of_section_rewrites_it(project_log_dir, ws_broadcasts):
    """Retry этапа: первая запись секции удаляет её записи прошлого прогона,
    чужие секции не тронуты."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "optimization", "message": "старый прогон"},
        {"timestamp": "t2", "level": "info", "stage": "excel", "message": "оставить"},
    ])
    audit_logger.begin_log_run("P1")
    audit_logger.persist_log("P1", "▶ новая оптимизация", "info", "optimization")
    audit_logger.persist_log("P1", "шаг 2", "info", "optimization")

    assert _stages_and_messages(log_path) == [
        ("excel", "оставить"),
        ("optimization", "▶ новая оптимизация"),
        ("optimization", "шаг 2"),
    ]
    reset_events = [m for _, m in ws_broadcasts if m.type == "log_stage_reset"]
    assert len(reset_events) == 1  # только на ПЕРВОЙ записи секции
    assert reset_events[0].data["stages"] == ["optimization"]


def test_header_written_before_running_survives(project_log_dir, ws_broadcasts):
    """Оркестратор пишет заголовок этапа ДО update_pipeline_log(running) —
    заголовок принадлежит новому прогону и не должен стираться."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "text_analysis", "message": "старая строка"},
    ])
    audit_logger.begin_log_run("P1")
    # 1. manager._log: заголовок этапа (job.stage уже text_analysis)
    audit_logger.persist_log("P1", "═══ ЭТАП 2: Текстовый анализ ═══", "info", "text_analysis")
    # 2. runner вызывает running — раньше здесь стоял деструктивный хук
    audit_logger.update_pipeline_log("P1", "text_analysis", "running")
    # 3. рабочие строки этапа
    audit_logger.persist_log("P1", "анализ идёт", "info", "text_analysis")

    assert _stages_and_messages(log_path) == [
        ("text_analysis", "═══ ЭТАП 2: Текстовый анализ ═══"),
        ("text_analysis", "анализ идёт"),
    ]


def test_alias_group_block_context_clears_gemma_legacy(project_log_dir, ws_broadcasts):
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "gemma_enrichment", "message": "legacy"},
        {"timestamp": "t2", "level": "info", "stage": "block_context", "message": "старая"},
        {"timestamp": "t3", "level": "info", "stage": "excel", "message": "оставить"},
    ])
    audit_logger.begin_log_run("P1")
    audit_logger.persist_log("P1", "обогащение", "info", "block_context")

    assert _stages_and_messages(log_path) == [
        ("excel", "оставить"),
        ("block_context", "обогащение"),
    ]


def test_no_clearing_without_begin_log_run(project_log_dir, ws_broadcasts):
    """Записи вне action-контекста (prepare-очередь) просто дописываются."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "prepare_data", "message": "старая"},
    ])
    audit_logger.persist_log("P1", "новая", "info", "prepare_data")

    assert _stages_and_messages(log_path) == [
        ("prepare_data", "старая"),
        ("prepare_data", "новая"),
    ]
    assert not [m for _, m in ws_broadcasts if m.type == "log_stage_reset"]


def test_reset_broadcast_sent_even_when_file_empty(project_log_dir, ws_broadcasts):
    """Файл пуст (заархивирован), но в памяти вкладки могли остаться записи —
    сигнал «секция начата заново» обязателен и при removed=0."""
    audit_logger.begin_log_run("P1")
    audit_logger.persist_log("P1", "первая строка", "info", "findings_merge")

    reset_events = [m for _, m in ws_broadcasts if m.type == "log_stage_reset"]
    assert len(reset_events) == 1
    assert reset_events[0].data["stages"] == ["findings_merge"]


def test_batch_system_log_is_not_cleared(project_log_dir, ws_broadcasts):
    audit_logger.begin_log_run("__BATCH__")
    _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "prepare", "message": "история"},
    ])
    audit_logger.persist_log("__BATCH__", "pause requested", "warn", "prepare")
    log_path = project_log_dir / "audit_log.jsonl"
    assert len(_stages_and_messages(log_path)) == 2  # ничего не удалено


def test_log_to_project_orders_reset_before_line(project_log_dir, ws_broadcasts):
    """WS: кадр log_stage_reset обязан уйти РАНЬШЕ кадра с первой строкой —
    иначе фронт стёр бы только что показанную строку нового прогона."""
    _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "findings_merge", "message": "старая"},
    ])
    audit_logger.begin_log_run("P1")
    job = AuditJob(job_id="j1", project_id="P1", stage=AuditStage.FINDINGS_MERGE)
    asyncio.run(audit_logger.log_to_project(job, "═══ Свод замечаний ═══"))

    types = [m.type for _, m in ws_broadcasts]
    assert types == ["log_stage_reset", "log"]
    log_msg = ws_broadcasts[1][1]
    assert log_msg.data["message"] == "═══ Свод замечаний ═══"
    assert log_msg.data["stage"] == "findings_merge"


def test_update_pipeline_log_running_does_not_touch_audit_log(project_log_dir, ws_broadcasts):
    """running больше НЕ чистит секцию (старый хук удалён): заголовки, уже
    записанные оркестратором, не теряются."""
    log_path = _seed_log(project_log_dir, [
        {"timestamp": "t1", "level": "info", "stage": "optimization", "message": "строка"},
    ])
    audit_logger.begin_log_run("P1")
    audit_logger.update_pipeline_log("P1", "optimization", "running")
    assert _stages_and_messages(log_path) == [("optimization", "строка")]
    assert not [m for _, m in ws_broadcasts if m.type == "log_stage_reset"]


def test_clear_stage_log_entries_keeps_corrupted_lines(project_log_dir):
    log_path = project_log_dir / "audit_log.jsonl"
    log_path.write_text(
        '{"stage": "excel", "message": "x"}\n'
        "НЕ JSON — обрывок после kill\n",
        encoding="utf-8",
    )
    removed = audit_logger.clear_stage_log_entries("P1", ("excel",))
    assert removed == 1
    assert log_path.read_text(encoding="utf-8").strip() == "НЕ JSON — обрывок после kill"


def test_reset_audit_log_broadcasts_log_reset(project_log_dir, ws_broadcasts):
    _seed_log(project_log_dir, [
        {"timestamp": "2026-07-16T10:00:00", "level": "info", "stage": "excel", "message": "a"},
    ])
    audit_logger.reset_audit_log("P1")

    assert not (project_log_dir / "audit_log.jsonl").exists()  # заархивирован
    assert [m.type for _, m in ws_broadcasts] == ["log_reset"]


# ─── 5. WSMessage-фабрики ───────────────────────────────────────────


def test_ws_log_reset_shape():
    msg = WSMessage.log_reset("P1")
    assert msg.type == "log_reset"
    assert msg.project == "P1"
    assert msg.data == {}


def test_ws_log_stage_reset_shape():
    msg = WSMessage.log_stage_reset("P1", ["block_context", "gemma_enrichment"])
    assert msg.type == "log_stage_reset"
    assert msg.data["stages"] == ["block_context", "gemma_enrichment"]
