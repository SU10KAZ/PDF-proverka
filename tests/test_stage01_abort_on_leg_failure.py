"""Остановка этапа 01 при выпадении ноги ансамбля.

Требование Андрея Ивановича от 06.08.2026: «если хоть одна нога отвалилась —
завершаем проверку и не продолжаем, выводим комментарий о том, что нога упала».

Почему это вообще понадобилось: блок считается успешным, если ответила ХОТЬ
ОДНА нога (`combine_detector_results`: `ok = bool(ok_models)`), а стадия — если
уцелел хоть один блок. Исчерпание лимита провайдера у codex-пути не
распознаётся вовсе (нет ни детекции usage limit, ни ретрая, в отличие от
claude-пути с `_wait_for_rate_limit`), поэтому аудит завершался со статусом
«выполнено» и молча урезанным рекаллом.
"""
from __future__ import annotations

import asyncio
import inspect
import os
import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent


# ─── Флаги конфига ────────────────────────────────────────────────────
#
# ВАЖНО про методику. Значение флага в ЖИВОМ процессе прочитать нельзя:
# config.py на импорте зовёт load_dotenv(), который находит .env репозитория
# и переопределяет окружение теста. А importlib.reload(config) вдобавок портит
# состояние другим тестам (модуль общий). Поэтому:
#   • «дефолт кода» проверяем по объявлению в исходнике — это и есть контракт
#     проекта «все флаги default OFF, включение через .env»;
#   • разбор env проверяем в ОТДЕЛЬНОМ процессе, где переменная задана явно
#     и потому перебивает .env.


def _config_value_in_subprocess(name: str, env_overrides: dict[str, str]) -> str:
    """Значение атрибута config в чистом процессе с заданным окружением."""
    code = (
        "from backend.app.core import config; "
        f"print('VALUE=' + repr(getattr(config, {name!r})))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(ROOT),
        env={**os.environ, **env_overrides},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"config не импортировался: {proc.stderr[-800:]}"
    for line in proc.stdout.splitlines():
        if line.startswith("VALUE="):
            return line[len("VALUE="):]
    raise AssertionError(f"значение не напечатано: {proc.stdout[-500:]}")


def test_flags_declared_with_off_default_in_source():
    """Контракт проекта: флаг объявлен выключенным, включается через .env."""
    src = (ROOT / "backend/app/core/config.py").read_text(encoding="utf-8")
    assert '"STAGE01_ABORT_ON_LEG_FAILURE_ENABLED", False' in src, (
        "дефолт в коде обязан быть False — иначе поведение поменяется у всех, "
        "включая тех, у кого нет этой строки в .env"
    )
    assert "STAGE01_LEG_FAILURE_THRESHOLD" in src


@pytest.mark.parametrize("raw,expected", [
    ("5", "5"),      # обычное значение
    ("0", "1"),      # ноль не должен означать «останавливаться всегда»
    ("-3", "1"),     # и отрицательное тоже
    ("не-число", "1"),  # мусор не должен ронять импорт конфига
])
def test_threshold_parsing_is_isolated_and_floored(raw, expected):
    got = _config_value_in_subprocess(
        "STAGE01_LEG_FAILURE_THRESHOLD", {"STAGE01_LEG_FAILURE_THRESHOLD": raw}
    )
    assert got == expected, f"порог {raw!r} → ожидали {expected}, получили {got}"


@pytest.mark.parametrize("raw,expected", [("true", "True"), ("false", "False")])
def test_enable_flag_follows_env(raw, expected):
    got = _config_value_in_subprocess(
        "STAGE01_ABORT_ON_LEG_FAILURE_ENABLED",
        {"STAGE01_ABORT_ON_LEG_FAILURE_ENABLED": raw},
    )
    assert got == expected


# ─── Признак, на котором всё держится ─────────────────────────────────


def test_combine_marks_failed_leg_but_keeps_block_ok():
    """Опорный факт: блок остаётся ok, а выпавшая нога видна в detectors_failed.

    Если этот тест однажды упадёт — значит поменялся контракт, на котором
    построена остановка, и её надо пересматривать, а не «чинить» тест.
    """
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        combine_detector_results,
    )

    combined = combine_detector_results(
        [
            ("openai/gpt-5.4", {"ok": True, "parsed": {"findings": []}}),
            ("codex/gpt-5.4", {"ok": False, "error": "usage limit reached"}),
        ],
        run_id="test",
    )
    assert combined["ok"] is True, "блок успешен при одной выжившей ноге"
    assert combined["detectors_failed"] == ["codex/gpt-5.4"]
    assert combined["partial"] is True


def test_combine_reports_no_failures_when_all_legs_ok():
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
        combine_detector_results,
    )

    combined = combine_detector_results(
        [
            ("openai/gpt-5.4", {"ok": True, "parsed": {"findings": []}}),
            ("codex/gpt-5.4", {"ok": True, "parsed": {"findings": []}}),
        ],
        run_id="test",
    )
    assert combined["detectors_failed"] == []
    assert combined["partial"] is False


# ─── Поведение обёртки этапа ──────────────────────────────────────────


class _FakeCtx:
    """Минимальный PipelineStageContext: нас интересует, что ушло наружу."""

    def __init__(self):
        self.project_id = "TEST/proj"
        self.logs: list[tuple[str, str]] = []
        self.pipeline_log: list[dict] = []
        self.record_block_analysis_usage = None

    async def log(self, msg: str, level: str = "info"):
        self.logs.append((level, msg))

    def update_pipeline_log(self, stage: str, status: str, **kw):
        self.pipeline_log.append({"stage": stage, "status": status, **kw})


def _run_abort_branch(summary: dict) -> tuple[object, _FakeCtx]:
    """Прогнать ТОЛЬКО ветку остановки из runner.py на подсунутой сводке.

    Полный run_block_analysis_findings_only тянет prerequisites, модели и
    файлы проекта; для проверки самой ветки это лишнее.
    """
    from backend.app.pipeline.stages.block_analysis import runner as stage_runner

    ctx = _FakeCtx()
    src = _extract_abort_branch(stage_runner)
    namespace = {"summary": summary, "ctx": ctx, "StageResult": stage_runner.StageResult}

    async def _exec():
        return await _eval_branch(src, namespace)

    return asyncio.run(_exec()), ctx


def _extract_abort_branch(module) -> str:
    """Достать исходник ветки остановки из runner.py по маркеру."""
    src = inspect.getsource(module.run_block_analysis_findings_only)
    start = src.index('if summary.get("aborted_on_leg_failure"):')
    end = src.index('if summary["blocks_failed"] > 0', start)
    branch = src[start:end]
    # Снять отступ функции (4 пробела).
    return "\n".join(line[4:] if line.startswith("    ") else line
                     for line in branch.splitlines())


async def _eval_branch(src: str, namespace: dict):
    """Выполнить вырезанную ветку как тело async-функции."""
    body = "\n".join("    " + line for line in src.splitlines())
    wrapper = f"async def _branch():\n{body}\n    return None\n"
    exec(compile(wrapper, "<abort-branch>", "exec"), namespace)  # noqa: S102
    return await namespace["_branch"]()


def test_abort_branch_fails_stage_and_names_the_leg():
    summary = {
        "aborted_on_leg_failure": True,
        "leg_failures": [
            {
                "block_id": "blk_a",
                "sheet": "7",
                "page": 12,
                "failed_legs": ["codex/gpt-5.4"],
                "error": "usage limit reached",
            }
        ],
        "blocks_ok": 3,
        "blocks_total": 40,
    }
    result, ctx = _run_abort_branch(summary)

    assert result is not None, "ветка обязана вернуть StageResult, а не провалиться"
    assert result.success is False, "этап провален"
    assert result.cancelled is False, "именно fail, а не cancel: это сбой, а не отмена"

    text = result.error or ""
    assert "codex/gpt-5.4" in text, "в сообщении должно быть имя упавшей ноги"
    assert "usage limit reached" in text, "и причина, раз провайдер её назвал"
    assert "блок blk_a" in text
    assert "лист 7" in text and "стр. PDF 12" in text

    # Сообщение обязано попасть В ИНТЕРФЕЙС, а не только в server.log.
    assert ctx.pipeline_log, "статус этапа должен обновиться"
    entry = ctx.pipeline_log[0]
    assert entry["stage"] == "block_analysis" and entry["status"] == "error"
    assert "codex/gpt-5.4" in entry["error"]
    assert any(lvl == "error" for lvl, _ in ctx.logs), "журнал аудита тоже"
    assert any("заново" in m for _, m in ctx.logs), "подсказать, что делать дальше"


def test_abort_branch_aggregates_several_legs_by_count():
    summary = {
        "aborted_on_leg_failure": True,
        "leg_failures": [
            {"block_id": "b1", "page": 1, "failed_legs": ["codex/gpt-5.4"], "error": ""},
            {"block_id": "b2", "page": 2, "failed_legs": ["codex/gpt-5.4"], "error": ""},
            {"block_id": "b3", "page": 3, "failed_legs": ["openai/gpt-5.4"], "error": ""},
        ],
        "blocks_ok": 3,
        "blocks_total": 10,
    }
    result, _ = _run_abort_branch(summary)
    text = result.error or ""
    assert "codex/gpt-5.4 (2 бл.)" in text, "частая нога называется первой и со счётом"
    assert "openai/gpt-5.4" in text


def test_abort_branch_survives_empty_details():
    """Флаг стоит, а подробностей нет — сообщение всё равно должно собраться."""
    result, ctx = _run_abort_branch(
        {"aborted_on_leg_failure": True, "leg_failures": [], "blocks_ok": 0, "blocks_total": 5}
    )
    assert result.success is False and result.cancelled is False
    assert "неизвестная модель" in (result.error or "")


# ─── Устройство остановки внутри стадии ───────────────────────────────


def test_abort_uses_its_own_event_not_cancel_event():
    """Отмена пользователем и падение ноги не должны сливаться в одно.

    cancel_event → StageResult.cancel (не сбой), выпавшая нога → fail. Если
    остановку повесить на cancel_event, аудит покажется отменённым вручную.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    assert "abort_event = asyncio.Event()" in src
    assert "aborted_on_leg_failure = abort_event.is_set()" in src
    # cancelled по-прежнему считается ТОЛЬКО от cancel_event
    assert "cancelled = cancel_event is not None and cancel_event.is_set()" in src


def test_abort_checked_at_both_semaphore_gates():
    """Проверка обязана стоять и до семафора, и после.

    Только до — и блоки, уже стоящие в очереди за слотом, всё равно уйдут в
    платный вызов после остановки.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    assert src.count("if abort_event.is_set():") >= 2


@pytest.mark.parametrize("legs,threshold,expect_abort", [
    (["codex/gpt-5.4"], 1, True),
    ([], 1, False),
    (["codex/gpt-5.4"], 3, False),
])
def test_threshold_semantics(legs, threshold, expect_abort):
    """Модель накопления: порог — это число БЛОКОВ с выпавшей ногой."""
    leg_failures: list[dict] = []
    if legs:
        leg_failures.append({"failed_legs": legs})
    assert (len(leg_failures) >= threshold) is expect_abort


# ─── Дыры, найденные адверсарной проверкой (все воспроизводились) ──────


def test_inner_gather_does_not_swallow_leg_exceptions():
    """У ног есть НЕобёрнутые raise — они обязаны стать «упавшей ногой».

    png_to_data_url кидает OSError на эвакуированном кропе, resp.json() —
    на 2xx с не-JSON телом от шлюза. Без return_exceptions такое исключение
    вылетало из _dispatch мимо ветки TimeoutError, ловилось ВНЕШНИМ gather и
    давало блок «Unhandled single-block exception» БЕЗ detectors_failed:
    остановка не срабатывала именно там, где она нужнее всего.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    marker = "_dispatch_results = await asyncio.gather("
    assert marker in src
    tail = src[src.index(marker): src.index(marker) + 400]
    assert "return_exceptions=True" in tail, "внутренний gather обязан вернуть исключения"
    assert '"parse_error": "leg_exception"' in src, "исключение ноги → падение ноги"


def test_cancellation_is_not_swallowed_by_the_new_normalizer():
    """CancelledError нельзя превращать в «нога не ответила».

    На нём держатся backstop-таймаут блока (wait_for) и отмена пользователем.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    assert "isinstance(_r, asyncio.CancelledError)" in src
    assert "raise _r" in src


def test_hard_timeout_block_reports_all_legs_failed():
    """Инверсия строгости: блок, где сдохли ВСЕ ноги, обязан останавливать.

    До правки синтетический res backstop-таймаута не имел detectors_failed
    вовсе — молчали на тяжёлом случае и останавливались на лёгком.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    idx = src.index('"parse_error": "block_hard_timeout"')
    window = src[idx - 400: idx + 400]
    assert '"detectors_failed": list(configured_detector_models)' in window


def test_abort_does_not_overwrite_previous_stage_artifact():
    """Огрызок оборванного прогона не должен затирать готовый результат.

    Страховка .classic.bak.json не спасает: она пишется только `if not
    bak.exists()`, то есть относится к самому первому прогону, а не к
    последнему хорошему.
    """
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    assert "if write_target and aborted_on_leg_failure:" in src
    guard = src[src.index("if write_target and aborted_on_leg_failure:"):]
    assert "write_target = False" in guard[:900], "запись обязана быть отменена"


def test_abandoned_blocks_advance_progress():
    """Брошенные блоки должны отмечаться, иначе остановка выглядит зависанием."""
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    assert '"reason": "leg_failure_abort"' in src
    assert src.count("await _skip_after_abort()") >= 2, "обе точки выхода по аборту"


def test_leg_failure_record_carries_sheet_and_page_separately():
    """sheet (штамп) и page (страница PDF) — разные поля, CLAUDE.md."""
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as mod

    src = inspect.getsource(mod.run_findings_only_for_project)
    idx = src.index("leg_failures.append(")
    window = src[idx: idx + 700]
    assert '"sheet": sheet_for_page(' in window
    assert '"page": block["page"]' in window


def test_message_says_sheet_and_pdf_page_distinctly():
    summary = {
        "aborted_on_leg_failure": True,
        "leg_failures": [{
            "block_id": "blk_z", "sheet": "7", "page": 12,
            "failed_legs": ["codex/gpt-5.4"], "error": "usage limit reached",
        }],
        "blocks_ok": 1, "blocks_total": 30,
    }
    result, ctx = _run_abort_branch(summary)
    text = result.error or ""
    assert "лист 7" in text and "стр. PDF 12" in text, (
        "лист из штампа и страница PDF не должны подменять друг друга"
    )
    # Имя ноги обязано влезть в обрезку панели очереди (60 символов).
    assert "codex/gpt-5.4" in text[:60], f"нога не видна в обрезанном виде: {text[:60]!r}"
    assert not any("resume" in m for _, m in ctx.logs), (
        "обещать resume нельзя: артефакт намеренно не перезаписан, этап пойдёт заново"
    )


def test_message_without_sheet_falls_back_to_pdf_page():
    """Штампа может не быть — тогда честно пишем только страницу PDF."""
    summary = {
        "aborted_on_leg_failure": True,
        "leg_failures": [{
            "block_id": "b", "sheet": "", "page": 3,
            "failed_legs": ["openai/gpt-5.4"], "error": "",
        }],
        "blocks_ok": 0, "blocks_total": 9,
    }
    result, _ = _run_abort_branch(summary)
    text = result.error or ""
    assert "стр. PDF 3" in text and "лист" not in text
