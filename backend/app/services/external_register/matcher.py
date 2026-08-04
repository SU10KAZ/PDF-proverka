"""LLM-driven сопоставление записей реестра СУ-10 с findings'ами платформы.

Архитектура:
  - префильтр через section_map (берём только findings из соответствующего
    подпроекта; иначе записать в needs_review=False, unmatched);
  - чанкуем по section_code: все записи реестра одной подсекции + все её
    findings → одна LLM-сессия (~30 батчей вместо 239 одиночных);
  - вызов через claude -p, --output-format json, минимум tools (только Read/Write
    в /tmp/external_register_matcher);
  - парсим возвращённый JSON, валидируем, применяем через service.apply_auto_match().

Per-finding badge выставляется внутри service.apply_auto_match() —
не дублируем логику здесь.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

from backend.app.core.config import get_claude_cli, get_stage_model
from backend.app.services.common.cli_utils import parse_cli_json_output
from backend.app.services.common.object_service import get_object_by_id
from backend.app.services.common.process_runner import run_command
from backend.app.services.external_register import section_map, service
from backend.app.services.external_register.models import (
    FindingMatch,
    MatchStatus,
    RegisterEntry,
    RegisterFile,
)

logger = logging.getLogger(__name__)


MATCH_TIMEOUT_SEC = 240   # на один чанк подсекции
MAX_FINDINGS_PER_CHUNK = 60
MAX_ENTRIES_PER_CHUNK = 20


# ─── Загрузка findings для подсекции ──────────────────────────────────────


def _load_subproject_findings(object_id: str, project_id: str) -> list[dict]:
    findings_path = service._findings_output_dir(object_id, project_id) / "03_findings.json"
    if not findings_path.exists():
        logger.info("No findings for %s (%s)", project_id, findings_path)
        return []
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Bad findings JSON %s: %s", findings_path, e)
        return []
    return data.get("findings", data.get("items", []))


def _slim_finding(f: dict) -> dict:
    """Урезанный finding для отправки в LLM — без громоздких полей."""
    return {
        "id": f.get("id"),
        "severity": f.get("severity"),
        "category": f.get("category"),
        "sheet": f.get("sheet"),
        "page": f.get("page"),
        "problem": (f.get("problem") or "")[:400],
        "norm": (f.get("norm") or "")[:300],
    }


def _slim_entry(e: RegisterEntry) -> dict:
    return {
        "key": e.key,
        "sheet_ref": e.sheet_ref[:200],
        "problem": e.problem[:400],
        "description": e.description[:600],
    }


# ─── Промпт ───────────────────────────────────────────────────────────────


def _build_prompt(entries: list[RegisterEntry], findings: list[dict]) -> str:
    entries_json = json.dumps([_slim_entry(e) for e in entries], ensure_ascii=False, indent=2)
    findings_json = json.dumps([_slim_finding(f) for f in findings], ensure_ascii=False, indent=2)

    return f"""Ты — эксперт по сопоставлению строительных замечаний.

Тебе дан список внешних замечаний (отправленных нами заказчику ранее) и
список наших findings (внутренних замечаний платформы). Для каждого внешнего
замечания нужно найти один наиболее похожий finding (или сказать «нет совпадения»).

Критерии совпадения:
1. Совпадение листа/страницы (sheet/page) — главный сигнал.
2. Совпадение смысла проблемы (тот же объект документации, та же ошибка).
3. Совпадение нормативной ссылки — сильный бонус.

Уровни уверенности:
- 0.9-1.0: лист + проблема + норма явно совпадают
- 0.7-0.89: совпадает по смыслу, лист/страница может различаться
- 0.5-0.69: тематически близко, но детали расходятся
- < 0.5: не совпадает (верни null)

Возвращай СТРОГО JSON-массив (без markdown-ограждений):
[
  {{"key": "<entry.key>", "finding_id": "F-XXX" | null, "confidence": 0.0-1.0, "rationale": "<краткое обоснование>"}},
  ...
]

Внешние замечания (entries):
{entries_json}

Наши findings:
{findings_json}

Верни массив той же длины, что entries. Никаких комментариев вне JSON.
"""


# ─── JSON-extract ─────────────────────────────────────────────────────────


_JSON_ARRAY_RE = re.compile(r"\[\s*(?:\{.*\})?\s*(?:,\s*\{.*\}\s*)*\]", re.DOTALL)


def _extract_json_array(text: str) -> Optional[list]:
    """Достать JSON-массив из текста LLM (возможна обёртка ```json … ```)."""
    if not text:
        return None
    # Снять markdown-ограждение
    t = text.strip()
    t = re.sub(r"^```(?:json)?\s*", "", t)
    t = re.sub(r"\s*```$", "", t)
    # Попробовать прямо
    try:
        v = json.loads(t)
        if isinstance(v, list):
            return v
    except json.JSONDecodeError:
        pass
    # Найти первую квадратную скобку
    m = _JSON_ARRAY_RE.search(t)
    if not m:
        # last-ditch: между первой [ и последней ]
        l, r = t.find("["), t.rfind("]")
        if l != -1 and r > l:
            try:
                return json.loads(t[l:r + 1])
            except json.JSONDecodeError:
                return None
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


# ─── LLM-запуск ───────────────────────────────────────────────────────────


async def _run_one_chunk(
    entries: list[RegisterEntry],
    findings: list[dict],
    model: str,
) -> list[dict]:
    """Один LLM-вызов на чанк entries vs findings → list of match dicts."""
    if not entries or not findings:
        return []

    prompt = _build_prompt(entries, findings)
    cmd = [
        get_claude_cli(),
        "-p",
        "--model", model,
        "--allowedTools", "Read,Write",
        "--output-format", "json",
    ]

    # Запускаем из чистой cwd, чтобы Claude CLI не подтянул проектный CLAUDE.md
    # (он содержит инструкции для аудита, не для матчинга).
    with tempfile.TemporaryDirectory(prefix="extreg_match_") as tmpdir:
        env_overrides = {k: None for k in os.environ if k.startswith("CLAUDE")}
        exit_code, stdout, stderr = await run_command(
            cmd,
            input_text=prompt,
            timeout=MATCH_TIMEOUT_SEC,
            env_overrides=env_overrides,
            cwd=tmpdir,
        )

    if exit_code != 0:
        logger.warning("matcher: claude -p returned %d, stderr=%s", exit_code, (stderr or "")[:500])
        return []

    cli_result = parse_cli_json_output(stdout or "")
    if cli_result.is_error:
        logger.warning("matcher: CLIResult is_error, text=%s", (cli_result.result_text or "")[:500])
        return []

    arr = _extract_json_array(cli_result.result_text or "")
    if not isinstance(arr, list):
        logger.warning(
            "matcher: failed to parse JSON, raw=%s",
            (cli_result.result_text or "")[:500],
        )
        return []
    return arr


# ─── Внешний API matcher'а ────────────────────────────────────────────────


async def match_register(
    object_id: str,
    register_id: str,
    *,
    only_section: Optional[str] = None,
    progress_cb=None,
    model: Optional[str] = None,
) -> dict:
    """Запустить полный matching для реестра.

    Args:
      only_section — если задано, обработать только эту подсекцию (для теста /
          ручного re-run).
      progress_cb — async (current, total, section_code) → None (опционально).
      model — переопределить модель (default — get_stage_model('findings_critic')).

    Returns:
      Dict со статистикой: {"sections_processed", "entries_processed",
      "auto_matched", "needs_review", "unmatched", "errors"}
    """
    obj = get_object_by_id(object_id)
    if obj is None:
        raise ValueError(f"Unknown object_id: {object_id}")

    register = service.load_register(object_id, register_id)
    if register is None:
        raise ValueError(f"Register not found: {register_id} for {object_id}")

    # Группируем entries по section_code
    by_section: dict[str, list[RegisterEntry]] = {}
    for entry in register.entries:
        # Подтверждённые ручные match'и не трогаем
        if entry.match_status == MatchStatus.CONFIRMED:
            continue
        by_section.setdefault(entry.section_code, []).append(entry)

    if only_section is not None:
        by_section = {k: v for k, v in by_section.items() if k == only_section}

    chosen_model = model or get_stage_model("findings_critic") or "claude-sonnet-5"

    stats = {
        "sections_processed": 0,
        "entries_processed": 0,
        "auto_matched": 0,
        "needs_review": 0,
        "unmatched": 0,
        "errors": 0,
    }

    total_sections = len(by_section)
    for idx, (section_code, entries) in enumerate(sorted(by_section.items())):
        if progress_cb:
            try:
                await progress_cb(idx, total_sections, section_code)
            except Exception:
                pass

        project_id = section_map.lookup(section_code)
        if not project_id:
            logger.info("Skipping unmapped section %s (%d entries)", section_code, len(entries))
            stats["unmatched"] += len(entries)
            continue

        try:
            findings = _load_subproject_findings(object_id, project_id)
        except Exception as e:
            logger.error("Failed loading findings for %s: %s", project_id, e)
            stats["errors"] += 1
            continue

        if not findings:
            logger.info("Section %s → no findings in %s", section_code, project_id)
            stats["unmatched"] += len(entries)
            continue

        # Чанкуем findings и entries
        findings_chunks = [
            findings[i:i + MAX_FINDINGS_PER_CHUNK]
            for i in range(0, len(findings), MAX_FINDINGS_PER_CHUNK)
        ] or [[]]
        entries_chunks = [
            entries[i:i + MAX_ENTRIES_PER_CHUNK]
            for i in range(0, len(entries), MAX_ENTRIES_PER_CHUNK)
        ]

        # Для каждой entry — берём best match среди всех findings_chunks
        # (чтобы поддержать > MAX_FINDINGS_PER_CHUNK без cross-chunk потерь).
        entry_best: dict[str, dict] = {}
        for entries_chunk in entries_chunks:
            for findings_chunk in findings_chunks:
                try:
                    results = await _run_one_chunk(entries_chunk, findings_chunk, chosen_model)
                except Exception as e:
                    logger.error("LLM call failed for %s: %s", section_code, e)
                    stats["errors"] += 1
                    continue

                for r in results:
                    if not isinstance(r, dict):
                        continue
                    key = r.get("key")
                    if not key:
                        continue
                    try:
                        conf = float(r.get("confidence", 0.0))
                    except (TypeError, ValueError):
                        conf = 0.0
                    fid = r.get("finding_id")
                    rationale = (r.get("rationale") or "")[:300]
                    if fid is None or conf <= 0:
                        continue
                    prev = entry_best.get(key)
                    if prev is None or conf > prev["confidence"]:
                        entry_best[key] = {
                            "finding_id": fid,
                            "confidence": conf,
                            "rationale": rationale,
                        }

        # Применяем
        for entry in entries:
            best = entry_best.get(entry.key)
            stats["entries_processed"] += 1
            if best is None:
                stats["unmatched"] += 1
                continue
            updated = service.apply_auto_match(
                object_id=object_id,
                register_id=register_id,
                entry_key=entry.key,
                match=FindingMatch(
                    project_id=project_id,
                    finding_id=best["finding_id"],
                    confidence=best["confidence"],
                    rationale=best["rationale"],
                ),
            )
            if updated is None:
                continue
            if updated.match_status == MatchStatus.AUTO_MATCHED:
                stats["auto_matched"] += 1
            elif updated.match_status == MatchStatus.NEEDS_REVIEW:
                stats["needs_review"] += 1
            else:
                stats["unmatched"] += 1

        stats["sections_processed"] += 1

    service.mark_matched_at(object_id, register_id)
    if progress_cb:
        try:
            await progress_cb(total_sections, total_sections, "done")
        except Exception:
            pass
    return stats


def match_register_sync(*args, **kwargs) -> dict:
    """Sync-обёртка для запуска из CLI / тестов."""
    return asyncio.run(match_register(*args, **kwargs))
