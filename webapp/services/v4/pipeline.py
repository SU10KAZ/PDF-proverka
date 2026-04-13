"""
pipeline.py — оркестрация v4 stages 2-4 (после extraction).

Запускает:
  Stage 2: build_canonical_memory → canonical_memory_v2.json
  Stage 3: candidate_generators    → candidates_v2.json
  Stage 4: finding_formatter       → 03_findings.json (совместимый формат)

Используется claude_runner.run_findings_merge_v4() после того как
extraction отработал и все typed_facts_batch_*.json лежат в _output.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from webapp.services.v4 import canonical_memory, formatter

log = logging.getLogger(__name__)

# ─── V4 Config loader ─────────────────────────────────────────────────────

_DISCIPLINES_DIR = Path(__file__).parent.parent.parent.parent / "prompts" / "disciplines"


def load_v4_config(discipline_code: str) -> dict:
    """Загрузить v4_config.json для дисциплины.

    Fallback chain:
      1. prompts/disciplines/{CODE}/v4_config.json
      2. prompts/disciplines/_generic/v4_config.json
      3. Пустой дефолтный конфиг (class 2+3 generic)
    """
    # 1. Discipline-specific
    disc_path = _DISCIPLINES_DIR / discipline_code / "v4_config.json"
    if disc_path.exists():
        try:
            config = json.loads(disc_path.read_text(encoding="utf-8"))
            log.info("[v4] Loaded v4_config for %s (%s)", discipline_code, disc_path)
            return config
        except Exception as e:
            log.warning("[v4] Failed to load %s: %s", disc_path, e)

    # 2. Generic fallback
    generic_path = _DISCIPLINES_DIR / "_generic" / "v4_config.json"
    if generic_path.exists():
        try:
            config = json.loads(generic_path.read_text(encoding="utf-8"))
            log.info("[v4] Using generic v4_config (no config for %s)", discipline_code)
            return config
        except Exception as e:
            log.warning("[v4] Failed to load generic config: %s", e)

    # 3. Hardcoded minimal fallback
    log.warning("[v4] No v4_config found for %s, using empty defaults", discipline_code)
    return {
        "extraction": {"entity_types": ["item", "spec_row", "note", "room"]},
        "canonical_memory": {},
        "generators": {
            "class3_cross_view": {"watch_fields": {"item": ["designation", "description"]}},
        },
        "field_labels": {},
        "field_units": {},
        "norms": {},
    }


def run_memory_stage(output_dir: Path, config: dict | None = None) -> dict:
    """Stage 2: построить canonical memory."""
    log.info("[v4] Stage 2: building canonical memory")
    memory = canonical_memory.build_from_typed_facts(output_dir, config=config)
    memory_path = canonical_memory.save_canonical_memory(memory, output_dir)
    stats = memory.stats()
    log.info("[v4] canonical memory stats: %s", stats)
    return {
        "memory_path": str(memory_path),
        "stats": stats,
    }


def run_candidates_stage(output_dir: Path, config: dict | None = None) -> dict:
    """Stage 3: генераторы кандидатов замечаний."""
    from webapp.services.v4 import generators

    log.info("[v4] Stage 3: running candidate generators")

    memory_path = output_dir / "canonical_memory_v2.json"
    memory_dict = json.loads(memory_path.read_text(encoding="utf-8"))

    # Собираем все кандидаты через 3 генератора
    all_candidates: list[dict] = []
    by_generator: dict[str, list[dict]] = {}

    for name, fn in (
        ("class2_identity", generators.generate_class2_identity_candidates),
        ("class3_cross_view", generators.generate_class3_cross_view_candidates),
        ("class4_requirement_conflict", generators.generate_class4_requirement_conflict_candidates),
    ):
        try:
            cands = fn(memory_dict, config=config)
        except TypeError:
            # Fallback: старая сигнатура без config
            cands = fn(memory_dict)
        except Exception as e:
            log.exception("[v4] Generator %s failed: %s", name, e)
            cands = []
        by_generator[name] = cands
        all_candidates.extend(cands)

    stats = {
        f"{name}": len(cands) for name, cands in by_generator.items()
    }
    stats["total"] = len(all_candidates)

    result = {
        "by_generator": by_generator,
        "all_candidates": all_candidates,
        "stats": stats,
    }

    cand_path = output_dir / "candidates_v2.json"
    cand_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("[v4] candidates stats: %s", stats)
    return {
        "candidates_path": str(cand_path),
        "stats": stats,
    }


def _merge_text_analysis_findings(formatted: dict, output_dir: Path) -> int:
    """Добавить замечания из 01_text_analysis.json в findings.

    Возвращает количество добавленных замечаний.
    """
    text_path = output_dir / "01_text_analysis.json"
    if not text_path.exists():
        return 0

    try:
        text_data = json.loads(text_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0

    text_findings = text_data.get("text_findings", text_data.get("findings", []))
    if not text_findings:
        return 0

    existing = formatted.get("findings", [])
    next_id = len(existing) + 1

    added = 0
    for tf in text_findings:
        # Парсинг source → sheet / page
        source_str = tf.get("source") or ""
        sheet = tf.get("sheet") or ""
        page = tf.get("page")

        if source_str and not sheet:
            import re
            m_sheet = re.search(r'[Лл]ист\s+(\d+)', source_str)
            if m_sheet:
                sheet = f"Лист {m_sheet.group(1)}"
            m_page = re.search(r'стр\.?\s*(\d+)', source_str)
            if m_page and page is None:
                page = int(m_page.group(1))

        # Текст замечания
        problem_text = tf.get("finding") or tf.get("problem") or tf.get("description") or ""

        # Рекомендация: если нет явной — формируем из нормы
        solution = tf.get("solution") or tf.get("recommendation") or ""
        if not solution:
            norm = tf.get("norm") or ""
            if norm:
                solution = f"Исправить в соответствии с {norm}"

        finding = {
            "id": f"F-{next_id:03d}",
            "severity": tf.get("severity", "РЕКОМЕНДАТЕЛЬНОЕ"),
            "category": tf.get("category", "documentation"),
            "sheet": sheet,
            "page": page,
            "problem": problem_text,
            "description": problem_text,
            "norm": tf.get("norm") or tf.get("norm_reference") or "",
            "norm_quote": tf.get("norm_quote"),
            "norm_confidence": tf.get("norm_confidence"),
            "solution": solution,
            "risk": tf.get("risk") or "",
            "related_block_ids": tf.get("related_block_ids") or [],
            "evidence": tf.get("evidence") or [],
            "source_ref": source_str,
            "quality": {
                "v4_source": "text_analysis",
                "original_id": tf.get("id", ""),
            },
        }
        existing.append(finding)
        next_id += 1
        added += 1

    formatted["findings"] = existing

    # Пересчитать meta
    by_severity: dict[str, int] = {}
    for f in existing:
        sev = f.get("severity", "НЕИЗВЕСТНО")
        by_severity[sev] = by_severity.get(sev, 0) + 1
    formatted["meta"]["total_findings"] = len(existing)
    formatted["meta"]["by_severity"] = by_severity
    formatted["meta"]["text_analysis_merged"] = added

    return added


def run_formatter_stage(
    output_dir: Path,
    project_id: str,
    blocks_analyzed: int = 0,
    config: dict | None = None,
) -> dict:
    """Stage 4: format candidates → 03_findings.json."""
    log.info("[v4] Stage 4: formatting findings")

    cand_path = output_dir / "candidates_v2.json"
    candidates = json.loads(cand_path.read_text(encoding="utf-8"))

    formatted = formatter.format_candidates_to_findings(
        candidates, project_id, blocks_analyzed=blocks_analyzed, config=config
    )

    # Добавить замечания из LLM-анализа текста (01_text_analysis)
    text_added = _merge_text_analysis_findings(formatted, output_dir)
    if text_added:
        log.info("[v4] merged %d text-analysis findings", text_added)

    findings_path = formatter.save_findings(formatted, output_dir)
    log.info(
        "[v4] wrote %s (%d findings)",
        findings_path.name, len(formatted["findings"]),
    )
    return {
        "findings_path": str(findings_path),
        "total_findings": len(formatted["findings"]),
        "by_severity": formatted["meta"].get("by_severity", {}),
    }


async def run_post_extraction_pipeline(
    project_id: str,
    blocks_analyzed: int = 0,
    stage_callback=None,
    discipline_code: str = "EOM",
) -> dict:
    """Запустить stages 2-4 после того как extraction отработал.

    Args:
        project_id: ID проекта.
        blocks_analyzed: сколько блоков обработано (для meta.blocks_analyzed в findings).
        stage_callback: async callable(stage_key, status) — для обновления pipeline_log.
        discipline_code: код дисциплины для загрузки v4_config.

    Returns:
        dict с результатами каждой стадии.
    """
    from webapp.services.project_service import resolve_project_dir
    output_dir = resolve_project_dir(project_id) / "_output"

    # Загружаем дисциплинарный конфиг
    config = load_v4_config(discipline_code)

    results: dict = {}

    # Stage 2: canonical memory
    if stage_callback:
        await stage_callback("v4_memory", "running")
    try:
        results["memory"] = run_memory_stage(output_dir, config=config)
        if stage_callback:
            await stage_callback("v4_memory", "done")
    except Exception as e:
        log.exception("[v4] memory stage failed: %s", e)
        if stage_callback:
            await stage_callback("v4_memory", "error")
        raise

    # Stage 3: candidate generators (с конфигом дисциплины)
    if stage_callback:
        await stage_callback("v4_candidates", "running")
    try:
        results["candidates"] = run_candidates_stage(output_dir, config=config)
        if stage_callback:
            await stage_callback("v4_candidates", "done")
    except Exception as e:
        log.exception("[v4] candidates stage failed: %s", e)
        if stage_callback:
            await stage_callback("v4_candidates", "error")
        raise

    # Stage 4: formatter
    if stage_callback:
        await stage_callback("v4_formatter", "running")
    try:
        results["formatter"] = run_formatter_stage(
            output_dir, project_id, blocks_analyzed=blocks_analyzed, config=config
        )
        if stage_callback:
            await stage_callback("v4_formatter", "done")
    except Exception as e:
        log.exception("[v4] formatter stage failed: %s", e)
        if stage_callback:
            await stage_callback("v4_formatter", "error")
        raise

    return results
