"""
run_v4_pipeline.py
-------------------
Standalone v4 pipeline:
  1. Load project → block_batches
  2. For each batch → Gemini extraction (new prompt) → typed_facts_batch_*.json
  3. build_canonical_memory_v2 → canonical_memory_v2.json
  4. candidate_generators_v2 → candidates_v2.json
  5. (optional) judge stage — skipped in MVP
  6. finding_formatter → 03_findings_v4.json
  7. evaluate_v4 vs etalon.jsonl

Usage:
    python prototype_v4/run_v4_pipeline.py projects/EOM/133_23-ГК-ГРЩ
    python prototype_v4/run_v4_pipeline.py projects/EOM/133_23-ГК-ГРЩ --skip-extraction
    python prototype_v4/run_v4_pipeline.py projects/EOM/133_23-ГК-ГРЩ --only-batch 1
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from pathlib import Path

# Чтобы можно было импортировать из webapp
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("v4_pipeline")


# ─── Extraction stage: run Gemini with new prompt ─────────────────────────


async def run_extraction_batch(
    batch_data: dict,
    project_info: dict,
    project_id: str,
    total_batches: int,
    output_dir: Path,
    extraction_prompt_path: Path,
) -> tuple[bool, str]:
    """Запустить extraction для одного batch через llm_runner (Gemini/GPT)."""
    from webapp.services import prompt_builder
    from webapp.services.llm_runner import run_llm
    from webapp.services.project_service import resolve_project_dir

    batch_id = batch_data["batch_id"]
    log.info(f"Batch {batch_id}: extraction starting...")

    # Используем prompt_builder но подменяем шаблон на наш
    # Подход: собираем messages как для обычного block_batch, потом заменяем system
    try:
        # Базовый messages (interleaved с изображениями)
        messages = prompt_builder.build_block_batch_messages(
            batch_data, project_info, project_id, total_batches
        )
    except Exception as e:
        log.error(f"Batch {batch_id}: failed to build messages: {e}")
        return (False, str(e))

    # Загружаем наш новый extraction prompt
    extraction_template = extraction_prompt_path.read_text(encoding="utf-8")

    # Подставляем плейсхолдеры в наш prompt
    blocks = batch_data.get("blocks", [])
    block_ids = [b["block_id"] for b in blocks]
    block_pages = [b.get("page") for b in blocks if b.get("page")]

    from webapp.services.task_builder import (
        _load_document_graph,
        _build_structured_block_context,
        _get_md_file_path,
        _extract_page_to_sheet_map,
    )

    graph = _load_document_graph(project_id)
    if graph:
        md_context = _build_structured_block_context(graph, block_ids, block_pages)
    else:
        md_context = "(document_graph.json not available)"

    md_file_path = _get_md_file_path(project_info, project_id)
    page_to_sheet = _extract_page_to_sheet_map(md_file_path)

    block_lines = []
    for block in blocks:
        pdf_page = block.get("page", "?")
        sheet_info = page_to_sheet.get(pdf_page, "")
        sheet_suffix = f", Sheet {sheet_info}" if sheet_info else ""
        block_lines.append(
            f"- block_id: {block['block_id']}, page: {pdf_page}{sheet_suffix}, "
            f"OCR: {block.get('ocr_label', 'image')}"
        )

    extraction_prompt = (
        extraction_template
        .replace("{BATCH_ID}", str(batch_id))
        .replace("{BATCH_ID_PADDED}", f"{batch_id:03d}")
        .replace("{TOTAL_BATCHES}", str(total_batches))
        .replace("{PROJECT_ID}", project_id)
        .replace("{SECTION}", (project_info or {}).get("section", "EOM"))
        .replace("{BLOCK_COUNT}", str(len(blocks)))
        .replace("{BLOCK_LIST}", "\n".join(block_lines))
        .replace("{BLOCK_MD_CONTEXT}", md_context or "(no context)")
        .replace("{OUTPUT_PATH}", str(output_dir))
    )

    # Убираем CLI-специфичные инструкции (Read/Write tool)
    # Наш extraction prompt изначально содержит "через Read tool" — это для Claude CLI,
    # а для OpenRouter API нужно просто передать изображения в messages
    import re
    cli_patterns = [
        r"^.*Read tool.*$",
        r"^.*Write tool.*$",
        r"^.*WRITE via Write.*$",
        r"^.*прочитать КАЖДЫЙ через Read.*$",
        r"^.*Пиши JSON через Write.*$",
    ]
    for pat in cli_patterns:
        extraction_prompt = re.sub(pat, "", extraction_prompt, flags=re.IGNORECASE | re.MULTILINE)

    # Заменяем system сообщение
    messages[0] = {"role": "system", "content": extraction_prompt}

    # Запуск LLM
    try:
        start = time.monotonic()
        result = await run_llm(
            stage=f"block_batch_{batch_id:03d}",
            messages=messages,
            timeout=900,  # 15 минут на batch
        )
        duration = time.monotonic() - start
    except Exception as e:
        log.error(f"Batch {batch_id}: LLM call failed: {e}")
        return (False, str(e))

    if result.is_error:
        log.error(f"Batch {batch_id}: LLM returned error: {result.error_message}")
        return (False, result.error_message or "unknown error")

    # Сохраняем typed_facts
    out_path = output_dir / f"typed_facts_batch_{batch_id:03d}.json"
    if result.json_data:
        out_path.write_text(
            json.dumps(result.json_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info(f"Batch {batch_id}: saved to {out_path.name} "
                 f"({duration:.0f}s, ${result.cost_usd:.2f}, "
                 f"{result.input_tokens}→{result.output_tokens} tokens)")
        return (True, "")
    else:
        # Сохраняем raw text если JSON не распарсился
        raw_path = out_path.with_suffix(".raw.txt")
        raw_path.write_text(result.text or "", encoding="utf-8")
        log.error(f"Batch {batch_id}: JSON parse failed, raw saved to {raw_path.name}")
        return (False, "JSON parse failed")


async def run_all_extractions(
    project_dir: Path,
    extraction_prompt_path: Path,
    only_batch: int | None = None,
) -> int:
    """Запустить extraction для всех batches проекта."""
    project_id = str(project_dir.relative_to(project_dir.parent.parent)).replace("\\", "/")

    # Загрузить project_info
    info_path = project_dir / "project_info.json"
    if not info_path.exists():
        log.error(f"project_info.json не найден в {project_dir}")
        return 1

    project_info = json.loads(info_path.read_text(encoding="utf-8"))

    # Загрузить block_batches
    out_dir = project_dir / "_output"
    batches_path = out_dir / "block_batches.json"
    if not batches_path.exists():
        log.error(f"block_batches.json не найден. Сначала запусти blocks.py batches")
        return 1

    batches_data = json.loads(batches_path.read_text(encoding="utf-8"))
    batches = batches_data.get("batches", [])
    total = len(batches)

    if only_batch is not None:
        batches = [b for b in batches if b["batch_id"] == only_batch]
        if not batches:
            log.error(f"Batch {only_batch} не найден")
            return 1

    log.info(f"Running extraction on {len(batches)} batch(es) (of {total} total)")

    # Sequential для безопасности (параллель — позже)
    errors = 0
    for batch in batches:
        ok, err = await run_extraction_batch(
            batch, project_info, project_id, total, out_dir, extraction_prompt_path
        )
        if not ok:
            errors += 1
            log.error(f"Batch {batch['batch_id']}: {err}")

    log.info(f"Extraction done: {len(batches) - errors}/{len(batches)} successful")
    return errors


# ─── Post-extraction: canonical memory + candidates ───────────────────────


def run_canonical_memory(project_dir: Path) -> bool:
    """Построить canonical_memory_v2.json."""
    log.info("Building canonical memory...")

    from build_canonical_memory_v2 import build_from_typed_facts

    memory = build_from_typed_facts(project_dir)
    stats = memory.stats()

    if stats.get("total_lines", 0) == 0 and stats.get("total_panels", 0) == 0:
        log.error(f"Canonical memory is empty! typed_facts_batch_*.json may be missing or invalid")
        return False

    out_path = project_dir / "_output" / "canonical_memory_v2.json"
    out_path.write_text(
        json.dumps(memory.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"Saved: {out_path.name}")
    log.info(f"Stats: {stats}")
    return True


def run_candidate_generators(project_dir: Path) -> dict:
    """Запустить candidate generators."""
    log.info("Running candidate generators...")

    from candidate_generators_v2 import generate_all_candidates

    mem_path = project_dir / "_output" / "canonical_memory_v2.json"
    memory = json.loads(mem_path.read_text(encoding="utf-8"))

    results = generate_all_candidates(memory)

    out_path = project_dir / "_output" / "candidates_v2.json"
    out_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"Saved: {out_path.name}")
    log.info(f"Stats: {results['stats']}")
    return results


# ─── Finding formatter ─────────────────────────────────────────────────────


SEVERITY_MAP = {
    "CRITICAL": "КРИТИЧЕСКОЕ",
    "EXPLOITATION": "ЭКСПЛУАТАЦИОННОЕ",
    "RECOMMENDED": "РЕКОМЕНДАТЕЛЬНОЕ",
    "CHECK_ADJACENT": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

CATEGORY_MAP = {
    2: "documentation",  # identity
    3: "documentation",  # cross-view
    4: "protection",     # requirement conflict (mostly breakers)
    6: "calculation",
    7: "calculation",
}


def format_candidates_to_findings(candidates: dict, project_id: str) -> dict:
    """Конвертировать candidates_v2 в формат 03_findings.json для UI."""
    all_candidates = candidates.get("all_candidates", [])

    findings = []
    for i, c in enumerate(all_candidates, start=1):
        class_id = c.get("issue_class_id", 0)
        severity_raw = c.get("candidate_claim", {}).get("proposed_severity", "RECOMMENDED")
        severity = SEVERITY_MAP.get(severity_raw, "РЕКОМЕНДАТЕЛЬНОЕ")
        category = CATEGORY_MAP.get(class_id, "documentation")

        # Собираем evidence pages
        evidence = c.get("evidence", [])
        pages = sorted(set(e.get("page") for e in evidence if e.get("page")))
        sheets = sorted(set(e.get("sheet") for e in evidence if e.get("sheet")))

        finding = {
            "id": f"F-{i:03d}",
            "severity": severity,
            "category": category,
            "sheet": ", ".join(sheets) if sheets else None,
            "page": pages if len(pages) > 1 else (pages[0] if pages else None),
            "problem": c.get("candidate_claim", {}).get("summary", "")[:300],
            "description": c.get("candidate_claim", {}).get("summary", ""),
            "solution": None,
            "norm": None,
            "norm_quote": None,
            "source_block_ids": [
                e.get("block_id") for e in evidence if e.get("block_id")
            ],
            "evidence": [
                {
                    "type": "block",
                    "block_id": e.get("block_id"),
                    "page": e.get("page"),
                    "sheet": e.get("sheet"),
                }
                for e in evidence
            ],
            "quality": {
                "v4_candidate_id": c.get("candidate_id"),
                "v4_class": class_id,
                "v4_subtype": c.get("subtype"),
                "v4_entity_key": c.get("entity_key"),
                "v4_field": c.get("field"),
                "v4_matching_policy": c.get("matching_policy"),
                "v4_flags": c.get("flags", {}),
            },
        }
        findings.append(finding)

    return {
        "meta": {
            "project_id": project_id,
            "audit_completed": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "total_findings": len(findings),
            "source": "v4_pipeline",
            "by_severity": {
                sev: sum(1 for f in findings if f["severity"] == sev)
                for sev in set(f["severity"] for f in findings)
            },
        },
        "findings": findings,
    }


def run_finding_formatter(project_dir: Path, project_id: str) -> bool:
    """Преобразовать candidates → 03_findings_v4.json."""
    log.info("Formatting findings...")

    cand_path = project_dir / "_output" / "candidates_v2.json"
    candidates = json.loads(cand_path.read_text(encoding="utf-8"))

    formatted = format_candidates_to_findings(candidates, project_id)

    out_path = project_dir / "_output" / "03_findings_v4.json"
    out_path.write_text(
        json.dumps(formatted, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info(f"Saved: {out_path.name} ({len(formatted['findings'])} findings)")
    return True


# ─── Evaluation ────────────────────────────────────────────────────────────


def run_evaluation(project_dir: Path, etalon_path: Path) -> bool:
    """Оценить candidates против etalon.jsonl."""
    log.info("Running evaluation...")

    from evaluate_v4 import evaluate, load_candidates, load_etalon, print_report

    cand_path = project_dir / "_output" / "candidates_v2.json"
    if not cand_path.exists():
        log.error(f"{cand_path} не найден")
        return False

    candidates = load_candidates(cand_path)
    etalon = load_etalon(etalon_path)

    result = evaluate(candidates, etalon)
    print_report(result, title=f"v4 Pipeline — {project_dir.name}")

    # Save result
    out_path = project_dir / "_output" / "v4_eval_result.json"
    out = {
        "counts": result["counts"],
        "metrics": result["metrics"],
        "per_class_recall": result["per_class_recall"],
        "missed_keeps": [
            {"etalon_no": e.get("etalon_no"), "text": e.get("raw_text", "")[:150]}
            for e in result["missed_keeps"]
        ],
        "tp_summary": [
            {"match_type": m["match_type"], "etalon_no": m["etalon"].get("etalon_no")}
            for m in result["tp"]
        ],
    }
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"Saved: {out_path.name}")
    return True


# ─── Main ──────────────────────────────────────────────────────────────────


async def main_async():
    parser = argparse.ArgumentParser(description="v4 pipeline standalone runner")
    parser.add_argument("project", help="Путь к проекту (projects/EOM/...)")
    parser.add_argument("--skip-extraction", action="store_true", help="Пропустить extraction (использовать существующие typed_facts)")
    parser.add_argument("--only-batch", type=int, help="Запустить только указанный batch_id")
    parser.add_argument("--etalon", default="etalon.jsonl", help="Путь к эталону")
    parser.add_argument("--extraction-prompt", default="prototype_v4/block_extraction_task_v2.md")
    args = parser.parse_args()

    project_dir = Path(args.project).resolve()
    if not project_dir.exists():
        log.error(f"{project_dir} не найден")
        return 1

    project_id = str(project_dir.relative_to(project_dir.parent.parent)).replace("\\", "/")
    log.info(f"Running v4 pipeline on: {project_id}")

    extraction_prompt_path = BASE_DIR / args.extraction_prompt

    # STAGE 1: Extraction (unless skipped)
    if not args.skip_extraction:
        log.info("═══ STAGE 1: Extraction ═══")
        if not extraction_prompt_path.exists():
            log.error(f"Extraction prompt не найден: {extraction_prompt_path}")
            return 1
        errors = await run_all_extractions(project_dir, extraction_prompt_path, args.only_batch)
        if errors > 0:
            log.error(f"Extraction failed: {errors} errors")
            return 1
    else:
        log.info("Skipping extraction (--skip-extraction)")

    # STAGE 2: Canonical memory
    log.info("═══ STAGE 2: Canonical Memory ═══")
    if not run_canonical_memory(project_dir):
        return 1

    # STAGE 3: Candidate generators
    log.info("═══ STAGE 3: Candidate Generators ═══")
    run_candidate_generators(project_dir)

    # STAGE 4: Finding formatter
    log.info("═══ STAGE 4: Finding Formatter ═══")
    run_finding_formatter(project_dir, project_id)

    # STAGE 5: Evaluation
    log.info("═══ STAGE 5: Evaluation ═══")
    etalon_path = BASE_DIR / args.etalon
    if etalon_path.exists():
        run_evaluation(project_dir, etalon_path)
    else:
        log.warning(f"Эталон {etalon_path} не найден, пропускаем evaluation")

    log.info("═══ DONE ═══")
    return 0


def main():
    sys.exit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
