"""Read-only pipeline projection for completed runs predating stage history."""
from __future__ import annotations

from . import run_storage


def restore_pipeline(session_id: str, pair_id: str, state: dict) -> dict:
    # Pin reads to this state, never to a newer current_run pointer or a donor.
    if (state.get("engine") != "projectchange_v3" or state.get("pipeline_stages")
            or state.get("status") not in {"COMPLETED", "REVIEW"}
            or not state.get("run_id")):
        return {}
    try:
        result = run_storage.read(run_storage.artifact_path(
            session_id, pair_id, "project_change_v3_result", state["run_id"]))
    except (OSError, ValueError):
        return {}
    if (not isinstance(result, dict) or result.get("schema") != "projectchange_v3_final/2"
            or any(result.get(k) != v for k, v in (
                ("session_id", session_id), ("pair_id", pair_id), ("run_id", state["run_id"])))):
        return {}

    stages = {}

    def mark(stage, status, message, **details):
        stages[stage] = {"status": status, "message": message, **details}

    source = result.get("source_manifest") or {}
    if all(source.get(k) for k in ("old_pdf_sha256", "new_pdf_sha256", "structure_sha256")):
        mark("SOURCE_PREPARATION", "COMPLETED", "Подготовленные источники сохранены в результате.")
    reused = bool(state.get("resumed_from_run_id"))
    if isinstance(result.get("projectchange_regions"), dict) and state.get("semantic_region_count") is not None:
        count = state["semantic_region_count"]
        mark("MAPPING", "REUSED" if reused else "COMPLETED", "Смысловые области сохранены в результате.",
             processed=count, total=count, unit="region")
    coverage = result.get("coverage") or {}
    if isinstance(result.get("projectchanges"), list) and "complete" in coverage:
        mark("MINING", ("REUSED" if reused else "COMPLETED") if coverage["complete"] else "PARTIAL",
             "Поиск изменений завершён." if coverage["complete"] else "Обработаны не все страницы.")
    review = result.get("supplemental_review")
    if isinstance(review, dict):
        pending = coverage.get("content_unmatched_pending", 0)
        batches = review.get("batches", 0)
        status = ("PARTIAL" if pending or review.get("failed") else "COMPLETED") if batches else "SKIPPED"
        message = (f"Осталось непроверенных страниц: {pending}." if pending else "Разбор страниц без пары завершён.")
        if not batches:
            message = "Разбор отключён." if not review.get("enabled") else "Нет страниц, требующих дополнительного разбора."
        mark("UNMATCHED_REVIEW", status, message, processed=review.get("accepted", 0), total=batches, unit="batch")
    if isinstance(result.get("dedupe"), dict):
        mark("DEDUPE", "COMPLETED", "Свод изменений сохранён в результате.")
    verification = result.get("source_verification")
    quality = result.get("quality") or {}
    if isinstance(verification, dict):
        completed = quality.get("source_verification_completed", 0)
        pending = quality.get("source_verification_pending", 0)
        attention = any(quality.get("source_verification_" + key) for key in
                        ("corrected", "conflicts", "unreadable", "pending", "failed")) or verification.get("failed")
        status = ("PARTIAL" if attention else "COMPLETED") if verification.get("batches") else "SKIPPED"
        message = f"Проверено параметров: {completed}; осталось: {pending}."
        if not verification.get("enabled"):
            message = "Проверка отключена."
        mark("SOURCE_VERIFICATION", status, message)
    mark("RESULT", "COMPLETED" if state.get("human_mapping_published") is True else "PARTIAL",
         "Результат сохранён и доступен для просмотра.")
    return {"pipeline_stages": stages, "pipeline_history_source": "saved_result"}
