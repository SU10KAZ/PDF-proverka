"""Filesystem paths for comparison sessions, suggestions and user links."""
from __future__ import annotations

import os
from pathlib import Path

from backend.app.core.config import ROOT_DIR


def _safe_id(value: str) -> str:
    safe = "".join(char for char in str(value or "") if char.isalnum() or char in "-_")
    if not safe:
        raise ValueError("invalid id")
    return safe


def comparison_root_path() -> Path:
    raw = os.environ.get("COMPARISON_ROOT", "").strip()
    return Path(raw).expanduser().resolve() if raw else ROOT_DIR / "comparison"


def comparison_root() -> Path:
    root = comparison_root_path()
    root.mkdir(parents=True, exist_ok=True)
    return root


def sessions_root_path() -> Path:
    return comparison_root_path() / "sessions"


def session_dir(session_id: str) -> Path:
    return sessions_root_path() / _safe_id(session_id)


def session_json_path(session_id: str) -> Path:
    return session_dir(session_id) / "session.json"


def pairs_root(session_id: str) -> Path:
    return session_dir(session_id) / "pairs"


def pair_dir(session_id: str, pair_id: str) -> Path:
    return pairs_root(session_id) / _safe_id(pair_id)


def pair_json_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "pair.json"


def sheet_match_suggestions_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "sheet_match_suggestions.json"


def sheet_links_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "sheet_links.json"


def sheet_link_repairs_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "sheet_link_repairs.json"


def text_comparison_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_comparison.json"


def text_exclusions_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_exclusions.json"


def text_differences_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_differences.json"


def text_ai_review_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_ai_review.json"


def text_final_comparison_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_final_comparison.json"


def project_change_summary_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "project_change_summary.json"


def high_level_project_changes_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "high_level_project_changes.json"


def graphic_change_ledger_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "graphic_change_ledger.json"


def text_entities_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_entities.json"


def graph_entities_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "graph_entities.json"


def entity_links_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "entity_links.json"


def production_dir(session_id: str, pair_id: str) -> Path:
    """Private artifacts for the additive production comparison flow."""
    return pair_dir(session_id, pair_id) / "production"


def production_state_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "state.json"


def production_sheet_relations_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "sheet_relations.json"


def production_text_preparation_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "text_preparation.json"


def production_text_differences_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "text_differences.json"


def production_text_fact_production_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "text_fact_production.json"


def production_text_semantic_validation_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "text_semantic_validation.json"


def production_text_atoms_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "text_atoms.json"


def production_graphic_ledger_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "graphic_change_ledger.json"


def production_source_snapshot_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "source_snapshot.json"


def production_entity_relations_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "entity_relations.json"


def production_bound_atoms_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "bound_atoms.json"


def production_effective_bound_atoms_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "effective_bound_atoms.json"


def production_review_questions_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "review_questions.json"


def production_review_answers_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "review_answers.json"


def production_review_application_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "review_application.json"


def production_ai_resolutions_path(session_id: str, pair_id: str) -> Path:
    """Разрешения ИИ — ОТДЕЛЬНЫЙ файл, а не строка в ответах человека.

    review_answers.json хранит ровно один ответ на вопрос; запись машинного
    ответа туда молча затёрла бы ответ инженера.
    """
    return production_dir(session_id, pair_id) / "ai_resolutions.json"


def production_ai_routing_inventory_path(session_id: str, pair_id: str) -> Path:
    """Инвентаризация маршрутизации — отдельный файл и в режиме «Быстро».

    Она отвечает на вопрос «что ИИ мог бы взять на себя», и ответ на него
    обязан существовать даже там, где ИИ не звали: иначе сравнить режимы
    можно только запустив оба.
    """
    return production_dir(session_id, pair_id) / "ai_routing_inventory.json"


def production_ai_table_identity_path(session_id: str, pair_id: str) -> Path:
    """Тождества строк таблиц, разрешённые ИИ, и изменения по ним.

    Отдельно от electrical_table_changes: тот артефакт объявлен полностью
    детерминированным (constraints.uses_model = False), и подмешивать в него
    находки, у которых в основании лежит ответ модели, значит соврать о его
    происхождении.
    """
    return production_dir(session_id, pair_id) / "ai_table_identity.json"


def production_automatic_synthesis_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "automatic_unified_synthesis.json"


def production_unified_synthesis_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "unified_synthesis.json"


def production_engineer_decisions_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "engineer_decisions.json"


def production_final_report_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "final_report.json"


def production_electrical_table_changes_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "electrical_table_changes.json"


def production_preliminary_report_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "preliminary_report.json"


def production_ai_v2_run_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "ai_v2_run.json"


def production_ai_v2_materialization_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "ai_v2_materialization.json"


def production_ai_question_closure_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "ai_question_closure.json"


def production_human_review_plan_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "human_review_plan.json"


def production_human_review_decisions_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "human_review_decisions.json"


def production_document_inconsistencies_path(session_id: str, pair_id: str) -> Path:
    """Внутренние противоречия листов пары.

    Отдельный файл, а не строка в изменениях: у противоречия одного листа
    нет второй стороны, и попав в перечень изменений оно подделало бы
    «было → стало».
    """
    return production_dir(session_id, pair_id) / "document_inconsistencies.json"


def production_direct_page_mode2_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "direct_page_mode2.json"


def production_page_graphic_bundle_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "page_graphic_bundle.json"


def production_document_graphic_bundle_path(session_id: str, pair_id: str) -> Path:
    return production_dir(session_id, pair_id) / "document_graphic_bundle.json"


def index_json_path() -> Path:
    return comparison_root_path() / "index.json"


__all__ = [
    "comparison_root_path",
    "comparison_root",
    "sessions_root_path",
    "session_dir",
    "session_json_path",
    "pairs_root",
    "pair_dir",
    "pair_json_path",
    "sheet_match_suggestions_path",
    "sheet_links_path",
    "sheet_link_repairs_path",
    "text_comparison_path",
    "text_exclusions_path",
    "text_differences_path",
    "text_ai_review_path",
    "text_final_comparison_path",
    "project_change_summary_path",
    "high_level_project_changes_path",
    "graphic_change_ledger_path",
    "text_entities_path",
    "graph_entities_path",
    "entity_links_path",
    "production_dir",
    "production_state_path",
    "production_sheet_relations_path",
    "production_text_preparation_path",
    "production_text_differences_path",
    "production_text_fact_production_path",
    "production_text_semantic_validation_path",
    "production_text_atoms_path",
    "production_graphic_ledger_path",
    "production_source_snapshot_path",
    "production_entity_relations_path",
    "production_bound_atoms_path",
    "production_effective_bound_atoms_path",
    "production_review_questions_path",
    "production_review_answers_path",
    "production_review_application_path",
    "production_automatic_synthesis_path",
    "production_unified_synthesis_path",
    "production_engineer_decisions_path",
    "production_final_report_path",
    "production_document_inconsistencies_path",
    "production_electrical_table_changes_path",
    "production_preliminary_report_path",
    "production_ai_v2_run_path",
    "production_ai_v2_materialization_path",
    "production_human_review_plan_path",
    "production_human_review_decisions_path",
    "production_ai_question_closure_path",
    "production_direct_page_mode2_path",
    "production_page_graphic_bundle_path",
    "production_document_graphic_bundle_path",
    "index_json_path",
]
