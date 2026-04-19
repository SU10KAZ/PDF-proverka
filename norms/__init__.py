"""Пакет нормативной базы: извлечение, верификация, очередь пропусков.

Источник истины — внешний проект Norms-main (status_index.json). Локальный
norms_db.json больше не authoritative — оставлен только ради paragraph cache
и обратной совместимости старых helper'ов.
"""

from norms._core import (  # noqa: F401
    # Пути
    NORMS_DIR,
    BASE_DIR,
    NORMS_DB_PATH,
    NORMS_PARAGRAPHS_PATH,
    PROJECTS_DIR,
    # Извлечение норм
    extract_norms_from_text,
    extract_norms_from_findings,
    format_norms_for_template,
    format_findings_to_fix,
    # Детерминированная проверка
    generate_deterministic_checks,
    # Очередь пропусков
    build_missing_norms_queue,
    render_missing_norms_queue_md,
    write_missing_norms_queue,
    # Слияние LLM-результатов
    merge_llm_norm_results,
    merge_chunked_llm_results,
    format_llm_work_for_template,
    # Валидация
    validate_norm_checks,
    # БД норм (legacy — только paragraph cache остаётся)
    load_norms_db,
    save_norms_db,
    # Paragraph cache
    load_norms_paragraphs,
    save_norms_paragraphs,
    update_paragraphs_from_project,
    normalize_paragraph_key,
    get_paragraph,
    upsert_paragraph,
    merge_paragraph_checks,
    paragraph_cache_stats,
    # Утилиты
    normalize_doc_number,
    merge_norm_check,
    update_from_project,
    get_stale_norms,
    print_stats,
    # Константы
    NORM_CONFIDENCE_THRESHOLDS,
    # Классификация
    classify_norm_status,
    classify_norm_quote_status,
    compute_norm_confidence,
    enrich_findings_from_norm_checks,
    compute_norm_policy_class,
    should_review_norm,
)
from norms.external_provider import (  # noqa: F401
    load_status_index,
    resolve_norm_status,
    NORMS_STATUS_INDEX_PATH,
)
