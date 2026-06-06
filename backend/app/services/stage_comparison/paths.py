"""Пути хранения раздела «Сравнение стадий».

Все runtime-данные раздела (сессии, рендеренные страницы, crop'ы, diff'ы)
лежат под `comparison/` в корне проекта. Расположение настраивается env'ом
`COMPARISON_ROOT`; по умолчанию = `<ROOT_DIR>/comparison`.

Структура:
  comparison/
    .gitkeep
    index.json
    sessions/
      <session_id>/
        session.json
        pairs/
          <pair_id>/
            pair.json
            page_alignment.json
            links.json
            graphic_diffs.json
            text_diff.json
            pages/
              left/page_0001.png
              right/page_0001.png
            crops/
              left/<block_id>.png
              right/<block_id>.png
            previews/

Также сохраняется fallback на старое расположение
`backend/app/data/stage_comparison_sessions/<id>.json` — чтобы старые сессии
оставались читаемыми (новые туда НЕ создаются).
"""
from __future__ import annotations

import os
from pathlib import Path

from backend.app.core.config import APP_DATA_DIR, ROOT_DIR


def _safe_id(value: str) -> str:
    safe = "".join(c for c in (value or "") if c.isalnum() or c in "-_")
    if not safe:
        raise ValueError("invalid id")
    return safe


def comparison_root() -> Path:
    """Корень comparison/. Создаётся при первом обращении."""
    raw = os.environ.get("COMPARISON_ROOT", "").strip()
    if raw:
        root = Path(raw).expanduser().resolve()
    else:
        root = ROOT_DIR / "comparison"
    root.mkdir(parents=True, exist_ok=True)
    keep = root / ".gitkeep"
    if not keep.exists():
        try:
            keep.touch()
        except OSError:
            pass
    return root


def sessions_root() -> Path:
    p = comparison_root() / "sessions"
    p.mkdir(parents=True, exist_ok=True)
    return p


def session_dir(session_id: str) -> Path:
    p = sessions_root() / _safe_id(session_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


def session_json_path(session_id: str) -> Path:
    return session_dir(session_id) / "session.json"


def pairs_root(session_id: str) -> Path:
    p = session_dir(session_id) / "pairs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def pair_dir(session_id: str, pair_id: str) -> Path:
    p = pairs_root(session_id) / _safe_id(pair_id)
    p.mkdir(parents=True, exist_ok=True)
    return p


def pair_json_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "pair.json"


def page_alignment_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "page_alignment.json"


def links_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "links.json"


def graphic_diffs_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "graphic_diffs.json"


def text_diff_cache_path(session_id: str, pair_id: str) -> Path:
    return pair_dir(session_id, pair_id) / "text_diff.json"


def text_llm_diff_path(session_id: str, pair_id: str) -> Path:
    """Семантический LLM-анализ MD-файлов через Claude Sonnet.

    Кладётся отдельно от технического text_diff.json: они логически разные.
    Структура файла описана в text_llm.py docstring.
    """
    return pair_dir(session_id, pair_id) / "text_llm_diff.json"


def text_llm_prompt_path(session_id: str, pair_id: str) -> Path:
    """Сохранённый prompt для ручного запуска, если provider недоступен."""
    return pair_dir(session_id, pair_id) / "text_llm_prompt.md"


def text_llm_text_only_md_path(session_id: str, pair_id: str, side: str) -> Path:
    """Очищенный от image/imagine-блоков Markdown, который фактически идёт в LLM.

    side: 'left' | 'right'. Файлы пишутся для отладки и для эксперта, который
    хочет проверить, что именно ушло в Claude. Лежат внутри pair-папки и не
    попадают в git.
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    return pair_dir(session_id, pair_id) / f"text_llm_{side}_text_only.md"


# ─── Text enrichment (Qwen image descriptions for enriched MD) ───────────


def text_enrichment_dir(session_id: str, pair_id: str) -> Path:
    """Корень `text_enrichment/` для пары.

    Здесь живут enriched MD (`left_enriched.md`/`right_enriched.md`),
    итоговый `image_descriptions.json` для каждой стороны, кеш описаний
    по хешу картинки + модели и сохранённые prompt'ы/raw-фрагменты
    ответов модели для отладки.
    """
    p = pair_dir(session_id, pair_id) / "text_enrichment"
    p.mkdir(parents=True, exist_ok=True)
    return p


def text_enrichment_md_path(session_id: str, pair_id: str, side: str) -> Path:
    """Путь enriched MD для стороны (`left`/`right`)."""
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    return text_enrichment_dir(session_id, pair_id) / f"{side}_enriched.md"


def text_enrichment_descriptions_path(session_id: str, pair_id: str, side: str) -> Path:
    """JSON-сводка описаний image/imagine-блоков MD для стороны."""
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    return text_enrichment_dir(session_id, pair_id) / f"{side}_image_descriptions.json"


def text_enrichment_cache_dir(session_id: str, pair_id: str) -> Path:
    """Cache: `<sha256>.json` per image+model+prompt_version."""
    p = text_enrichment_dir(session_id, pair_id) / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def text_enrichment_prompts_dir(session_id: str, pair_id: str) -> Path:
    """Сохранённые prompt'ы для отладки (по одному файлу на блок)."""
    p = text_enrichment_dir(session_id, pair_id) / "prompts"
    p.mkdir(parents=True, exist_ok=True)
    return p


def text_enrichment_raw_dir(session_id: str, pair_id: str) -> Path:
    """Raw-фрагменты ответов модели для отладки."""
    p = text_enrichment_dir(session_id, pair_id) / "raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ─── Large sheet enrichment (page-level tile-first OCR for huge sheets) ───


def _page_token(page: int) -> str:
    try:
        n = int(page)
    except (TypeError, ValueError):
        raise ValueError("page must be an integer")
    if n < 1:
        raise ValueError("page must be >= 1")
    return f"page_{n:04d}"


def large_sheet_enrichment_dir(session_id: str, pair_id: str) -> Path:
    """Корень `large_sheet_enrichment/` для пары.

    Здесь живут page-level артефакты для больших/плотных листов: обзорный и
    high-res рендеры, words.json с координатами, zones.json, tiles/,
    tile_results.json, page_enriched.json/md и diagnostics.json. Это
    независимая ветка от `text_enrichment/` (Qwen image descriptions) — она
    не перезаписывает существующий pipeline.
    """
    p = pair_dir(session_id, pair_id) / "large_sheet_enrichment"
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_side_dir(session_id: str, pair_id: str, side: str) -> Path:
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    p = large_sheet_enrichment_dir(session_id, pair_id) / side
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_page_dir(session_id: str, pair_id: str, side: str, page: int) -> Path:
    """`large_sheet_enrichment/<side>/page_NNNN/` — все артефакты одной страницы."""
    p = large_sheet_side_dir(session_id, pair_id, side) / _page_token(page)
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_tiles_dir(session_id: str, pair_id: str, side: str, page: int) -> Path:
    p = large_sheet_page_dir(session_id, pair_id, side, page) / "tiles"
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_prompts_dir(session_id: str, pair_id: str, side: str, page: int) -> Path:
    p = large_sheet_page_dir(session_id, pair_id, side, page) / "prompts"
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_cache_dir(session_id: str, pair_id: str, side: str, page: int) -> Path:
    """`large_sheet_enrichment/<side>/page_NNNN/cache/` — per-tile Qwen cache
    по `sha256(tile image + nearby_text + model + prompt_version + zone_hint)`."""
    p = large_sheet_page_dir(session_id, pair_id, side, page) / "cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_raw_dir(session_id: str, pair_id: str, side: str, page: int) -> Path:
    p = large_sheet_page_dir(session_id, pair_id, side, page) / "raw"
    p.mkdir(parents=True, exist_ok=True)
    return p


def large_sheet_artifact_path(
    session_id: str, pair_id: str, side: str, page: int, name: str
) -> Path:
    """Путь к именованному артефакту страницы (overview.png, words.json, …).

    name санитайзится, чтобы исключить traversal; результат всегда лежит
    внутри page-папки.
    """
    safe = "".join(c for c in (name or "") if c.isalnum() or c in "-_.")
    safe = safe.replace("..", "_")
    if not safe:
        raise ValueError("invalid artifact name")
    return large_sheet_page_dir(session_id, pair_id, side, page) / safe


# ─── Block equivalence precheck (pre-Qwen gate, observe mode) ────────────


def block_equivalence_dir(session_id: str, pair_id: str) -> Path:
    """Корень `block_equivalence/` для пары — артефакты pre-Qwen прекчека
    эквивалентности блоков OLD↔NEW (Stage 1: observe). Независим от
    `text_enrichment/` и не влияет на Qwen-конвейер."""
    p = pair_dir(session_id, pair_id) / "block_equivalence"
    p.mkdir(parents=True, exist_ok=True)
    return p


def block_equivalence_report_path(session_id: str, pair_id: str) -> Path:
    """`.../block_equivalence/block_equivalence_report.json`."""
    return block_equivalence_dir(session_id, pair_id) / "block_equivalence_report.json"


def block_equivalence_debug_dir(session_id: str, pair_id: str) -> Path:
    """`.../block_equivalence/debug/` — debug PNG `{block_id}_diff.png` для
    changed-visual блоков."""
    p = block_equivalence_dir(session_id, pair_id) / "debug"
    p.mkdir(parents=True, exist_ok=True)
    return p


# ─── Enriched comparison (Opus over enriched MD) ─────────────────────────


def enriched_comparison_dir(session_id: str, pair_id: str) -> Path:
    """Корень `enriched_comparison/` для пары.

    Здесь хранится результат смыслового сравнения двух enriched MD через
    Claude Opus: `comparison_result.json`, prompt (`prompt.md`), сжатый raw
    response (`raw_response.txt`) и метаданные job (`job.json`).
    """
    p = pair_dir(session_id, pair_id) / "enriched_comparison"
    p.mkdir(parents=True, exist_ok=True)
    return p


def enriched_comparison_result_path(session_id: str, pair_id: str) -> Path:
    """Итоговый JSON с changes[], summary, warnings."""
    return enriched_comparison_dir(session_id, pair_id) / "comparison_result.json"


def enriched_comparison_prompt_path(session_id: str, pair_id: str) -> Path:
    """Сохранённый prompt (system + user) для отладки и ручного запуска."""
    return enriched_comparison_dir(session_id, pair_id) / "prompt.md"


def enriched_comparison_raw_path(session_id: str, pair_id: str) -> Path:
    """Excerpt сырого ответа Opus (первые ~5000 символов)."""
    return enriched_comparison_dir(session_id, pair_id) / "raw_response.txt"


def enriched_comparison_job_path(session_id: str, pair_id: str) -> Path:
    """Метаданные последнего запуска: provider, model, duration, status."""
    return enriched_comparison_dir(session_id, pair_id) / "job.json"


def enriched_comparison_fallback_progress_path(session_id: str, pair_id: str) -> Path:
    """Live-прогресс evidence_first_s2_fallback: per-chunk Opus + ETA.

    Пишется на границах чанков во время прогона fallback (см.
    `run_evidence_first_fallback`), читается UI/aggregate чтобы показывать
    «чанк k / N · осталось ~m мин» вместо статичного `comparing`.
    """
    return enriched_comparison_dir(session_id, pair_id) / "fallback_progress.json"


# ─── Unified findings (отдельный файл, не смешивается с findings.json) ───


def unified_findings_path(session_id: str) -> Path:
    """`comparison/sessions/<sid>/unified_findings.json` — сводка по unified pipeline.

    Лежит отдельно от `findings.json`, чтобы старый pipeline и новый не
    конфликтовали друг с другом в production.
    """
    return session_dir(session_id) / "unified_findings.json"


def unified_findings_grouped_path(session_id: str) -> Path:
    """`comparison/sessions/<sid>/unified_findings_grouped.json` — deterministic post-processing
    слой поверх unified_findings.json: группировка дублей, отделение формальных
    штампов от значимых отличий. Без LLM. См. `unified_grouping.py`.
    """
    return session_dir(session_id) / "unified_findings_grouped.json"


def expert_review_path(session_id: str) -> Path:
    """`comparison/sessions/<sid>/expert_review.json` — решения эксперта
    по raw-расхождениям (accepted/rejected + причина). Ключ хранения —
    стабильный raw `id` (chg_…); группированный вид агрегирует на лету.
    """
    return session_dir(session_id) / "expert_review.json"


def v2_review_status_path(session_id: str, pair_id: str) -> Path:
    """`comparison/sessions/<sid>/pairs/<pid>/v2_review_status.json` —
    ручные статусы верификации инженера для режима «V2» вкладки
    «Расхождения». Хранится отдельно от `comparison_result.json`, чтобы
    production-артефакт сравнения НИКОГДА не мутировался ручной разметкой.
    Скоупится парой: ключ — стабильный `v2_…` id изменения.
    """
    return pair_dir(session_id, pair_id) / "v2_review_status.json"


def pages_dir(session_id: str, pair_id: str, side: str) -> Path:
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    p = pair_dir(session_id, pair_id) / "pages" / side
    p.mkdir(parents=True, exist_ok=True)
    return p


def crops_dir(session_id: str, pair_id: str, side: str) -> Path:
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    p = pair_dir(session_id, pair_id) / "crops" / side
    p.mkdir(parents=True, exist_ok=True)
    return p


def previews_dir(session_id: str, pair_id: str) -> Path:
    p = pair_dir(session_id, pair_id) / "previews"
    p.mkdir(parents=True, exist_ok=True)
    return p


def index_json_path() -> Path:
    return comparison_root() / "index.json"


# ─── Session-level: findings / jobs / reports ────────────────────────────

def findings_path(session_id: str) -> Path:
    """Сессионный findings.json (единый список по всем парам)."""
    return session_dir(session_id) / "findings.json"


def jobs_root(session_id: str) -> Path:
    p = session_dir(session_id) / "jobs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def job_json_path(session_id: str, job_id: str) -> Path:
    safe = "".join(c for c in (job_id or "") if c.isalnum() or c in "-_")
    if not safe:
        raise ValueError("invalid job_id")
    return jobs_root(session_id) / f"{safe}.json"


def reports_root(session_id: str) -> Path:
    p = session_dir(session_id) / "reports"
    p.mkdir(parents=True, exist_ok=True)
    return p


def templates_root() -> Path:
    """`comparison/templates/` — снимки конфигурации пар по identity ключу.

    Используются разделом «Сравнение стадий», чтобы сохранять связи блоков
    и карту страниц для конкретной пары PDF и автоматически применять их
    при повторном создании сессии с теми же файлами.
    """
    p = comparison_root() / "templates"
    p.mkdir(parents=True, exist_ok=True)
    return p


def pair_template_path(key: str) -> Path:
    """Путь файла шаблона по детерминированному ключу (sha1)."""
    safe = "".join(c for c in (key or "") if c.isalnum() or c in "-_")
    if not safe:
        raise ValueError("invalid template key")
    return templates_root() / f"{safe}.json"


def report_path(session_id: str, report_id: str) -> Path:
    """Полный путь файла отчёта. report_id — имя файла без manipulation."""
    safe = "".join(c for c in (report_id or "") if c.isalnum() or c in "-_.")
    if not safe:
        raise ValueError("invalid report_id")
    # Защита от path traversal: убираем любые "/" и ".."
    safe = safe.replace("..", "_")
    candidate = (reports_root(session_id) / safe).resolve()
    # Дополнительная защита: путь должен лежать внутри reports_root
    root = reports_root(session_id).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        raise ValueError("report_path_outside_reports_root")
    return candidate


# ─── Legacy fallback (старое расположение, только чтение) ─────────────────

LEGACY_SESSIONS_DIR = APP_DATA_DIR / "stage_comparison_sessions"


def legacy_session_json_path(session_id: str) -> Path:
    return LEGACY_SESSIONS_DIR / f"{_safe_id(session_id)}.json"


def legacy_cache_dir(session_id: str) -> Path:
    return LEGACY_SESSIONS_DIR / f"{_safe_id(session_id)}_cache"


__all__ = [
    "comparison_root",
    "sessions_root",
    "session_dir",
    "session_json_path",
    "pairs_root",
    "pair_dir",
    "pair_json_path",
    "page_alignment_path",
    "links_path",
    "graphic_diffs_path",
    "templates_root",
    "pair_template_path",
    "text_diff_cache_path",
    "text_llm_diff_path",
    "text_llm_prompt_path",
    "text_llm_text_only_md_path",
    "text_enrichment_dir",
    "text_enrichment_md_path",
    "text_enrichment_descriptions_path",
    "text_enrichment_cache_dir",
    "text_enrichment_prompts_dir",
    "text_enrichment_raw_dir",
    "large_sheet_enrichment_dir",
    "large_sheet_side_dir",
    "large_sheet_page_dir",
    "large_sheet_tiles_dir",
    "large_sheet_prompts_dir",
    "large_sheet_cache_dir",
    "large_sheet_raw_dir",
    "large_sheet_artifact_path",
    "block_equivalence_dir",
    "block_equivalence_report_path",
    "block_equivalence_debug_dir",
    "enriched_comparison_dir",
    "enriched_comparison_result_path",
    "enriched_comparison_prompt_path",
    "enriched_comparison_raw_path",
    "enriched_comparison_job_path",
    "unified_findings_path",
    "unified_findings_grouped_path",
    "expert_review_path",
    "v2_review_status_path",
    "pages_dir",
    "crops_dir",
    "previews_dir",
    "index_json_path",
    "LEGACY_SESSIONS_DIR",
    "legacy_session_json_path",
    "legacy_cache_dir",
]
