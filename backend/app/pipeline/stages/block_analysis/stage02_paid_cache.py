"""Stage 02 paid response cache.

Cache key = sha256(model | block_id | prompt_text | image_identity). Hit означает,
что точно такой же платный вопрос уже отправлялся в OpenRouter, и мы можем
вернуть сохранённый ответ без нового сетевого запроса и без записи в paid_cost.

Почему image_identity, а не байты PNG (schema v2, 2026-08-03)
------------------------------------------------------------
Раньше в ключ подмешивались САМИ БАЙТЫ картинки. Это ломалось об эфемерные
кропы (docs/block_crop_lifecycle.md): удалённый кроп восстанавливается
ре-рендером из 02_work/document.pdf и выглядит так же, но байт-в-байт не
совпадает (замер: 1818 КБ против 1770 КБ у одного блока). Каждый
восстановленный блок давал бы гарантированный промах кэша — то есть повторную
оплату ровно в том сценарии, ради которого кэш и заводили.

Идентичность считаем от того, что ОДНОЗНАЧНО задаёт картинку и не зависит от
конкретных байт: block_id + страница + координаты кропа + политика рендера
(dpi / min_long_side / compact) + итоговый размер в пикселях. Два кропа с
одинаковой пятёркой — это один и тот же вид одного и того же места листа,
отрендеренный одинаково.

Смена схемы v1 → v2 инвалидирует старые записи ЧИСТО: try_load_cached
сравнивает schema_version и на несовпадении возвращает None, а не отдаёт
ответ, посчитанный по другому правилу.

Назначение: в инциденте 2026-05-16 один и тот же блок M31A платился 9-15 раз
($0.3227 × 9 ≈ $2.90), потому что retry Stage 02 каждый раз дёргал OpenRouter
заново. С этим cache повторные запуски того же блока stage 02 будут zero-cost.

Cache хранится per-project: <project>/_output/_stage02_paid_response_cache/<key>.json
Это сознательно: инвалидация при смене Markdown/document_graph
происходит «бесплатно» — изменился prompt_text → изменился hash → cache miss.

Cache miss и cache hit оба возвращают совместимый dict из call_gpt_for_block:
один и тот же набор полей (ok, raw_content, parsed, input_tokens, output_tokens,
elapsed_ms). Дополнительно для hit: from_cache=True, cost_usd=0.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

CACHE_DIRNAME = "_stage02_paid_response_cache"
# v2: в ключ идёт image_identity вместо байтов PNG (см. docstring модуля).
CACHE_SCHEMA_VERSION = 2


def _stable_prompt_serialization(
    *,
    model: str,
    block_id: str,
    system_prompt: str,
    user_text: str,
    enrichment: dict,
    page_text: str,
) -> str:
    """Каноническая сериализация prompt-составляющих для устойчивого hash'а.

    Сортируем ключи enrichment, чтобы random dict-ordering не ломал hash.
    """
    enr = json.dumps(enrichment or {}, ensure_ascii=False, sort_keys=True)
    parts = [
        f"model={model}",
        f"block_id={block_id}",
        f"system={system_prompt}",
        f"user={user_text}",
        f"enrichment={enr}",
        f"page_text={page_text or ''}",
    ]
    return "\n---\n".join(parts)


def build_image_identity(block: dict, index_top: Optional[dict] = None) -> str:
    """Устойчивое обозначение картинки блока — без её байтов.

    Однозначно задаёт «какой участок листа и как отрендерен»:
    страница + координаты кропа + политика рендера + итоговый размер.
    Ре-рендер того же блока из того же PDF даёт ту же строку, хотя байты PNG
    отличаются, — именно это делает кэш устойчивым к эвакуации кропов.

    Если координат нет (старые индексы, ~9% записей), честно помечаем это
    маркером ``nocoords`` и добавляем размер файла: полностью «слепой» ключ
    хуже, чем ключ с более слабой, но явной идентичностью.
    """
    top = index_top or {}
    crop_px = block.get("crop_px")
    render_size = block.get("render_size")
    parts = [
        f"block_id={block.get('block_id') or ''}",
        f"page={block.get('page')}",
        f"file={block.get('file') or ''}",
        f"dpi={top.get('dpi')}",
        f"min_long_side={top.get('min_long_side')}",
        f"compact={bool(top.get('compact'))}",
        f"render_size={list(render_size) if render_size else None}",
    ]
    if isinstance(crop_px, (list, tuple)) and len(crop_px) == 4:
        parts.append(f"crop_px={[int(v) for v in crop_px]}")
    else:
        parts.append("crop_px=nocoords")
        parts.append(f"size_kb={block.get('size_kb')}")
    return "|".join(parts)


def compute_cache_key(
    *,
    model: str,
    block_id: str,
    system_prompt: str,
    user_text: str,
    enrichment: dict,
    page_text: str,
    image_identity: str,
) -> str:
    """sha256 from prompt + image identity. Hex digest, full length (64 chars)."""
    prompt_blob = _stable_prompt_serialization(
        model=model,
        block_id=block_id,
        system_prompt=system_prompt,
        user_text=user_text,
        enrichment=enrichment,
        page_text=page_text,
    )
    h = hashlib.sha256()
    h.update(prompt_blob.encode("utf-8"))
    h.update(b"\n--IMAGE--\n")
    h.update((image_identity or "").encode("utf-8"))
    return h.hexdigest()


def cache_dir_for_output(output_dir: Path) -> Path:
    """Каталог кэша для данного output_dir. Переживает смену прогона.

    Legacy-раскладка: ``<project>/_output/_stage02_paid_response_cache/`` —
    один каталог на проект, как и задумано в docstring модуля.

    v2-раскладка: ``output_dir`` — это ``<version>/03_analysis/runs/<run_id>``,
    то есть ОДНОРАЗОВЫЙ каталог: каждый запуск этапа получает новый run_id.
    Класть кэш внутрь него означало сделать его per-run — ровно то, от чего
    кэш и защищает: перезапуск после сбоя платил за уже оплаченные блоки
    заново (11.08: Stage 01 упал на 59-м из 135 блоков, и эти 59 ответов
    остались заперты в мёртвом run-каталоге). Поднимаем на уровень
    ``03_analysis`` — он общий для всех прогонов версии.
    """
    out = Path(output_dir)
    if out.parent.name == "runs" and out.parent.parent.name == "03_analysis":
        return out.parent.parent / CACHE_DIRNAME
    return out / CACHE_DIRNAME


def cache_file_for_key(output_dir: Path, cache_key: str) -> Path:
    return cache_dir_for_output(output_dir) / f"{cache_key}.json"


def cache_enabled() -> bool:
    """STAGE02_PAID_CACHE_ENABLED, читается runtime. Дефолт True."""
    raw = os.environ.get("STAGE02_PAID_CACHE_ENABLED")
    if raw is None or not raw.strip():
        return True
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def try_load_cached(
    output_dir: Path, cache_key: str
) -> Optional[dict]:
    """Возвращает cached response dict или None. Не бросает на disk/parse ошибки."""
    if not output_dir or not cache_key:
        return None
    path = cache_file_for_key(output_dir, cache_key)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cached = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("stage02_paid_cache: failed to load %s: %s", path, e)
        return None

    # Базовая валидация: должен быть response_dict от call_gpt_for_block.
    if not isinstance(cached, dict):
        return None
    if cached.get("schema_version") != CACHE_SCHEMA_VERSION:
        return None
    payload = cached.get("response")
    if not isinstance(payload, dict):
        return None
    # Возвращаем response в нужной форме + помечаем как cache hit.
    payload = dict(payload)
    payload["from_cache"] = True
    payload["cost_usd"] = 0.0
    payload["cache_key"] = cache_key
    payload["cached_at"] = cached.get("created_at", "")
    payload["original_cost_usd"] = cached.get("original_cost_usd", 0.0)
    return payload


def save_to_cache(
    output_dir: Path,
    cache_key: str,
    *,
    response: dict,
    model: str,
    block_id: str,
    original_cost_usd: float,
    source_job_id: str = "",
) -> None:
    """Сохранить ответ в cache. Не бросает на disk-ошибки."""
    if not output_dir or not cache_key:
        return
    path = cache_file_for_key(output_dir, cache_key)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "schema_version": CACHE_SCHEMA_VERSION,
            "cache_key": cache_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "model": model,
            "block_id": block_id,
            "original_cost_usd": round(float(original_cost_usd or 0.0), 8),
            "source_job_id": source_job_id or "",
            # response — это копия dict, который вернул бы call_gpt_for_block на miss.
            # Включает raw_content, parsed, input_tokens, output_tokens и т.д.
            "response": response,
        }
        # Атомарная запись через tmp.
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning("stage02_paid_cache: failed to save %s: %s", path, e)
