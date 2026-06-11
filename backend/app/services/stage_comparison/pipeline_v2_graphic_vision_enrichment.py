# -*- coding: utf-8 -*-
"""Pipeline V2 — Graphic Vision Enrichment (offline, runner injectable).

Слой описания графических блоков ПОСЛЕ Visual Equivalence Gate:

```text
visual_equivalence_gate_report.json
  → select_blocks_for_vision      (только send_to_vision / manual_review)
  → build_graphic_vision_enrichment_plan   (items + prompt + crop refs)
  → run_graphic_vision_enrichment(vision_runner=…)
  → graphic_vision_enrichment_report.json
```

Ключевые принципы:

* ``exclude_from_vision`` НЕ отправляется в vision: визуальная идентичность
  после выравнивания — самое сильное свидетельство «не менялось», vision на
  таких блоках генерирует только description-variance (ложные дельты);
* ``send_to_vision`` (видимое изменение) и ``manual_review`` (анти-dilution /
  неуверенность gate) — кандидаты на vision-описание;
* vision runner ИНЪЕКТИРУЕТСЯ (контракт ``vision_runner(prompt,
  left_image_path, right_image_path, options) -> dict``); модуль сам НЕ
  импортирует vision-модели/провайдеров и НЕ делает сетевых вызовов.
  ``vision_runner=None`` → ``skipped_no_runner``: кандидаты выбраны,
  prompt/crop refs записаны, реальных вызовов нет;
* fail-soft: ошибка одного item не валит отчёт; отсутствие visual gate →
  ``skipped_no_visual_gate``, а не исключение.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

REPORT_VERSION = 1
REPORT_KIND = "stage_comparison_pipeline_v2_graphic_vision_enrichment"

VisionRunner = Callable[[str, Optional[str], Optional[str], dict], Any]

_DEFAULT_OPTIONS = {
    "enabled": False,               # в dry-run слой по умолчанию выключен
    "max_items": 5,
    "include_manual_review": True,
    "include_exclude_from_vision": False,
    "write_prompts": True,
    "render_crops": True,           # рендерить PNG-кропы перед вызовом runner
    "render_long_side": 1600,
    "runner_model": "fake",
}

_VALID_CONFIDENCE = {"high", "medium", "low"}

# ─── prompt contract ─────────────────────────────────────────────────────────

VISION_PROMPT_TEMPLATE = """Ты — инженер-эксперт по проектной документации. \
Тебе даны два изображения ОДНОГО И ТОГО ЖЕ графического блока чертежа: \
OLD (старая стадия) и NEW (новая стадия).

Контекст блока:
- тип графики: {graphic_type}
- дисциплина: {discipline}
- лист OLD: стр. {left_page}{left_sheet}
- лист NEW: стр. {right_page}{right_sheet}
- вердикт визуального сравнения: {visual_status}

Задача:
1. Кратко опиши, что изображено на OLD.
2. Кратко опиши, что изображено на NEW.
3. Перечисли ВИДИМЫЕ изменения между OLD и NEW.
4. Выпиши инженерные сущности с ОБЕИХ сторон (буквально, как написано): \
оборудование, кабели/сечения, автоматы/номиналы, линии/подключения, \
обозначения, помещения/оси/этажи (если видны).

Жёсткие правила:
- НЕ придумывай того, чего не видно на изображении.
- НЕ делай юридических/нормативных выводов.
- Если надпись нечитаема — пиши «[нечитаемо]», не угадывай.
- Если изображение нечитабельно целиком — так и напиши в описании.
- Маркировки переписывай буквально (ЩР-1а, не «щит 1»).

Ответ — СТРОГО один JSON-объект без пояснений вокруг:
{{
  "old_description": "…",
  "new_description": "…",
  "observed_changes": ["…"],
  "engineering_entities_old": ["…"],
  "engineering_entities_new": ["…"],
  "possible_risks": ["…"],
  "confidence": "high|medium|low"
}}"""


# ─── helpers ─────────────────────────────────────────────────────────────────


def _opt(options: Optional[dict], key: str) -> Any:
    if isinstance(options, dict) and key in options:
        return options[key]
    return _DEFAULT_OPTIONS.get(key)


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _blocks_by_id(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    blocks = m.get("blocks")
    if isinstance(blocks, dict):
        return {k: v for k, v in blocks.items() if isinstance(v, dict)}
    if isinstance(blocks, list):
        return {b.get("block_id"): b for b in blocks if isinstance(b, dict)}
    return {}


def _pages_by_number(model: Any) -> dict:
    m = model if isinstance(model, dict) else {}
    out: dict = {}
    for p in m.get("pages") or []:
        if isinstance(p, dict) and p.get("page_number") is not None:
            out[p["page_number"]] = p
    return out


def _model_pdf_path(model: Any) -> Optional[str]:
    m = model if isinstance(model, dict) else {}
    src = m.get("source") if isinstance(m.get("source"), dict) else {}
    return src.get("pdf_path") or None


def _descriptor_for(graphic_report: Any, block_id: Any) -> dict:
    r = graphic_report if isinstance(graphic_report, dict) else {}
    for d in r.get("descriptors") or []:
        if isinstance(d, dict) and d.get("block_id") == block_id:
            return d
    return {}


def _first_nonempty(*values: Any, fallback: str = "unknown") -> str:
    for v in values:
        s = str(v or "").strip()
        if s and s.lower() != "unknown":
            return s
    return fallback


def _sheet_name_of(model_pages: dict, page_no: Any) -> str:
    page = model_pages.get(page_no)
    if isinstance(page, dict):
        return str(page.get("sheet_name") or "").strip()
    return ""


def _crop_source(block: Optional[dict], pdf_path: Optional[str]) -> dict:
    b = block if isinstance(block, dict) else {}
    return {
        "image_file": b.get("image_file"),
        "pdf_path": pdf_path,
        "page_number": b.get("page_number"),
        "bbox_norm": b.get("coords_norm"),
    }


def _crop_ref(source: dict, rendered_png: Optional[str]) -> str:
    if rendered_png:
        return str(rendered_png)
    img = source.get("image_file")
    if img:
        return str(img)
    pdf = source.get("pdf_path")
    page = source.get("page_number")
    if pdf:
        return f"{pdf}#page={page}"
    return ""


# ─── selection ───────────────────────────────────────────────────────────────


def select_blocks_for_vision(visual_gate_report: Any,
                             options: Optional[dict] = None
                             ) -> tuple[list[dict], dict, list[str]]:
    """Выбрать пары блоков для vision по решениям visual gate.

    Возвращает ``(selected_pairs, stats, warnings)``. Правила:

    * ``send_to_vision`` — берём всегда (приоритет при cap: у них есть
      подтверждённое визуальное изменение);
    * ``manual_review`` — только при ``include_manual_review=true``;
    * ``exclude_from_vision`` — НЕ берём (опция
      ``include_exclude_from_vision=true`` существует для отладки);
    * cap ``max_items`` — усечение явно warn'ится (no silent caps).
    """
    warnings: list[str] = []
    r = visual_gate_report if isinstance(visual_gate_report, dict) else {}
    pairs = [bp for bp in r.get("block_pairs") or [] if isinstance(bp, dict)]

    include_manual = _opt(options, "include_manual_review") is not False
    include_excluded = _opt(options, "include_exclude_from_vision") is True

    send, manual, excluded_taken = [], [], []
    stats = {"candidates_total": len(pairs), "excluded_by_visual_gate": 0,
             "manual_review_included": 0, "manual_review_skipped": 0,
             "other_skipped": 0, "dropped_by_cap": 0}
    for bp in pairs:
        decision = bp.get("decision")
        if decision == "send_to_vision":
            send.append(bp)
        elif decision == "manual_review":
            if include_manual:
                manual.append(bp)
            else:
                stats["manual_review_skipped"] += 1
        elif decision == "exclude_from_vision":
            stats["excluded_by_visual_gate"] += 1
            if include_excluded:
                excluded_taken.append(bp)
        else:
            stats["other_skipped"] += 1

    selected = send + manual + excluded_taken
    # max_items <= 0 = unlimited (cap выключен)
    max_items = _safe_int(_opt(options, "max_items"), 5)
    if max_items > 0 and len(selected) > max_items:
        stats["dropped_by_cap"] = len(selected) - max_items
        warnings.append(f"selection truncated by max_items={max_items}: "
                        f"{len(selected) - max_items} of {len(selected)} "
                        f"candidates dropped")
        selected = selected[:max_items]
    # счётчик «включённых manual» — по ФАКТИЧЕСКОЙ выборке (после cap),
    # иначе summary противоречил бы items
    stats["manual_review_included"] = sum(
        1 for bp in selected if bp.get("decision") == "manual_review")
    return selected, stats, warnings


# ─── prompt ──────────────────────────────────────────────────────────────────


def build_vision_prompt_for_block_pair(pair: dict, *,
                                       graphic_type: str = "unknown",
                                       discipline: str = "unknown",
                                       left_sheet_name: str = "",
                                       right_sheet_name: str = "") -> str:
    """Собрать строгий vision-prompt для пары графических блоков."""
    p = pair if isinstance(pair, dict) else {}
    lp = p.get("left_page_number")
    rp = p.get("right_page_number")
    return VISION_PROMPT_TEMPLATE.format(
        graphic_type=graphic_type or "unknown",
        discipline=discipline or "unknown",
        left_page="?" if lp is None else lp,
        right_page="?" if rp is None else rp,
        left_sheet=f" ({left_sheet_name})" if left_sheet_name else "",
        right_sheet=f" ({right_sheet_name})" if right_sheet_name else "",
        visual_status=p.get("status") or "unknown",
    )


# ─── plan ────────────────────────────────────────────────────────────────────


def build_graphic_vision_enrichment_plan(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        options: Optional[dict] = None) -> dict:
    """Построить план (items без vision-результатов) из готовых артефактов.

    Отсутствующий/непригодный visual gate → ``status=skipped_no_visual_gate``
    с пустыми items (модуль никогда не «угадывает» кандидатов сам — решения
    принимает только gate).
    """
    warnings: list[str] = []
    gate = visual_gate_report if isinstance(visual_gate_report, dict) else None
    if gate is None or not isinstance(gate.get("block_pairs"), list):
        return {
            "status": "skipped_no_visual_gate",
            "items": [],
            "stats": {"candidates_total": 0, "excluded_by_visual_gate": 0,
                      "manual_review_included": 0},
            "warnings": ["visual gate report unavailable (stage disabled, "
                         "failed or not run) — graphic vision enrichment "
                         "skipped"],
        }

    selected, stats, sel_warnings = select_blocks_for_vision(gate, options)
    warnings.extend(sel_warnings)

    left_blocks = _blocks_by_id(left_model)
    right_blocks = _blocks_by_id(right_model)
    left_pages = _pages_by_number(left_model)
    right_pages = _pages_by_number(right_model)
    left_pdf = _model_pdf_path(left_model)
    right_pdf = _model_pdf_path(right_model)
    write_prompts = _opt(options, "write_prompts") is not False

    items: list[dict] = []
    prompts_by_item_id: dict[str, str] = {}
    for bp in selected:
        lid, rid = bp.get("left_block_id"), bp.get("right_block_id")
        lb, rb = left_blocks.get(lid), right_blocks.get(rid)
        item_warnings: list[str] = []
        if lb is None:
            item_warnings.append("left block missing in normalized model")
        if rb is None:
            item_warnings.append("right block missing in normalized model")

        ld = _descriptor_for(left_graphic_report, lid)
        rd = _descriptor_for(right_graphic_report, rid)
        graphic_type = _first_nonempty(ld.get("graphic_type"),
                                       rd.get("graphic_type"))
        discipline = _first_nonempty(ld.get("discipline"),
                                     rd.get("discipline"))

        left_page = bp.get("left_page_number")
        if left_page is None:
            left_page = (lb or {}).get("page_number")
        right_page = bp.get("right_page_number")
        if right_page is None:
            right_page = (rb or {}).get("page_number")

        left_source = _crop_source(lb, left_pdf)
        right_source = _crop_source(rb, right_pdf)
        left_source["page_number"] = left_page
        right_source["page_number"] = right_page

        prompt = build_vision_prompt_for_block_pair(
            {**bp, "left_page_number": left_page,
             "right_page_number": right_page},
            graphic_type=graphic_type, discipline=discipline,
            left_sheet_name=_sheet_name_of(left_pages, left_page),
            right_sheet_name=_sheet_name_of(right_pages, right_page))

        item_id = f"gv_{lid}__{rid}"
        # полный prompt строится ВСЕГДА (runner получает его независимо от
        # write_prompts); write_prompts управляет только персистенцией
        prompts_by_item_id[item_id] = prompt
        items.append({
            "item_id": item_id,
            "left_block_id": lid,
            "right_block_id": rid,
            "left_page_number": left_page,
            "right_page_number": right_page,
            "visual_status": bp.get("status"),
            "visual_decision": bp.get("decision"),
            "visual_metrics": dict(bp["metrics"])
                if isinstance(bp.get("metrics"), dict) else None,
            "graphic_type": graphic_type,
            "discipline": discipline,
            "left_crop_source": left_source,
            "right_crop_source": right_source,
            "left_crop_ref": _crop_ref(left_source, None),
            "right_crop_ref": _crop_ref(right_source, None),
            "prompt": prompt if write_prompts else None,
            "vision_status": "pending",
            "result": None,
            "warnings": item_warnings,
        })

    return {"status": "ok", "items": items, "stats": stats,
            "warnings": warnings, "prompts_by_item_id": prompts_by_item_id}


# ─── runner result normalization ─────────────────────────────────────────────


_MAX_LIST_ITEMS = 50
_MAX_ITEM_CHARS = 500


def _str_list(value: Any) -> tuple[list[str], list[str]]:
    """Нормализовать список строк из ответа runner'а.

    Возвращает (список, warnings). Falsy-скаляры (0, False) сохраняются
    строкой; патологически длинные элементы/списки обрезаются с warning'ом
    (no silent caps).
    """
    warnings: list[str] = []
    if isinstance(value, str):
        value = [value] if value.strip() else []
    if not isinstance(value, list):
        return [], warnings
    out: list[str] = []
    for v in value:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if len(s) > _MAX_ITEM_CHARS:
            s = s[:_MAX_ITEM_CHARS] + "…"
            warnings.append(f"list item truncated to {_MAX_ITEM_CHARS} chars")
        out.append(s)
    if len(out) > _MAX_LIST_ITEMS:
        warnings.append(f"list truncated to {_MAX_LIST_ITEMS} of "
                        f"{len(out)} items")
        out = out[:_MAX_LIST_ITEMS]
    return out, warnings


def normalize_vision_runner_result(raw: Any) -> tuple[Optional[dict], list[str]]:
    """Привести сырой ответ runner'а к контракту result.

    Возвращает ``(result|None, warnings)``; None — ответ непригоден
    (item становится failed). Строка с JSON парсится fail-soft.
    """
    warnings: list[str] = []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (ValueError, TypeError):
            return None, ["runner returned non-JSON string"]
    if not isinstance(raw, dict):
        return None, [f"runner returned {type(raw).__name__}, expected dict"]

    old_desc = str(raw.get("old_description") or "").strip()
    new_desc = str(raw.get("new_description") or "").strip()
    if not old_desc and not new_desc:
        return None, ["runner result has no descriptions"]

    confidence = str(raw.get("confidence") or "").strip().lower()
    if confidence not in _VALID_CONFIDENCE:
        if confidence:
            warnings.append(f"invalid confidence {confidence!r} → low")
        confidence = "low"

    result = {"old_description": old_desc, "new_description": new_desc,
              "confidence": confidence}
    for key in ("observed_changes", "engineering_entities_old",
                "engineering_entities_new", "possible_risks"):
        values, list_warnings = _str_list(raw.get(key))
        result[key] = values
        warnings.extend(f"{key}: {w}" for w in list_warnings)
    return result, warnings


# ─── crop rendering (только при наличии runner'а) ────────────────────────────


def _render_crop_png(block: Optional[dict], pages: dict,
                     pdf_path: Optional[str], out_path: Path,
                     long_side: int) -> tuple[Optional[str], Optional[str]]:
    """Срендерить кроп блока в PNG. Возвращает (path|None, error|None).

    Тяжёлые зависимости (cv2/fitz) импортируются лениво и fail-soft:
    их отсутствие — ошибка рендера item'а, не падение модуля.
    """
    if not isinstance(block, dict):
        return None, "block missing in normalized model"
    try:
        import cv2  # noqa: PLC0415 — ленивый импорт по контракту fail-soft
        from backend.app.services.stage_comparison.block_equivalence_precheck import (  # noqa: PLC0415
            EqBlock,
            load_or_render_block_image,
        )
    except Exception as exc:  # noqa: BLE001 — окружение без cv2/fitz
        return None, f"render dependencies unavailable: {exc}"

    page_no = block.get("page_number") or 0
    page = pages.get(page_no) if isinstance(pages.get(page_no), dict) else {}
    eq = EqBlock(
        block_id=str(block.get("block_id") or ""),
        page=_safe_int(page_no, 0),
        block_type=str(block.get("block_type") or "image"),
        coords_norm=block.get("coords_norm"),
        coords_px=block.get("coords_px"),
        page_width=_safe_int(page.get("width"), 0),
        page_height=_safe_int(page.get("height"), 0),
        text="",
        image_file=block.get("image_file"),
        crop_url=block.get("crop_url"),
        raw=block,
    )
    try:
        img, meta = load_or_render_block_image(
            eq, source_pdf_path=pdf_path, render_long_side=long_side)
    except Exception as exc:  # noqa: BLE001 — битый PDF и т.п.
        return None, f"render failed: {exc}"
    if img is None:
        return None, f"render failed: {(meta or {}).get('status')}"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(out_path), img):
        return None, "cv2.imwrite failed"
    return str(out_path), None


# ─── main entry ──────────────────────────────────────────────────────────────


def run_graphic_vision_enrichment(
        left_model: Any, right_model: Any, visual_gate_report: Any, *,
        left_graphic_report: Any = None, right_graphic_report: Any = None,
        options: Optional[dict] = None,
        vision_runner: Optional[VisionRunner] = None,
        crops_dir: Optional[str | Path] = None) -> dict:
    """Полный прогон слоя: план → (опц.) рендер кропов → (опц.) vision.

    ``vision_runner=None`` → ``skipped_no_runner``: items с prompt/crop refs
    записаны, реальных вызовов нет. Ошибки runner'а/рендера — per-item
    fail-soft.
    """
    plan = build_graphic_vision_enrichment_plan(
        left_model, right_model, visual_gate_report,
        left_graphic_report=left_graphic_report,
        right_graphic_report=right_graphic_report,
        options=options)

    warnings = list(plan.get("warnings") or [])
    items = plan.get("items") or []
    stats = plan.get("stats") or {}
    runner_options = {"model": _opt(options, "runner_model"),
                      "render_long_side": _safe_int(
                          _opt(options, "render_long_side"), 1600)}
    render_crops = _opt(options, "render_crops") is not False

    attempted = succeeded = failed = skipped = 0

    if plan.get("status") != "skipped_no_visual_gate":
        left_pages = _pages_by_number(left_model)
        right_pages = _pages_by_number(right_model)
        left_pdf = _model_pdf_path(left_model)
        right_pdf = _model_pdf_path(right_model)
        left_blocks = _blocks_by_id(left_model)
        right_blocks = _blocks_by_id(right_model)

        prompts = plan.get("prompts_by_item_id") or {}
        for item in items:
            if vision_runner is None:
                item["vision_status"] = "skipped_no_runner"
                skipped += 1
                continue

            left_png = right_png = None
            if render_crops:
                if crops_dir is None:
                    # рендер запрошен, но писать некуда — честный fail item'а
                    # (а не тихий вызов runner'а без изображений)
                    item["warnings"].append("render_crops requested but "
                                            "crops_dir not provided")
                else:
                    base = Path(crops_dir)
                    left_png, lerr = _render_crop_png(
                        left_blocks.get(item["left_block_id"]), left_pages,
                        left_pdf, base / f"{item['item_id']}_left.png",
                        runner_options["render_long_side"])
                    right_png, rerr = _render_crop_png(
                        right_blocks.get(item["right_block_id"]), right_pages,
                        right_pdf, base / f"{item['item_id']}_right.png",
                        runner_options["render_long_side"])
                    for err in (lerr, rerr):
                        if err:
                            item["warnings"].append(err)
                # единая точка приоритета ссылок: rendered → image_file → pdf
                item["left_crop_ref"] = _crop_ref(
                    item.get("left_crop_source") or {}, left_png)
                item["right_crop_ref"] = _crop_ref(
                    item.get("right_crop_source") or {}, right_png)
                if left_png is None and right_png is None:
                    item["vision_status"] = "failed"
                    item["warnings"].append("no crop image available for "
                                            "vision call")
                    failed += 1
                    continue

            attempted += 1
            # полный prompt из плана (write_prompts влияет только на
            # персистенцию item["prompt"], не на вход runner'а)
            prompt = (prompts.get(item["item_id"]) or item.get("prompt")
                      or build_vision_prompt_for_block_pair(
                          {**item, "status": item.get("visual_status")},
                          graphic_type=item.get("graphic_type") or "unknown",
                          discipline=item.get("discipline") or "unknown"))
            try:
                raw = vision_runner(prompt, left_png, right_png,
                                    dict(runner_options))
            except Exception as exc:  # noqa: BLE001 — runner не валит отчёт
                item["vision_status"] = "failed"
                item["warnings"].append(f"vision runner error: "
                                        f"{type(exc).__name__}: {exc}")
                failed += 1
                continue
            result, norm_warnings = normalize_vision_runner_result(raw)
            item["warnings"].extend(norm_warnings)
            if result is None:
                item["vision_status"] = "failed"
                failed += 1
            else:
                item["vision_status"] = "ok"
                item["result"] = result
                succeeded += 1

    if failed:
        # per-item сбои поднимаются на уровень отчёта (иначе dry-run их
        # не видит — warnings собираются только с верхнего уровня)
        warnings.append(f"vision items failed: {failed} of {len(items)}")

    # ── статус отчёта ──
    if plan.get("status") == "skipped_no_visual_gate":
        status = "skipped_no_visual_gate"
    elif vision_runner is None and items:
        status = "skipped_no_runner"
    elif vision_runner is not None and items and failed and not succeeded:
        # ВСЁ упало (включая render-фейлы до вызова) — это failed,
        # а не «предупреждения»
        status = "failed"
    elif failed or warnings or any(i.get("warnings") for i in items):
        status = "completed_with_warnings"
    else:
        status = "ok"

    return {
        "version": REPORT_VERSION,
        "kind": REPORT_KIND,
        "status": status,
        "summary": {
            "candidates_total": stats.get("candidates_total", 0),
            "selected_total": len(items),
            "excluded_by_visual_gate": stats.get("excluded_by_visual_gate", 0),
            "manual_review_included": stats.get("manual_review_included", 0),
            "manual_review_skipped": stats.get("manual_review_skipped", 0),
            "other_skipped": stats.get("other_skipped", 0),
            "dropped_by_cap": stats.get("dropped_by_cap", 0),
            "vision_calls_attempted": attempted,
            "vision_calls_succeeded": succeeded,
            "vision_calls_failed": failed,
            "skipped_no_runner": skipped,
            "runner_model": runner_options["model"],
        },
        "items": items,
        "warnings": warnings,
    }


def write_graphic_vision_enrichment_report(out_path: str | Path,
                                           report: dict) -> Path:
    """Атомарно записать отчёт (tmp + os.replace)."""
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, out)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return out


__all__ = [
    "REPORT_VERSION",
    "REPORT_KIND",
    "VISION_PROMPT_TEMPLATE",
    "select_blocks_for_vision",
    "build_vision_prompt_for_block_pair",
    "build_graphic_vision_enrichment_plan",
    "normalize_vision_runner_result",
    "run_graphic_vision_enrichment",
    "write_graphic_vision_enrichment_report",
]
