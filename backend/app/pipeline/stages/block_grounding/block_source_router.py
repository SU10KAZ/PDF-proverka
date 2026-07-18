"""Роутер источника блока для Stage 02 — «вместо Gemma подаём точный текст чертежа».

Решение Андрея 2026-07-07: «вместо Gemma везде делаем сырые данные, а если это
однолинейки — делаем структурированные вектографом; в дальнейшем — свой
структурированный профиль для каждого типа графического блока».

Развилка на блок (4 ветки):
  1) однолинейная расчётная схема И гейт Вектографа пройден → ПОЛНЫЙ структурированный
     рендер (`render_graph_etalon_markdown`): связи QF↔код↔кабель↔потребитель привязаны
     геометрией колонок (мис-привязки соседства нет);
  2) известная схема ЭОМ/ОВ/ВК/ALIA И профильный гейт пройден → структурированный дисциплинарный
     Markdown: контейнеры, узлы, сети и честное состояние доказательности связей;
  3) есть содержательный вектор-слой (иначе) → СЫРОЙ вектор-текст блока (полигон-клип):
     100% полнота, 0 галлюцинаций OCR; связи домысливает LLM по соседству;
  4) вектор-слоя нет (скан/растр — клип тоньше порога) → image-only: Stage 01
     анализирует приложенный PNG без OCR-описания.

Источник вектор-текста — полигон-клип из PDF по `document_graph.json` (НЕ `pdfplumber_text`
из result.json, который у многих проектов пуст — см. память проекта).

Роутер является штатным источником Stage 01. Ошибка извлечения деградирует в image-only,
если PNG блока доступен.

Для ЭОМ, ГП, АР, КЖ, КМ, ТХ, ОВ, ВК и СС подключены собственные
структурированные профили уровня Вектографа.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Tuple

from .singleline_graph_geometry import (
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    build_singleline_graph,
    evaluate_vectograf_gate,
    render_graph_etalon_markdown,
)
from .block_profile_registry import load_prepared_package, make_package
from backend.app.pipeline.stages.block_context.reference_catalog import load_reference_rules

# Тоньше → считаем, что вектор-слоя нет (скан/растр) → fallback на Gemma.
_MIN_VECTOR_CHARS = 40
_REFERENCE_RULES = load_reference_rules()

_PATH_DISCIPLINES = {
    "EOM": "ЭОМ", "ЭОМ": "ЭОМ", "GP": "ГП", "ГП": "ГП",
    "AR": "АР", "АР": "АР", "KJ": "КЖ", "КЖ": "КЖ",
    "KM": "КМ", "КМ": "КМ", "TX": "ТХ", "ТХ": "ТХ",
    "OV": "ОВ", "ОВ": "ОВ", "VK": "ВК", "ВК": "ВК",
    "SS": "СС", "СС": "СС",
    # АИ (интерьеры) использует те же типы графических блоков, что АР:
    # планы, развёртки, ведомости отделки, потолки, дверные узлы.
    "AI": "АР", "АИ": "АР",
}

_TASK = (
    "## Задача:\n"
    "Выше — ТОЧНЫЙ текст блока (встроенный текст чертежа / структурный разбор вектор-слоя, "
    "это приоритетный источник ЧИСЕЛ, марок, сечений и связей). Найди проблемы и верни "
    "findings[]. Только проблемы. Не описывай что видишь. Если всё корректно — пустой массив."
)


def _locate(output_dir) -> Tuple[Optional[Path], Optional[Path]]:
    """(pdf_path, document_graph_path) по output_dir.

    PDF ищем вверх по родителям (до 4 уровней): legacy (project/_output → PDF в project/),
    V2 (<version_dir>/_output → 02_work/document.pdf) и v2-primary, где output_dir =
    <version_dir>/03_analysis/runs/<run_id> или .../latest — PDF лежит в
    <version_dir>/02_work на 2-3 уровня выше (один od.parent его НЕ находил: роутер и
    предикат пропуска Gemma молча выключались на всех v2-primary прогонах)."""
    od = Path(output_dir)
    dg = od / "document_graph.json"
    dgp = dg if dg.exists() else None
    parents = list(od.parents)[:4]
    for vd in parents:
        for cand in (vd / "02_work" / "document.pdf", vd / "document.pdf"):
            if cand.exists():
                return cand, dgp
    for vd in parents:
        work = vd / "02_work"
        if work.is_dir():
            cands = sorted(work.glob("*.pdf"))
            if cands:
                return cands[0], dgp
    cands = sorted(od.parent.glob("*.pdf"))
    if cands:
        return cands[0], dgp
    return None, dgp


def _locate_chandra_markdown(pdf_path: Path) -> Optional[Path]:
    """Найти исходный Markdown Chandra рядом с нормализованным PDF версии."""
    pdf_path = Path(pdf_path)
    candidates = [
        pdf_path.with_suffix(".md"),
        pdf_path.parent / "document.md",
        pdf_path.parent / f"{pdf_path.stem}_document.md",
    ]
    candidates.extend(sorted(pdf_path.parent.glob("*_document.md")))
    if pdf_path.parent.name == "02_work":
        input_dir = pdf_path.parent.parent / "01_input"
        candidates.extend(sorted(input_dir.glob("*_document.md")))
        candidates.extend(sorted(input_dir.glob("*.md")))
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_file():
            return candidate
    return None


def _load_chandra_description(pdf_path: Path, block_id: str):
    """Fail-soft загрузка разрешённых полей Chandra конкретного блока."""
    try:
        from ..crop_blocks.block_markdown import extract_chandra_block_description

        md_path = _locate_chandra_markdown(pdf_path)
        if md_path is None:
            return None
        return extract_chandra_block_description(
            md_path.read_text(encoding="utf-8"), str(block_id)
        )
    except (OSError, UnicodeError):
        return None


def _classification_context(chandra, block_text: str, page_text: str) -> str:
    """Контекст выбора профиля; текст чертежа Chandra сюда не попадает."""
    if chandra is not None and chandra.classification_text:
        # Описание Chandra является самостоятельным семантическим источником.
        # Даже точный текст полигона может содержать примечания и ссылки на другие
        # чертежи (например, «см. кладочные планы») и не должен менять тип блока.
        return chandra.classification_text
    return (block_text or "") + "\n" + (page_text or "")


def _classification_metadata(
    chandra, profile_id: Optional[str], source_override: Optional[str] = None
) -> dict:
    """Аудируемое объяснение того, откуда взят тип блока."""
    if chandra is None:
        return {
            "profile_id": profile_id,
            "source": source_override or "vector_pdf_fallback",
            "confidence": "medium" if profile_id else "unclassified",
            "block_title": None,
            "chandra_drawing_text_used": False,
        }
    block_title = (
        chandra.short_description or chandra.description or chandra.block_type
    )
    return {
        "profile_id": profile_id,
        "source": source_override or "chandra_md",
        "confidence": (
            "high" if profile_id and (source_override in (None, "chandra_md"))
            else "medium" if profile_id else "needs_profile"
        ),
        "block_title": block_title,
        "block_type": chandra.block_type,
        "short_description": chandra.short_description,
        "description": chandra.description,
        "chandra_drawing_text_used": False,
        "page_context_fallback_used": source_override == "vector_page_fallback",
        "ignored_chandra_fields": list(
            (_REFERENCE_RULES.get("classification") or {}).get(
                "ignored_chandra_fields", ["Текст на чертеже", "Сущности", "ENRICHED"]
            )
        ),
    }


def _discipline_hint(output_dir) -> Optional[str]:
    """Дисциплина версии из пути хранения; защищает от междисциплинарных ложных профилей."""
    output_path = Path(output_dir)
    parts = output_path.parts
    if "disciplines" in parts:
        pos = parts.index("disciplines") + 1
        if pos < len(parts):
            raw = str(parts[pos]).upper()
            # Незнакомая дисциплина — тоже значимый hint: она не должна
            # проваливаться в свободный перебор чужих профильных построителей.
            return _PATH_DISCIPLINES.get(raw, raw)
    # Изолированные эксперименты/legacy-копии могут не повторять полный путь
    # ``disciplines/<code>``, но сохраняют канонический project_info. Без этого
    # AI-копия свободно перебирала ГП/ЭОМ и ошибочно структурировала 36/73 блоков
    # чужими профилями. Метаданные версии надёжнее имени временного каталога.
    for parent in list(output_path.parents)[:5]:
        for candidate in (
            parent / "01_input" / "project_info.json",
            parent / "project_info.json",
        ):
            if not candidate.is_file():
                continue
            try:
                info = json.loads(candidate.read_text(encoding="utf-8"))
                raw = str(info.get("section") or "").strip().upper()
                if raw:
                    return _PATH_DISCIPLINES.get(raw, raw)
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
    # Legacy: .../<объект>/<discipline>/<document>/_output.
    for part in reversed(parts):
        value = _PATH_DISCIPLINES.get(str(part).upper())
        if value:
            return value
    return None


def _extract_block(pdf_path: Path, dg: dict, block_id: str):
    """(page_text, block_text, bbox_norm, polygon_norm, page_pdf) для image-блока. Один open PDF."""
    import fitz  # локально, как в остальных block_grounding

    from .md_mirror_reconcile import _block_text

    doc = fitz.open(str(pdf_path))
    try:
        for p in dg.get("pages", []):
            pi = p.get("page_index", p.get("page"))
            if pi is None or pi >= doc.page_count:
                continue
            for b in p.get("image_blocks", []):
                bid = b.get("id") or b.get("block_id")
                if str(bid) != str(block_id):
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                poly = b.get("polygon_points_norm")
                bbox = b.get("coords_norm")
                clipped = (
                    _clip_words_to_polygon(words, poly, pw, ph)
                    if poly
                    else _clip_words_to_bbox(words, bbox, pw, ph)
                )
                return page.get_text(), _block_text(clipped), bbox, poly, (pi or 0) + 1
        return None
    finally:
        doc.close()


def vector_text_block_index(
    output_dir,
    *,
    include_text_blocks: bool = True,
    include_image_blocks: bool = True,
) -> dict:
    """Точный PDF-векторный текст image- и text-блоков за один проход.

    В отличие от :func:`vector_covered_block_ids`, индекс включает короткие
    подписи и ``text_blocks``.  Это нужно детерминированным evidence-проверкам:
    значимый токен может состоять из одного обозначения, а текст в
    ``document_graph`` для text-блока является OCR-производным и потому не
    может служить источником истины.

    Формат записи::

        {block_id: {
            "text": str,
            "page": int | str | None,
            "page_index": int,
            "block_kind": "image" | "text",
            "source": "pdf_vector_text",
            "source_file": str,
            "router_eligible": bool,
        }}

    Смещения внутри ``text`` остаются смещениями Python Unicode-строки.
    Любая ошибка даёт пустой индекс (fail-soft).
    """
    try:
        import fitz  # локально, как в остальных block_grounding

        from .md_mirror_reconcile import _block_text

        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return {}
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        doc = fitz.open(str(pdf))
        try:
            out: dict = {}
            for p in dg.get("pages", []):
                pi = p.get("page_index", p.get("page"))
                if pi is None or pi >= doc.page_count:
                    continue
                page = doc[pi]
                pw, ph = float(page.rect.width), float(page.rect.height)
                words = page.get_text("words")
                page_number = p.get("page")
                if page_number is None:
                    page_number = (pi or 0) + 1
                block_fields = []
                if include_text_blocks:
                    block_fields.append(("text", "text_blocks"))
                if include_image_blocks:
                    block_fields.append(("image", "image_blocks"))
                for block_kind, field in block_fields:
                    for b in p.get(field, []) or []:
                        bid = b.get("id") or b.get("block_id")
                        if not bid:
                            continue
                        poly = b.get("polygon_points_norm")
                        clipped = (
                            _clip_words_to_polygon(words, poly, pw, ph)
                            if poly
                            else _clip_words_to_bbox(
                                words, b.get("coords_norm"), pw, ph
                            )
                        )
                        txt = _block_text(clipped)
                        if not str(txt or "").strip():
                            continue
                        out[str(bid)] = {
                            "text": txt,
                            "page": page_number,
                            "page_index": pi,
                            "block_kind": block_kind,
                            "source": "pdf_vector_text",
                            "source_file": str(pdf),
                            "router_eligible": (
                                block_kind == "image"
                                and len(str(txt).strip()) >= _MIN_VECTOR_CHARS
                            ),
                        }
            return out
        finally:
            doc.close()
    except Exception:
        return {}


def vector_covered_block_ids(output_dir) -> dict:
    """{block_id: вектор-текст} для image-блоков с ГОДНЫМ вектор-слоем (≥ _MIN_VECTOR_CHARS).

    ОБЩИЙ предикат с `resolve_block_source`: тот же полигон-клип, тот же порог. Блок попадает
    сюда ТОГДА И ТОЛЬКО ТОГДА, когда роутер на Stage 02 отдаст ему вектор-текст (не gemma_fallback).
    Используется гейтом пропуска стадии Gemma: эти блоки можно НЕ гонять через Gemma — роутер и так
    подаст их точный текст. Один open PDF (батч). fail-soft → {}.

    ВАЖНО: значение (вектор-текст) кладётся в placeholder-enrichment пропущенного блока —
    страховка на случай, если роутер на Stage 02 всё же fail-soft'нётся: блок не станет слепым.
    """
    try:
        return {
            block_id: record["text"]
            for block_id, record in vector_text_block_index(
                output_dir,
                include_text_blocks=False,
            ).items()
            if isinstance(record, dict) and record.get("router_eligible") is True
        }
    except Exception:
        return {}


def resolve_block_package(
    output_dir, block_id: str, page, *, panel_hint: str = "ВРУ",
    prefer_prepared: bool = True,
) -> dict:
    """Канонический пакет Stage 01: эталон + профиль + граф + Markdown + LLM-текст.

    source_kind:
      structured_singleline | structured_electrical | structured_general_plan | structured_architecture | structured_structure | structured_hvac | structured_water | structured_alia_scheme | raw_vector | image_only |
      no_sources | block_not_found | error.
    fail-soft: при любой проблеме возвращается пакет без текста и прод-путь сохраняется.
    """
    if prefer_prepared:
        prepared = load_prepared_package(Path(output_dir), str(block_id))
        if prepared is not None:
            return prepared
    try:
        pdf, dgp = _locate(output_dir)
        if pdf is None or dgp is None:
            return make_package(block_id=block_id, page=page, source_kind="no_sources", user_text=None)
        dg = json.loads(dgp.read_text(encoding="utf-8"))
        ex = _extract_block(pdf, dg, str(block_id))
        if ex is None:
            return make_package(block_id=block_id, page=page, source_kind="block_not_found", user_text=None)
        page_text, block_text, bbox, poly, page_pdf = ex
        head = f"# Блок {block_id} | страница PDF {page or page_pdf}\n\n"
        discipline_hint = _discipline_hint(output_dir)
        allows = lambda code: discipline_hint in (None, code)
        chandra = _load_chandra_description(pdf, str(block_id))
        profile_context = _classification_context(chandra, block_text, page_text)
        block_profile_context = _classification_context(chandra, block_text, "")
        legacy_page_context = (block_text or "") + "\n" + (page_text or "")
        legacy_block_context = block_text or ""
        profile_sources: dict[str, str] = {}

        def remember_profile_source(profile_id, source: str) -> None:
            if profile_id:
                profile_sources[str(profile_id)] = source

        def package(**kwargs):
            graph = kwargs.get("graph") if isinstance(kwargs.get("graph"), dict) else None
            profile_id = str(
                kwargs.get("profile_id") or (graph or {}).get("profile_id") or ""
            ).strip() or None
            source = profile_sources.get(str(profile_id or ""))
            if source is None and profile_id == "raw_vector":
                source = "vector_pdf_fallback"
            classification = _classification_metadata(chandra, profile_id, source)
            if graph is not None:
                graph["classification"] = classification
            # Структурный профиль — индекс, а не замена первичного источника.
            # Для АИ сохраняем рядом полный точный текст полигона: профиль может
            # ошибиться в типе или не извлечь марку/размер, а LLM не должен
            # принимать отсутствие поля в производном графе за отсутствие в РД.
            if (
                kwargs.get("source_kind") == "structured_architecture"
                and block_text
                and kwargs.get("user_text")
            ):
                kwargs["user_text"] += (
                    "\n\n## Полный точный текст полигона из PDF\n"
                    "Это первичный проектный источник; служебное название профиля выше "
                    "не является данными проекта.\n```\n"
                    + block_text
                    + "\n```\n"
                )
            kwargs["classification"] = classification
            return make_package(**kwargs)

        # (1) однолинейка + гейт → полный структурированный рендер
        if allows("ЭОМ") and len((block_text or "").strip()) >= _MIN_VECTOR_CHARS:
            graph = None
            try:
                graph = build_singleline_graph(
                    pdf, page_text, panel_hint=panel_hint, bbox_norm=bbox, polygon_norm=poly
                )
            except Exception:
                graph = None
            if graph and evaluate_vectograf_gate(graph).get("use"):
                etalon = render_graph_etalon_markdown(graph)
                if etalon and len(etalon) > 200:
                    gate = evaluate_vectograf_gate(graph)
                    remember_profile_source("electrical_singleline", "vectograph_pdf")
                    return package(
                        block_id=block_id, page=page or page_pdf,
                        source_kind="structured_singleline", discipline="ЭОМ",
                        profile_id="electrical_singleline", graph=graph, gate=gate,
                        markdown=etalon, user_text=head + etalon + "\n\n" + _TASK,
                    )

        # (2) остальные профили ЭОМ. Они идут до общих инженерных профилей:
        # план электрощитовой содержит слова «план» и «оборудование», из-за чего
        # без дисциплинарного приоритета мог ошибочно попасть в ВК.
        try:
            import re
            from .electrical_geometry import (
                PROFILE_PANEL,PROFILE_SINGLELINE,build_electrical_graph_from_source,
                classify_electrical_profile,evaluate_electrical_gate,render_electrical_markdown,
            )
            electrical_source="chandra_md" if chandra is not None else "vector_block_pdf"
            electrical_profile=classify_electrical_profile(block_profile_context) if allows("ЭОМ") else None
            if electrical_profile is None and chandra is not None and allows("ЭОМ"):
                electrical_profile=classify_electrical_profile(legacy_block_context)
                electrical_source="vector_block_fallback"
            if electrical_profile==PROFILE_SINGLELINE:
                electrical_profile=PROFILE_PANEL if len(re.findall(r"\bQF\s*\d",block_text,re.I))>=3 else None
                electrical_source="vector_block_fallback"
            remember_profile_source(electrical_profile,electrical_source)
            electrical_graph=build_electrical_graph_from_source(
                pdf,page_index=page_pdf-1,bbox_norm=bbox,polygon_norm=poly,
                block_id=str(block_id),profile_hint=electrical_profile,
                subtype_hint="многоуровневая схема распределения" if electrical_profile==PROFILE_PANEL else "графический блок ЭОМ",
            ) if electrical_profile else None
        except Exception:
            electrical_graph=None
        if electrical_graph and evaluate_electrical_gate(electrical_graph).get("use"):
            structured=render_electrical_markdown(electrical_graph)
            if structured and len(structured)>150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_electrical",discipline="ЭОМ",graph=electrical_graph,
                  gate=evaluate_electrical_gate(electrical_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (3) генеральный план, разбивка, рельеф, покрытия, МАФ и водоотвод ГП.
        try:
            from .general_plan_geometry import (build_gp_graph_from_source,classify_gp_profile,
                evaluate_gp_gate,render_gp_markdown)
            # Chandra описывает конкретный полигон. Штамп листа используется только
            # как fallback, когда исходной секции Chandra нет.
            gp_source="chandra_md" if chandra is not None else "vector_page_fallback"
            gp_profile=classify_gp_profile(profile_context) if allows("ГП") else None
            if gp_profile is None and chandra is not None and allows("ГП"):
                gp_profile=classify_gp_profile(legacy_block_context)
                gp_source="vector_block_fallback"
                if gp_profile is None:
                    gp_profile=classify_gp_profile(legacy_page_context)
                    gp_source="vector_page_fallback"
            remember_profile_source(gp_profile,gp_source)
            gp_graph=build_gp_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=gp_profile) if gp_profile else None
        except Exception:
            gp_graph=None
        if gp_graph and evaluate_gp_gate(gp_graph).get("use"):
            structured=render_gp_markdown(gp_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_general_plan",discipline="ГП",graph=gp_graph,
              gate=evaluate_gp_gate(gp_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (4) архитектурные планы, фасады, разрезы, развёртки и узлы АР.
        try:
            from .architecture_geometry import (build_ar_graph_from_source,classify_ar_profile,
                evaluate_ar_gate,render_ar_markdown)
            ar_context=profile_context
            ar_upper=ar_context.upper()
            ar_marker=chandra is not None or any(x in ar_upper for x in ("АРХИТЕКТУРНЫЕ РЕШЕНИЯ","КЛАДОЧ","МАРКИРОВОЧ",
              "РАЗВЕРТК","ФАСАД","ОТДЕЛКА КВАРТИР","ЧИСТОВАЯ ОТДЕЛКА","УЗЛЫ КРОВЛИ",
              "ЛЕСТНИЦ","ОГРАЖДЕНИЯ ЛЕСТНИЧНЫХ"))
            ar_source="chandra_md" if chandra is not None else "vector_page_fallback"
            ar_profile=classify_ar_profile(ar_context) if ar_marker and allows("АР") else None
            if ar_profile is None and chandra is not None and allows("АР"):
                ar_profile=classify_ar_profile(legacy_block_context)
                ar_source="vector_block_fallback"
                if ar_profile is None:
                    ar_profile=classify_ar_profile(legacy_page_context)
                    ar_source="vector_page_fallback"
            remember_profile_source(ar_profile,ar_source)
            ar_graph=build_ar_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=ar_profile) if ar_profile else None
        except Exception:
            ar_graph=None
        if ar_graph and evaluate_ar_gate(ar_graph).get("use"):
            structured=render_ar_markdown(ar_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_architecture",discipline="АР",graph=ar_graph,
              gate=evaluate_ar_gate(ar_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (5) железобетонные планы, армирование, сечения и закладные детали КЖ.
        try:
            from .structural_geometry import (build_kj_graph_from_source,classify_kj_profile,
                evaluate_structural_gate,render_structural_markdown)
            kj_context=profile_context;kj_upper=kj_context.upper()
            kj_marker=chandra is not None or any(x in kj_upper for x in ("АРМИРОВАН","ОПАЛУБ","ВЕРТИКАЛЬНЫЕ КОНСТРУКЦ",
              "ПЛИТА ПЕРЕКРЫТИЯ","СЕЧЕНИЯ АРМИРОВАНИЯ","ЗАКЛАДНАЯ ДЕТАЛЬ"))
            kj_source="chandra_md" if chandra is not None else "vector_page_fallback"
            kj_profile=classify_kj_profile(kj_context) if kj_marker and allows("КЖ") else None
            if kj_profile is None and chandra is not None and allows("КЖ"):
                kj_profile=classify_kj_profile(legacy_block_context)
                kj_source="vector_block_fallback"
                if kj_profile is None:
                    kj_profile=classify_kj_profile(legacy_page_context)
                    kj_source="vector_page_fallback"
            remember_profile_source(kj_profile,kj_source)
            kj_graph=build_kj_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=kj_profile) if kj_profile else None
        except Exception:
            kj_graph=None
        if kj_graph and evaluate_structural_gate(kj_graph).get("use"):
            structured=render_structural_markdown(kj_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_structure",discipline="КЖ",graph=kj_graph,
              gate=evaluate_structural_gate(kj_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # Металлоконструкции и фасадные подсистемы КМ используют ту же
        # доказательную основу, но собственные профили элементов и соединений.
        try:
            from .structural_geometry import (build_kj_graph_from_source,classify_km_profile,
                evaluate_structural_gate,render_structural_markdown)
            km_context=profile_context;km_upper=km_context.upper()
            km_marker=chandra is not None or any(x in km_upper for x in ("КОНСТРУКЦИИ МЕТАЛЛИЧЕСКИЕ","МЕТАЛЛИЧЕСКИХ КОНСТРУКЦИЙ",
              "НАВЕСНОЙ ФАСАДНОЙ СИСТЕМЫ","НВФ","MOCKUP","СТРЕМЯНКА"))
            km_source="chandra_md" if chandra is not None else "vector_page_fallback"
            km_profile=classify_km_profile(km_context) if km_marker and allows("КМ") else None
            if km_profile is None and chandra is not None and allows("КМ"):
                km_profile=classify_km_profile(legacy_block_context)
                km_source="vector_block_fallback"
                if km_profile is None:
                    km_profile=classify_km_profile(legacy_page_context)
                    km_source="vector_page_fallback"
            remember_profile_source(km_profile,km_source)
            km_graph=build_kj_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=km_profile,subtype_hint="металлическая конструкция") if km_profile else None
        except Exception:
            km_graph=None
        if km_graph and evaluate_structural_gate(km_graph).get("use"):
            structured=render_structural_markdown(km_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_structure",discipline="КМ",graph=km_graph,
              gate=evaluate_structural_gate(km_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (6) автостоянки, лифты и мусороудаление ТХ.
        try:
            from .technology_geometry import (build_tx_graph_from_source,classify_tx_profile,
                evaluate_tx_gate,render_tx_markdown)
            tx_context=profile_context;tx_upper=tx_context.upper()
            tx_marker=chandra is not None or any(x in tx_upper for x in ("ТЕХНОЛОГИЧЕСКИЕ РЕШЕНИЯ","ВЕРТИКАЛЬНЫЙ ТРАНСПОРТ",
              "МАШИНОМЕСТ","ЛИФТОВЫХ ШАХТ","МУСОРОУДАЛЕНИЕ"))
            tx_source="chandra_md" if chandra is not None else "vector_page_fallback"
            tx_profile=classify_tx_profile(tx_context) if tx_marker and allows("ТХ") else None
            if tx_profile is None and chandra is not None and allows("ТХ"):
                tx_profile=classify_tx_profile(legacy_block_context)
                tx_source="vector_block_fallback"
                if tx_profile is None:
                    tx_profile=classify_tx_profile(legacy_page_context)
                    tx_source="vector_page_fallback"
            remember_profile_source(tx_profile,tx_source)
            tx_graph=build_tx_graph_from_source(pdf,page_index=page_pdf-1,bbox_norm=bbox,
              polygon_norm=poly,block_id=str(block_id),profile_hint=tx_profile) if tx_profile else None
        except Exception:
            tx_graph=None
        if tx_graph and evaluate_tx_gate(tx_graph).get("use"):
            structured=render_tx_markdown(tx_graph)
            if structured and len(structured)>150:return package(block_id=block_id,page=page or page_pdf,
              source_kind="structured_technology",discipline="ТХ",graph=tx_graph,
              gate=evaluate_tx_gate(tx_graph),markdown=structured,user_text=head+structured+"\n\n"+_TASK)

        # (6) планы, аксонометрии, узлы, профили и оборудование ВК.
        # ВК идёт раньше ОВ: водомерный/насосный узел по общей лексике похож на
        # гидравлическую схему ОВ, но известный block_id дисциплины точнее текста.
        try:
            from .water_geometry import (
                build_water_graph_from_source,
                classify_water_profile,
                evaluate_water_gate,
                render_water_markdown,
            )
            water_source="chandra_md" if chandra is not None else "vector_block_pdf"
            water_profile, water_subtype = classify_water_profile(
                block_profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("ВК") else (None, None)
            if water_profile is None and chandra is not None and allows("ВК"):
                water_profile, water_subtype = classify_water_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                water_source="vector_block_fallback"
            remember_profile_source(water_profile,water_source)
            water_graph = build_water_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=water_profile, subtype_hint=water_subtype,
            ) if water_profile else None
        except Exception:
            water_graph = None
        if water_graph and evaluate_water_gate(water_graph).get("use"):
            structured = render_water_markdown(water_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_water",discipline="ВК",graph=water_graph,
                  gate=evaluate_water_gate(water_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (5) планы, аксонометрии, гидравлика, узлы, разрезы и оборудование ОВ
        try:
            from .hvac_geometry import (
                build_hvac_graph_from_source,
                classify_hvac_profile,
                evaluate_hvac_gate,
                render_hvac_markdown,
            )
            hvac_source="chandra_md" if chandra is not None else "vector_block_pdf"
            hvac_profile, hvac_subtype = classify_hvac_profile(
                block_profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("ОВ") else (None, None)
            if hvac_profile is None and chandra is not None and allows("ОВ"):
                hvac_profile, hvac_subtype = classify_hvac_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                hvac_source="vector_block_fallback"
            remember_profile_source(hvac_profile,hvac_source)
            hvac_graph = build_hvac_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=hvac_profile, subtype_hint=hvac_subtype,
            ) if hvac_profile else None
        except Exception:
            hvac_graph = None
        if hvac_graph and evaluate_hvac_gate(hvac_graph).get("use"):
            structured = render_hvac_markdown(hvac_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_hvac",discipline="ОВ",graph=hvac_graph,
                  gate=evaluate_hvac_gate(hvac_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        if len((block_text or "").strip()) < _MIN_VECTOR_CHARS:
            return package(block_id=block_id,page=page or page_pdf,
              source_kind="image_only",user_text=None)  # Stage 01 анализирует PNG

        # (5) профили графических схем ALIA → дисциплинарная структура вместо сырого текста
        try:
            from .alia_scheme_geometry import (
                build_alia_scheme_graph_from_source,
                classify_alia_scheme_profile,
                evaluate_alia_scheme_gate,
                render_alia_scheme_markdown,
            )
            alia_profile = classify_alia_scheme_profile(block_profile_context) if allows("СС") else None
            remember_profile_source(
                alia_profile,
                "chandra_md" if chandra is not None else "vector_block_pdf",
            )
            alia_graph = build_alia_scheme_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox,
                polygon_norm=poly, block_id=str(block_id),
                profile_hint=alia_profile,
            ) if allows("СС") else None
            if alia_graph and alia_profile is None:
                remember_profile_source(alia_graph.get("profile_id"),"vector_block_fallback")
        except Exception:
            alia_graph = None
        if alia_graph and evaluate_alia_scheme_gate(alia_graph).get("use"):
            structured = render_alia_scheme_markdown(alia_graph)
            if structured and len(structured) > 200:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_alia_scheme",discipline="СС",graph=alia_graph,
                  gate=evaluate_alia_scheme_gate(alia_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (6) планы, подключения, принципиальные схемы, узлы и физические сборки ALIA
        try:
            from .alia_remaining_geometry import (
                build_remaining_graph_from_source,
                classify_remaining_profile,
                evaluate_remaining_gate,
                render_remaining_markdown,
            )
            remaining_profile, remaining_subtype = classify_remaining_profile(
                profile_context, block_id=str(block_id),
                prefer_block_hint=chandra is None,
            ) if allows("СС") else (None, None)
            remaining_context = profile_context
            remaining_source="chandra_md" if chandra is not None else "vector_page_fallback"
            if remaining_profile is None and chandra is not None and allows("СС"):
                remaining_profile, remaining_subtype = classify_remaining_profile(
                    legacy_block_context, block_id=str(block_id)
                )
                remaining_context = legacy_block_context
                remaining_source="vector_block_fallback"
                if remaining_profile is None:
                    remaining_profile, remaining_subtype = classify_remaining_profile(
                        legacy_page_context, block_id=str(block_id)
                    )
                    remaining_context = legacy_page_context
                    remaining_source="vector_page_fallback"
            remember_profile_source(remaining_profile,remaining_source)
            remaining_graph = build_remaining_graph_from_source(
                pdf, page_index=page_pdf - 1, bbox_norm=bbox, polygon_norm=poly,
                block_id=str(block_id), context_text=remaining_context,
                profile_hint=remaining_profile, subtype_hint=remaining_subtype,
            ) if allows("СС") else None
            if remaining_graph and remaining_profile is None:
                remember_profile_source(remaining_graph.get("profile_id"),"vector_page_fallback")
        except Exception:
            remaining_graph = None
        if remaining_graph and evaluate_remaining_gate(remaining_graph).get("use"):
            structured = render_remaining_markdown(remaining_graph)
            if structured and len(structured) > 150:
                return package(block_id=block_id,page=page or page_pdf,
                  source_kind="structured_alia_scheme",discipline="СС",graph=remaining_graph,
                  gate=evaluate_remaining_gate(remaining_graph),markdown=structured,
                  user_text=head+structured+"\n\n"+_TASK)

        # (7) иначе → сырой вектор-текст блока (полный, без потерь)
        body = (
            "## Точный текст блока из вектор-слоя PDF (встроенный текст чертежа, без ошибок OCR):\n"
            f"```\n{block_text}\n```\n"
        )
        return package(block_id=block_id,page=page or page_pdf,
          source_kind="raw_vector",profile_id="raw_vector",markdown=body,
          user_text=head+body+"\n"+_TASK)
    except Exception as exc:
        return make_package(block_id=block_id,page=page,source_kind="error",
          user_text=None,error=f"{type(exc).__name__}: {exc}")


def resolve_block_source(
    output_dir, block_id: str, page, *, panel_hint: str = "ВРУ"
) -> Tuple[Optional[str], str]:
    """Совместимый интерфейс: вернуть только LLM-текст и вид источника."""
    package = resolve_block_package(output_dir, block_id, page, panel_hint=panel_hint)
    return package.get("user_text"), str(package.get("source_kind") or "error")
