"""
Сервис для работы с замечаниями аудита.
Чтение, фильтрация, сводка из 03_findings.json.
"""
import json
from pathlib import Path
from typing import Optional

from webapp.config import SEVERITY_CONFIG
from webapp.models.findings import FindingsResponse, FindingsSummary
from webapp.services.project_service import resolve_project_dir


def _get_findings_path(project_id: str) -> Path:
    """Выбрать лучший файл замечаний: 03a (верифицированный) или 03 (базовый)."""
    output_dir = resolve_project_dir(project_id) / "_output"
    verified = output_dir / "03a_norms_verified.json"
    if verified.exists():
        return verified
    return output_dir / "03_findings.json"


def get_findings(
    project_id: str,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    sheet: Optional[str] = None,
    search: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> Optional[FindingsResponse]:
    """Получить замечания проекта с фильтрацией и пагинацией."""
    path = _get_findings_path(project_id)
    data = _load_json(path)
    if data is None:
        return None

    items = data.get("findings", data.get("items", []))
    _enrich_sheet_page(items, project_id)
    audit_date = data.get("audit_date", data.get("generated_at"))

    # Фильтрация
    filtered = items
    if severity:
        sev_upper = severity.upper()
        filtered = [f for f in filtered if sev_upper in f.get("severity", "").upper()]
    if category:
        cat_lower = category.lower()
        filtered = [f for f in filtered if cat_lower in f.get("category", "").lower()]
    if sheet:
        filtered = [f for f in filtered if sheet in str(f.get("sheet", ""))]
    if search:
        s_lower = search.lower()
        filtered = [
            f for f in filtered
            if s_lower in json.dumps(f, ensure_ascii=False).lower()
        ]

    # Сводка по критичности (по всем, не отфильтрованным)
    by_severity = {}
    for item in items:
        sev = item.get("severity", "НЕИЗВЕСТНО")
        by_severity[sev] = by_severity.get(sev, 0) + 1

    # Сортировка по критичности
    sev_order = {s: cfg["order"] for s, cfg in SEVERITY_CONFIG.items()}
    filtered.sort(key=lambda f: sev_order.get(f.get("severity", ""), 99))

    # Пагинация (после фильтрации и сортировки)
    filtered_total = len(filtered)
    if offset is not None:
        filtered = filtered[offset:]
    if limit is not None:
        filtered = filtered[:limit]

    return FindingsResponse(
        project_id=project_id,
        total=len(items),
        filtered_total=filtered_total,
        by_severity=by_severity,
        findings=filtered,
        audit_date=audit_date,
    )


def get_finding_by_id(project_id: str, finding_id: str) -> Optional[dict]:
    """Получить одно замечание по ID."""
    path = _get_findings_path(project_id)
    data = _load_json(path)
    if data is None:
        return None

    items = data.get("findings", data.get("items", []))
    for item in items:
        if item.get("id", "") == finding_id:
            return item
    return None


def get_all_summaries() -> list[FindingsSummary]:
    """Сводка замечаний по всем проектам."""
    from webapp.services.project_service import iter_project_dirs
    summaries = []
    for project_id, entry in iter_project_dirs():
        path = entry / "_output" / "03a_norms_verified.json"
        if not path.exists():
            path = entry / "_output" / "03_findings.json"
        data = _load_json(path)
        if data is None:
            continue

        items = data.get("findings", data.get("items", []))
        by_severity = {}
        for item in items:
            sev = item.get("severity", "НЕИЗВЕСТНО")
            by_severity[sev] = by_severity.get(sev, 0) + 1

        summaries.append(FindingsSummary(
            project_id=project_id,
            total=len(items),
            by_severity=by_severity,
            audit_date=data.get("audit_date", data.get("generated_at")),
        ))

    return summaries


def get_all_optimization_summaries() -> list[dict]:
    """Сводка оптимизаций по всем проектам."""
    from webapp.services.project_service import iter_project_dirs
    summaries = []
    for project_id, entry in iter_project_dirs():
        opt_path = entry / "_output" / "optimization.json"
        data = _load_json(opt_path)
        if data is None:
            continue

        meta = data.get("meta", {})
        items = data.get("items", [])

        # Агрегация по типам
        by_type = {}
        for item in items:
            t = item.get("type", "unknown")
            by_type[t] = by_type.get(t, 0) + 1

        # Статистика savings
        savings_values = [it.get("savings_pct", 0) for it in items if it.get("savings_pct", 0) > 0]
        avg_savings = round(sum(savings_values) / len(savings_values), 1) if savings_values else 0

        # Review stats
        review_path = entry / "_output" / "optimization_review.json"
        review_data = _load_json(review_path)
        review_stats = None
        if review_data:
            verdicts = review_data.get("meta", {}).get("verdicts", {})
            review_stats = {
                "total_reviewed": review_data.get("meta", {}).get("total_reviewed", 0),
                "pass": verdicts.get("pass", 0),
                "issues": sum(v for k, v in verdicts.items() if k != "pass"),
            }

        summaries.append({
            "project_id": project_id,
            "total_items": len(items),
            "by_type": by_type,
            "estimated_savings_pct": meta.get("estimated_savings_pct", 0),
            "avg_savings_pct": avg_savings,
            "top3_summary": meta.get("top3_summary", ""),
            "analysis_date": meta.get("analysis_date", ""),
            "review_applied": meta.get("review_applied", False),
            "review_stats": review_stats,
        })

    return summaries


def get_finding_block_map(project_id: str) -> Optional[dict]:
    """Маппинг finding_id → [block_ids] через совпадение страниц."""
    import re

    findings_path = _get_findings_path(project_id)
    findings_data = _load_json(findings_path)
    if findings_data is None:
        return None

    blocks_by_page, block_info, all_block_ids = _load_blocks_data(project_id)
    block_id_re = re.compile(r'\b([A-Z0-9]{3,5}-[A-Z0-9]{3,5}-[A-Z0-9]{2,4})\b')

    items = findings_data.get("findings", findings_data.get("items", []))
    result: dict[str, list[str]] = {}

    def _norm_bid(bid: str) -> str:
        """Нормализация block_id: убрать префикс 'block_' если есть."""
        return bid[6:] if bid and bid.startswith("block_") else (bid or "")

    for f in items:
        fid = f.get("id", "")
        if not fid:
            continue

        matched_blocks: list[str] = []
        seen: set[str] = set()

        # 1. evidence array (наивысший приоритет — точная трассировка)
        evidence = f.get("evidence")
        if evidence and isinstance(evidence, list):
            for ev in evidence:
                raw_bid = ev.get("block_id", "")
                bid = _norm_bid(raw_bid)
                if ev.get("type") == "image" and bid in all_block_ids and bid not in seen:
                    matched_blocks.append(bid)
                    seen.add(bid)

        # 2. related_block_ids (fallback от evidence)
        if not matched_blocks:
            related = f.get("related_block_ids")
            if related and isinstance(related, list):
                for raw_bid in related:
                    bid = _norm_bid(raw_bid)
                    if bid in all_block_ids and bid not in seen:
                        matched_blocks.append(bid)
                        seen.add(bid)

        # 2. Явные block_id в description (fallback)
        if not matched_blocks:
            desc = f.get("description", "")
            for m in block_id_re.finditer(desc):
                bid = m.group(1)
                if bid in all_block_ids and bid not in seen:
                    matched_blocks.append(bid)
                    seen.add(bid)

        # 3. По страницам из sheet (последний fallback)
        if not matched_blocks:
            pages = _parse_pages_from_text(f.get("sheet") or "")
            for page in sorted(pages):
                for bid in blocks_by_page.get(page, []):
                    if bid not in seen:
                        matched_blocks.append(bid)
                        seen.add(bid)

        if matched_blocks:
            result[fid] = matched_blocks

    return {
        "project_id": project_id,
        "block_map": result,
        "block_info": block_info,
    }


def _enrich_sheet_page(findings: list[dict], project_id: str):
    """Обогатить findings: разделить sheet/page, подставить sheet_no из document_graph."""
    import re

    # Загрузить маппинг page → sheet_no из document_graph
    graph_path = resolve_project_dir(project_id) / "_output" / "document_graph.json"
    page_to_sheet: dict[int, str] = {}
    graph_data = _load_json(graph_path)
    if graph_data:
        for p in graph_data.get("pages", []):
            page_num = p.get("page")
            sheet_no = p.get("sheet_no")
            if page_num is not None and sheet_no:
                page_to_sheet[page_num] = str(sheet_no)

    # Паттерн для парсинга старого формата "Лист X (стр. PDF N)"
    # Также ловит "Лист 10/Сводная спецификация (стр. PDF 15)" и "Лист 13, 14 (стр. PDF 13–14)"
    old_format_re = re.compile(
        r'(?:Лист(?:ы)?)\s*(.+?)\s*\(стр\.?\s*PDF\s*([\d.,\s\-–]+)\)',
        re.IGNORECASE,
    )
    pdf_page_re = re.compile(r'стр\.?\s*(?:PDF\s*)?([\d]+)', re.IGNORECASE)

    for f in findings:
        sheet_val = f.get("sheet", "")
        page_val = f.get("page")

        # Если page уже заполнен (новый формат) — только проверить sheet
        if page_val is not None:
            # page может быть int или list[int]
            pages = page_val if isinstance(page_val, list) else [page_val]
            if not sheet_val or sheet_val == str(page_val):
                # sheet пустой или совпадает с page → подставить из графа
                sheets = []
                for pg in pages:
                    if isinstance(pg, int) and pg in page_to_sheet:
                        sheets.append(page_to_sheet[pg])
                if sheets:
                    unique = list(dict.fromkeys(sheets))
                    f["sheet"] = "Лист " + ", ".join(unique) if len(unique) <= 3 else f"Листы {unique[0]}–{unique[-1]}"
            continue

        # Старый формат: разобрать "Лист X (стр. PDF N)"
        if not sheet_val:
            continue

        m = old_format_re.search(sheet_val)
        if m:
            # Извлечь page из "(стр. PDF N)"
            pdf_str = m.group(2).strip().replace('–', '-').replace('—', '-')
            pages_parsed = []
            for part in re.split(r'[,\s]+', pdf_str):
                part = part.strip()
                if '-' in part:
                    bounds = part.split('-')
                    try:
                        pages_parsed.extend(range(int(bounds[0]), int(bounds[-1]) + 1))
                    except (ValueError, IndexError):
                        pass
                elif part.isdigit():
                    pages_parsed.append(int(part))
            if pages_parsed:
                f["page"] = pages_parsed[0] if len(pages_parsed) == 1 else pages_parsed
                # Пересобрать sheet из графа если возможно
                sheets = [page_to_sheet[pg] for pg in pages_parsed if pg in page_to_sheet]
                if sheets:
                    unique = list(dict.fromkeys(sheets))
                    f["sheet"] = "Лист " + ", ".join(unique)
                else:
                    # Оставить лист из оригинала, убрав "(стр. PDF ...)"
                    sheet_part = m.group(1).strip().rstrip(',').rstrip('/')
                    f["sheet"] = f"Лист {sheet_part}"
        else:
            # Попытаться извлечь хотя бы page из текста
            pm = pdf_page_re.search(sheet_val)
            if pm:
                try:
                    f["page"] = int(pm.group(1))
                except ValueError:
                    pass


def _load_blocks_data(project_id: str) -> tuple[dict, dict, set]:
    """Загрузить блоки: blocks_by_page, block_info, all_block_ids."""
    import re

    blocks_by_page: dict[int, list[str]] = {}
    all_block_ids: set[str] = set()
    block_info: dict[str, dict] = {}

    blocks_path = resolve_project_dir(project_id) / "_output" / "02_blocks_analysis.json"
    blocks_data = _load_json(blocks_path)
    if blocks_data:
        block_list = blocks_data.get("blocks") or blocks_data.get("block_analyses") or []
        for block in block_list:
            bid = block.get("block_id", "")
            page = block.get("page")
            if bid and page is not None:
                all_block_ids.add(bid)
                blocks_by_page.setdefault(page, []).append(bid)

    index_path = resolve_project_dir(project_id) / "_output" / "blocks" / "index.json"
    index_data = _load_json(index_path)
    if index_data:
        for b in index_data.get("blocks", []):
            bid = b.get("block_id", "")
            if bid:
                block_info[bid] = {
                    "block_id": bid,
                    "page": b.get("page"),
                    "ocr_label": b.get("ocr_label", ""),
                }
                page = b.get("page")
                if page is not None:
                    all_block_ids.add(bid)
                    if bid not in blocks_by_page.get(page, []):
                        blocks_by_page.setdefault(page, []).append(bid)

    return blocks_by_page, block_info, all_block_ids


def _parse_pages_from_text(text: str) -> set[int]:
    """Извлечь номера страниц/листов из строки.

    Поддерживает: 'стр. 8-20', 'листы 19-27', 'лист 23', 'листы 6-7, 21'.
    """
    import re
    pages: set[int] = set()
    # Ищем "стр." или "лист(ы)" с числами
    pattern = re.compile(r'(?:стр\.|листы?)\s*([\d,\s\-–]+)', re.IGNORECASE)
    for m in pattern.finditer(text):
        pages_str = m.group(1)
        for part in re.split(r'[,;]\s*', pages_str):
            part = part.strip().replace('–', '-')
            if '-' in part:
                bounds = part.split('-')
                try:
                    start, end = int(bounds[0].strip()), int(bounds[-1].strip())
                    pages.update(range(start, end + 1))
                except ValueError:
                    pass
            else:
                try:
                    pages.add(int(part))
                except ValueError:
                    pass
    return pages


def _load_sheet_to_page_map(project_id: str) -> dict[str, int]:
    """Маппинг sheet_no → page из document_graph.json."""
    graph_path = resolve_project_dir(project_id) / "_output" / "document_graph.json"
    graph_data = _load_json(graph_path)
    if not graph_data:
        return {}
    result: dict[str, int] = {}
    for p in graph_data.get("pages", []):
        sheet_no = p.get("sheet_no")
        page_num = p.get("page")
        if sheet_no and page_num is not None:
            result[str(sheet_no)] = page_num
    return result


def get_optimization_block_map(project_id: str) -> Optional[dict]:
    """Маппинг optimization_id → [block_ids] через document_graph и page."""
    import re

    opt_path = resolve_project_dir(project_id) / "_output" / "optimization.json"
    opt_data = _load_json(opt_path)
    if opt_data is None:
        return None

    blocks_by_page, block_info, all_block_ids = _load_blocks_data(project_id)
    sheet_to_page = _load_sheet_to_page_map(project_id)

    block_id_re = re.compile(r'\b([A-Z0-9]{3,5}-[A-Z0-9]{3,5}-[A-Z0-9]{2,4})\b')

    items = opt_data.get("items", [])
    result: dict[str, list[str]] = {}

    for item in items:
        oid = item.get("id", "")
        if not oid:
            continue

        matched_blocks: list[str] = []
        seen: set[str] = set()

        # 1. Явные block_id в текстовых полях
        for field in ("current", "proposed", "risks"):
            text = item.get(field, "")
            for m in block_id_re.finditer(text):
                bid = m.group(1)
                if bid in all_block_ids and bid not in seen:
                    matched_blocks.append(bid)
                    seen.add(bid)

        # 2. По полю page (если есть — новый формат)
        page_val = item.get("page")
        if page_val is not None:
            pages_list = page_val if isinstance(page_val, list) else [page_val]
            for pg in pages_list:
                if isinstance(pg, int):
                    for bid in blocks_by_page.get(pg, []):
                        if bid not in seen:
                            matched_blocks.append(bid)
                            seen.add(bid)

        # 3. По листам из section → конвертируем через document_graph
        if not matched_blocks:
            section = item.get("section", "")
            sheet_nums = _parse_pages_from_text(section)
            for sn in sorted(sheet_nums):
                # Попробовать sheet_no как ключ в маппинге
                pdf_page = sheet_to_page.get(str(sn))
                if pdf_page is not None:
                    for bid in blocks_by_page.get(pdf_page, []):
                        if bid not in seen:
                            matched_blocks.append(bid)
                            seen.add(bid)
                else:
                    # Fallback: прямое совпадение (лист = страница PDF)
                    for bid in blocks_by_page.get(sn, []):
                        if bid not in seen:
                            matched_blocks.append(bid)
                            seen.add(bid)

        if matched_blocks:
            result[oid] = matched_blocks

    return {
        "project_id": project_id,
        "block_map": result,
        "block_info": block_info,
    }


def _load_json(path: Path) -> Optional[dict]:
    """Безопасное чтение JSON."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return None
