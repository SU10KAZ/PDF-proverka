"""
Сервис для работы с замечаниями аудита.
Чтение, фильтрация, сводка из 03_findings.json.
"""
import json
import re
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
    main = output_dir / "03_findings.json"
    if main.exists():
        return main
    return output_dir / "03_findings_pre_merge.json"


def _practicality_score(finding: dict) -> int:
    quality = finding.get("quality")
    if isinstance(quality, dict):
        return int(quality.get("practicality_score", 50))
    return 50


def get_findings(
    project_id: str,
    severity: Optional[str] = None,
    category: Optional[str] = None,
    sheet: Optional[str] = None,
    search: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    group: bool = False,
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
    filtered.sort(
        key=lambda f: (
            sev_order.get(f.get("severity", ""), 99),
            -_practicality_score(f),
        )
    )

    # Группировка похожих (до пагинации)
    if group:
        filtered = group_similar_findings(filtered)

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
        if not path.exists():
            path = entry / "_output" / "03_findings_pre_merge.json"
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

    # ── Текстовые evidence из document_graph ──
    text_evidence = _build_text_evidence(project_id, items)

    return {
        "project_id": project_id,
        "block_map": result,
        "block_info": block_info,
        "text_evidence": text_evidence,
    }


def _escape_with_markdown(text: str) -> str:
    """Экранирует HTML, но сохраняет markdown **bold** → <strong>."""
    import html as html_mod
    # Разбиваем на фрагменты: **bold** и обычный текст
    parts = re.split(r'(\*\*[^*]+\*\*)', text)
    result = []
    for part in parts:
        if part.startswith("**") and part.endswith("**") and len(part) > 4:
            inner = html_mod.escape(part[2:-2])
            result.append(f"<strong>{inner}</strong>")
        else:
            result.append(html_mod.escape(part))
    return "".join(result)


def _clean_latex(text: str) -> str:
    """Конвертирует LaTeX-разметку из OCR в читаемый plain text."""
    if not text or '\\' not in text:
        return text
    # \text{ кг/м} → кг/м
    text = re.sub(r'\\text\s*\{([^}]*)\}', r'\1', text)
    # ^{...} → (...)  |  ^3 → ³  |  ^2 → ²
    text = text.replace('^3', '³').replace('^2', '²')
    text = re.sub(r'\^\{([^}]*)\}', r'\1', text)
    # \frac{a}{b} → a/b
    text = re.sub(r'\\frac\s*\{([^}]*)\}\s*\{([^}]*)\}', r'\1/\2', text)
    # Символы: \cdot → ·, \times → ×, \leq → ≤, \geq → ≥, \pm → ±, \degree → °
    _latex_symbols = {
        '\\cdot': '·', '\\times': '×', '\\leq': '≤', '\\geq': '≥',
        '\\pm': '±', '\\degree': '°', '\\infty': '∞', '\\approx': '≈',
        '\\neq': '≠', '\\sim': '~', '\\sqrt': '√',
    }
    for cmd, char in _latex_symbols.items():
        text = text.replace(cmd, char)
    # Оставшиеся \command → убрать бэкслеш
    text = re.sub(r'\\([a-zA-Z]+)', r'\1', text)
    return text


def _text_to_html(raw: str) -> str:
    """Конвертирует raw текст из document_graph в HTML.

    Формат данных из OCR (Chandra):
    - Табличные ячейки: каждая на отдельной строке, заканчивается \\t
    - Строки таблицы разделены пустыми строками
    - Заголовки столбцов: обычные строки без \\t перед первым табличным рядом
    - Обычный текст: строки без \\t
    - **bold** → <strong>
    - LaTeX-разметка (\text{}, ^3 и др.) → читаемый текст
    """
    import html as html_mod
    raw = _clean_latex(raw)
    lines = raw.split("\n")

    # Нормализация: strip пробелы, объединить строки-только-\t с последней непустой строкой
    normalized: list[str] = []
    for ln in lines:
        s = ln.strip(" ")
        if s.strip("\t ") == "" and "\t" in s:
            # Строка содержит только \t (и пробелы) — присоединяем к последней непустой строке
            for j in range(len(normalized) - 1, -1, -1):
                if normalized[j].strip():
                    normalized[j] = normalized[j].rstrip() + "\t"
                    break
        elif s.strip() == "":
            normalized.append("")
        else:
            normalized.append(s.strip())
    lines = normalized

    # Trim
    while lines and not lines[-1].strip():
        lines.pop()
    while lines and not lines[0].strip():
        lines.pop(0)
    if not lines:
        return ""

    # Определяем наличие табличных строк (содержат \t)
    tab_line_indices = [i for i, ln in enumerate(lines) if "\t" in ln]
    non_empty = [i for i, ln in enumerate(lines) if ln.strip()]
    has_table = len(tab_line_indices) > 0 and len(tab_line_indices) / max(len(non_empty), 1) > 0.2

    if has_table:
        return _render_table_block(lines, tab_line_indices, html_mod)
    else:
        return _render_text_block(lines, html_mod)


def _render_table_block(lines: list[str], tab_line_indices: list[int], html_mod) -> str:
    """Рендерит блок с табличными данными.

    Поддерживает два формата OCR:
    - Формат A: каждая строка = полный ряд с \t между столбцами
      (1.1\tКладовая\t57,3\tВ1)
    - Формат B: каждая ячейка на своей строке, заканчивается \t,
      ряды разделены пустыми строками
    """
    parts: list[str] = []
    first_tab = tab_line_indices[0]

    # --- Заголовок (строки до первого таба) ---
    pre_lines = [ln.strip() for ln in lines[:first_tab] if ln.strip()]

    # --- Определяем формат: A (multi-tab per line) vs B (single-tab per line) ---
    tab_counts = []
    for i in tab_line_indices:
        ln = lines[i]
        tab_counts.append(ln.count("\t"))
    avg_tabs = sum(tab_counts) / max(len(tab_counts), 1)

    # Формат A: среднее кол-во табов на строку > 1.5
    is_format_a = avg_tabs > 1.5

    if is_format_a:
        return _render_table_format_a(lines, first_tab, tab_line_indices, pre_lines, parts)
    else:
        return _render_table_format_b(lines, first_tab, tab_line_indices, pre_lines, parts)


def _render_table_format_a(lines, first_tab, tab_line_indices, pre_lines, parts):
    """Формат A: каждая строка — полный ряд таблицы с \t-разделителями."""

    # Заголовок
    if pre_lines:
        parts.append("<div class='te-header'>" + "<br>".join(
            _escape_with_markdown(ln) for ln in pre_lines
        ) + "</div>")

    # Собираем ряды таблицы
    table_rows: list[list[str]] = []
    tail_start = len(lines)

    for i in range(first_tab, len(lines)):
        ln = lines[i]
        if "\t" in ln and ln.strip():
            cells = [c.strip() for c in ln.split("\t")]
            # Убираем пустые trailing ячейки
            while cells and not cells[-1]:
                cells.pop()
            if cells:
                table_rows.append(cells)
            tail_start = i + 1
        elif ln.strip() and not table_rows:
            # Обычная строка до таблицы — добавляем к заголовку
            continue
        elif ln.strip():
            # Обычная строка после таблицы — начало хвоста
            tail_start = i
            break

    # Убираем полностью пустые столбцы
    if table_rows:
        max_cols = max(len(r) for r in table_rows)
        # Определяем непустые столбцы
        non_empty_cols = []
        for ci in range(max_cols):
            has_data = any(ci < len(r) and r[ci] for r in table_rows)
            if has_data:
                non_empty_cols.append(ci)

        thtml = "<table class='te-table'>"
        for row in table_rows:
            thtml += "<tr>"
            for ci in non_empty_cols:
                cell = _escape_with_markdown(row[ci]) if ci < len(row) else ""
                thtml += f"<td>{cell}</td>"
            thtml += "</tr>"
        thtml += "</table>"
        parts.append(thtml)

    # Хвост
    tail = [ln.strip() for ln in lines[tail_start:] if ln.strip()]
    if tail:
        parts.append("<div class='te-note'>" + "<br>".join(
            _escape_with_markdown(ln) for ln in tail
        ) + "</div>")

    return "\n".join(parts)


def _render_table_format_b(lines, first_tab, tab_line_indices, pre_lines, parts):
    """Формат B: каждая ячейка на отдельной строке, заканчивается \t."""

    # Собираем ВСЕ ряды сначала, чтобы определить кол-во столбцов по моде
    all_row_sizes: list[int] = []
    current_size = 0
    for i in range(first_tab, len(lines)):
        ln = lines[i]
        if "\t" in ln and ln.strip():
            current_size += 1
        elif not ln.strip():
            if current_size > 0:
                all_row_sizes.append(current_size)
                current_size = 0
    if current_size > 0:
        all_row_sizes.append(current_size)

    # Определяем кол-во столбцов по самому частому размеру ряда (моде)
    if all_row_sizes:
        from collections import Counter
        size_counts = Counter(all_row_sizes)
        num_cols = size_counts.most_common(1)[0][0]
    else:
        num_cols = 1

    # Разделяем pre_lines на заголовок и названия столбцов
    title_lines: list[str] = []
    col_headers: list[str] = []

    # Если num_cols == 1 но pre_lines содержит несколько коротких строк — это заголовки столбцов
    # Переопределяем num_cols по количеству заголовков
    if num_cols == 1 and pre_lines and len(pre_lines) >= 3:
        # Ищем паттерн: первые строки = название, последние N = заголовки столбцов
        # Эвристика: берём максимальное N с title, где заголовки короткие
        total_cells = sum(all_row_sizes)
        best_cols = None
        best_title = None
        best_headers = None
        for n_headers in range(min(len(pre_lines), 8), 1, -1):
            candidate_headers = pre_lines[-n_headers:]
            candidate_title = pre_lines[:-n_headers]
            if any(len(h) > 40 for h in candidate_headers):
                continue
            if not candidate_title:
                continue  # Нужен хотя бы заголовок таблицы
            # Берём первый подходящий (максимальный n_headers с title)
            best_cols = n_headers
            best_title = candidate_title
            best_headers = candidate_headers
            break
        if best_cols and best_cols >= 2:
            num_cols = best_cols
            col_headers = best_headers
            title_lines = best_title
        else:
            title_lines = pre_lines
    elif pre_lines and num_cols > 1 and len(pre_lines) >= num_cols:
        col_headers = pre_lines[-num_cols:]
        title_lines = pre_lines[:-num_cols]
    elif pre_lines:
        title_lines = pre_lines

    if title_lines:
        parts.append("<div class='te-header'>" + "<br>".join(
            _escape_with_markdown(ln) for ln in title_lines
        ) + "</div>")

    # Группируем ячейки по рядам
    # Non-tab строки внутри ряда = продолжение предыдущей ячейки (описание)
    table_rows: list[list[str]] = []
    current_row: list[str] = []
    tail_start = len(lines)

    i = first_tab
    in_table = True
    while i < len(lines) and in_table:
        ln = lines[i]
        if "\t" in ln and ln.strip():
            current_row.append(ln.strip().strip("\t").strip())
        elif not ln.strip():
            if current_row:
                table_rows.append(current_row)
                current_row = []
            has_more_tabs = any(j > i for j in tab_line_indices)
            if not has_more_tabs:
                tail_start = i + 1
                in_table = False
        elif ln.strip():
            # Non-tab строка — продолжение предыдущей ячейки
            # (описание без \t) ИЛИ конец таблицы
            has_more_tabs = any(j > i for j in tab_line_indices)
            if has_more_tabs and current_row:
                # Дописываем к предыдущей ячейке
                current_row[-1] += " " + ln.strip()
            elif has_more_tabs and not current_row:
                # Начало нового ряда без таба — первая ячейка
                current_row.append(ln.strip())
            else:
                if current_row:
                    table_rows.append(current_row)
                    current_row = []
                tail_start = i
                in_table = False
        i += 1

    if current_row:
        table_rows.append(current_row)
        tail_start = i

    # Перегруппировка: если все/почти все ряды одноячеечные — группировать по num_cols
    if table_rows and num_cols > 1:
        single_count = sum(1 for r in table_rows if len(r) == 1)
        if single_count > len(table_rows) * 0.7:
            flat = []
            for r in table_rows:
                flat.extend(r)

            # Проверим — нужен ли дополнительный столбец (безымянный, для кодов типа A.0)
            # Эвристика: первая ячейка при num_cols+1 группировке — короткий код (< 8 символов)
            if col_headers and len(flat) >= num_cols + 1:
                nc_plus = num_cols + 1
                # Проверяем первые 3 ряда при nc_plus: первая ячейка каждого ряда должна быть короткой
                sample_firsts = [flat[i] for i in range(0, min(len(flat), nc_plus * 3), nc_plus)]
                avg_first_len = sum(len(c) for c in sample_firsts) / max(len(sample_firsts), 1)
                if avg_first_len < 10:
                    col_headers = [""] + col_headers
                    num_cols = nc_plus

            table_rows = [flat[i:i+num_cols] for i in range(0, len(flat), num_cols)]

    # Рендер
    if table_rows:
        max_cols = max(len(r) for r in table_rows)
        thtml = "<table class='te-table'>"
        if col_headers and len(col_headers) == max_cols:
            thtml += "<tr>"
            for h in col_headers:
                thtml += f"<th>{_escape_with_markdown(h)}</th>"
            thtml += "</tr>"
        for row in table_rows:
            thtml += "<tr>"
            for ci in range(max_cols):
                cell = _escape_with_markdown(row[ci]) if ci < len(row) else ""
                thtml += f"<td>{cell}</td>"
            thtml += "</tr>"
        thtml += "</table>"
        parts.append(thtml)

    tail = [ln.strip() for ln in lines[tail_start:] if ln.strip()]
    if tail:
        parts.append("<div class='te-note'>" + "<br>".join(
            _escape_with_markdown(ln) for ln in tail
        ) + "</div>")

    return "\n".join(parts)


def _render_text_block(lines: list[str], html_mod) -> str:
    """Рендерит обычный текстовый блок в HTML параграфы."""
    paragraphs: list[str] = []
    current: list[str] = []
    for ln in lines:
        stripped = ln.strip()
        if stripped:
            current.append(_escape_with_markdown(stripped))
        else:
            if current:
                paragraphs.append("<br>".join(current))
                current = []
    if current:
        paragraphs.append("<br>".join(current))

    if len(paragraphs) == 1:
        return f"<p>{paragraphs[0]}</p>"
    return "\n".join(f"<p>{p}</p>" for p in paragraphs)


def _build_ocr_html_index(project_dir: Path) -> dict[str, str]:
    """Индекс block_id → HTML-контент из OCR HTML файла.

    OCR HTML содержит готовые таблицы и текст с правильным форматированием.
    Каждый блок начинается с <p>BLOCK: XXXX-XXXX-XXX</p> внутри div.block-content.
    """
    # Ищем *_ocr.html в папке проекта
    ocr_files = list(project_dir.glob("*_ocr.html"))
    if not ocr_files:
        return {}

    ocr_path = ocr_files[0]
    try:
        html_content = ocr_path.read_text(encoding="utf-8")
    except OSError:
        return {}

    index: dict[str, str] = {}

    # Разбиваем на блоки по div.block
    block_pattern = re.compile(
        r'<div\s+class="block[^"]*">\s*'
        r'<div\s+class="block-header">[^<]*</div>\s*'
        r'<div\s+class="block-content">\s*'
        r'(.*?)'
        r'</div>\s*</div>',
        re.DOTALL
    )

    for match in block_pattern.finditer(html_content):
        content = match.group(1)
        # Ищем BLOCK: ID
        block_id_match = re.search(r'<p>BLOCK:\s*([A-Z0-9]+-[A-Z0-9]+-[A-Z0-9]+)</p>', content)
        if not block_id_match:
            continue
        block_id = block_id_match.group(1)

        # Убираем служебные строки (BLOCK: ..., Created: ..., stamp-info)
        cleaned = content
        # Удаляем <p>BLOCK: ...</p>
        cleaned = re.sub(r'<p>BLOCK:\s*[^<]+</p>\s*', '', cleaned)
        # Удаляем <p><b>Created:</b>...</p>
        cleaned = re.sub(r'<p><b>Created:</b>[^<]*</p>\s*', '', cleaned)
        # Удаляем stamp-info div
        cleaned = re.sub(r'<div\s+class="stamp-info[^"]*">[^<]*(?:<[^>]+>[^<]*)*</div>\s*', '', cleaned)
        # Удаляем лишние пустые теги
        cleaned = re.sub(r'<p>\s*</p>', '', cleaned)

        cleaned = cleaned.strip()
        if cleaned:
            index[block_id] = cleaned

    return index


def _build_text_evidence(project_id: str, findings: list[dict]) -> dict[str, list[dict]]:
    """Маппинг finding_id → [{text_block_id, role, text, page}] из document_graph."""
    output_dir = resolve_project_dir(project_id) / "_output"
    project_dir = resolve_project_dir(project_id)
    graph_path = output_dir / "document_graph.json"
    if not graph_path.exists():
        return {}

    try:
        graph = json.loads(graph_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

    # Пробуем загрузить готовый HTML из OCR файла (приоритет)
    ocr_index = _build_ocr_html_index(project_dir)

    # Индекс text_block_id → {text, html, page}
    text_index: dict[str, dict] = {}
    for page_data in graph.get("pages", []):
        page_num = page_data.get("page", 0)
        for tb in page_data.get("text_blocks", []):
            tb_id = tb.get("id", "")
            if tb_id:
                raw = (tb.get("text") or "")[:2000]
                # Приоритет: OCR HTML → fallback на _text_to_html
                html = ocr_index.get(tb_id) or _text_to_html(raw)
                text_index[tb_id] = {
                    "text": raw[:500],
                    "html": html,
                    "page": page_num,
                }

    result: dict[str, list[dict]] = {}
    for f in findings:
        fid = f.get("id", "")
        if not fid:
            continue

        text_refs: list[dict] = []
        seen: set[str] = set()

        # 1. evidence_text_refs (приоритет — точная трассировка с ролями)
        etr = f.get("evidence_text_refs")
        if etr and isinstance(etr, list):
            for ref in etr:
                tb_id = ref.get("text_block_id", "")
                if tb_id and tb_id in text_index and tb_id not in seen:
                    seen.add(tb_id)
                    info = text_index[tb_id]
                    text_refs.append({
                        "text_block_id": tb_id,
                        "role": ref.get("role", ""),
                        "used_for": ref.get("used_for", ""),
                        "text": info["text"],
                        "html": info["html"],
                        "page": info["page"],
                    })

        # 2. evidence[type=text] (fallback)
        ev = f.get("evidence")
        if ev and isinstance(ev, list):
            for e in ev:
                if e.get("type") == "text":
                    tb_id = e.get("block_id", "")
                    if tb_id and tb_id in text_index and tb_id not in seen:
                        seen.add(tb_id)
                        info = text_index[tb_id]
                        text_refs.append({
                            "text_block_id": tb_id,
                            "role": "",
                            "used_for": "",
                            "text": info["text"],
                            "html": info["html"],
                            "page": info["page"],
                        })

        # 3. source_block_ids (last fallback — могут быть текстовые)
        sids = f.get("source_block_ids")
        if sids and isinstance(sids, list):
            for tb_id in sids:
                if tb_id and tb_id in text_index and tb_id not in seen:
                    seen.add(tb_id)
                    info = text_index[tb_id]
                    text_refs.append({
                        "text_block_id": tb_id,
                        "role": "",
                        "used_for": "",
                        "text": info["text"],
                        "html": info["html"],
                        "page": info["page"],
                    })

        if text_refs:
            result[fid] = text_refs

    return result


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


def _normalize_problem_pattern(text: str) -> str:
    """Нормализует текст замечания для группировки: убирает конкретные числа, марки, номера."""
    if not text:
        return ""
    s = text.strip()
    # Марки элементов (В4-13, А4-14, ДВ12-5П, ОГ-8, К6.2 и т.д.)
    s = re.sub(r'[А-ЯA-Z]{1,4}\d[\w.*-]*', '_MARK_', s)
    # Названия материалов в кавычках
    s = re.sub(r'[«"].*?[»"]', '_MAT_', s)
    # Числа с единицами (площади, длины, массы и т.д.)
    s = re.sub(r'\d+[\s,.]?\d*\s*(?:м[²³]|мм|кг(?:/м)?|шт\.?|%)', '_NUM_', s)
    # Номера помещений, листов, страниц
    s = re.sub(r'(?:пом(?:ещени[еяй])?|комнат[аыу]|зон[аыу]?)[\s.]*(?:№?\s*)?\d+\w*', '_ROOM_', s, flags=re.IGNORECASE)
    s = re.sub(r'(?:лист(?:ы|а|ов)?)\s*\d+[\s,и\d]*', '_SHEET_', s, flags=re.IGNORECASE)
    s = re.sub(r'(?:стр\.?\s*(?:PDF\s*)?)\d+', '_PAGE_', s, flags=re.IGNORECASE)
    # Содержимое скобок (детали конкретного экземпляра)
    s = re.sub(r'\([^)]*\)', '(...)', s)
    # Оставшиеся числа (id, коэффициенты, проценты)
    s = re.sub(r'\b\d+[.,]?\d*\b', '_N_', s)
    # Нормализовать пробелы
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def group_similar_findings(findings: list[dict]) -> list[dict]:
    """Группирует похожие замечания по нормализованному паттерну problem + severity + category.

    Возвращает список, где каждый элемент — либо одиночное замечание (без изменений),
    либо объединённая группа с полем `_group`.
    """
    from collections import OrderedDict

    groups: OrderedDict[str, list[dict]] = OrderedDict()

    for f in findings:
        problem = f.get("problem") or f.get("description") or f.get("finding") or ""
        severity = f.get("severity", "")
        category = f.get("category", "")

        pattern = _normalize_problem_pattern(problem)
        key = f"{severity}||{category}||{pattern}"

        if key not in groups:
            groups[key] = []
        groups[key].append(f)

    result = []
    for key, items in groups.items():
        if len(items) == 1:
            result.append(items[0])
        else:
            # Собрать объединённую группу
            # Лидер — первый по порядку
            leader = items[0]

            # Собрать все sheet/page
            all_sheets = []
            all_pages = []
            all_block_ids = []
            all_evidence = []
            for it in items:
                sh = it.get("sheet")
                if sh and sh not in all_sheets:
                    all_sheets.append(sh)
                pg = it.get("page")
                if pg:
                    if isinstance(pg, list):
                        all_pages.extend(pg)
                    elif pg not in all_pages:
                        all_pages.append(pg)
                for bid in (it.get("related_block_ids") or []):
                    if bid not in all_block_ids:
                        all_block_ids.append(bid)
                for ev in (it.get("evidence") or []):
                    all_evidence.append(ev)

            # Объединённое замечание
            merged = {
                **leader,
                "_group": {
                    "count": len(items),
                    "merged_ids": [it.get("id", "") for it in items],
                    "items": items,
                },
                "sheet": ", ".join(all_sheets) if all_sheets else leader.get("sheet", ""),
                "page": sorted(set(all_pages)) if all_pages else leader.get("page"),
                "related_block_ids": all_block_ids,
                "evidence": all_evidence,
            }
            result.append(merged)

    return result


def _load_json(path: Path) -> Optional[dict]:
    """Безопасное чтение JSON."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
        return None
