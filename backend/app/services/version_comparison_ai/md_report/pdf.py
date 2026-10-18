"""PDF отчёта из структурированного report_v1 (reportlab, A4 книжная) и проверка готового файла.

Генератор — reportlab: он уже стоит в окружении выпуска (``dependencies.freeze.txt``), умеет таблицы
с повтором шапки на новой странице и встраивает TTF-шрифт с кириллицей. Модель PDF не генерирует:
текст берётся из полей отчёта и экранируется, разметка источника в PDF не попадает.
Проверка — PyMuPDF: извлечение текста, встроенные шрифты, поля страниц, переходы и числа.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape

from . import schemas, stages

FONT_DIRS = ("/usr/share/fonts/truetype/dejavu", "/usr/share/fonts/dejavu")
FONT = "MdrSans"
FONT_BOLD = "MdrSans-Bold"
INK = "#1d2b3a"
MUTED = "#5d6b78"
ACCENT = "#8f2d2d"
RULE = "#c9d1d8"
FILL = "#eef2f5"
HEAD_FILL = "#1d2b3a"
MARGIN_MM = 16


def _fonts() -> None:
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    if FONT in pdfmetrics.getRegisteredFontNames():
        return
    for d in FONT_DIRS:
        reg, bold = Path(d) / "DejaVuSans.ttf", Path(d) / "DejaVuSans-Bold.ttf"
        if reg.exists() and bold.exists():
            pdfmetrics.registerFont(TTFont(FONT, str(reg)))
            pdfmetrics.registerFont(TTFont(FONT_BOLD, str(bold)))
            from reportlab.lib.fonts import addMapping
            addMapping(FONT, 0, 0, FONT)
            addMapping(FONT, 1, 0, FONT_BOLD)
            return
    raise RuntimeError("не найден шрифт DejaVu Sans с кириллицей (" + ", ".join(FONT_DIRS) + ")")


def _t(text: Any) -> str:
    return escape(re.sub(r"\s+", " ", str(text or "")).strip())


def _date(iso: str | None) -> str:
    if not iso:
        return ""
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return iso


def source_label(s: dict, own: str | None = None) -> str:
    """«V11→V12, л.5; стр. PDF 13 → 14; MD 3078–3085» — понятная ссылка без служебного синтаксиса.

    ``own`` — переход строки таблицы: для ссылок на него же переход не повторяется."""
    tr = s.get("transition") or "заголовок сводки"
    head = [] if own and tr == own else [tr]
    if s.get("sheet"):
        head.append(s["sheet"])
    parts = [", ".join(head)] if head else []
    pb, pa = s.get("pdf_before"), s.get("pdf_after")
    if pb or pa:
        parts.append("стр. PDF " + " → ".join(str(x) if x else "—" for x in (pb, pa)))
    a, b = s["md_lines"]
    parts.append(f"MD {a}" if a == b else f"MD {a}–{b}")
    return "; ".join(parts)


def _sources_line(sources: list[dict], limit: int = 3, own: str | None = None) -> str:
    shown = [source_label(s, own) for s in sources[:limit]]
    more = f" и ещё {len(sources) - limit}" if len(sources) > limit else ""
    return "; ".join(shown) + more


def _styles():
    from reportlab.lib.colors import HexColor
    from reportlab.lib.styles import ParagraphStyle
    base = ParagraphStyle("base", fontName=FONT, fontSize=10, leading=13.4, textColor=HexColor(INK))
    return {
        "kicker": ParagraphStyle("kicker", base, fontName=FONT_BOLD, fontSize=8, leading=10, textColor=HexColor(MUTED)),
        "h1": ParagraphStyle("h1", base, fontName=FONT_BOLD, fontSize=20, leading=24, spaceAfter=2),
        "sub": ParagraphStyle("sub", base, fontSize=9, leading=12, textColor=HexColor(MUTED), spaceAfter=8),
        "h2": ParagraphStyle("h2", base, fontName=FONT_BOLD, fontSize=12.5, leading=16, spaceBefore=10, spaceAfter=6),
        "body": base,
        "small": ParagraphStyle("small", base, fontSize=8, leading=10.4, textColor=HexColor(MUTED)),
        "cell": ParagraphStyle("cell", base, fontSize=9.2, leading=12),
        "cell_b": ParagraphStyle("cell_b", base, fontName=FONT_BOLD, fontSize=9.2, leading=12),
        "cell_muted": ParagraphStyle("cell_muted", base, fontSize=8.8, leading=11.6, textColor=HexColor(MUTED)),
        "head": ParagraphStyle("head", base, fontName=FONT_BOLD, fontSize=8.8, leading=11,
                               textColor=HexColor("#ffffff")),
        "tile_n": ParagraphStyle("tile_n", base, fontName=FONT_BOLD, fontSize=19, leading=22),
        "tile_l": ParagraphStyle("tile_l", base, fontSize=8.4, leading=10.4, textColor=HexColor(MUTED)),
        "card_t": ParagraphStyle("card_t", base, fontName=FONT_BOLD, fontSize=10.6, leading=13.6, spaceAfter=3),
        "warn": ParagraphStyle("warn", base, fontName=FONT_BOLD, fontSize=10, leading=13, textColor=HexColor(ACCENT)),
    }


def _scope_text(t: dict) -> tuple[str, str]:
    scope = schemas.normalize_scope(t["change_scope"]) or t["change_scope"]
    sig = schemas.normalize_significance(t["significance"]) or t["significance"]
    return scope, sig


def render(report: dict, out: Path, *, analysed_at: str, sha256: str = "", incomplete: list[str] | None = None) -> Path:
    """Сформировать PDF из report_v1. ``incomplete`` — пометка неполного результата (в отчёт выводится явно)."""
    _fonts()
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.platypus import (CondPageBreak, KeepTogether, LongTable, Paragraph, SimpleDocTemplate, Spacer,
                                    Table, TableStyle)
    st = _styles()
    code = report["project_code"]
    trs = report["transitions"]
    dist = stages.distribution(report)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp.pdf")
    footer_left = f"{code} | {report['input_filename']} | анализ {analysed_at}"

    def deco(canvas, doc):
        canvas.saveState()
        w, h = A4
        canvas.setStrokeColor(HexColor(RULE))
        canvas.setLineWidth(0.5)
        canvas.line(MARGIN_MM * mm, 12 * mm, w - MARGIN_MM * mm, 12 * mm)
        canvas.setFont(FONT, 7.5)
        canvas.setFillColor(HexColor(MUTED))
        text = footer_left
        while canvas.stringWidth(text, FONT, 7.5) > w - 2 * MARGIN_MM * mm - 70 and len(text) > 20:
            text = text[:-2]
        canvas.drawString(MARGIN_MM * mm, 8.5 * mm, text if text == footer_left else text + "…")
        canvas.drawRightString(w - MARGIN_MM * mm, 8.5 * mm, f"стр. {doc.page} из {doc._mdr_pages}")
        canvas.restoreState()

    def story() -> list:
        s: list = []
        width = A4[0] - 2 * MARGIN_MM * mm
        s.append(Paragraph(_t(f"{code} / короткий отчёт").upper(), st["kicker"]))
        s.append(Paragraph("Как менялась документация", st["h1"]))
        first, last = trs[0], trs[-1]
        span = " – ".join(x for x in (_date(first.get("from_date")), _date(last.get("to_date"))) if x)
        s.append(Paragraph(_t(f"Выдачи {first['from_version']} – {last['to_version']}" + (f" · {span}" if span else "")
                              + " · оценка последствий для закупки и монтажа · по MD-сводке, исходные PDF не проверялись"),
                           st["sub"]))
        if incomplete:
            s.append(Paragraph(_t("НЕПОЛНЫЙ РЕЗУЛЬТАТ: " + "; ".join(incomplete)), st["warn"]))
            s.append(Spacer(1, 4))
        tiles = [(dist["technical"], "с техническими изменениями"),
                 (dist["no_content_change"], "без изменений содержания"),
                 (dist["formatting_only"], "только оформление")]
        if dist["unknown"]:
            tiles.append((dist["unknown"], "оценить не удалось"))
        cells = [[Paragraph(str(n), st["tile_n"]), Paragraph(_t(lbl), st["tile_l"])] for n, lbl in tiles]
        tile_w = width / len(tiles)
        tt = Table([[Table([[c[0]], [c[1]]], colWidths=[tile_w - 10]) for c in cells]],
                   colWidths=[tile_w] * len(tiles))
        tt.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), HexColor(FILL)),
                                ("LINEBEFORE", (1, 0), (-1, -1), 2, HexColor("#ffffff")),
                                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4)]))
        s.append(tt)
        s.append(Paragraph(_t(f"Переходов между выдачами: {dist['total']}. Масштаб оценён качественно по содержанию "
                              "сводки, а не по числу листов или пунктов."), st["small"]))
        s.append(Spacer(1, 6))
        s.append(Paragraph(_t(" ".join(report["summary"])), st["body"]))
        if report.get("summary_sources"):
            s.append(Spacer(1, 2))
            s.append(Paragraph(_t("Основание: " + _sources_line(report["summary_sources"])), st["small"]))
        s.append(Paragraph("Оценка по каждой паре версий", st["h2"]))
        rows = [[Paragraph("Переход", st["head"]), Paragraph("Масштаб и значимость", st["head"]),
                 Paragraph("Изменения и последствия для работ по предыдущей выдаче", st["head"])]]
        for t in trs:
            scope, sig = _scope_text(t)
            label = f"{t['from_version']}→{t['to_version']}"
            dates = "<br/>".join(_t(x) for x in (_date(t.get("from_date")), _date(t.get("to_date"))) if x)
            left = [Paragraph(_t(label), st["cell_b"])]
            if dates:
                left.append(Paragraph(dates, st["small"]))
            mid = [Paragraph(_t(scope), st["cell_b"])]
            if sig != "Нет новых технических изменений":
                mid.append(Paragraph(_t(f"Значимость: {sig.lower()}"), st["cell_muted"]))
            right = [Paragraph(_t(t["main_changes"]), st["cell"]),
                     Paragraph(_t(t["construction_impact"]), st["cell_muted"])]
            if t.get("sources"):
                right.append(Paragraph(_t("Источник: " + _sources_line(t["sources"], 2, own=label)), st["small"]))
            rows.append([left, mid, right])
        # Колонки не уже самого длинного слова: слово не переносится по буквам («Существенны|е»).
        from reportlab.pdfbase.pdfmetrics import stringWidth

        def word_w(texts: list[str], size: float) -> float:
            return max((stringWidth(w, FONT_BOLD, size) for x in texts for w in x.split()), default=0)
        pad = 12
        c1 = max(21 * mm, word_w([f"{t['from_version']}→{t['to_version']}" for t in trs], 9.2) + pad)
        c2 = max(31 * mm, word_w([_scope_text(t)[0] for t in trs], 9.2) + pad)
        table = LongTable(rows, colWidths=[c1, c2, width - c1 - c2], repeatRows=1)
        style = [("BACKGROUND", (0, 0), (-1, 0), HexColor(HEAD_FILL)), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                 ("LINEBELOW", (0, 1), (-1, -1), 0.4, HexColor(RULE)),
                 ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                 ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5)]
        for k in range(1, len(rows)):
            if k % 2 == 0:
                style.append(("BACKGROUND", (0, k), (-1, k), HexColor("#f7f9fa")))
        table.setStyle(TableStyle(style))
        s.append(table)
        sig_list = report.get("significant_changes") or []
        s.append(CondPageBreak(60 * mm))
        s.append(Paragraph("Что выделить отдельно", st["h2"]))
        if sig_list:
            s.append(Paragraph(_t("Наиболее значимые связанные изменения для согласования и проверки. Приоритет "
                                  "означает внимание, а не доказанное нарушение."), st["small"]))
            s.append(Spacer(1, 4))
            for k, c in enumerate(sig_list, 1):
                body = [Paragraph(f"<font color='{ACCENT}'>{k:02d}</font>&nbsp;&nbsp;{_t(c['title'])}"
                                  f" <font size='8' color='{MUTED}'>({_t(c['priority'])}, {_t(c['version_range'])})</font>",
                                  st["card_t"]),
                        Paragraph(f"<font name='{FONT_BOLD}'>Было → стало.</font> {_t(c['before_after'])}", st["cell"]),
                        Spacer(1, 2),
                        Paragraph(f"<font name='{FONT_BOLD}'>Последствие.</font> {_t(c['consequence'])}", st["cell"]),
                        Spacer(1, 2),
                        Paragraph(_t("Источник: " + _sources_line(c.get("sources") or [], 3)), st["small"])]
                card = Table([[body]], colWidths=[width])
                card.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), HexColor(FILL)),
                                          ("LINEBEFORE", (0, 0), (0, -1), 2.2, HexColor(ACCENT)),
                                          ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                                          ("TOPPADDING", (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6)]))
                s.append(KeepTogether([card, Spacer(1, 5)]))
        else:
            s.append(Paragraph(_t("Серьёзных изменений, требующих отдельного внимания, по проверенным результатам "
                                  "сводки не выявлено."), st["body"]))
        s.append(Paragraph("Основание и границы вывода", st["h2"]))
        for line in report.get("limitations") or []:
            s.append(Paragraph("• " + _t(line), st["small"]))
        basis = report.get("analysis_basis") or {}
        checked = "Исходные PDF проверялись." if basis.get("source_pdf_checked") else \
            "Исходные PDF не проверялись: номера листов и страниц PDF приведены по заголовкам сводки."
        s.append(Spacer(1, 3))
        s.append(Paragraph(_t(f"Источник: {report['input_filename']}" + (f" (SHA-256 {sha256[:16]}…)" if sha256 else "")
                              + f". {checked} Анализ выполнен {analysed_at}."), st["small"]))
        return s

    def build(pages: int) -> int:
        doc = SimpleDocTemplate(str(tmp), pagesize=A4, leftMargin=MARGIN_MM * mm, rightMargin=MARGIN_MM * mm,
                                topMargin=14 * mm, bottomMargin=18 * mm, title=f"{code} — оценка изменений версий",
                                author="AuditManager", subject="Анализ сводки изменений версий")
        doc._mdr_pages = pages
        doc.build(story(), onFirstPage=deco, onLaterPages=deco)
        return doc.page
    pages = build(0)
    if build(pages) != pages:      # число страниц не меняется от подстановки «из N», но проверяем
        build(build(pages))
    tmp.replace(out)
    return out


# ------------------------------------------------------------------ проверка готового PDF

def _squash(text: str) -> str:
    return re.sub(r"\s+", "", text or "")


def check(pdf_path: Path, report: dict, preview_dir: Path | None = None, *,
          forbidden: tuple[str, ...] = ()) -> dict[str, Any]:
    """Открыть PDF и проверить: страницы A4, встроенный шрифт, текст в полях, все переходы, числа, служебное."""
    import fitz
    problems, warnings = [], []
    doc = fitz.open(str(pdf_path))
    info: dict[str, Any] = {"pages": doc.page_count, "fonts": [], "words": 0}
    text_all = ""
    fonts: set[tuple[str, str]] = set()
    for i, page in enumerate(doc):
        w, h = page.rect.width, page.rect.height
        if abs(w - 595.3) > 2 or abs(h - 841.9) > 2:
            problems.append(f"стр. {i + 1}: не A4 книжная ({w:.0f}×{h:.0f})")
        text = page.get_text()
        if len(text.strip()) < 40:
            problems.append(f"стр. {i + 1}: почти пустая")
        text_all += text + "\n"
        for f in page.get_fonts(full=True):
            fonts.add((f[3], f[1]))
        for b in page.get_text("dict")["blocks"]:
            for ln in b.get("lines", []):
                for sp in ln.get("spans", []):
                    if not sp["text"].strip():
                        continue
                    x0, y0, x1, y1 = sp["bbox"]
                    if x0 < 20 or x1 > w - 20 or y0 < 15 or y1 > h - 15:
                        problems.append(f"стр. {i + 1}: текст за полями «{sp['text'][:40]}»")
                    if "DejaVu" not in sp["font"] and "MdrSans" not in sp["font"]:
                        problems.append(f"стр. {i + 1}: текст набран не встроенным шрифтом ({sp['font']})")
        if preview_dir is not None:
            preview_dir.mkdir(parents=True, exist_ok=True)
            page.get_pixmap(dpi=90).save(str(preview_dir / f"page-{i + 1}.png"))
    info["fonts"] = sorted({f"{n} ({ext})" for n, ext in fonts})
    used = [n for n, ext in fonts if "DejaVu" in n or "MdrSans" in n]
    if not used or any(ext == "n/a" for n, ext in fonts if n in used):
        problems.append("шрифт с кириллицей не встроен")
    squashed = _squash(text_all)
    for t in report["transitions"]:
        label = f"{t['from_version']}→{t['to_version']}"
        if _squash(label) not in squashed:
            problems.append(f"в тексте PDF нет перехода {label}")
    for c in report.get("significant_changes") or []:
        if _squash(c["title"])[:40] not in squashed:
            problems.append(f"в тексте PDF нет серьёзного изменения «{c['title'][:60]}»")
        for n in stages.numbers(c["before_after"]):
            if n.replace(".", ",") not in squashed and n not in squashed:
                problems.append(f"число {n} из «{c['title'][:40]}» не найдено в тексте PDF")
    for marker in ("**", "<b>", "</", "```", "{\"", "md_from", "claim_id", "transition_id") + forbidden:
        if marker in text_all:
            problems.append(f"служебная разметка или идентификатор в тексте PDF: «{marker}»")
    claim = re.search(r"\bG\d{2}\.T\d{2}", text_all)
    if claim:
        problems.append(f"служебный идентификатор утверждения в тексте PDF: «{claim.group(0)}»")
    ids = [t.get("transition_id") for t in report["transitions"] if t.get("transition_id")]
    leaked = sorted({m.group(0) for m in re.finditer(r"\b(" + "|".join(ids) + r")\b", text_all)}) if ids else []
    if leaked:
        warnings.append("в тексте PDF встречаются обозначения, совпадающие со служебными: " + ", ".join(leaked))
    if report.get("analysis_basis", {}).get("source_pdf_checked") is False and \
            re.search(r"(?<!не )провер\w+ по PDF|PDF проверен", text_all):
        problems.append("текст PDF утверждает проверку исходных PDF, хотя source_pdf_checked = false")
    if "⁇" in text_all or "�" in text_all:
        warnings.append("в тексте PDF есть знак замены (возможна нечитаемая буква)")
    info["words"] = len(re.findall(r"\w+", text_all))
    if doc.page_count > max(3, 2 + len(report["transitions"]) // 8):
        warnings.append(f"страниц {doc.page_count} — больше ориентира")
    doc.close()
    return {"ok": not problems, "problems": problems, "warnings": warnings, **info}
