"""PDF отчёта из структурированного report_v2 (reportlab, A4 книжная) и проверка готового файла.

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


def _pages(xs: list[int]) -> str:
    return ", ".join(map(str, xs)) if xs else "—"


def source_label(f: dict) -> str:
    """Паспорт факта одной строкой: «V11→V12, л.5; стр. PDF 13 → 14; MD 3078»."""
    head = [f.get("transition") or "заголовок сводки"]
    if f.get("sheet"):
        head.append(f["sheet"])
    return f"{', '.join(head)}; стр. PDF {_pages(f.get('pages_a'))} → {_pages(f.get('pages_b'))}; MD {f['md_line']}"


def _sources_line(ids: list[str]) -> str:
    """Все ID фактов строки — без сокращения «и ещё N»: паспорт каждого — в приложении «Реестр источников»."""
    return ", ".join(ids)


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
    """Сформировать PDF из report_v2. ``incomplete`` — пометка неполного результата (в отчёт выводится явно)."""
    _fonts()
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.platypus import (CondPageBreak, KeepTogether, LongTable, PageBreak, Paragraph, SimpleDocTemplate,
                                    Spacer, Table, TableStyle)
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
        s.append(Paragraph(_t("Ссылки вида «Ф0123» — факты сводки; паспорт каждого (переход, лист, страницы PDF A → B, "
                              "строка MD, исходная запись, подтверждение) — в приложении «Реестр источников»."),
                           st["small"]))
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
            right = [Paragraph(_t(t["main_changes"]), st["cell"])]
            for ch in t.get("changes") or []:
                note = " (связь значений между версиями не подтверждена)" if ch.get("chain_unconfirmed") else ""
                right.append(Paragraph(f"• <font name='{FONT_BOLD}'>{_t(ch['object'])}</font> — {_t(ch['before'])} → "
                                       f"{_t(ch['after'])}{_t(note)} <font size='7.6' color='{MUTED}'>"
                                       f"[{_t(_sources_line(ch['sources']))}]</font>", st["cell"]))
            impact = _t(t["construction_impact"])
            if t.get("impact_sources"):
                impact += f" <font size='7.6'>[{_t(_sources_line(t['impact_sources']))}]</font>"
            right.append(Paragraph(impact, st["cell_muted"]))
            for mark in t.get("document_marks") or []:
                right.append(Paragraph(_t("Отметка документа: " + mark), st["small"]))
            comp = (t.get("completeness") or {}).get("text")
            if comp:
                right.append(Paragraph(_t("Полнота: " + comp), st["small"]))
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
                        Paragraph(f"<font name='{FONT_BOLD}'>Было → стало.</font> {_t(c['before_after'])}"
                                  + (_t(" (связь значений между версиями не подтверждена — изменения по отдельным "
                                        "фактам)") if c.get("chain_unconfirmed") else ""), st["cell"]),
                        Spacer(1, 2),
                        Paragraph(f"<font name='{FONT_BOLD}'>Последствие.</font> {_t(c['consequence'])}", st["cell"]),
                        Spacer(1, 2),
                        Paragraph(_t("Источники: " + _sources_line(c.get("sources") or [])), st["small"])]
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
        if report.get("reference_warnings"):
            s.append(Paragraph("• " + _t(f"Отброшено ссылок на факты, которых нет в реестре или которые относятся к "
                                         f"другому переходу: {len(report['reference_warnings'])}."), st["small"]))
        basis = report.get("analysis_basis") or {}
        checked = "Исходные PDF проверялись." if basis.get("source_pdf_checked") else \
            "Исходные PDF не проверялись: номера листов и страниц PDF приведены по заголовкам сводки."
        s.append(Spacer(1, 3))
        s.append(Paragraph(_t(f"Источник: {report['input_filename']}" + (f" (SHA-256 {sha256[:16]}…)" if sha256 else "")
                              + f". {checked} Анализ выполнен {analysed_at}."), st["small"]))
        s += _coverage_table(report, st, width)
        s += _registry_table(report, st, width)
        return s

    def _coverage_table(report: dict, st: dict, width: float) -> list:
        cov = report.get("coverage") or []
        if not cov:
            return []
        out = [CondPageBreak(50 * mm), Paragraph("Реестр покрытия", st["h2"])]
        if not any(c.get("registry") for c in cov):
            return out + [Paragraph(_t("Реестр покрытия в сводке не передан: полнота по набору обработанных листов "
                                       "не подтверждена."), st["small"])]
        out.append(Paragraph(_t("Каждый лист перехода — в одной категории (по сохранённым данным сравнения). Снятая "
                                "отметка документа — изменение отметки, не оборудования, и не доказательство "
                                "согласования."), st["small"]))
        out.append(Spacer(1, 3))
        cols = ["совпадает по движку", "только служебное", "описано ИИ", "СО целиком", "неоднозначно", "не обработано"]
        heads = ["Переход", "Листов", "Совп.", "Служ.", "ИИ", "СО", "Неодн.", "Не обр."]
        rows = [[Paragraph(h, st["head"]) for h in heads]]
        for c in cov:
            counts = c.get("counts") or {}
            rows.append([Paragraph(_t(c["label"]), st["cell_b"]), Paragraph(str(c.get("sheets_total") or "—"), st["cell"])]
                        + [Paragraph(str(counts.get(k, "—")), st["cell"]) for k in cols])
        w0 = 24 * mm
        rest = (width - w0) / 7
        tab = LongTable(rows, colWidths=[w0] + [rest] * 7, repeatRows=1)
        tab.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), HexColor(HEAD_FILL)),
                                 ("LINEBELOW", (0, 1), (-1, -1), 0.4, HexColor(RULE)),
                                 ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                 ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
        out.append(tab)
        for c in cov:
            for kind, items in (("не обработано", c.get("not_processed")), ("неоднозначно", c.get("ambiguous"))):
                for x in items or []:
                    out.append(Paragraph(_t(f"{c['label']}, {kind}: {x}"), st["small"]))
        return out

    def _registry_table(report: dict, st: dict, width: float) -> list:
        facts = report.get("facts") or []
        out = [PageBreak(), Paragraph("Приложение. Реестр источников", st["h2"]),
               Paragraph(_t(f"Все факты, на которые ссылается отчёт: {len(facts)} из {report.get('facts_total', len(facts))} "
                            "пунктов сводки. ID — порядковый номер пункта в исходном MD; строка MD — номер строки "
                            "исходного файла."), st["small"]), Spacer(1, 3)]
        if not facts:
            return out + [Paragraph(_t("Ссылок на факты нет."), st["small"])]
        rows = [[Paragraph(h, st["head"]) for h in ("ID", "Переход, лист, стр. PDF A → B", "MD", "Подтверждение",
                                                     "Пункт сводки")]]
        for f in facts:
            title = f.get("sheet_title") or ""
            where = f["transition"] + (f", {f['sheet']}" if f.get("sheet") else
                                       f", {title[:60]}{'…' if len(title) > 60 else ''}" if title else "")
            where += f"; стр. {_pages(f.get('pages_a'))} → {_pages(f.get('pages_b'))}"
            rec = f.get("record") or {}
            conf = f.get("confirmation") or ""
            if rec.get("change_id") or rec.get("entry_id"):
                conf += f" (запись {rec.get('change_id') or rec.get('entry_id')})"
            rows.append([Paragraph(_t(f["fact_id"]), st["cell_b"]), Paragraph(_t(where), st["cell_muted"]),
                         Paragraph(str(f["md_line"]), st["cell_muted"]), Paragraph(_t(conf), st["cell_muted"]),
                         Paragraph(_t(f.get("text")), st["cell_muted"])])
        c = [15 * mm, 40 * mm, 11 * mm, 34 * mm]
        tab = LongTable(rows, colWidths=c + [width - sum(c)], repeatRows=1)
        tab.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), HexColor(HEAD_FILL)),
                                 ("LINEBELOW", (0, 1), (-1, -1), 0.3, HexColor(RULE)),
                                 ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                 ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                                 ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3)]))
        return out + [tab]

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
    for f in report.get("facts") or []:
        if f["fact_id"] not in squashed:
            problems.append(f"в реестре источников PDF нет факта {f['fact_id']}")
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
    if doc.page_count > max(3, 2 + len(report["transitions"]) // 8) + 1 + len(report.get("facts") or []) // 25:
        warnings.append(f"страниц {doc.page_count} — больше ориентира")
    doc.close()
    return {"ok": not problems, "problems": problems, "warnings": warnings, **info}


# ------------------------------------------------------------------ отчёт для заказчика

CLIENT_KICKER = "#1f6f5c"


def render_client(view: dict, out: Path) -> Path:
    """PDF для заказчика из вида ``client.build``: одна хронологическая таблица; внизу — шифр и номер страницы.

    Источники, реестры и диагностика сюда не выводятся — они остаются в данных отчёта."""
    _fonts()
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import LongTable, Paragraph, SimpleDocTemplate, Spacer, TableStyle
    st = _styles()
    base = st["cell"]
    sty = {
        "kicker": ParagraphStyle("c_kicker", base, fontName=FONT_BOLD, fontSize=8.6, leading=11,
                                 textColor=HexColor(CLIENT_KICKER)),
        "h1": ParagraphStyle("c_h1", base, fontName=FONT_BOLD, fontSize=19, leading=23, spaceBefore=4, spaceAfter=6),
        "sub": ParagraphStyle("c_sub", base, fontSize=9.4, leading=12.4, textColor=HexColor(MUTED)),
        "head": ParagraphStyle("c_head", base, fontName=FONT_BOLD, fontSize=8.4, leading=10.6,
                               textColor=HexColor("#ffffff")),
        "tr": ParagraphStyle("c_tr", base, fontName=FONT_BOLD, fontSize=9.4, leading=12),
        "date": ParagraphStyle("c_date", base, fontSize=7.8, leading=10, textColor=HexColor(MUTED)),
        "scale": ParagraphStyle("c_scale", base, fontName=FONT_BOLD, fontSize=7.8, leading=10),
        "lead": ParagraphStyle("c_lead", base, fontName=FONT_BOLD, fontSize=9.4, leading=12.2, spaceAfter=3),
        "pt": ParagraphStyle("c_pt", base, fontSize=8.5, leading=10.9, spaceAfter=2),
        "cont": ParagraphStyle("c_cont", base, fontSize=7.4, leading=9.4, textColor=HexColor(MUTED)),
        "imp": ParagraphStyle("c_imp", base, fontSize=8.4, leading=10.8, spaceBefore=1,
                              textColor=HexColor("#33414e")),
    }
    code = view["code"]
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp.pdf")

    def deco(canvas, doc):
        canvas.saveState()
        w, _ = A4
        canvas.setStrokeColor(HexColor(RULE))
        canvas.setLineWidth(0.5)
        canvas.line(MARGIN_MM * mm, 12 * mm, w - MARGIN_MM * mm, 12 * mm)
        canvas.setFont(FONT, 7.5)
        canvas.setFillColor(HexColor(MUTED))
        canvas.drawString(MARGIN_MM * mm, 8.5 * mm, code)
        canvas.drawRightString(w - MARGIN_MM * mm, 8.5 * mm, f"стр. {doc.page} из {doc._mdr_pages}")
        canvas.restoreState()

    def story() -> list:
        width = A4[0] - 2 * MARGIN_MM * mm
        kicker = " / ".join(x for x in (view.get("object"), code) if x).upper()
        s: list = [Paragraph(_t(kicker), sty["kicker"]), Paragraph(_t(view["title"]), sty["h1"])]
        if view.get("section"):
            s.append(Paragraph(_t(view["section"]), sty["sub"]))
        s.append(Paragraph(_t(f"Выдачи {view['versions']} · {view['period']}"), sty["sub"]))
        s.append(Spacer(1, 5 * mm))
        rows = [[Paragraph("Переход и даты выдач", sty["head"]), Paragraph("Масштаб изменений", sty["head"]),
                 Paragraph("Изменения и последствия для работ по предыдущей выдаче", sty["head"])]]
        # Переход — группа подстрок (суть, пункты, вопросы, последствие): таблица переносится между ними,
        # а не обрывает строку. Короткий переход держится целиком (NOSPLIT); длинный переносится, и на новой
        # странице в первых колонках повторяются переход, даты и пометка «продолжение».
        from reportlab.pdfbase.pdfmetrics import stringWidth
        pad = 10.5
        w_tr = max([stringWidth(r["label"], FONT_BOLD, sty["tr"].fontSize) for r in view["rows"]] +
                   [stringWidth(d, FONT, sty["date"].fontSize) for r in view["rows"] for d in r["dates"] if d] +
                   [stringWidth("продолжение", FONT, sty["date"].fontSize)]) + pad
        w_sc = max(stringWidth(w, FONT_BOLD, sty["scale"].fontSize) for r in view["rows"] for w in r["scale"].split()) + pad
        w_tr, w_sc = max(w_tr, 19 * mm), max(w_sc, 22 * mm)
        w_tx = width - w_tr - w_sc
        style = [("BACKGROUND", (0, 0), (-1, 0), HexColor(HEAD_FILL)), ("VALIGN", (0, 0), (-1, -1), "TOP"),
                 ("TOPPADDING", (0, 0), (-1, -1), 1.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5),
                 ("LEFTPADDING", (0, 0), (-1, -1), 5), ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                 ("TOPPADDING", (0, 0), (-1, 0), 5), ("BOTTOMPADDING", (0, 0), (-1, 0), 6)]
        group_start, cont = [0], {}
        frame_h = A4[1] - 32 * mm
        for r in view["rows"]:
            left = [Paragraph(_t(r["label"]), sty["tr"])] + [Paragraph(_t(d), sty["date"]) for d in r["dates"] if d]
            cont_left = [Paragraph(_t(r["label"]), sty["tr"]), Paragraph("продолжение", sty["cont"])] + \
                [Paragraph(_t(d), sty["date"]) for d in r["dates"] if d]
            parts = [Paragraph(_t(r["lead"]), sty["lead"])]
            for p in r["points"]:
                obj = f"<font name='{FONT_BOLD}'>{_t(p['object'])}:</font> " if p.get("object") else ""
                parts.append(Paragraph(obj + _t(p["text"]), sty["pt"]))
            for q in r.get("questions") or []:
                parts.append(Paragraph(f"<font name='{FONT_BOLD}'>Уточнить:</font> {_t(_uncap(q))}", sty["pt"]))
            if r.get("impact"):
                parts.append(Paragraph(_t(r["impact"]), sty["imp"]))
            # Подстроки не ниже левой ячейки «продолжения»: короткие пункты склеиваются, чтобы при переносе
            # под подставленными переходом и датами не оставалось пустоты.
            hs = [x.wrap(w_tx - 10, 1e6)[1] + 4 for x in parts]
            min_h = sum(x.wrap(w_tr - 10, 1e6)[1] for x in cont_left) + 2
            chunks, cur, cur_h = [], [], 0.0
            for x, h in zip(parts, hs):
                cur.append(x)
                cur_h += h
                if cur_h >= min_h:
                    chunks.append(cur)
                    cur, cur_h = [], 0.0
            if cur:
                if chunks and cur_h < min_h:
                    chunks[-1] += cur
                else:
                    chunks.append(cur)
            first, last = len(rows), len(rows) + len(chunks) - 1
            for k, chunk in enumerate(chunks):
                rows.append([left if k == 0 else "", Paragraph(_t(r["scale"]), sty["scale"]) if k == 0 else "", chunk])
                group_start.append(first)
                cont[len(rows) - 1] = (cont_left, Paragraph(_t(r["scale"]), sty["scale"]))
            style += [("TOPPADDING", (0, first), (-1, first), 6), ("BOTTOMPADDING", (0, last), (-1, last), 7),
                      ("LINEBELOW", (0, last), (-1, last), 0.4, HexColor(RULE))]
            height = sum(hs) + 13
            if height < 0.25 * frame_h:
                style.append(("NOSPLIT", (0, first), (-1, last)))
            if r["points"]:
                style.append(("BACKGROUND", (0, first), (-1, last), HexColor(FILL)))
        tab = _ContTable(rows, colWidths=[w_tr, w_sc, w_tx], repeatRows=1)
        tab._group_start, tab._cont = group_start, cont
        tab.setStyle(TableStyle(style))
        return s + [tab]

    def build(pages: int) -> int:
        doc = SimpleDocTemplate(str(tmp), pagesize=A4, leftMargin=MARGIN_MM * mm, rightMargin=MARGIN_MM * mm,
                                topMargin=14 * mm, bottomMargin=18 * mm, title=f"{code} — {view['title']}",
                                author="", subject=view.get("section") or "")
        doc._mdr_pages = pages
        doc.build(story(), onFirstPage=deco, onLaterPages=deco)
        return doc.page
    pages = build(0)
    if build(pages) != pages:
        build(build(pages))
    tmp.replace(out)
    return out


def _cont_table_class():
    from reportlab.platypus import LongTable

    class ContTable(LongTable):
        """Таблица, которая при переносе подставляет в первые колонки продолжения переход, даты и «продолжение»."""
        _group_start: list[int] = []
        _cont: dict = {}

        def split(self, availWidth, availHeight):
            parts = LongTable.split(self, availWidth, availHeight)
            if len(parts) != 2 or not self._group_start:
                return parts
            head, tail = parts
            n = len(head._cellvalues)          # первая строка продолжения в исходной нумерации
            hdr = len(tail._cellvalues) - (len(self._cellvalues) - n)
            if n < len(self._group_start) and self._group_start[n] < n and n in self._cont:
                left, scale = self._cont[n]
                tail._cellvalues[hdr][0], tail._cellvalues[hdr][1] = left, scale
                tail._argH = list(tail._argH)
                tail._argH[hdr] = None          # высота строки пересчитывается под подставленные колонки
            tail._group_start = [0] * hdr + [g - n + hdr if g >= n else hdr for g in self._group_start[n:]]
            tail._cont = {k - n + hdr: v for k, v in self._cont.items() if k >= n}
            return parts
    return ContTable


def _ContTable(*a, **kw):
    return _cont_table_class()(*a, **kw)


def _uncap(q: str) -> str:
    """«Уточнить: …» — вопрос без повтора глагола («Уточнить марку…» → «марку…»)."""
    q = re.sub(r"^(Уточнить|Требуется уточнить|Необходимо уточнить)\s*[:,]?\s*", "", q.strip())
    return q[:1].lower() + q[1:] if q[:2] != q[:2].upper() else q


# Служебное, которого не должно быть в PDF для заказчика.
CLIENT_FORBIDDEN = (r"\b[ФF]\d{4}\b", r"Основание", r"Реестр", r"Полнота", r"Приложение", r"MD", r"SHA",
                    r"по MD-сводке", r"исходные PDF", r"Что выделить", r"границы вывода", r"\.md\b", r"анализ \d",
                    r"gpt-|codex|Sol\b|Луна", r"стр\. PDF", r"md_from", r"\bT\d{2}\b", r"claim", r"и ещё \d",
                    r"Дополнительных технических последствий", r"первое описание")


def check_client(pdf_path: Path, view: dict, preview_dir: Path | None = None) -> dict[str, Any]:
    """Проверка PDF для заказчика: A4, встроенный шрифт, поля, все переходы и даты, жирные первые предложения,
    нет служебных ID/реестров/подписей процесса, нет пустых страниц, подвал — только шифр и страница."""
    import fitz
    problems, warnings = [], []
    doc = fitz.open(str(pdf_path))
    text_all, bold_all = "", ""
    for i, page in enumerate(doc):
        w, h = page.rect.width, page.rect.height
        if abs(w - 595.3) > 2 or abs(h - 841.9) > 2:
            problems.append(f"стр. {i + 1}: не A4 книжная")
        body = [b for b in page.get_text("dict")["blocks"] if b.get("lines")]
        text = page.get_text()
        if len(text.strip()) < 120:
            problems.append(f"стр. {i + 1}: почти пустая")
        text_all += text + "\n"
        footer = []
        for b in body:
            for ln in b["lines"]:
                for sp in ln["spans"]:
                    if not sp["text"].strip():
                        continue
                    x0, y0, x1, y1 = sp["bbox"]
                    if x0 < 20 or x1 > w - 20 or y0 < 15 or y1 > h - 15:
                        problems.append(f"стр. {i + 1}: текст за полями «{sp['text'][:40]}»")
                    if "MdrSans" not in sp["font"] and "DejaVu" not in sp["font"]:
                        problems.append(f"стр. {i + 1}: шрифт {sp['font']}")
                    if "Bold" in sp["font"]:
                        bold_all += sp["text"]
                    if y0 > h - 12 * 72 / 25.4:
                        footer.append(sp["text"].strip())
        if i > 0:
            # Перенос: первая строка таблицы на странице — с переходом в левой колонке (новый или «продолжение»).
            spans = [(sp["bbox"], sp["text"].strip()) for bl in body for ln in bl["lines"] for sp in ln["spans"]
                     if sp["text"].strip()]
            head = [bb for bb, t in spans if t.startswith("Изменения и последствия")]
            if head:
                hy = head[0][1]       # шапка таблицы — несколько строк в левых колонках: граница по самой нижней
                y0 = max(bb[3] for bb, t in spans if hy - 2 <= bb[1] <= hy + 30)
                below = sorted((bb for bb, t in spans if bb[1] > y0 + 2), key=lambda bb: bb[1])
                top = below[0][1] if below else None
                ok = top is not None and any(re.match(r"V\d+→V\d+", t) and abs(bb[1] - top) < 14 and bb[0] < w * 0.2
                                             for bb, t in spans)
                if not ok:
                    problems.append(f"стр. {i + 1}: продолжение перехода без перехода и дат в первой строке")
        expect = {view["code"], f"стр. {i + 1} из {doc.page_count}"}
        if set(footer) != expect:
            problems.append(f"стр. {i + 1}: подвал «{' | '.join(footer)}» — ожидаются только шифр и номер страницы")
        if preview_dir is not None:
            preview_dir.mkdir(parents=True, exist_ok=True)
            page.get_pixmap(dpi=90).save(str(preview_dir / f"page-{i + 1}.png"))
    sq, sqb = _squash(text_all), _squash(bold_all)
    for r in view["rows"]:
        if _squash(r["label"]) not in sq:
            problems.append(f"нет перехода {r['label']}")
        for d in r["dates"]:
            if d and d not in text_all:
                problems.append(f"{r['label']}: нет даты {d} целиком")
        if _squash(r["lead"]) not in sqb:
            problems.append(f"{r['label']}: первое предложение не жирное в PDF")
        for q in r.get("questions") or []:
            if _squash(_uncap(q))[:60] not in sq:
                problems.append(f"{r['label']}: вопрос «{q[:40]}» не найден в тексте PDF")
        for p in r["points"]:
            if _squash(p["text"])[:60] not in sq:
                problems.append(f"{r['label']}: пункт «{p['object'][:40]}» не найден в тексте PDF")
    for rx in CLIENT_FORBIDDEN:
        m = re.search(rx, text_all)
        if m:
            problems.append(f"служебное в PDF для заказчика: «{m.group(0)}»")
    if "⁇" in text_all or "�" in text_all:
        problems.append("знак замены в тексте (нечитаемая буква)")
    cut = re.search(r"\w{3,}…(?=[\s,.;:)]|$)", text_all)
    if cut:
        problems.append(f"обрезанное значение в тексте: «{cut.group(0)}»")
    if re.search(r"(?m)^\s*[,;:]", text_all) or ": ," in text_all:
        problems.append("строка начинается со знака препинания")
    if doc.page_count > 4:
        warnings.append(f"страниц {doc.page_count} — больше ориентира 3–4")
    info = {"pages": doc.page_count, "words": len(re.findall(r"\w+", text_all))}
    doc.close()
    return {"ok": not problems, "problems": problems, "warnings": warnings, **info}
