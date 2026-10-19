"""Программный разбор MD-сводки изменений: заголовок, переходы, листы, пункты — с номерами строк оригинала.

Формат — тот, что выдаёт «Сводка изменений (MD)» (``sheet_changes/series_summary.py``), но разбор
не полагается на заявленные в заголовке счётчики: структура инвентаризуется заново.
Номер строки — порядковый номер в исходном файле, считая с 1 по символу ``\\n``; BOM и ``\\r``
снимаются только в рабочем представлении и привязку к оригиналу не сдвигают.

Нераспознанное не считается «без изменений»: такие места попадают в ``issues`` и в блоки анализа.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

H1 = re.compile(r"^#\s+(.+?)\s*$")
H2 = re.compile(r"^##\s+(.+?)\s*$")
TRANSITION = re.compile(r"^##\s+(\d+)\.\s+(?:(.+?):\s+)?(.+?)\s+→\s+(.+?)\s*$")
H3 = re.compile(r"^###\s+(.+?)\s*$")
PAGES = re.compile(r"\s*\(((?:A стр\. PDF \d+)?(?:\s*→\s*)?(?:B стр\. PDF \d+)?)\)\s*$")
PAGE_A = re.compile(r"A стр\. PDF (\d+)")
PAGE_B = re.compile(r"B стр\. PDF (\d+)")
SINGLE = re.compile(r"^(Лист добавлен|Лист удалён):\s*(.+)$")
SHEET_LABEL = re.compile(r"^((?:СО\s+)?л\.\s*[^\s·]+)\s+·\s+")
DATE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")
FILES = re.compile(r"^A:\s.*файл «(.*?)»\.\s+B:\s.*файл «(.*?)»\.?\s*$")
ITALIC = re.compile(r"^-?\s*_(.+?)_\s*$")
NO_CONTENT = "изменений содержания нет"
# Пометки сводки о том, что описания нет: это неполнота источника, а не отсутствие изменений.
INCOMPLETE_MARKERS = ("не досчитано", "ошибка ИИ", "не отправлен", "ещё не описан", "описывается",
                      "не описан (отменено)", "ИИ-описание не запускалось", "не запускалось",
                      "Спецификация не описана")
UNCLEAR = re.compile(r"Требует уточнения:\s*(\d+)")
# Реестр покрытия в конце сводки (``series_summary``): не переход, в анализ не передаётся, разбирается отдельно.
REGISTRY_TITLE = "## Реестр покрытия"
REGISTRY_ROW = re.compile(r"^-\s+(\S+?→\S+?):\s+листов\s+(\d+)\s+—\s+(.+?)\.?\s*$")
REGISTRY_SUB = re.compile(r"^\s+-\s+(не обработано|неоднозначно|отметка документа):\s*(.+)$")


def decode(raw: bytes) -> str:
    """Текст UTF-8 (BOM допускается). Другие кодировки — отказ: тихая перекодировка исказит цитаты."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(f"файл не в кодировке UTF-8 (байт {exc.start})") from exc


def split_lines(text: str) -> list[str]:
    """Строки оригинала по ``\\n``; завершающий перевод строки не даёт лишней пустой строки."""
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [ln[:-1] if ln.endswith("\r") else ln for ln in lines]


def _iso(label: str) -> str | None:
    found = DATE.findall(label or "")
    if not found:
        return None
    d, m, y = found[-1]
    return f"{y}-{m}-{d}"


def _version(label: str) -> str:
    """Короткое обозначение версии: первое слово подписи («V3», «(1)»); иначе подпись целиком."""
    head = (label or "").split()
    return head[0] if head else (label or "")


def _sha(lines: list[str]) -> str:
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def _sheet(title_line: str, n: int) -> dict[str, Any]:
    text = H3.match(title_line).group(1)
    kind = "pair"
    m = SINGLE.match(text)
    if m:
        kind = "added" if m.group(1) == "Лист добавлен" else "removed"
        text = m.group(2)
    pa = pb = None
    pm = PAGES.search(text)
    if pm:
        inner = pm.group(1)
        a, b = PAGE_A.search(inner), PAGE_B.search(inner)
        pa, pb = (int(a.group(1)) if a else None), (int(b.group(1)) if b else None)
        text = text[:pm.start()]
    labels = []
    rest = text
    while True:
        lm = SHEET_LABEL.match(rest)
        if not lm:
            break
        labels.append(re.sub(r"\s+", " ", lm.group(1)))
        rest = rest[lm.end():]
    return {"heading": title_line.strip(), "kind": kind, "sheet": " · ".join(labels) or None, "title": rest.strip(),
            "a_page": pa, "b_page": pb, "line_start": n, "line_end": n, "items": 0, "unclear": 0,
            "not_described": []}


def _registry_line(sec: dict, line: str, n: int) -> None:
    m = REGISTRY_ROW.match(line)
    if m:
        counts = {}
        for part in m.group(3).split(";"):
            cm = re.match(r"^\s*(.+?)\s+(\d+)\s*$", part)
            if cm:
                counts[cm.group(1)] = int(cm.group(2))
        sec["rows"].append({"label": m.group(1), "sheets_total": int(m.group(2)), "counts": counts, "line": n,
                            "details": []})
        return
    m = REGISTRY_SUB.match(line)
    if m and sec["rows"]:
        sec["rows"][-1]["details"].append({"kind": m.group(1), "text": m.group(2).strip(), "line": n})


def parse(raw: bytes, filename: str = "") -> dict[str, Any]:
    text = decode(raw)
    lines = split_lines(text)
    n_lines = len(lines)
    issues: list[dict[str, Any]] = []
    title, code = None, None
    sections: list[dict[str, Any]] = []      # переходы и нераспознанные разделы уровня ##
    first_h2 = None
    for i, line in enumerate(lines, 1):
        if line.startswith("## ") or line == "##":
            first_h2 = first_h2 or i
            if line.strip() == REGISTRY_TITLE:
                sections.append({"kind": "registry", "heading": line.strip(), "line_start": i, "line_end": i,
                                 "rows": [], "sheets": [], "notes": [], "items": 0})
                continue
            m = TRANSITION.match(line)
            if m:
                a_label, b_label = m.group(3).strip(), m.group(4).strip()
                sections.append({
                    "kind": "transition", "number": int(m.group(1)), "code": (m.group(2) or "").strip() or None,
                    "heading": line.strip(), "a_label": a_label, "b_label": b_label,
                    "from_version": _version(a_label), "to_version": _version(b_label),
                    "from_date": _iso(a_label), "to_date": _iso(b_label), "a_file": None, "b_file": None,
                    "line_start": i, "line_end": i, "sheets": [], "notes": [], "explicit_no_content_changes": False,
                    "source_incomplete": [], "items": 0, "unclear": 0})
            else:
                sections.append({"kind": "unknown", "heading": line.strip(), "line_start": i, "line_end": i,
                                 "sheets": [], "notes": [], "items": 0})
                issues.append({"code": "UNRECOGNIZED_SECTION", "line": i,
                               "text": f"раздел не распознан как переход A → B: «{line.strip()[:120]}»"})
            continue
        if not sections:
            m = H1.match(line)
            if m and title is None:
                title = m.group(1)
                cm = re.match(r"^Сводка изменений\s+(.+)$", title)
                code = cm.group(1).strip() if cm else None
            continue
        sec = sections[-1]
        sec["line_end"] = i
        if sec["kind"] == "registry":
            _registry_line(sec, line, i)
            continue
        if H3.match(line):
            sec["sheets"].append(_sheet(line, i))
            continue
        cur = sec["sheets"][-1] if sec["sheets"] else None
        if cur:
            cur["line_end"] = i
        stripped = line.strip()
        if not stripped:
            continue
        if sec["kind"] == "transition" and not cur:
            fm = FILES.match(stripped)
            if fm:
                sec["a_file"], sec["b_file"] = fm.group(1), fm.group(2)
                continue
        im = ITALIC.match(stripped)
        note = im.group(1) if im else None
        if note:
            if cur:
                um = UNCLEAR.search(note)
                if um:
                    cur["unclear"] += int(um.group(1))
                    sec["unclear"] = sec.get("unclear", 0) + int(um.group(1))
                    continue
            if any(mk in note for mk in INCOMPLETE_MARKERS):
                where = {"line": i, "text": note, "sheet": cur["heading"] if cur else None}
                (cur["not_described"] if cur else sec["notes"]).append(where)
                if sec["kind"] == "transition":
                    sec["source_incomplete"].append(where)
                continue
            if not cur:
                sec["notes"].append({"line": i, "text": note})
                if NO_CONTENT in note and sec["kind"] == "transition":
                    sec["explicit_no_content_changes"] = True
                continue
        if stripped.startswith("- "):
            sec["items"] += 1
            if cur:
                cur["items"] += 1
            continue
        if not cur:
            sec["notes"].append({"line": i, "text": stripped[:200]})
    header_end = (first_h2 - 1) if first_h2 else n_lines
    transitions = [s for s in sections if s["kind"] == "transition"]
    for k, t in enumerate(transitions, 1):
        t["transition_id"] = f"T{k:02d}"
        t["label"] = f"{t['from_version']}→{t['to_version']}"
        for j, s in enumerate(t["sheets"], 1):
            s["sheet_id"] = f"{t['transition_id']}-S{j:03d}"
        t["chars"] = sum(len(lines[x - 1]) + 1 for x in range(t["line_start"], t["line_end"] + 1))
        t["sha256"] = _sha(lines[t["line_start"] - 1:t["line_end"]])
        if t["explicit_no_content_changes"] and (t["sheets"] or t["items"]):
            issues.append({"code": "NO_CHANGES_WITH_CONTENT", "line": t["line_start"],
                           "text": f"{t['label']}: отметка «без изменений», но есть листы или пункты"})
        if not t["explicit_no_content_changes"] and not t["sheets"] and not t["items"] and not t["source_incomplete"]:
            issues.append({"code": "EMPTY_TRANSITION", "line": t["line_start"],
                           "text": f"{t['label']}: нет ни листов, ни отметки об отсутствии изменений"})
        for s in t["sheets"]:
            # Лист только с вопросами («Требует уточнения: N») не пустой: в нём есть неясность.
            if not s["items"] and not s["not_described"] and not s["unclear"]:
                issues.append({"code": "EMPTY_SHEET", "line": s["line_start"],
                               "text": f"{t['label']}: лист без пунктов — «{s['heading'][:100]}»"})
    for u in (s for s in sections if s["kind"] == "unknown"):
        u["section_id"] = f"U{u['line_start']:05d}"
    # Порядок версий берётся из документа. Разрыв цепочки и повтор пары — сообщаются, пары не достраиваются.
    seen: dict[tuple[str, str], str] = {}
    for prev, nxt in zip(transitions, transitions[1:]):
        if prev["b_label"] != nxt["a_label"]:
            issues.append({"code": "CHAIN_GAP", "line": nxt["line_start"],
                           "text": f"после {prev['label']} идёт {nxt['label']}: версия B предыдущего перехода "
                                   f"«{prev['b_label']}» не совпадает с A следующего «{nxt['a_label']}»"})
    for t in transitions:
        key = (t["a_label"], t["b_label"])
        if key in seen:
            issues.append({"code": "DUPLICATE_PAIR", "line": t["line_start"],
                           "text": f"пара {t['label']} повторяется (уже была в {seen[key]})"})
        seen.setdefault(key, t["transition_id"])
        if t["source_incomplete"]:
            issues.append({"code": "SOURCE_INCOMPLETE", "line": t["line_start"],
                           "text": f"{t['label']}: в сводке есть неописанные места ({len(t['source_incomplete'])})"})
    codes = {t["code"] for t in transitions if t.get("code")}
    if not code and len(codes) == 1:
        code = next(iter(codes))
    if not transitions:
        issues.append({"code": "NO_TRANSITIONS", "line": 1, "text": "в файле не найдено ни одного перехода A → B"})
    return {
        "filename": filename, "title": title, "project_code": code, "lines_total": n_lines,
        "chars_total": sum(len(x) + 1 for x in lines),
        "header": {"line_start": 1, "line_end": header_end, "text": "\n".join(lines[:header_end])},
        "transitions": transitions, "unknown_sections": [s for s in sections if s["kind"] == "unknown"],
        "registry": next(({k: s[k] for k in ("heading", "line_start", "line_end", "rows")}
                          for s in sections if s["kind"] == "registry"), None),
        "issues": issues,
        "inventory": {"transitions": len(transitions), "sheets": sum(len(t["sheets"]) for t in transitions),
                      "items": sum(t["items"] for t in transitions),
                      "unclear": sum(t.get("unclear", 0) for t in transitions),
                      "explicit_no_content_changes": sum(1 for t in transitions if t["explicit_no_content_changes"]),
                      "with_source_gaps": sum(1 for t in transitions if t["source_incomplete"])},
    }


def locate(manifest: dict[str, Any], line: int) -> dict[str, Any]:
    """Переход и лист, к которым относится строка оригинала (для ссылок «лист, стр. PDF»)."""
    for t in manifest["transitions"]:
        if t["line_start"] <= line <= t["line_end"]:
            sheet = next((s for s in t["sheets"] if s["line_start"] <= line <= s["line_end"]), None)
            return {"transition": t, "sheet": sheet}
    return {"transition": None, "sheet": None}
