"""Текстовый diff двух MD-файлов с группировкой по заголовкам."""
from __future__ import annotations

import re
from difflib import SequenceMatcher, unified_diff
from pathlib import Path
from typing import Optional


_HEADING_RE = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")


def _safe_read_lines(path: str | Path) -> list[str]:
    try:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return []
        text = p.read_text(encoding="utf-8", errors="replace")
    except (OSError, UnicodeDecodeError):
        return []
    # splitlines() игнорирует trailing newline — нам так удобнее
    return text.splitlines()


def _heading_for_line(lines: list[str], line_idx: int) -> Optional[str]:
    """Найти ближайший заголовок (вверх) для строки в файле."""
    if line_idx < 0 or line_idx >= len(lines):
        return None
    for i in range(line_idx, -1, -1):
        m = _HEADING_RE.match(lines[i])
        if m:
            return m.group(2).strip()
    return None


def _truncate(s: str, limit: int = 280) -> str:
    s = (s or "").rstrip()
    if len(s) <= limit:
        return s
    return s[:limit] + "…"


def build_text_diff(
    left_md_path: str | Path | None,
    right_md_path: str | Path | None,
    *,
    max_changes: int = 2000,
) -> dict:
    """Построить структурированный diff двух MD-файлов.

    Возвращает:
      {
        "left_present": bool,
        "right_present": bool,
        "added": int,
        "removed": int,
        "modified": int,
        "changes": [
            {
              "type": "added" | "removed" | "modified",
              "left_lineno": int | None,
              "right_lineno": int | None,
              "left_text": str | None,
              "right_text": str | None,
              "heading": str | None,
              "similarity": float (только для modified)
            }, ...
        ],
        "warnings": [...]
      }
    """
    warnings: list[str] = []
    left_lines = _safe_read_lines(left_md_path) if left_md_path else []
    right_lines = _safe_read_lines(right_md_path) if right_md_path else []
    if left_md_path and not left_lines:
        warnings.append(f"MD-файл слева пустой или не читается: {left_md_path}")
    if right_md_path and not right_lines:
        warnings.append(f"MD-файл справа пустой или не читается: {right_md_path}")

    sm = SequenceMatcher(None, left_lines, right_lines, autojunk=False)
    opcodes = sm.get_opcodes()

    changes: list[dict] = []
    added_count = 0
    removed_count = 0
    modified_count = 0

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            continue
        if tag == "delete":
            for offs in range(i2 - i1):
                idx = i1 + offs
                changes.append({
                    "type": "removed",
                    "left_lineno": idx + 1,
                    "right_lineno": None,
                    "left_text": _truncate(left_lines[idx] if idx < len(left_lines) else ""),
                    "right_text": None,
                    "heading": _heading_for_line(left_lines, idx),
                })
                removed_count += 1
                if len(changes) >= max_changes:
                    break
        elif tag == "insert":
            for offs in range(j2 - j1):
                idx = j1 + offs
                changes.append({
                    "type": "added",
                    "left_lineno": None,
                    "right_lineno": idx + 1,
                    "left_text": None,
                    "right_text": _truncate(right_lines[idx] if idx < len(right_lines) else ""),
                    "heading": _heading_for_line(right_lines, idx),
                })
                added_count += 1
                if len(changes) >= max_changes:
                    break
        elif tag == "replace":
            # Пытаемся склеить парами по индексу; «хвост» уходит в added/removed
            left_chunk = left_lines[i1:i2]
            right_chunk = right_lines[j1:j2]
            common = min(len(left_chunk), len(right_chunk))
            for k in range(common):
                left_idx = i1 + k
                right_idx = j1 + k
                ltxt = left_chunk[k]
                rtxt = right_chunk[k]
                sim = SequenceMatcher(None, ltxt, rtxt).ratio() if (ltxt or rtxt) else 0.0
                changes.append({
                    "type": "modified",
                    "left_lineno": left_idx + 1,
                    "right_lineno": right_idx + 1,
                    "left_text": _truncate(ltxt),
                    "right_text": _truncate(rtxt),
                    "heading": _heading_for_line(right_lines, right_idx)
                        or _heading_for_line(left_lines, left_idx),
                    "similarity": round(sim, 3),
                })
                modified_count += 1
                if len(changes) >= max_changes:
                    break
            # хвост слева
            if len(left_chunk) > common and len(changes) < max_changes:
                for k in range(common, len(left_chunk)):
                    left_idx = i1 + k
                    changes.append({
                        "type": "removed",
                        "left_lineno": left_idx + 1,
                        "right_lineno": None,
                        "left_text": _truncate(left_chunk[k]),
                        "right_text": None,
                        "heading": _heading_for_line(left_lines, left_idx),
                    })
                    removed_count += 1
                    if len(changes) >= max_changes:
                        break
            # хвост справа
            if len(right_chunk) > common and len(changes) < max_changes:
                for k in range(common, len(right_chunk)):
                    right_idx = j1 + k
                    changes.append({
                        "type": "added",
                        "left_lineno": None,
                        "right_lineno": right_idx + 1,
                        "left_text": None,
                        "right_text": _truncate(right_chunk[k]),
                        "heading": _heading_for_line(right_lines, right_idx),
                    })
                    added_count += 1
                    if len(changes) >= max_changes:
                        break

        if len(changes) >= max_changes:
            warnings.append(f"Достигнут лимит изменений ({max_changes}); вывод усечён.")
            break

    return {
        "left_present": bool(left_lines),
        "right_present": bool(right_lines),
        "left_lines_total": len(left_lines),
        "right_lines_total": len(right_lines),
        "added": added_count,
        "removed": removed_count,
        "modified": modified_count,
        "changes": changes,
        "warnings": warnings,
    }


__all__ = ["build_text_diff"]
