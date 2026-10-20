"""Профили разделов для отчёта заказчику: каждый раздел — по своим особенностям, без подгонки под другой.

Профиль — название раздела, приоритеты отбора (текст в задание модели) и признаки «важных» фактов (★ в пакете
и разбор покрытия). Раздел определяется по названию с титульного листа, затем по шифру; не определён —
нейтральный профиль без приоритетов (``known=False``). Файлы — ``prompts/client_disciplines/``.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

DIR = Path(__file__).resolve().parent.parent / "prompts" / "client_disciplines"
IMPORTANT_KEYS = ("req", "mark", "size", "rating")


@lru_cache(maxsize=1)
def _registry() -> list[dict[str, Any]]:
    data = json.loads((DIR / "_registry.json").read_text(encoding="utf-8"))
    out = []
    for d in data["disciplines"]:
        out.append(d | {"_match": [re.compile(p) for p in d.get("match") or []],
                        "_title": re.compile(d["title"], re.I) if d.get("title") else None})
    return out


def suffix(code: str | None) -> str:
    """Шифр раздела после последнего «-»: «133/23-ГК-ОВ2.1» → «ОВ2.1»."""
    return re.split(r"\s+", (code or "").strip())[0].rsplit("-", 1)[-1] if code else ""


def _profile(d: dict | None) -> dict[str, Any]:
    if d is None:
        return {"key": "_neutral", "name": "", "known": False,
                "priorities": (DIR / "_neutral.md").read_text(encoding="utf-8").strip(), "important": {}}
    imp = {k: re.compile(v, re.I) for k, v in (d.get("important") or {}).items() if k in IMPORTANT_KEYS and v}
    return {"key": d["key"], "name": d["name"], "known": True,
            "priorities": (DIR / f"{d['key']}.md").read_text(encoding="utf-8").strip(), "important": imp}


def by_key(key: str) -> dict[str, Any]:
    """Профиль по ключу, записанному в отчёт; неизвестный ключ — нейтральный профиль."""
    d = next((x for x in _registry() if x["key"] == key), None)
    return _profile(d) | {"by": "report" if d else "none"}


def resolve(code: str | None, title_text: str = "") -> dict[str, Any]:
    """Профиль раздела: по названию с титульного листа, иначе по шифру, иначе нейтральный."""
    text = re.sub(r"\s+", " ", title_text or "")
    if text:
        for d in _registry():
            if d["_title"] and d["_title"].search(text):
                return _profile(d) | {"by": "title"}
    suf = suffix(code)
    for d in _registry():
        if any(p.fullmatch(suf) for p in d["_match"]):
            return _profile(d) | {"by": "code"}
    return _profile(None) | {"by": "none"}


def describe(prof: dict) -> dict[str, Any]:
    """Сведения о профиле для задания отчёта (без регулярных выражений)."""
    return {k: prof.get(k) for k in ("key", "name", "known", "by")}
