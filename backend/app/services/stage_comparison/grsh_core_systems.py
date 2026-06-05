"""GRSH core-systems extraction/rendering (B1) + cross-side guard (B2).

The GRSH feeder врезка (контур B) extracts отходящие фидеры very well, but the
ЯДРО ГРЩ (вводы, шинопроводы, вводные QF, секционный/АВР/ПСВ, УЗИП/ОПН, ТТ/ТШП,
учёт, АУКРМ, ГЗШ/ДСУП, штамп) was not surfaced to Opus. This module closes that
gap natively (no extra Qwen call):

  * `build_core_systems(structured, grsh_connections, text_layer, source_side)`
    → a structured `core_systems` dict (11 fixed categories, each a list of
    items with value/state/source/confidence/evidence) + `diagnostics`.
    Items come from THREE already-available sources: Qwen `structured` slots,
    Qwen `grsh_connections`, and the block-PDF vector **text-layer** (PyMuPDF) —
    text-layer items get `field_state="ocr_only"` (authoritative literal text,
    not Qwen vision). Categories with no data → a single `not_extracted` item
    (never silently dropped, never treated as `removed`).
    This dict is stored in the Qwen description payload by `enrich_side`.

  * `render_core_systems_md(core_systems)` → the `GRSH_CORE_SYSTEMS` markdown
    section rendered into enriched MD by `build_enriched_md`.

  * `core_diff_index_lines(core_systems)` → compact core anchors for
    IMAGE_DIFF_INDEX (QF 3200/50кА, шинопровод, АВР/ПСВ, УЗИП/ОПН, ТТ/ТШП,
    Меркурий/TS, АУКРМ, ГЗШ/ДСУП, штамп).

B2 (extraction-level guard): if a category has data ONLY from the text-layer
(Qwen structured missed it) it is flagged `ocr_only` + `requires_human_review`
in `diagnostics` — so the other side's absence is treated as `not_extracted`,
not `removed`. `apply_cross_side_guard` is the comparison-level counterpart
(reclassifies a change's added/removed when the 'absent' side's text-layer
actually contains the element).
"""
from __future__ import annotations

import re
from typing import Any, Optional

# ── payload detection ───────────────────────────────────────────────────────
def is_grsh_core_payload(payload: Any) -> bool:
    """True for the GRSH feeder-extraction payload (контур B / v9): has
    `grsh_feeder_table`/`grsh_feeders` or a `structured` electrical_singleline.
    Distinct from md_image_enrichment.is_grsh_payload (the OLD v7 verified_anchors)."""
    if not isinstance(payload, dict):
        return False
    if payload.get("core_systems"):
        return True
    if payload.get("grsh_feeder_table") or payload.get("grsh_feeders"):
        return True
    st = payload.get("structured")
    if isinstance(st, dict) and st.get("profile") == "electrical_singleline":
        return True
    return payload.get("graphic_profile") == "electrical_singleline"


# ── text-layer extraction (local, no model) ─────────────────────────────────
def extract_text_layer(pdf_path: str) -> str:
    """Vector text-layer of a block-PDF via PyMuPDF. Best-effort; '' on failure."""
    try:
        import fitz  # PyMuPDF
    except Exception:  # noqa: BLE001
        return ""
    try:
        doc = fitz.open(pdf_path)
        try:
            return "\n".join(page.get_text() for page in doc)
        finally:
            doc.close()
    except Exception:  # noqa: BLE001
        return ""


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _lc(s: Any) -> str:
    return _norm(s).lower().replace("ё", "е")


# ── core categories ─────────────────────────────────────────────────────────
# (key, human label, text-layer keyword patterns matched lc-substring, ё→е).
CORE_CATEGORIES: list[tuple[str, str, list[str]]] = [
    ("inputs", "inputs / вводы (ТП, Т1/Т2, L1-L2-L3, рабочий/резервный)",
     ["к тп1", "к тп2", "ввод 1 к", "ввод 2 к", "рабочий ввод", "резервный ввод",
      "s=1250", "l1,l2,l3", "l1-l2-l3", "граница проектирования", "граница балансов"]),
    ("busbars", "busbars / шинопроводы",
     ["шинопровод"]),
    ("main_breakers", "main_breakers / вводные QF + Iкз",
     ["3200а", "2500а", "2000а", "1600а", "50ка", "qf1 3", "qf2 3", "qf3 3", "qs1"]),
    ("sectional_and_avr", "sectional_and_avr / секционный, АВР, ПСВ",
     ["авр", "псв", "секц", "qs1", "sf/sa"]),
    ("surge_protection", "surge_protection / УЗИП, ОПН, FU",
     ["узип", "опн", "fu 125", "fu1..fu3", "fu1", "fu2", "fu4"]),
    ("current_transformers", "current_transformers / ТТ, ТШП, коэффициенты",
     ["тшп", "2000/5", "1500/5", "750/5", "200/5", "150/5", "40/5", "50/5", "20/5",
      "0,5s", "0.5s", "1тт", "2тт", "та1", "та4", "та7"]),
    ("metering", "metering / Меркурий, анализаторы, TS1/TS2, Wh, счётчики",
     ["меркурий", "анализатор", "ts1", "ts2", "счетчик", "мультиметр", "wh1", "wh2",
      " wh", "pw1", "pw2"]),
    ("compensation", "compensation / АУКРМ, КУ",
     ["аукрм", "ку1", "квар"]),
    ("earthing_dsup", "earthing_dsup / ГЗШ, ДСУП, уравнивание потенциалов",
     ["гзш", "ре-шине", "pe (гзш)", "заземл", "уравниван", "металлоконстр",
      "металлические трубы", "контур заземления", "полосов"]),
    ("notes", "notes / общие указания",
     ["гост", "ip31", "ip 31", "по отд. проект", "встроенная"]),
    ("title_block", "title_block / штамп, стадия, наименование",
     ["стадия", "щит индивидуального", "~380", "380/ 220", "380/220",
      "балансовой принадлежности", "эксплуатационной ответ"]),
]
CORE_CATEGORY_KEYS = [c[0] for c in CORE_CATEGORIES]
_CATEGORY_LABEL = {k: lbl for k, lbl, _ in CORE_CATEGORIES}

# structured-slot → category, for cross-confirmation / enrichment
_STRUCT_SLOT_CAT = {"compensation": "compensation", "earthing": "earthing_dsup", "metering": "metering"}

# Источники, означающие «Qwen структурно извлёк это» (не только текст-слой).
_QWEN_SOURCES = {"qwen_structured", "qwen_connections"}


def _scan_text_layer(text_layer: str, patterns: list[str], *, cap: int = 14) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in (text_layer or "").splitlines():
        line = _norm(raw)
        if not line:
            continue
        low = _lc(line)
        if any(p in low for p in patterns):
            if low in seen:
                continue
            seen.add(low)
            out.append(line)
            if len(out) >= cap:
                break
    return out


def _struct_slot_items(structured: dict, slot: str, *, cap: int = 12) -> list[dict]:
    items = structured.get(slot) if isinstance(structured, dict) else None
    if not isinstance(items, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        val = (it.get("ref") or it.get("name") or it.get("device") or it.get("consumer") or "")
        detail = (it.get("detail") or it.get("ct_ratio") or it.get("rating") or "")
        state = it.get("field_state") or "present"
        text = _norm(f"{val} {('— ' + str(detail)) if detail else ''}").strip(" —")
        if not text:
            continue
        key = _lc(text)
        if key in seen:
            continue
        seen.add(key)
        out.append({"value": text, "state": state, "source": "qwen_structured"})
        if len(out) >= cap:
            break
    return out


def _connection_core_items(grsh_connections: Any, category: str, *, cap: int = 10) -> list[dict]:
    if not isinstance(grsh_connections, list):
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for c in grsh_connections:
        if not isinstance(c, dict):
            continue
        via = _lc(c.get("via"))
        frm = _lc(c.get("from"))
        ev = _norm(c.get("evidence_text"))
        want = False
        if category == "busbars" and "шинопровод" in via and any(ch.isdigit() for ch in via):
            want = True
        elif category == "inputs" and ("ввод" in via or frm.startswith("тп") or "l1" in frm or "ввод" in _lc(ev)):
            want = True
        elif category == "current_transformers" and ("тшп" in _lc(ev) or "та" in frm[:3] or "тт" in _lc(ev)):
            want = True
        if not want:
            continue
        conf = c.get("confidence")
        val = _norm(f"{c.get('from')} → {c.get('to')} via {c.get('via')}")
        key = _lc(val)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "value": val, "state": "present", "source": "qwen_connections",
            "confidence": conf if isinstance(conf, (int, float)) else None,
            "evidence": ev[:160] or None,
        })
        if len(out) >= cap:
            break
    return out


# ── B1: build structured core_systems ───────────────────────────────────────
def build_core_systems(
    structured: Optional[dict],
    grsh_connections: Any,
    text_layer: str,
    *,
    source_side: str = "?",
) -> dict:
    """Build the structured `core_systems` dict from already-available sources.
    Returns {"source_side", "categories": {cat: [item,...]}, "diagnostics": {...}}.
    Each item: {"value","state","source", optional "confidence","evidence"}.
    Empty category → one {"state":"not_extracted"} item (never dropped)."""
    structured = structured if isinstance(structured, dict) else {}
    categories: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}
    present: list[str] = []
    not_extracted: list[str] = []
    ocr_only_cats: list[str] = []

    for key, _label, patterns in CORE_CATEGORIES:
        items: list[dict] = []
        items += _connection_core_items(grsh_connections, key)
        slot = next((s for s, cat in _STRUCT_SLOT_CAT.items() if cat == key), None)
        if slot:
            items += _struct_slot_items(structured, slot)
        for ln in _scan_text_layer(text_layer, patterns):
            items.append({"value": ln, "state": "ocr_only", "source": "text_layer"})
        # dedup by normalized leading value
        uniq: list[dict] = []
        seen: set[str] = set()
        for it in items:
            k = _lc(it.get("value"))
            if not k or k in seen:
                continue
            seen.add(k)
            uniq.append(it)
        if not uniq:
            categories[key] = [{"value": None, "state": "not_extracted", "source": "none"}]
            counts[key] = 0
            not_extracted.append(key)
            continue
        categories[key] = uniq
        counts[key] = len(uniq)
        present.append(key)
        # B2 extraction guard: data ONLY from text-layer (Qwen structured missed it).
        if uniq and all(it.get("source") not in _QWEN_SOURCES for it in uniq):
            ocr_only_cats.append(key)

    diagnostics = {
        "source_side": source_side,
        "counts": counts,
        "categories_present": present,
        "categories_not_extracted": not_extracted,
        "ocr_only_categories": ocr_only_cats,
        # B2 extraction-level: каждая ocr_only-категория требует ручной сверки —
        # её отсутствие на другой стороне НЕ значит removed (Qwen мог не извлечь).
        "requires_human_review_categories": list(ocr_only_cats),
    }
    return {"source_side": source_side, "categories": categories, "diagnostics": diagnostics}


def _fmt_item(it: dict) -> str:
    if it.get("state") == "not_extracted" or not it.get("value"):
        return "not_extracted (требуется targeted Qwen core extraction; НЕ трактовать как removed)"
    parts = []
    val = it.get("value")
    parts.append(f'"{val}"' if it.get("source") == "text_layer" else str(val))
    parts.append(str(it.get("state") or "present"))
    parts.append(str(it.get("source") or ""))
    conf = it.get("confidence")
    if isinstance(conf, (int, float)):
        parts.append(f"conf={conf}")
    ev = it.get("evidence")
    if ev:
        parts.append(f'evidence="{ev}"')
    return " | ".join(p for p in parts if p)


def render_core_systems_md(core_systems: Optional[dict]) -> str:
    """Render the GRSH_CORE_SYSTEMS markdown section from a built core_systems dict.
    Always emits every category (incl. not_extracted). '' if no dict."""
    if not isinstance(core_systems, dict):
        return ""
    cats = core_systems.get("categories")
    if not isinstance(cats, dict) or not cats:
        return ""
    side = core_systems.get("source_side") or "?"
    lines = [
        "GRSH_CORE_SYSTEMS — ядро ГРЩ "
        f"(source_side={side}; источники: qwen_structured / qwen_connections / "
        "text_layer[ocr_only]; «not_extracted» = НЕ извлечено, НЕ трактовать как removed):"
    ]
    for key, label, _patterns in CORE_CATEGORIES:
        items = cats.get(key) or [{"value": None, "state": "not_extracted", "source": "none"}]
        lines.append(f"- {label}:")
        for it in items:
            lines.append(f"  · {_fmt_item(it)}")
    return "\n".join(lines)


# Backward-compat wrapper (offline rebuild / legacy payloads w/o core_systems).
def render_grsh_core_systems_md(
    structured: Optional[dict], grsh_connections: Any, text_layer: str,
    *, source_side: str = "?",
) -> str:
    cs = build_core_systems(structured, grsh_connections, text_layer, source_side=source_side)
    return render_core_systems_md(cs)


# ── IMAGE_DIFF_INDEX core anchors ───────────────────────────────────────────
# Compact per-category anchor labels surfaced in IMAGE_DIFF_INDEX.
_DIFF_INDEX_CAT_LABEL = {
    "inputs": "вводы",
    "busbars": "шинопровод",
    "main_breakers": "вводные QF/Iкз",
    "sectional_and_avr": "АВР/ПСВ",
    "surge_protection": "УЗИП/ОПН",
    "current_transformers": "ТТ/ТШП",
    "metering": "учёт(Меркурий/TS)",
    "compensation": "АУКРМ",
    "earthing_dsup": "ГЗШ/ДСУП",
    "title_block": "штамп",
}


def core_diff_index_lines(core_systems: Optional[dict], *, per_cat: int = 3) -> list[str]:
    """Compact `core:` lines for IMAGE_DIFF_INDEX. One bullet per present
    category with a short label + a few literal anchors; not_extracted categories
    are listed explicitly so absence ≠ removed."""
    if not isinstance(core_systems, dict):
        return []
    cats = core_systems.get("categories")
    if not isinstance(cats, dict) or not cats:
        return []
    out: list[str] = ["core:"]
    for key in CORE_CATEGORY_KEYS:
        if key in ("notes",):
            continue
        label = _DIFF_INDEX_CAT_LABEL.get(key, key)
        items = cats.get(key) or []
        real = [it for it in items if it.get("value") and it.get("state") != "not_extracted"]
        if not real:
            out.append(f"- {label}: not_extracted")
            continue
        vals = []
        seen: set[str] = set()
        for it in real:
            v = _norm(it.get("value"))[:48]
            k = _lc(v)
            if k in seen:
                continue
            seen.add(k)
            vals.append(v)
            if len(vals) >= per_cat:
                break
        out.append(f"- {label}: " + "; ".join(vals))
    return out


# ── B2: comparison-level cross-side guard ────────────────────────────────────
_TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9][A-Za-zА-Яа-яЁё0-9./\-]{2,}")
_STOP = {
    "кВт", "квт", "квар", "ввод", "линия", "автомат", "кабель", "потребитель",
    "схема", "стадия", "изменено", "режим", "корпус", "только", "новой", "старой",
    "added", "removed", "changed", "значение", "сторон", "сторона",
}


def _salient_tokens(*texts: str) -> set[str]:
    out: set[str] = set()
    for t in texts:
        for m in _TOKEN_RE.findall(_lc(t)):
            if m in _STOP or len(m) < 3:
                continue
            if any(ch.isdigit() for ch in m) or re.search(r"[a-zа-я]", m):
                out.add(m)
    return out


def apply_cross_side_guard(
    changes: list[dict], left_text_layer: str, right_text_layer: str, *, min_hits: int = 1,
) -> tuple[list[dict], dict]:
    """Reclassify false added/removed/present_one_side when the 'absent' side's
    text-layer actually contains the element. Non-destructive (marks, never
    deletes). Returns (changes, stats)."""
    left_lc = _lc(left_text_layer)
    right_lc = _lc(right_text_layer)
    guarded = examined = 0
    out: list[dict] = []
    for ch in changes:
        if not isinstance(ch, dict):
            out.append(ch)
            continue
        ctype = (ch.get("type") or "").lower()
        if ctype not in ("added", "removed", "present_one_side"):
            out.append(ch)
            continue
        examined += 1
        if ctype == "added":
            absent_sides = [("left", left_lc)]
            anchor = _salient_tokens(ch.get("new_value"), ch.get("title"),
                                     (ch.get("evidence_right") or {}).get("quote", ""))
        elif ctype == "removed":
            absent_sides = [("right", right_lc)]
            anchor = _salient_tokens(ch.get("old_value"), ch.get("title"),
                                     (ch.get("evidence_left") or {}).get("quote", ""))
        else:
            absent_sides = [("left", left_lc), ("right", right_lc)]
            anchor = _salient_tokens(ch.get("old_value"), ch.get("new_value"), ch.get("title"))
        cand = sorted(list(anchor), key=len, reverse=True)[:8]
        hit_side = None
        hit_tokens: list[str] = []
        for side_name, side_lc in absent_sides:
            hits = [t for t in cand if t in side_lc]
            if len(hits) >= min_hits:
                hit_side, hit_tokens = side_name, hits
                break
        if hit_side:
            guarded += 1
            ch = dict(ch)
            ch["requires_human_review"] = True
            ch["disputed"] = True
            ch["cross_side_guard"] = {
                "original_type": ctype,
                "present_in_text_layer_side": hit_side,
                "matched_tokens": hit_tokens[:6],
                "note": (
                    f"Элемент помечен как {ctype}, но его маркеры присутствуют в "
                    f"text-layer стороны '{hit_side}' — вероятно НЕ извлечён Qwen, а не "
                    f"реально добавлен/удалён. not_extracted / требуется проверка."
                ),
            }
        out.append(ch)
    return out, {"examined": examined, "guarded": guarded}
