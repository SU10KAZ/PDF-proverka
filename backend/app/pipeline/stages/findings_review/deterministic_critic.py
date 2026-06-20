"""
Детерминированный critic замечаний (+ опциональный точечный LLM-проход).

Зачем
-----
Раньше «Critic замечаний» (`findings_critic`) запускался как агентный
`claude -p --allowedTools Read,Write` и читал многомегабайтные `03_findings.json`
+ `02_blocks_analysis.json` + `document_graph.json` инструментом Read по 2000
строк за вызов. На крупных проектах прогон не доживал до записи
`03_findings_review.json` (таймаут 1200 c / лимит ходов → `is_error`, пустой
результат), и этап падал с «critic produced no review artifact».

Решение
-------
5 проверок critic (см. docs/critic_corrector.md) разделены:

  1. evidence_presence  — Python (детерминированно)
  2. block_exists       — Python
  3. evidence_relevance — точечный bounded LLM (best-effort)
  4. page_sheet_correct — Python
  5. text_consistency   — точечный bounded LLM (best-effort)

Детерминированные проверки 1/2/4 выполняются всегда и всегда дают валидный
файл вердиктов. LLM-проход (3/5) — best-effort: любая ошибка/таймаут/непарсимый
ответ → эти замечания остаются `pass`. Поэтому этап НЕ МОЖЕТ заблокировать
конвейер так, как агентный критик: файл вердиктов пишется детерминированно из
Python.

Формат вывода совпадает с тем, что читает findings_review/runner.py:

    {"meta": {"total_reviewed": N, "verdicts": {"pass": x, ...}, ...},
     "reviews": [{"finding_id", "verdict", "failed_checks",
                  "reason", "evidence_checked", "suggested_fix"}]}

verdict ∈ {pass, no_evidence, phantom_block, weak_evidence,
           page_mismatch, contradicts_text}
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

# Точечный LLM-вызов: (prompt) -> текст ответа модели. None → semantic-проход
# пропускается (все кандидаты остаются pass). DI для тестов.
LLMCall = Callable[[str], Awaitable[str]]

DEFAULT_LLM_BATCH = 12
_MAX_CLAIM_CHARS = 500
_MAX_BLOCK_TEXT_CHARS = 320
_MAX_PAGE_TEXT_CHARS = 360
_MAX_BLOCKS_PER_FINDING = 6

# block_id вида block_007_1 / A64J-JJPV-A7Y / page_12_text в тексте описания.
_BLOCK_ID_RE = re.compile(
    r"\b(?:block_[0-9a-zA-Z_]+|page_\d+_[a-z]+|[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{3,})\b"
)

_SEMANTIC = {"weak_evidence", "contradicts_text"}
_ALL_VERDICTS = {
    "pass", "no_evidence", "phantom_block",
    "weak_evidence", "page_mismatch", "contradicts_text",
}


# ═══════════════════════════════════════════════════════════════════════════
# Результат
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class DeterministicCriticResult:
    reviews: list[dict] = field(default_factory=list)
    findings_total: int = 0
    deterministic_issues: int = 0       # no_evidence/phantom_block/page_mismatch
    semantic_issues: int = 0            # weak_evidence/contradicts_text
    llm_candidates: int = 0
    llm_used: bool = False
    llm_failed: bool = False
    # #91: сколько кандидатов остались БЕЗ семантической проверки из-за сбоя LLM
    # (батч упал/не распарсился → по fail-soft остаются pass). Делает деградацию
    # видимой, а не молчаливой.
    semantic_unverified: int = 0
    error: Optional[str] = None

    @property
    def issues(self) -> int:
        return self.deterministic_issues + self.semantic_issues

    @property
    def passed(self) -> int:
        return self.findings_total - self.issues

    def verdict_counts(self) -> dict:
        c: Counter = Counter()
        for r in self.reviews:
            c[r.get("verdict", "pass")] += 1
        out = {"pass": c.get("pass", 0)}
        for v in sorted(_ALL_VERDICTS - {"pass"}):
            if c.get(v):
                out[v] = c[v]
        return out

    def to_review_dict(self, project_id: str = "") -> dict:
        return {
            "meta": {
                "project_id": project_id,
                "review_date": datetime.now().isoformat(),
                "stage": "findings_critic",
                "mode": "deterministic+llm" if self.llm_used else "deterministic",
                "total_reviewed": self.findings_total,
                "verdicts": self.verdict_counts(),
                "deterministic_issues": self.deterministic_issues,
                "semantic_issues": self.semantic_issues,
                "llm_candidates": self.llm_candidates,
                "llm_failed": self.llm_failed,
                "semantic_unverified": self.semantic_unverified,
            },
            "reviews": self.reviews,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Индексы по входным JSON
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class _Index:
    image_block_ids: set = field(default_factory=set)
    text_block_ids: set = field(default_factory=set)
    block_page: dict = field(default_factory=dict)
    block_sheet: dict = field(default_factory=dict)
    block_text: dict = field(default_factory=dict)
    page_to_sheet: dict = field(default_factory=dict)
    page_text: dict = field(default_factory=dict)


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def iter_findings(data) -> list:
    """Достать список замечаний из 03_findings.json (list или dict-обёртка)."""
    if isinstance(data, list):
        return [f for f in data if isinstance(f, dict)]
    if isinstance(data, dict):
        for key in ("findings", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [f for f in value if isinstance(f, dict)]
    return []


def _block_findings_text(block: dict) -> str:
    parts = []
    for fnd in block.get("findings") or []:
        if isinstance(fnd, dict):
            txt = fnd.get("finding") or fnd.get("description") or fnd.get("title") or ""
        else:
            txt = str(fnd)
        if txt:
            parts.append(str(txt))
    return " | ".join(parts)


def build_index(blocks_analysis, doc_graph) -> _Index:
    idx = _Index()

    block_analyses = []
    if isinstance(blocks_analysis, dict):
        block_analyses = blocks_analysis.get("block_analyses") or []
    elif isinstance(blocks_analysis, list):
        block_analyses = blocks_analysis

    for block in block_analyses:
        if not isinstance(block, dict):
            continue
        bid = block.get("block_id") or block.get("id")
        if not bid:
            continue
        bid = str(bid)
        idx.image_block_ids.add(bid)
        page = _as_int(block.get("page"))
        if page is not None:
            idx.block_page[bid] = page
        sheet = block.get("sheet")
        if sheet not in (None, ""):
            idx.block_sheet[bid] = str(sheet)
        label = block.get("label") or block.get("block_type") or block.get("sheet_type") or ""
        text = _block_findings_text(block)
        combined = " — ".join(p for p in (str(label), text) if p)
        if combined:
            idx.block_text[bid] = combined[:_MAX_BLOCK_TEXT_CHARS]

    pages = []
    if isinstance(doc_graph, dict):
        pages = doc_graph.get("pages") or []
    for page in pages:
        if not isinstance(page, dict):
            continue
        pnum = _as_int(page.get("page"))
        if pnum is None:
            continue
        sheet_no = (
            page.get("sheet_no_raw")
            or page.get("sheet_no_normalized")
            or page.get("sheet_no")
        )
        if sheet_no not in (None, ""):
            idx.page_to_sheet[pnum] = str(sheet_no)
        text_parts = []
        for tb in page.get("text_blocks") or []:
            if not isinstance(tb, dict):
                continue
            tbid = tb.get("block_id") or tb.get("id")
            if tbid:
                idx.text_block_ids.add(str(tbid))
            txt = tb.get("text_norm") or tb.get("text") or ""
            if txt:
                text_parts.append(str(txt))
        idx.text_block_ids.add(f"page_{pnum}_text")
        if text_parts:
            idx.page_text[pnum] = " ".join(text_parts)[:_MAX_PAGE_TEXT_CHARS]
        for ib in page.get("image_blocks") or []:
            if isinstance(ib, dict):
                ibid = ib.get("block_id") or ib.get("id")
                if ibid:
                    idx.image_block_ids.add(str(ibid))

    return idx


# ═══════════════════════════════════════════════════════════════════════════
# Детерминированные проверки 1/2/4
# ═══════════════════════════════════════════════════════════════════════════

def _block_ids_in_text(*texts: str) -> list:
    found = []
    for t in texts:
        if not t:
            continue
        found.extend(_BLOCK_ID_RE.findall(t))
    seen, out = set(), []
    for b in found:
        if b not in seen:
            seen.add(b)
            out.append(b)
    return out


def _referenced_block_ids(finding: dict):
    """Вернуть (image_refs, text_refs, evidence_pages)."""
    image_refs, text_refs, pages = [], [], set()

    for ev in finding.get("evidence") or []:
        if not isinstance(ev, dict):
            continue
        bid = ev.get("block_id")
        etype = (ev.get("type") or "").lower()
        if bid:
            (text_refs if etype == "text" else image_refs).append(str(bid))
        p = _as_int(ev.get("page"))
        if p is not None:
            pages.add(p)

    for bid in finding.get("related_block_ids") or []:
        if bid:
            image_refs.append(str(bid))
    for bid in finding.get("source_block_ids") or []:
        if bid:
            image_refs.append(str(bid))
    for ref in finding.get("evidence_text_refs") or []:
        if isinstance(ref, dict):
            bid = ref.get("block_id")
            if bid:
                text_refs.append(str(bid))
        elif ref:
            text_refs.append(str(ref))

    for bid in _block_ids_in_text(finding.get("description", ""),
                                  finding.get("problem", ""),
                                  finding.get("title", "")):
        if bid.startswith("page_") and bid.endswith("_text"):
            text_refs.append(bid)
        else:
            image_refs.append(bid)

    return list(dict.fromkeys(image_refs)), list(dict.fromkeys(text_refs)), pages


def _mk_review(fid, verdict, failed, reason, evidence_checked, suggested_fix=None) -> dict:
    return {
        "finding_id": fid,
        "verdict": verdict,
        "failed_checks": failed,
        "reason": reason,
        "evidence_checked": evidence_checked,
        "suggested_fix": suggested_fix,
    }


def deterministic_verdict(finding: dict, idx: _Index):
    """Вердикт по проверкам 1/2/4 или None (структурно чисто → на LLM 3/5)."""
    fid = finding.get("id") or finding.get("finding_id")
    image_refs, text_refs, ev_pages = _referenced_block_ids(finding)

    # ─── Проверка 1: evidence_presence ───
    if not image_refs and not text_refs:
        return _mk_review(
            fid, "no_evidence", ["evidence_presence"],
            "Нет evidence[] / related_block_ids / ссылки на block_id в тексте.",
            [],
        )

    # ─── Проверка 2: block_exists ───
    existing_image = [b for b in image_refs if b in idx.image_block_ids]
    existing_text = [b for b in text_refs if (b in idx.text_block_ids or b.endswith("_text"))]
    if image_refs and not existing_image and not existing_text:
        return _mk_review(
            fid, "phantom_block", ["block_exists"],
            f"block_id отсутствуют в 02_blocks_analysis.json: {image_refs}.",
            [],
        )

    evidence_checked = existing_image + existing_text

    # ─── Проверка 4: page_sheet_correct ───
    ref_pages = set(ev_pages)
    for b in existing_image:
        if b in idx.block_page:
            ref_pages.add(idx.block_page[b])
    fpage = _normalize_finding_page(finding.get("page"))
    # Консервативно: page_mismatch ставим только при ТОЧНОЙ привязке evidence
    # (≤2 страниц). Широкий разброс evidence (сводное замечание по многим листам)
    # не позволяет уверенно судить о неверной странице → не флагуем.
    if (
        fpage is not None and 1 <= len(ref_pages) <= 2
        and not _page_in(fpage, ref_pages)
    ):
        fsheet = str(finding.get("sheet") or "")
        ref_sheets = {idx.page_to_sheet.get(p, "") for p in ref_pages}
        ref_sheets |= {idx.block_sheet.get(b, "") for b in existing_image}
        ref_sheets.discard("")
        if not (fsheet and fsheet in ref_sheets):
            return _mk_review(
                fid, "page_mismatch", ["page_sheet_correct"],
                f"page={fpage}/sheet={fsheet} не совпадает со страницами evidence "
                f"{sorted(ref_pages)} (sheets {sorted(ref_sheets)}).",
                evidence_checked,
            )

    return None  # структурно чисто → кандидат на семантический LLM-проход


def _normalize_finding_page(page):
    """Поле page может быть int или list ([27, 29]). Вернуть множество int или
    None. Для list проверяем «хотя бы одна страница совпадает» (лениво)."""
    if isinstance(page, list):
        ints = [p for p in (_as_int(x) for x in page) if p is not None]
        return _PageSet(ints) if ints else None
    return _as_int(page)


class _PageSet:
    """Обёртка для finding.page-списка: `x in ref_pages` истинно, если хоть одна
    страница списка совпадает (несовпадение = ни одна не попала)."""
    def __init__(self, pages):
        self.pages = set(pages)

    def __eq__(self, other):  # not used for `in`, kept for repr safety
        return self.pages == getattr(other, "pages", other)

    def __repr__(self):
        return f"{sorted(self.pages)}"


def _page_in(fpage, ref_pages) -> bool:
    if isinstance(fpage, _PageSet):
        return bool(fpage.pages & ref_pages)
    return fpage in ref_pages


# ═══════════════════════════════════════════════════════════════════════════
# Точечный LLM-проход (проверки 3/5) — компактный, bounded, fail-soft
# ═══════════════════════════════════════════════════════════════════════════

def _finding_context(finding: dict, idx: _Index) -> str:
    fid = finding.get("id") or finding.get("finding_id")
    image_refs, _text_refs, ev_pages = _referenced_block_ids(finding)
    lines = [f"### {fid}"]
    for key in ("problem", "title"):
        if finding.get(key):
            lines.append(f"title: {finding[key]}")
            break
    if finding.get("category"):
        lines.append(f"category: {finding['category']}")
    claim = (finding.get("description") or finding.get("problem") or "")[:_MAX_CLAIM_CHARS]
    if claim:
        lines.append(f"claim: {claim}")
    if finding.get("norm"):
        lines.append(f"norm: {finding['norm']}")

    shown = 0
    for b in image_refs:
        if shown >= _MAX_BLOCKS_PER_FINDING:
            break
        if b not in idx.image_block_ids:
            continue
        p = idx.block_page.get(b, "?")
        s = idx.block_sheet.get(b, "?")
        txt = idx.block_text.get(b, "")
        lines.append(f"- block {b} (page {p}, sheet {s}): {txt}")
        shown += 1

    pages_for_text = []
    fpage = finding.get("page")
    if isinstance(fpage, list):
        pages_for_text.extend(_as_int(x) for x in fpage)
    else:
        pages_for_text.append(_as_int(fpage))
    pages_for_text.extend(sorted(ev_pages))
    for p in dict.fromkeys(x for x in pages_for_text if x is not None):
        snippet = idx.page_text.get(p)
        if snippet:
            lines.append(f"page {p} text: {snippet}")
            break

    return "\n".join(lines)


def build_semantic_prompt(findings: list, idx: _Index) -> str:
    body = "\n\n".join(_finding_context(f, idx) for f in findings)
    return (
        "Ты — строгий критик замечаний аудита проектной документации. "
        "Для КАЖДОГО замечания ниже оцени две проверки:\n"
        "  3) evidence_relevance — evidence-блоки семантически подтверждают суть замечания;\n"
        "  5) text_consistency — замечание НЕ противоречит тексту страницы.\n\n"
        "Вердикты: \"pass\" (обе ок), \"weak_evidence\" (evidence не подтверждает "
        "суть), \"contradicts_text\" (замечание противоречит тексту). По умолчанию "
        "ставь \"pass\"; иное — ТОЛЬКО при явной проблеме.\n\n"
        "Верни СТРОГО JSON без иного текста и без использования инструментов:\n"
        '{"verdicts":[{"finding_id":"...","verdict":"pass|weak_evidence|'
        'contradicts_text","reason":"кратко"}]}\n\n'
        "Замечания:\n\n" + body
    )


def parse_semantic_response(text: str) -> dict:
    """Распарсить ответ LLM в {finding_id: {verdict, reason}}. Терпим к мусору."""
    if not text:
        return {}
    raw = text.strip()
    fence = re.search(r"```(?:json)?\s*(.+?)```", raw, re.DOTALL)
    if fence:
        raw = fence.group(1).strip()
    data = None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end > start:
            try:
                data = json.loads(raw[start:end + 1])
            except json.JSONDecodeError:
                return {}
        else:
            return {}

    if isinstance(data, dict):
        verdicts = data.get("verdicts") or data.get("reviews") or []
    elif isinstance(data, list):
        verdicts = data
    else:
        verdicts = []

    out = {}
    for v in verdicts:
        if not isinstance(v, dict):
            continue
        fid = v.get("finding_id") or v.get("id")
        if fid is None:
            continue
        verdict = (v.get("verdict") or v.get("status") or "pass").lower()
        if verdict not in _SEMANTIC:
            verdict = "pass"
        out[str(fid)] = {"verdict": verdict, "reason": v.get("reason") or ""}
    return out


async def _run_semantic_pass(candidates, idx, llm_call, batch_size, on_log):
    """Прогнать кандидатов через bounded LLM батчами. Возвращает
    ({finding_id: {verdict, reason}}, llm_failed, unverified_count).

    unverified_count — число кандидатов в провалившихся батчах (raised/unparsed),
    которые по fail-soft остаются pass без семантической проверки (#91)."""
    merged, failed, unverified = {}, False, 0
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start:start + batch_size]
        prompt = build_semantic_prompt(batch, idx)
        try:
            text = await llm_call(prompt)
        except Exception as exc:  # noqa: BLE001 — fail-soft по дизайну
            logger.warning("Semantic critic LLM call failed: %s", exc)
            failed = True
            unverified += len(batch)
            if on_log:
                await on_log(f"Semantic critic: LLM-вызов упал ({exc}); батч → pass")
            continue
        parsed = parse_semantic_response(text or "")
        if not parsed:
            failed = True
            unverified += len(batch)
            if on_log:
                await on_log("Semantic critic: ответ LLM не распарсился; батч → pass")
            continue
        merged.update(parsed)
    return merged, failed, unverified


# ═══════════════════════════════════════════════════════════════════════════
# Ядро (без I/O) — удобно тестировать
# ═══════════════════════════════════════════════════════════════════════════

def review_structural(findings_data, blocks_analysis, doc_graph):
    """Проверки 1/2/4 без LLM. Возвращает (reviews, candidates, result, idx)."""
    findings = iter_findings(findings_data)
    idx = build_index(blocks_analysis, doc_graph)
    result = DeterministicCriticResult(findings_total=len(findings))
    reviews, candidates = [], []
    for finding in findings:
        det = deterministic_verdict(finding, idx)
        if det is not None:
            reviews.append(det)
            result.deterministic_issues += 1
        else:
            candidates.append(finding)
    result.llm_candidates = len(candidates)
    return reviews, candidates, result, idx


# ═══════════════════════════════════════════════════════════════════════════
# Главная функция (I/O)
# ═══════════════════════════════════════════════════════════════════════════

async def run_deterministic_critic(
    output_dir: Path,
    *,
    project_id: str = "",
    findings_filename: str = "03_findings.json",
    review_filename: str = "03_findings_review.json",
    llm_call: Optional[LLMCall] = None,
    on_log: Optional[Callable[[str], Awaitable[None]]] = None,
    batch_size: int = DEFAULT_LLM_BATCH,
    write: bool = True,
) -> DeterministicCriticResult:
    """Прогнать детерминированный критик и (опц.) записать review-файл.

    llm_call=None → семантические проверки 3/5 пропускаются (кандидаты = pass).
    Любая ошибка LLM — fail-soft: кандидаты остаются pass, файл всё равно пишется.
    """
    output_dir = Path(output_dir)
    findings_data = _load_json(output_dir / findings_filename)
    if findings_data is None:
        return DeterministicCriticResult(error=f"{findings_filename} не найден/невалиден")

    blocks_analysis = _load_json(output_dir / "02_blocks_analysis.json") or {}
    doc_graph = _load_json(output_dir / "document_graph.json") or {}

    reviews, candidates, result, idx = review_structural(
        findings_data, blocks_analysis, doc_graph,
    )

    semantic = {}
    if candidates and llm_call is not None:
        result.llm_used = True
        if on_log:
            await on_log(
                f"Critic: {len(reviews)} структурных проблем, "
                f"{len(candidates)} замечаний → семантическая проверка LLM..."
            )
        semantic, failed, unverified = await _run_semantic_pass(
            candidates, idx, llm_call, batch_size, on_log,
        )
        result.llm_failed = failed
        result.semantic_unverified = unverified  # #91: видимая деградация

    for finding in candidates:
        fid = finding.get("id") or finding.get("finding_id")
        sem = semantic.get(str(fid))
        image_refs, text_refs, _ = _referenced_block_ids(finding)
        ev_checked = [b for b in image_refs if b in idx.image_block_ids] + [
            b for b in text_refs if (b in idx.text_block_ids or b.endswith("_text"))
        ]
        if sem and sem["verdict"] in _SEMANTIC:
            failed_check = (
                ["evidence_relevance"] if sem["verdict"] == "weak_evidence"
                else ["text_consistency"]
            )
            reviews.append(_mk_review(
                fid, sem["verdict"], failed_check,
                sem.get("reason") or "Семантическая проверка LLM.", ev_checked,
            ))
            result.semantic_issues += 1
        else:
            reason = "Прошёл структурные проверки (1/2/4)."
            if not result.llm_used:
                reason += " Семантическая проверка LLM не запускалась."
            reviews.append(_mk_review(fid, "pass", [], reason, ev_checked))

    order = {
        (f.get("id") or f.get("finding_id")): i
        for i, f in enumerate(iter_findings(findings_data))
    }
    reviews.sort(key=lambda r: order.get(r["finding_id"], 1_000_000))
    result.reviews = reviews

    if write:
        (output_dir / review_filename).write_text(
            json.dumps(result.to_review_dict(project_id), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if on_log:
        await on_log(
            f"Critic готов: {result.findings_total} замечаний "
            f"({result.passed} pass, {result.deterministic_issues} структурных + "
            f"{result.semantic_issues} семантических проблем)"
            + (" [LLM fail-soft]" if result.llm_failed else "")
        )
    return result


def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("deterministic_critic: не прочитан %s: %s", path, exc)
        return None
