"""Детерминированная сборка Evidence Package из РЕАЛЬНОГО прогона сравнения.

Только чтение. Никаких вызовов моделей. Никакой записи в артефакты пары.

Пакет собирается из тех же файлов, которые уже производит production-конвейер:
  review_questions.json   — вопрос Stage 5 и его контракт
  unified_synthesis.json  — review_item с before/after и provenance
  text_preparation.json   — фрагменты страниц (соседний контекст)
  sheet_relations.json    — отношение листов и его доказательства
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any


# ── Загрузка артефактов прогона ────────────────────────────────────────────

class Run:
    def __init__(self, production_dir: str | Path):
        self.dir = Path(production_dir)
        self.questions = self._load("review_questions.json")
        self.synthesis = self._load("unified_synthesis.json")
        self.preparation = self._load("text_preparation.json")
        self.sheet_relations = self._load("sheet_relations.json")
        self.differences = self._load("text_differences.json")

        self.review_items = {r["review_evidence_id"]: r for r in self.synthesis["review_items"]}
        self.relations = {r["relation_id"]: r for r in self.sheet_relations["relations"]}

        # фрагменты страниц в порядке чтения — источник соседнего контекста
        self.frags: dict[tuple[str, int], list[dict]] = {}
        for side_key, side in (("left", "LEFT"), ("right", "RIGHT")):
            for f in self.preparation["fragments"][side_key]:
                self.frags.setdefault((side, f["pdf_page"]), []).append(f)
        for v in self.frags.values():
            v.sort(key=lambda f: f.get("order", 0))

        # индекс фрагмент → позиция, чтобы взять окно соседей
        self.frag_pos: dict[str, tuple[str, int, int]] = {}
        for (side, page), lst in self.frags.items():
            for i, f in enumerate(lst):
                self.frag_pos[f["id"]] = (side, page, i)

        self.scope_map = _build_scope_map(self)

    def _load(self, name: str) -> dict:
        return json.loads((self.dir / name).read_text(encoding="utf-8"))


# ── Пакет доказательств ────────────────────────────────────────────────────

@dataclass
class EvidencePackage:
    """Единственный вход AI Analyst. Всё, что модель имеет право знать."""

    package_version: str = "ai-evidence-package.v1"
    question_id: str = ""
    review_evidence_id: str = ""
    atom_id: str = ""
    scope_ref: str = ""

    # отношение листов, на котором держится сравнение
    sheet_relation: dict[str, Any] = field(default_factory=dict)

    # само расхождение, как его увидел детерминированный Stage 3
    stage3_bucket: str = ""
    direction: str = ""
    before_value: str | None = None
    after_value: str | None = None

    # где это лежит физически
    locations: dict[str, Any] = field(default_factory=dict)

    # соседний контекст страниц — окно строк вокруг фрагмента
    left_context: list[str] = field(default_factory=list)
    right_context: list[str] = field(default_factory=list)

    # заголовки страниц обеих сторон — нужны, чтобы поймать неверную пару листов
    left_page_titles: list[str] = field(default_factory=list)
    right_page_titles: list[str] = field(default_factory=list)

    # ссылки на доказательства, которыми модель обязана оперировать
    evidence_refs: list[dict[str, Any]] = field(default_factory=list)

    # что уже установил детерминированный слой
    deterministic_state: dict[str, Any] = field(default_factory=dict)

    # контракт ответа
    resolution_contract: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=1)


_TITLE_RE = re.compile(r"экспликац|кровл|ведомост|спецификац|узел|схем", re.I)

CONTEXT_WINDOW = 6


def _page_titles(run: Run, side: str, page: int) -> list[str]:
    out, seen = [], set()
    for f in run.frags.get((side, page), []):
        t = (f.get("text") or "").strip()
        if _TITLE_RE.search(t) and t not in seen:
            seen.add(t)
            out.append(t)
    return out[:4]


def _context(run: Run, fragment_id: str, window: int = CONTEXT_WINDOW) -> list[str]:
    pos = run.frag_pos.get(fragment_id)
    if not pos:
        return []
    side, page, i = pos
    lst = run.frags[(side, page)]
    lo, hi = max(0, i - window), min(len(lst), i + window + 1)
    out = []
    for j in range(lo, hi):
        t = (lst[j].get("text") or "").strip()
        mark = "»" if j == i else " "
        out.append(f"{mark} {t}")
    return out


def build_package(run: Run, question: dict) -> EvidencePackage:
    ctx = question["context"]
    item = run.review_items[ctx["review_evidence_id"]]
    src = item["provenance"]["source_atom"]
    loc = src["locations"]

    rel = run.relations.get(run.scope_map.get(item["scope_ref"], ""), {})

    left_pages = sorted({l["page"] for l in loc.get("LEFT") or []})
    right_pages = sorted({l["page"] for l in loc.get("RIGHT") or []})
    # если сторона пуста — берём страницы из отношения листов
    if not left_pages:
        left_pages = list(rel.get("left_pages") or [])
    if not right_pages:
        right_pages = list(rel.get("right_pages") or [])

    left_ctx: list[str] = []
    for l in loc.get("LEFT") or []:
        left_ctx += _context(run, l["fragment_id"])
    right_ctx: list[str] = []
    for l in loc.get("RIGHT") or []:
        right_ctx += _context(run, l["fragment_id"])

    lt, rt = [], []
    for p in left_pages:
        lt += _page_titles(run, "LEFT", p)
    for p in right_pages:
        rt += _page_titles(run, "RIGHT", p)

    return EvidencePackage(
        question_id=question["question_id"],
        review_evidence_id=item["review_evidence_id"],
        atom_id=item["atom_id"],
        scope_ref=item["scope_ref"],
        sheet_relation={
            "relation_id": rel.get("relation_id"),
            "left_pages": rel.get("left_pages"),
            "right_pages": rel.get("right_pages"),
            "relation_type": rel.get("relation_type"),
            "status": rel.get("status"),
            "confidence": rel.get("confidence"),
            "evidence": rel.get("evidence"),
        },
        stage3_bucket=src.get("stage3_bucket", ""),
        direction=item["direction"],
        before_value=item["before_value"],
        after_value=item["after_value"],
        locations={"LEFT": loc.get("LEFT") or [], "RIGHT": loc.get("RIGHT") or []},
        left_context=left_ctx,
        right_context=right_ctx,
        left_page_titles=lt,
        right_page_titles=rt,
        evidence_refs=item["evidence_refs"],
        deterministic_state={
            "dimension": item["dimension"],
            "outcome": item["outcome"],
            "subject_ref": item["subject_ref"],
            "project_entity_ref": item["project_entity_ref"],
            "facet_ref": item["facet_ref"],
            "confidence": item["confidence"],
            "reason_codes": item["reason_codes"],
            "structured_fact": src.get("structured_fact"),
            "text_fact_producer_facts_total": run.synthesis["diagnostics"].get("input_text_atoms"),
        },
        resolution_contract=ctx["typed_resolution_contract"],
    )


def _build_scope_map(run: "Run") -> dict[str, str]:
    """scope_ref → relation_id: сводим по множеству страниц LEFT/RIGHT.

    В артефактах прогона scope_ref и relation_id — разные идентификаторы, прямой
    ссылки между ними нет. Но обе сущности покрывают одну и ту же пару страниц,
    поэтому сопоставление по страницам однозначно (12 scope против 12 групп).
    """
    scope_pages: dict[str, tuple[set, set]] = {}
    for item in run.review_items.values():
        loc = item["provenance"]["source_atom"]["locations"]
        L, R = scope_pages.setdefault(item["scope_ref"], (set(), set()))
        L.update(l["page"] for l in loc.get("LEFT") or [])
        R.update(l["page"] for l in loc.get("RIGHT") or [])

    out: dict[str, str] = {}
    for scope, (L, R) in scope_pages.items():
        best, best_score = None, -1
        for g in run.differences["sheet_groups"]:
            gl, gr = set(g["left_pages"]), set(g["right_pages"])
            score = len(L & gl) + len(R & gr) - 0.01 * len((L | R) - (gl | gr))
            if score > best_score:
                best, best_score = g["id"], score
        out[scope] = best
    return out
