"""Единый реестр профильных графов и эталонных блоков.

Профиль (`profile_id`) определяет не внешний вид UI, а предметную грамматику графа:
какие узлы, контейнеры, сети и доказанные рёбра допустимы для данного типа блока.
Эталон выбирается из проверенного корпуса по точному `profile_id`; его данные не
копируются в новый граф и используются только как ссылка на принятую грамматику.
"""
from __future__ import annotations

import collections
import json
import math
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from backend.app.pipeline.stages.block_context.reference_catalog import (
    catalog_runtime_info,
    load_catalog_manifest,
    load_reference_records,
    load_reference_rules,
)


# v4: эталон выбирается не только по профилю, но и по смысловому описанию,
# подтипу и лёгкой структурной сигнатуре текущего графа.
# v5: текст и геометрия блока клипуются строго по его полигону/прямоугольнику;
# пакеты v4 могли содержать подписи соседних блоков из внешнего запаса 1%.
# v6: эталоны и правила перенесены во встроенный каталог pipeline-stage;
# сохранённые v5-пакеты могли ссылаться на исследовательскую папку experiments.
SCHEMA_VERSION = 6
ARTIFACT_DIRNAME = "block_vector_graphs"

DISCIPLINE_TITLES = {
    "ЭОМ": "Электроснабжение и электрооборудование",
    "ГП": "Генеральный план",
    "АР": "Архитектурные решения",
    "КЖ": "Железобетонные конструкции",
    "КМ": "Металлические конструкции",
    "ТХ": "Технологические решения",
    "ОВ": "Отопление и вентиляция",
    "ВК": "Водоснабжение и канализация",
    "СС": "Слаботочные системы и автоматизация",
}

SOURCE_DISCIPLINES = {
    "structured_singleline": "ЭОМ",
    "structured_electrical": "ЭОМ",
    "structured_general_plan": "ГП",
    "structured_architecture": "АР",
    "structured_structure": "КЖ",
    "structured_technology": "ТХ",
    "structured_hvac": "ОВ",
    "structured_water": "ВК",
    "structured_alia_scheme": "СС",
}


_REFERENCE_RULES = load_reference_rules()
_SELECTION_RULES = _REFERENCE_RULES.get("selection") or {}
_REFERENCE_STOP_WORDS = set(_REFERENCE_RULES.get("semantic_stop_words") or [])
_REFERENCE_GENERIC_SUBTYPES = set(_REFERENCE_RULES.get("generic_subtypes") or [])


def _catalog_file(discipline: str) -> str:
    meta = (load_catalog_manifest().get("disciplines") or {}).get(discipline) or {}
    return f"pipeline:block_context/reference_catalog/{meta.get('file') or 'manifest.json'}"


def _reference_tokens(value: str) -> list[str]:
    """Небольшой независимый от внешних библиотек нормализатор описаний."""
    words = re.findall(r"[a-zа-я0-9]+", str(value or "").lower().replace("ё", "е"))
    result = []
    endings = (
        "иями", "ями", "ами", "ого", "ему", "ому", "ыми", "ими", "ий", "ый", "ой",
        "ая", "яя", "ое", "ее", "ых", "их", "ов", "ев", "ей", "ам", "ям", "ах", "ях",
        "ом", "ем", "ую", "юю", "ия", "ие", "ию", "ии", "иям", "иях", "ами", "ями",
        "а", "я", "ы", "и", "у", "ю", "е", "о",
    )
    for word in words:
        if len(word) < 3 or word in _REFERENCE_STOP_WORDS or word.isdigit():
            continue
        stem = word
        if not re.fullmatch(r"[a-z0-9]+", word):
            for ending in endings:
                if len(word) - len(ending) >= 4 and word.endswith(ending):
                    stem = word[:-len(ending)]
                    break
        result.append(stem)
    return result


def _cosine(left: collections.Counter, right: collections.Counter) -> float:
    if not left or not right:
        return 0.0
    common = set(left) & set(right)
    dot = sum(float(left[key]) * float(right[key]) for key in common)
    lnorm = math.sqrt(sum(float(value) ** 2 for value in left.values()))
    rnorm = math.sqrt(sum(float(value) ** 2 for value in right.values()))
    return dot / (lnorm * rnorm) if lnorm and rnorm else 0.0


def _candidate_description_value(value: Any) -> str:
    """В корпусах встречаются и строки, и старые JSON-описания блока."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        preferred = []
        for key in ("short_description", "content_summary", "description", "summary", "title"):
            text = value.get(key)
            if isinstance(text, str) and text.strip():
                preferred.append(text.strip())
        return " ".join(preferred)
    return ""


@lru_cache(maxsize=1)
def _reference_candidates() -> dict[tuple[str, str], tuple[dict[str, Any], ...]]:
    """Все кандидаты встроенного production-каталога, сгруппированные по профилю."""
    grouped: dict[tuple[str, str], dict[str, dict[str, Any]]] = collections.defaultdict(dict)
    for item in load_reference_records():
        discipline = str(item.get("discipline") or "").strip()
        block_id = str(item.get("block_id") or "").strip()
        profile_id = str(item.get("profile_id") or "").strip()
        if not discipline or not block_id or not profile_id:
            continue
        text = _candidate_description_value(item.get("description"))
        quality_raw = item.get("quality") or [0, 0, 0]
        quality = tuple(int(value or 0) for value in quality_raw[:3])
        if len(quality) < 3:
            quality = (*quality, *(0 for _ in range(3 - len(quality))))
        grouped[(discipline, profile_id)][block_id] = {
            "block_id": block_id,
            "profile_id": profile_id,
            "discipline": discipline,
            "subtype": str(item.get("subtype") or "").strip(),
            "description": text,
            "tokens": tuple(_reference_tokens(text)),
            "quality": quality,
            "coverage_file": _catalog_file(discipline),
            "source_layer_state": str(item.get("source_layer_state") or "unknown"),
            "structure_signature": item.get("structure_signature") or {},
        }
    return {key: tuple(sorted(items.values(), key=lambda value: value["block_id"]))
            for key, items in grouped.items()}


def _graph_signature(graph: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not isinstance(graph, dict):
        return {}
    node_types = collections.Counter(
        str(node.get("node_type") or "") for node in graph.get("nodes") or []
        if isinstance(node, dict) and node.get("node_type")
    )
    validation = graph.get("validation") or {}
    counts = {
        "nodes_total": len(graph.get("nodes") or []),
        "containers_total": len(graph.get("containers") or []),
        "networks_total": len(graph.get("networks") or []),
        "edges_total": len(graph.get("edges") or []),
    }
    for key, value in validation.items():
        if (key.endswith("_total") or key.endswith("_segments_total")) and isinstance(value, (int, float)):
            counts[str(key)] = max(0.0, float(value))
    if not node_types and not any(float(value) > 0 for value in counts.values()):
        return {}
    return {"node_types": node_types, "counts": counts}


def _structure_similarity(left: dict[str, Any], right: dict[str, Any]) -> float:
    if not left or not right:
        return 0.0
    type_score = _cosine(left.get("node_types") or collections.Counter(),
                         right.get("node_types") or collections.Counter())
    left_counts, right_counts = left.get("counts") or {}, right.get("counts") or {}
    common = [key for key in left_counts.keys() & right_counts.keys()
              if max(float(left_counts[key]), float(right_counts[key])) > 0]
    count_scores = []
    for key in common:
        a, b = math.log1p(float(left_counts[key])), math.log1p(float(right_counts[key]))
        count_scores.append(1.0 - abs(a - b) / max(a, b, 1.0))
    count_score = sum(count_scores) / len(count_scores) if count_scores else 0.0
    if type_score and count_scores:
        return 0.65 * type_score + 0.35 * count_score
    return type_score or count_score


def _subtype_similarity(current: str, candidate: str) -> float:
    left = str(current or "").strip().lower().replace("-", "_").replace(" ", "_")
    right = str(candidate or "").strip().lower().replace("-", "_").replace(" ", "_")
    if left in _REFERENCE_GENERIC_SUBTYPES or right in _REFERENCE_GENERIC_SUBTYPES:
        return 0.0
    if left == right:
        return 1.0
    if left in right or right in left:
        return 0.75
    ls, rs = set(left.split("_")), set(right.split("_"))
    return len(ls & rs) / len(ls | rs) if ls and rs else 0.0


def _semantic_similarities(current_text: str, candidates: tuple[dict[str, Any], ...]) -> dict[str, float]:
    current = _reference_tokens(current_text)
    if not current:
        return {candidate["block_id"]: 0.0 for candidate in candidates}
    documents = [set(candidate.get("tokens") or ()) for candidate in candidates]
    df = collections.Counter(token for document in documents for token in document)
    total = len(candidates)
    def vector(tokens):
        counts = collections.Counter(tokens)
        return collections.Counter({token: count * (math.log((total + 1) / (df.get(token, 0) + 1)) + 1.0)
                                    for token, count in counts.items()})
    current_vector = vector(current)
    return {candidate["block_id"]: _cosine(current_vector, vector(candidate.get("tokens") or ()))
            for candidate in candidates}


@lru_cache(maxsize=1)
def _reference_index() -> dict[tuple[str, str], dict[str, Any]]:
    """Наиболее полный канонический пример каждого профиля встроенного каталога."""
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for key, candidates in _reference_candidates().items():
        if not candidates:
            continue
        winner = max(candidates, key=lambda item: (item["quality"], item["block_id"]))
        result[key] = {
            "block_id": winner["block_id"],
            "profile_id": winner["profile_id"],
            "discipline": winner["discipline"],
            "coverage_file": winner["coverage_file"],
            "source_layer_state": winner["source_layer_state"],
            "covered_facts": winner["quality"][1],
            "selection": "точное совпадение профиля; наиболее полный пример встроенного каталога",
        }
    return result


def select_reference(
    profile_id: Optional[str], discipline: Optional[str], *, source_kind: str,
    graph: Optional[dict[str, Any]] = None,
    classification: Optional[dict[str, Any]] = None,
    current_block_id: Optional[str] = None,
) -> dict[str, Any]:
    """Выбрать ближайший проверенный эталон внутри дисциплины и профиля."""
    profile = str(profile_id or "").strip() or "raw_vector"
    code = str(discipline or SOURCE_DISCIPLINES.get(source_kind) or "ОБЩ").strip()
    exact = _reference_index().get((code, profile))
    candidates = _reference_candidates().get((code, profile)) or ()
    if (_SELECTION_RULES.get("exclude_current_block", True)
            and current_block_id and len(candidates) > 1):
        alternatives = tuple(candidate for candidate in candidates
                             if candidate["block_id"] != str(current_block_id))
        if alternatives:
            candidates = alternatives
    classification = classification if isinstance(classification, dict) else {}
    semantic_text = " ".join(str(classification.get(key) or "") for key in (
        "block_type", "short_description", "description", "block_title",
    )).strip()
    subtype = str(((graph or {}).get("validation") or {}).get("subtype") or "").strip()
    current_signature = _graph_signature(graph)
    has_dynamic_context = bool(_reference_tokens(semantic_text)) or (
        subtype.lower().replace(" ", "_") not in _REFERENCE_GENERIC_SUBTYPES
    ) or bool(current_signature)
    if candidates and has_dynamic_context:
        # Сначала исключаем кандидатов с известными потерями фактов, если в том же
        # профиле есть полностью проверенные альтернативы.
        complete = [candidate for candidate in candidates if candidate["quality"][0] == 1]
        pool = tuple(
            complete if complete and _SELECTION_RULES.get("prefer_complete_candidates", True)
            else candidates
        )
        semantic = _semantic_similarities(semantic_text, pool)
        max_covered = max((candidate["quality"][1] for candidate in pool), default=0) or 1
        max_evidence = max((candidate["quality"][2] for candidate in pool), default=0) or 1
        ranked = []
        for candidate in pool:
            subtype_score = _subtype_similarity(subtype, candidate.get("subtype") or "")
            candidate_signature = candidate.get("structure_signature") or {}
            structure_score = _structure_similarity(current_signature, candidate_signature)
            quality_tuple = candidate["quality"]
            quality_weights = _SELECTION_RULES.get("quality_weights") or {}
            quality_score = (
                float(quality_weights.get("complete", 0.5)) * float(bool(quality_tuple[0]))
                + float(quality_weights.get("covered_facts", 0.3))
                * min(1.0, quality_tuple[1] / max_covered)
                + float(quality_weights.get("evidence", 0.2))
                * min(1.0, quality_tuple[2] / max_evidence)
            )
            factors = {
                "semantic": semantic.get(candidate["block_id"], 0.0),
                "subtype": subtype_score,
                "structure": structure_score,
                "quality": quality_score,
            }
            # Подтип уже получен дисциплинарным классификатором и потому надёжнее
            # отдельных совпавших слов длинного описания.
            configured_weights = _SELECTION_RULES.get("weights") or {}
            weights = {
                key: float(configured_weights.get(key, default))
                for key, default in {
                    "semantic": 0.35, "subtype": 0.40,
                    "structure": 0.15, "quality": 0.10,
                }.items()
            }
            if not semantic_text:
                weights["semantic"] = 0.0
            if subtype.lower().replace(" ", "_") in _REFERENCE_GENERIC_SUBTYPES:
                weights["subtype"] = 0.0
            if not current_signature or not candidate_signature:
                weights["structure"] = 0.0
            weight_total = sum(weights.values()) or 1.0
            score = sum(factors[key] * weights[key] for key in factors) / weight_total
            ranked.append((score, quality_tuple, candidate, factors))
        ranked.sort(key=lambda item: (item[0], item[1], item[2]["block_id"]), reverse=True)
        score, _, winner, factors = ranked[0]
        strongest_match = max(factors["semantic"], factors["subtype"], factors["structure"])
        if strongest_match >= float(_SELECTION_RULES.get("strongest_match_min", 0.08)):
            alternatives = []
            top_alternatives = max(0, int(_SELECTION_RULES.get("top_alternatives", 3)))
            for alternative_score, _, alternative, alternative_factors in ranked[1:1 + top_alternatives]:
                alternatives.append({
                    "block_id": alternative["block_id"],
                    "subtype": alternative.get("subtype") or None,
                    "match_score": round(alternative_score, 4),
                    "match_factors": {
                        key: round(value, 4) for key, value in alternative_factors.items()
                    },
                })
            return {
                "block_id": winner["block_id"],
                "profile_id": profile,
                "discipline": code,
                "subtype": winner.get("subtype") or None,
                "coverage_file": winner.get("coverage_file"),
                "source_layer_state": winner.get("source_layer_state"),
                "covered_facts": winner["quality"][1],
                "selection": "точное совпадение дисциплины и профиля; лучший эталон по смыслу, подтипу и структуре",
                "selection_mode": "dynamic_similarity",
                "match_score": round(score, 4),
                "candidate_count": len(pool),
                "match_factors": {key: round(value, 4) for key, value in factors.items()},
                "selection_confidence": (
                    "single_candidate" if len(pool) == 1 else
                    "high" if score >= float(
                        (_SELECTION_RULES.get("confidence") or {}).get("high_min", 0.55)
                    ) else "medium" if score >= float(
                        (_SELECTION_RULES.get("confidence") or {}).get("medium_min", 0.35)
                    ) else "low"
                ),
                "alternatives": alternatives,
                "explanation": (
                    f"Сравнено эталонов: {len(pool)}; смысл: {factors['semantic']:.2f}; "
                    f"подтип: {factors['subtype']:.2f}; структура: {factors['structure']:.2f}; "
                    f"полнота: {factors['quality']:.2f}."
                ),
            }
    if exact:
        result = dict(exact)
        result.update({
            "selection_mode": "canonical_profile_fallback",
            "match_score": None,
            "candidate_count": len(candidates),
            "explanation": "Недостаточно признаков текущего блока; выбран наиболее полный эталон профиля.",
        })
        return result
    return {
        "block_id": None,
        "profile_id": profile,
        "discipline": code,
        "coverage_file": None,
        "source_layer_state": None,
        "covered_facts": 0,
        "selection": "встроенная грамматика профиля; корпусной блок не найден",
    }
def make_package(
    *,
    block_id: str,
    page: Any,
    source_kind: str,
    user_text: Optional[str],
    graph: Optional[dict[str, Any]] = None,
    markdown: Optional[str] = None,
    gate: Optional[dict[str, Any]] = None,
    discipline: Optional[str] = None,
    profile_id: Optional[str] = None,
    classification: Optional[dict[str, Any]] = None,
    error: Optional[str] = None,
) -> dict[str, Any]:
    graph = graph if isinstance(graph, dict) else None
    profile = str(profile_id or (graph or {}).get("profile_id") or "").strip() or None
    code = discipline or SOURCE_DISCIPLINES.get(source_kind)
    effective_classification = classification or (graph or {}).get("classification") or {}
    try:
        reference = select_reference(
            profile, code, source_kind=source_kind,
            graph=graph, classification=effective_classification,
            current_block_id=block_id,
        )
    except Exception:
        # Подбор ближайшего эталона улучшает представление, но не является
        # обязательным этапом построения графа. Повреждённый или частично
        # заполненный корпус не должен лишать пользователя самого результата.
        reference = select_reference(profile, code, source_kind=source_kind)
        reference["selection_mode"] = "canonical_fallback_after_selection_error"
        reference["explanation"] = (
            "Не удалось сравнить эталоны; выбран наиболее полный эталон "
            "того же профиля. Векторный граф блока сохранён."
        )
    readiness = (graph or {}).get("readiness") or {}
    validation = (graph or {}).get("validation") or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "block_id": str(block_id),
        "page": page,
        "source_kind": source_kind,
        "discipline": code,
        "discipline_title": DISCIPLINE_TITLES.get(str(code), str(code or "Общий профиль")),
        "profile_id": profile,
        "classification": effective_classification,
        "reference_catalog": catalog_runtime_info(),
        "reference": reference,
        "gate": gate or {},
        "readiness": readiness,
        "validation": validation,
        "graph": graph,
        "markdown": markdown,
        "user_text": user_text,
        "error": error,
    }


def artifact_filename(block_id: str) -> str:
    safe = re.sub(r"[^0-9A-Za-zА-Яа-я._-]+", "_", str(block_id)).strip("._")
    return f"{safe or 'block'}.json"


def artifact_path(output_dir: Path, block_id: str) -> Path:
    return Path(output_dir) / ARTIFACT_DIRNAME / artifact_filename(block_id)


def load_prepared_package(output_dir: Path, block_id: str) -> Optional[dict[str, Any]]:
    path = artifact_path(output_dir, block_id)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        isinstance(payload, dict)
        and payload.get("schema_version") == SCHEMA_VERSION
        and str(payload.get("block_id")) == str(block_id)
        and payload.get("source_kind")
    ):
        return payload
    return None
