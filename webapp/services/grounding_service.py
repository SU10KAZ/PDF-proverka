"""
Grounding Service — Python-level привязка findings к блокам.

Запускается ПЕРЕД Critic, чтобы уменьшить ложную привязку и дать
Critic более точные данные для проверки.

Стратегия: простой lexical overlap + page-aware ranking.
Не перетирает хорошие existing evidence.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Optional


def _tokenize(text: str) -> list[str]:
    """Простая токенизация: слова из 3+ символов, lowercase."""
    return [w.lower() for w in re.findall(r"[A-Za-zА-Яа-яЁё0-9]{3,}", text)]


def _compute_overlap(tokens_a: list[str], tokens_b: list[str]) -> float:
    """Доля совпадающих токенов (Jaccard-like, но по counts)."""
    if not tokens_a or not tokens_b:
        return 0.0
    ca = Counter(tokens_a)
    cb = Counter(tokens_b)
    intersection = sum((ca & cb).values())
    union = sum((ca | cb).values())
    return intersection / union if union > 0 else 0.0


def _finding_is_well_grounded(finding: dict) -> bool:
    """Проверить, что finding уже хорошо привязан."""
    evidence = finding.get("evidence", [])
    related = finding.get("related_block_ids", [])
    if evidence and any(e.get("type") == "image" for e in evidence):
        return True
    if len(related) >= 1:
        return True
    return False


def compute_grounding_candidates(
    findings: list[dict],
    blocks_analysis: list[dict],
    max_candidates: int = 3,
    min_score: float = 0.05,
) -> list[dict]:
    """Для каждого finding найти лучшие block-кандидаты.

    Args:
        findings: список замечаний из 03_findings.json.
        blocks_analysis: block_analyses из 02_blocks_analysis.json.
        max_candidates: максимум кандидатов на finding.
        min_score: минимальный порог overlap.

    Returns:
        Обогащённый список findings с полем grounding_candidates.
    """
    # Индекс блоков: block_id -> {page, tokens, summary}
    block_index: dict[str, dict] = {}
    for ba in blocks_analysis:
        bid = ba.get("block_id", "")
        if not bid:
            continue
        text_parts = []
        if ba.get("summary"):
            text_parts.append(ba["summary"])
        for f in ba.get("findings", []):
            if f.get("description"):
                text_parts.append(f["description"])
        for kv in ba.get("key_values_read", []):
            if isinstance(kv, str):
                text_parts.append(kv)
            elif isinstance(kv, dict):
                text_parts.append(str(kv.get("value", "")))
        block_index[bid] = {
            "page": ba.get("page", 0),
            "tokens": _tokenize(" ".join(text_parts)),
        }

    for finding in findings:
        # Не трогаем хорошо привязанные findings
        if _finding_is_well_grounded(finding):
            continue

        f_text = " ".join(filter(None, [
            finding.get("problem", ""),
            finding.get("description", ""),
            finding.get("solution", ""),
        ]))
        f_tokens = _tokenize(f_text)
        if not f_tokens:
            continue

        f_page = finding.get("page")
        f_pages = [f_page] if isinstance(f_page, int) else (f_page if isinstance(f_page, list) else [])

        # Ранжируем блоки
        candidates = []
        for bid, binfo in block_index.items():
            score = _compute_overlap(f_tokens, binfo["tokens"])
            # Page bonus: +50% если на той же странице
            if f_pages and binfo["page"] in f_pages:
                score *= 1.5
            if score >= min_score:
                candidates.append({
                    "block_id": bid,
                    "page": binfo["page"],
                    "score": round(score, 4),
                })

        candidates.sort(key=lambda c: c["score"], reverse=True)
        candidates = candidates[:max_candidates]

        if candidates:
            finding["grounding_candidates"] = candidates
            # Если нет related_block_ids — берём лучший кандидат
            if not finding.get("related_block_ids"):
                finding["related_block_ids"] = [candidates[0]["block_id"]]
            # Если нет evidence — добавляем из лучшего кандидата
            if not finding.get("evidence"):
                finding["evidence"] = [{
                    "type": "image",
                    "block_id": candidates[0]["block_id"],
                    "page": candidates[0]["page"],
                    "source": "grounding_service",
                }]

    return findings


def run_grounding(
    findings_path: Path,
    blocks_path: Path,
    output_path: Optional[Path] = None,
) -> dict:
    """Запустить grounding для проекта.

    Читает 03_findings.json и 02_blocks_analysis.json,
    обогащает findings полями grounding_candidates.
    Записывает результат обратно в findings_path (in-place).

    Returns: статистика {total, already_grounded, newly_grounded}.
    """
    if not findings_path.exists() or not blocks_path.exists():
        return {"error": "findings or blocks not found", "total": 0}

    findings_data = json.loads(findings_path.read_text(encoding="utf-8"))
    blocks_data = json.loads(blocks_path.read_text(encoding="utf-8"))

    findings = findings_data.get("findings", findings_data.get("items", []))
    blocks_analysis = blocks_data.get("block_analyses", [])

    already_grounded = sum(1 for f in findings if _finding_is_well_grounded(f))

    findings = compute_grounding_candidates(findings, blocks_analysis)

    newly_grounded = sum(
        1 for f in findings
        if f.get("grounding_candidates") and not _finding_is_well_grounded(f)
    )

    # Записываем обратно
    if "findings" in findings_data:
        findings_data["findings"] = findings
    elif "items" in findings_data:
        findings_data["items"] = findings

    target = output_path or findings_path
    target.write_text(
        json.dumps(findings_data, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    return {
        "total": len(findings),
        "already_grounded": already_grounded,
        "newly_grounded": newly_grounded,
        "grounding_candidates_added": sum(
            1 for f in findings if f.get("grounding_candidates")
        ),
    }
