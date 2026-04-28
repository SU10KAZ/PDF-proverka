#!/usr/bin/env python3
"""Compare a Chandra model against existing Gemini Pro reference on audit blocks."""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

import sys

sys.path.insert(0, str(ROOT))

from scripts.run_chandra_v1_diagnostics import (  # noqa: E402
    _chat_image,
    _client_config,
    _extract_output,
    _load_model,
    _request_json,
    _unload_all,
)
from scripts.run_chandra_vision_model_eval import (  # noqa: E402
    _load_blocks_index,
    _prepare_images,
    _safe_model_dir,
    _ts,
)


AUDIT_BLOCKS = [
    "3TFL-TNRF-7G6",
    "66NE-7DYY-GQN",
    "9DH3-MEUR-DR4",
    "947C-9UJT-RYU",
    "9PPJ-YP6U-HV6",
    "97AH-VUFP-LJJ",
    "4CFG-LM4Y-7H4",
    "66M4-Y69W-VCG",
    "6PPA-4DDX-6FR",
    "6R7N-7LRD-AUR",
    "7DJ9-EQQ3-QMK",
    "6P9T-Q7GT-N9J",
]


def _save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_reference(project_dir: Path) -> dict[str, Any]:
    ref = project_dir / "_experiments" / "pro_b6_r800_big_project_validation" / "20260422_210243" / "reference_analyses.json"
    return json.loads(ref.read_text(encoding="utf-8"))


def _parse_json_block(text: str) -> tuple[dict[str, Any] | None, str | None]:
    s = (text or "").strip()
    if not s:
        return None, "empty"
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        val = json.loads(s)
        return val if isinstance(val, dict) else None, None if isinstance(val, dict) else "not_dict"
    except Exception:
        pass
    match = re.search(r"\{.*\}", s, re.S)
    if not match:
        return None, "json_not_found"
    try:
        val = json.loads(match.group(0))
        return val if isinstance(val, dict) else None, None if isinstance(val, dict) else "not_dict"
    except Exception as exc:
        return None, f"parse_error: {exc}"


def _stage02_prompt(block_id: str, label: str, page: int, variant: str = "standard") -> str:
    base = (
        "Проанализируй один фрагмент строительного чертежа и верни только JSON без markdown.\n"
        f"block_id: {block_id}\n"
        f"page: {page}\n"
        f"known_label: {label}\n\n"
        "Нужно вернуть объект строго такого вида:\n"
        "{\n"
        '  "block_id": "...",\n'
        '  "page": 0,\n'
        '  "label": "краткое название блока",\n'
        '  "sheet_type": "plan|section|detail|table|other",\n'
        '  "unreadable_text": false,\n'
        '  "unreadable_details": null,\n'
        '  "summary": "2-4 предложения по сути блока",\n'
        '  "key_values_read": ["читаемое значение с привязкой к элементу"],\n'
        '  "findings": [\n'
        "    {\n"
        '      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",\n'
        '      "category": "категория замечания",\n'
        '      "finding": "конкретная проблема на этом блоке",\n'
        '      "block_evidence": "что именно видно на чертеже",\n'
        '      "value_found": "точная читаемая надпись/значение или null"\n'
        "    }\n"
        "  ]\n"
        "}\n\n"
        "Правила:\n"
        "- Пиши все текстовые поля только по-русски.\n"
        "- Не придумывай нормы и не добавляй norm/norm_quote.\n"
        "- Если значение не читается, пиши 'нечитаемо: ...' в key_values_read, не угадывай.\n"
        "- Если явной проблемы не видно, findings должен быть пустым массивом.\n"
        "- Для таблиц/ведомостей считай только самые важные и репрезентативные строки, не перечисляй всю таблицу.\n"
        "- Для разрезов/схем/планов укажи тип чертежа, основные элементы и 2-5 читаемых деталей.\n"
        "- key_values_read: минимум 5 и максимум 20 пунктов.\n"
        "- findings: максимум 3 замечания.\n"
        "- Не выводи длинные числовые серии и не генерируй однотипные размеры подряд.\n"
    )
    if variant in {"recall_safe", "compact_recall"}:
        base += (
            "- Dense plan/table rule: никогда не перечисляй все марки подряд; выбери только 8-12 репрезентативных значений.\n"
            "- Если на чертеже есть размерные цепочки, суммы участков и общий размер: обязательно проверь арифметическую согласованность.\n"
            "- Если есть проемы/отверстия/арматура возле проемов: проверь неоднозначность выносок, количества стержней, шаг и симметрию маркировки.\n"
            "- Если есть ведомость деталей арматуры: проверь дубли одинаковых позиций, сомнительную геометрию гибов, минимальные прямые участки и округление размеров.\n"
            "- Если несоответствие видно явно из чисел на самом блоке, можно дать КРИТИЧЕСКОЕ или РЕКОМЕНДАТЕЛЬНОЕ.\n"
            "- Если проблема вероятна, но требует сверки со смежными чертежами или спецификацией, используй severity `ПРОВЕРИТЬ ПО СМЕЖНЫМ`.\n"
            "- Не бойся возвращать findings, если противоречие или неоднозначность действительно видны на изображении.\n"
            "- Но не выдумывай отсутствующие узлы, материалы или спецификации только потому, что их нет в кадре.\n"
        )
    if variant == "compact_recall":
        base += (
            "- Режим compact: key_values_read максимум 8 пунктов, findings максимум 2 пункта.\n"
            "- Если на плане десятки однотипных марок, НЕ перечисляй их по одной; опиши диапазон или 3-5 대표тивных марок.\n"
            "- Для ведомостей деталей выбери только 4-6 самых показательных позиций и только те параметры, которые нужны для finding.\n"
            "- Для маркировочных схем и планов: key_values_read держи в пределах 4-6 пунктов; указывай только реальные подписи с чертежа, без искусственной нумерации элементов.\n"
            "- Никогда не создавай строки вида 'Колонна 1', 'Колонна 2', 'Элемент 5', если таких надписей нет буквально на изображении.\n"
            "- Если на плане много одинаковых марок, запиши один агрегированный пункт вроде 'повторяются марки Стм200/Стм250' вместо длинного списка.\n"
            "- Если схема относится сразу к нескольким этажам и на ней видны абсолютные отметки типа '+58,780', обязательно упомяни их как отдельный key_values_read и проверь, не выглядят ли они этаж-специфичными.\n"
            "- Ответ должен быть компактным: summary 2-3 предложения, каждый finding 1-2 предложения.\n"
            "- Если JSON начинает разрастаться, лучше сократи key_values_read, а не findings.\n"
        )
    return base


def _run_one(
    model: str,
    image_path: Path,
    block_id: str,
    page: int,
    label: str,
    timeout: float,
    prompt_variant: str,
    max_output_tokens: int,
) -> dict[str, Any]:
    prompt = _stage02_prompt(block_id, label, page, variant=prompt_variant)
    result = _chat_image(model, image_path, prompt, timeout=timeout, max_output_tokens=max_output_tokens)
    parsed, parse_error = _parse_json_block(result.get("content") or "")
    return {
        **result,
        "parsed": parsed,
        "parse_error": parse_error,
    }


def _keyword_set(text: str) -> set[str]:
    tokens = re.findall(r"[A-Za-zА-Яа-я0-9\-]+", (text or "").lower())
    stop = {"и", "с", "по", "на", "для", "или", "the", "of", "a", "в", "к", "из", "тип", "схема", "чертеж", "чертёжа"}
    return {t for t in tokens if len(t) >= 3 and t not in stop}


def _verdict(reference: dict[str, Any], candidate: dict[str, Any] | None) -> tuple[str, list[str]]:
    notes: list[str] = []
    if not candidate:
        return "fail_parse", ["candidate JSON parse failed"]
    ref_findings = reference.get("findings") or []
    cand_findings = candidate.get("findings") or []
    ref_kv = reference.get("key_values_read") or []
    cand_kv = candidate.get("key_values_read") or []
    summary = candidate.get("summary") or ""

    if ref_findings and not cand_findings:
        notes.append("reference has findings, candidate has none")
        return "likely_degraded", notes
    if len(cand_kv) < max(3, int(0.35 * max(1, len(ref_kv)))):
        notes.append("candidate key_values_read much shorter than reference")
        return "likely_degraded", notes
    overlap = len(_keyword_set(reference.get("label") or "") & _keyword_set((candidate.get("label") or "") + " " + summary))
    if overlap == 0:
        notes.append("candidate label/summary poorly aligned with reference block type")
        return "uncertain", notes
    if len(cand_findings) > len(ref_findings) + 1:
        notes.append("candidate produced more findings than reference; may be improved or inflated")
        return "uncertain", notes
    if len(cand_kv) > len(ref_kv) and len(cand_findings) >= len(ref_findings):
        notes.append("candidate preserved findings presence and captured more key values")
        return "likely_improved", notes
    notes.append("candidate preserved drawing type and findings presence")
    return "equivalent", notes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--model", default="qwen/qwen3.6-35b-a3b")
    ap.add_argument("--image-max-side", type=int, default=1024)
    ap.add_argument("--context-length", type=int, default=8192)
    ap.add_argument("--load-timeout", type=float, default=900)
    ap.add_argument("--chat-timeout", type=float, default=480)
    ap.add_argument("--prompt-variant", default="standard", choices=["standard", "recall_safe", "compact_recall"])
    ap.add_argument("--max-output-tokens", type=int, default=1600)
    ap.add_argument("--block-id", action="append", dest="block_ids")
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    exp_dir = project_dir / "_experiments" / "chandra_stage02_reference_compare" / _ts()
    exp_dir.mkdir(parents=True, exist_ok=True)

    blocks = _load_blocks_index(project_dir)
    block_ids = args.block_ids or AUDIT_BLOCKS
    prepared = _prepare_images(
        project_dir=project_dir,
        exp_dir=exp_dir / "prepared",
        blocks=blocks,
        block_ids=block_ids,
        native_crops=True,
        native_max_long_side=args.image_max_side,
    )
    prepared_by_id = {img.block_id: img for img in prepared}
    ref = _load_reference(project_dir)

    _save_json(exp_dir / "manifest.json", {
        "project_dir": str(project_dir),
        "model": args.model,
        "audit_blocks": block_ids,
        "image_max_side": args.image_max_side,
        "api": "/api/v1/chat",
        "reasoning": "off",
        "prompt_variant": args.prompt_variant,
        "max_output_tokens": args.max_output_tokens,
    })

    _unload_all(exp_dir, "initial_cleanup")
    rows: list[dict[str, Any]] = []
    compare_rows: list[dict[str, Any]] = []
    try:
        load = _load_model(args.model, context_length=args.context_length, timeout=args.load_timeout)
        rows.append({"request": "load", "model": args.model, **load.__dict__})
        if not load.ok:
            raise RuntimeError(f"load failed: {load.error}")

        for bid in block_ids:
            img = prepared_by_id[bid]
            raw = _run_one(
                args.model,
                img.image_file,
                bid,
                img.page,
                img.label,
                args.chat_timeout,
                args.prompt_variant,
                args.max_output_tokens,
            )
            raw["model"] = args.model
            raw["block_id"] = bid
            raw["width"] = img.width
            raw["height"] = img.height
            rows.append(raw)

            verdict, notes = _verdict(ref[bid], raw.get("parsed"))
            cand = raw.get("parsed") or {}
            compare_rows.append({
                "block_id": bid,
                "reference_label": ref[bid].get("label"),
                "candidate_label": cand.get("label"),
                "reference_findings": len(ref[bid].get("findings") or []),
                "candidate_findings": len(cand.get("findings") or []),
                "reference_kv": len(ref[bid].get("key_values_read") or []),
                "candidate_kv": len(cand.get("key_values_read") or []),
                "elapsed_s": raw.get("elapsed_s"),
                "parse_ok": raw.get("parsed") is not None,
                "verdict": verdict,
                "notes": notes,
                "reference_summary": ref[bid].get("summary"),
                "candidate_summary": cand.get("summary"),
            })
            _save_json(exp_dir / "per_block" / f"{bid}.json", {
                "reference": ref[bid],
                "candidate_raw": raw,
                "compare": compare_rows[-1],
            })
    finally:
        _unload_all(exp_dir, "final_cleanup")

    _save_json(exp_dir / "candidate_results.json", rows)
    _save_json(exp_dir / "comparison.json", compare_rows)

    verdict_counts: dict[str, int] = {}
    for row in compare_rows:
        verdict_counts[row["verdict"]] = verdict_counts.get(row["verdict"], 0) + 1
    avg_elapsed = round(statistics.mean(r["elapsed_s"] for r in compare_rows if isinstance(r.get("elapsed_s"), (int, float))), 2)
    lines = [
        "# Chandra Stage02 Reference Compare",
        "",
        f"- Model: `{args.model}`",
        f"- Image max side: `{args.image_max_side}`",
        f"- Blocks: `{len(compare_rows)}`",
        f"- Avg elapsed: `{avg_elapsed}` sec/block",
        f"- Verdict counts: `{verdict_counts}`",
        "",
        "| Block | Verdict | Ref findings | Cand findings | Ref KV | Cand KV | Notes |",
        "|---|---|---:|---:|---:|---:|---|",
    ]
    for row in compare_rows:
        lines.append(
            f"| `{row['block_id']}` | `{row['verdict']}` | {row['reference_findings']} | "
            f"{row['candidate_findings']} | {row['reference_kv']} | {row['candidate_kv']} | "
            f"{'; '.join(row['notes'])} |"
        )
    (exp_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    side = ["# Side By Side", ""]
    for row in compare_rows:
        side.extend([
            f"## {row['block_id']}",
            "",
            f"- Verdict: `{row['verdict']}`",
            f"- Notes: {'; '.join(row['notes'])}",
            f"- Reference label: {row['reference_label']}",
            f"- Candidate label: {row['candidate_label']}",
            f"- Reference summary: {row['reference_summary']}",
            f"- Candidate summary: {row['candidate_summary']}",
            "",
        ])
    (exp_dir / "side_by_side.md").write_text("\n".join(side) + "\n", encoding="utf-8")

    rec_lines = [
        "# Recommendation",
        "",
        f"Model tested: `{args.model}`",
        "",
    ]
    degraded = verdict_counts.get("likely_degraded", 0) + verdict_counts.get("fail_parse", 0)
    if degraded == 0 and verdict_counts.get("equivalent", 0) + verdict_counts.get("likely_improved", 0) >= 8:
        rec_lines.append("Stage-02 style subset result is promising. The model is worth a deeper quality-validation pass against Gemini reference.")
    else:
        rec_lines.append("Stage-02 style subset result is mixed. The model can see drawings, but semantic parity with Gemini reference is not yet proven.")
    (exp_dir / "winner_recommendation.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")

    print(f"Artifacts: {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
