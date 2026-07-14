#!/usr/bin/env python3
"""Пересобрать русские Markdown-описания в уже готовых графовых артефактах.

Геометрия, машинные типы и связи не пересчитываются. Скрипт меняет только
`markdown` и соответствующий фрагмент `user_text`, поэтому безопасен для
массового обновления существующих проектов после улучшения терминологии.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path


def _renderer(source_kind: str, profile_id: str):
    if source_kind == "structured_singleline":
        from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import render_graph_etalon_markdown
        return render_graph_etalon_markdown
    if source_kind == "structured_electrical":
        from backend.app.pipeline.stages.block_grounding.electrical_geometry import render_electrical_markdown
        return render_electrical_markdown
    if source_kind == "structured_general_plan":
        from backend.app.pipeline.stages.block_grounding.general_plan_geometry import render_gp_markdown
        return render_gp_markdown
    if source_kind == "structured_architecture":
        from backend.app.pipeline.stages.block_grounding.architecture_geometry import render_ar_markdown
        return render_ar_markdown
    if source_kind == "structured_structure":
        from backend.app.pipeline.stages.block_grounding.structural_geometry import render_structural_markdown
        return render_structural_markdown
    if source_kind == "structured_technology":
        from backend.app.pipeline.stages.block_grounding.technology_geometry import render_tx_markdown
        return render_tx_markdown
    if source_kind == "structured_hvac":
        from backend.app.pipeline.stages.block_grounding.hvac_geometry import render_hvac_markdown
        return render_hvac_markdown
    if source_kind == "structured_water":
        from backend.app.pipeline.stages.block_grounding.water_geometry import render_water_markdown
        return render_water_markdown
    if source_kind == "structured_alia_scheme":
        from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import (
            ALL_REMAINING_PROFILES, render_remaining_markdown,
        )
        if profile_id in ALL_REMAINING_PROFILES:
            return render_remaining_markdown
        from backend.app.pipeline.stages.block_grounding.alia_scheme_geometry import render_alia_scheme_markdown
        return render_alia_scheme_markdown
    return None


def _replace_prompt_markdown(user_text: str | None, old: str | None, new: str) -> str | None:
    if not user_text:
        return user_text
    if old and old in user_text:
        return user_text.replace(old, new, 1)
    marker = "\n\n## Задача:\n"
    if marker in user_text:
        head, task = user_text.split(marker, 1)
        first_break = head.find("\n\n")
        prefix = head[:first_break] if first_break >= 0 else head
        return prefix + "\n\n" + new + marker + task
    return user_text


def localize_file(path: Path, *, dry_run: bool = False) -> str:
    try:
        package = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "invalid_json"
    graph = package.get("graph")
    source = str(package.get("source_kind") or "")
    profile = str(package.get("profile_id") or "")
    renderer = _renderer(source, profile)
    if renderer is None or not isinstance(graph, dict):
        return "skipped"
    try:
        markdown = renderer(graph)
    except Exception:
        return "render_error"
    if not markdown:
        return "empty_render"
    old = package.get("markdown")
    new_user_text = _replace_prompt_markdown(package.get("user_text"), old, markdown)
    if old == markdown and new_user_text == package.get("user_text"):
        return "unchanged"
    if dry_run:
        return "would_update"
    package["markdown"] = markdown
    package["user_text"] = new_user_text
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(package, ensure_ascii=False, default=str, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temp, path)
    return "updated"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("projects_v2"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    counts: Counter[str] = Counter()
    for path in args.root.rglob("block_vector_graphs/*.json"):
        counts[localize_file(path, dry_run=args.dry_run)] += 1
    print(json.dumps(dict(sorted(counts.items())), ensure_ascii=False, indent=2))
    return 1 if counts["render_error"] or counts["invalid_json"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
