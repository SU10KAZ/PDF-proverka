#!/usr/bin/env python3
"""CLI shadow-пилота Вектографа «АР. План потолков и освещения».

Детерминированно извлекает из векторного слоя PDF поквартирное описание
(помещения, потолочные зоны, световые выводы, группы, выключатели,
мастер-выключатели, размерные привязки) без LLM/OCR/растра.

Пример:
    python scripts/build_ar_ceiling_lighting_description.py \
      --pdf "<путь к вектор-PDF листа>" \
      --out-dir "experiments/vectograf/ar_ceiling_lighting/<block_id>"
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import (  # noqa: E402
    run_profile, write_artifacts)
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting.registry import (  # noqa: E402
    load_legend_registry)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pdf", required=True, help="путь к вектор-PDF листа")
    parser.add_argument("--out-dir", required=True, help="папка артефактов")
    parser.add_argument("--page", type=int, default=0, help="индекс страницы (default 0)")
    parser.add_argument("--block-id", default=None, help="идентификатор блока для имён файлов")
    parser.add_argument("--legend-registry", default=None,
                        help="путь к legend_registry.json (кросс-листовые эталоны)")
    parser.add_argument("--no-raw-inventory", action="store_true",
                        help="не писать raw_vector_inventory.json")
    parser.add_argument("--markdown-name", default=None,
                        help="имя итогового Markdown (default <block_id>_apartments.md)")
    args = parser.parse_args()

    pdf = Path(args.pdf)
    if not pdf.is_file():
        print(f"ОШИБКА: PDF не найден: {pdf}", file=sys.stderr)
        return 2

    registry_entries = load_legend_registry(args.legend_registry) if args.legend_registry else None
    result = run_profile(str(pdf), page_index=args.page, block_id=args.block_id,
                         legend_registry=registry_entries)
    paths = write_artifacts(result, args.out_dir, pdf_path=str(pdf),
                            include_raw_inventory=not args.no_raw_inventory,
                            markdown_name=args.markdown_name)

    summary = {"status": result["status"], "reason": result.get("reason"),
               "warnings": result.get("warnings") or [], "artifacts": paths,
               "elapsed_s": result.get("elapsed_s")}
    if result.get("graph"):
        summary["validation"] = result["graph"]["validation"]
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0 if result["status"] in ("complete", "partial", "no_graph") else 1


if __name__ == "__main__":
    raise SystemExit(main())
