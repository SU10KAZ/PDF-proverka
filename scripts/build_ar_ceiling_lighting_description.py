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
    build_ar_ceiling_lighting_result, write_artifacts)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--pdf", required=True, help="путь к вектор-PDF листа")
    parser.add_argument("--out-dir", required=True, help="папка артефактов")
    parser.add_argument("--page", type=int, default=0, help="индекс страницы (default 0)")
    parser.add_argument("--block-id", default=None, help="идентификатор блока для имён файлов")
    args = parser.parse_args()

    pdf = Path(args.pdf)
    if not pdf.is_file():
        print(f"ОШИБКА: PDF не найден: {pdf}", file=sys.stderr)
        return 2

    result = build_ar_ceiling_lighting_result(str(pdf), page_index=args.page,
                                              block_id=args.block_id)
    paths = write_artifacts(result, args.out_dir, pdf_path=str(pdf))

    v = result["graph"]["validation"]
    print(json.dumps({"artifacts": paths, "validation": v,
                      "elapsed_s": result["elapsed_s"]}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
