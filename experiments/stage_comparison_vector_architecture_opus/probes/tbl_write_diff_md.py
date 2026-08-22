"""tbl_write_diff_md — render the headline table-level diff for ss_table_graphic.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_write_diff_md
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_layer as T  # noqa: E402
from experiments.stage_comparison_vector_architecture_opus.probes import tbl_table_diff as D  # noqa: E402

OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"


def main() -> None:
    pairs = json.loads((TRACK_A / "block_pairs.json").read_text())["pairs"]
    pair = next(p for p in pairs if p["pair_id"] == "ss_table_graphic")
    comp = json.loads((TRACK_A / "comparisons/ss_table_graphic/comparison.json").read_text())
    ai = next(p for p in json.loads((TRACK_A / "ai_experiment/vector_output.json").read_text())["pairs"]
              if p["pair_id"] == "ss_table_graphic")

    tables = {}
    for side in ("left", "right"):
        info = pair[side]
        doc = fitz.open(str(ROOT / info["pdf"]))
        page = doc[info["page_index"]]
        w, h = page.rect.width, page.rect.height
        b = info["bbox_norm"]
        region = (b[0] * w, b[1] * h, b[2] * w, b[3] * h)
        ts = T.reconstruct(page, region=region)
        tables[side] = ts[0]
        doc.close()

    d = D.diff_tables(tables["left"], tables["right"], left_label="v002", right_label="v003")

    lines = ["# Табличный диф `ss_table_graphic` (v002 ↔ v003, стр. PDF 16)", ""]
    lines.append("## Что говорил Track A")
    lines.append("")
    lines.append("Вектор-плечо AI-эксперимента (`ai_experiment/vector_output.json`):")
    lines.append("")
    for m in ai["major_changes"]:
        lines.append(f"- **{m}**  ← ложное срабатывание")
    lines.append("")
    lines.append("Строки `differences` из `comparisons/ss_table_graphic/comparison.json`:")
    lines.append("")
    for x in comp["differences"]:
        lines.append(f"- `{x}`")
    lines.append("")
    lines.append("## Восстановленная таблица, левая сторона (v002)")
    lines.append("")
    lines.append("| Поз. | Наименование |")
    lines.append("|---|---|")
    for row in T.table_rows(tables["left"]):
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    lines.append("## Восстановленная таблица, правая сторона (v003)")
    lines.append("")
    lines.append("| Поз. | Наименование |")
    lines.append("|---|---|")
    for row in T.table_rows(tables["right"]):
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    lines.append("## Диф на уровне таблицы")
    lines.append("")
    lines.append(f"- Вердикт: **{d['verdict']}**")
    lines.append(f"- Выравнивание строк: по ключу первой колонки (`{d['row_alignment']}`)")
    lines.append(f"- Форма: {d['left_shape']} → {d['right_shape']}")
    lines.append(f"- Открытые (обрезанные) стороны: left={tables['left']['open_sides']}, "
                 f"right={tables['right']['open_sides']}")
    lines.append("")
    for s in d["sentences"]:
        lines.append(f"- {s}")
    lines.append("")
    (OUT / "tbl_diff_ss_table_graphic.md").write_text("\n".join(lines))
    print("\n".join(lines[-12:]))


if __name__ == "__main__":
    main()
