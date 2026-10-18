"""Build a local preview from the synthetic MD and saved Luna answers.

No model calls, PDF reads, project data writes or external assets. The saved
answers must match the checked-in synthetic source exactly and pass validation.
Mount the output directory at /specifications on the existing local stand.
"""
from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from backend.app.services.version_comparison.specification_compare import (  # noqa: E402
    change_entries, compare, render_entry, render_markdown,
)
from backend.app.services.version_comparison.specification_run import prepare  # noqa: E402
from backend.app.services.version_comparison.specification_source import validate_extraction  # noqa: E402


def esc(value) -> str:
    return html.escape(str(value))


def locations(row: dict) -> str:
    refs = dict.fromkeys((ref.get("sheet"), ref["page"]) for ref in row["sources"])
    return "; ".join(f"л.{sheet}, стр. {page}" if sheet else f"стр. {page}" for sheet, page in refs)


def source_table(rows: list[dict]) -> str:
    columns = [("pos", "Поз."), ("name", "Наименование"), ("mark", "Марка"),
               ("code", "Код"), ("supplier", "Поставщик"), ("unit", "Ед."),
               ("qty", "Кол-во"), ("mass", "Масса"), ("note", "Примечание")]
    header = "".join(f"<th scope='col'>{label}</th>" for _, label in columns)
    body = []
    for row in rows:
        cells = "".join(f"<td>{esc(row['fields'].get(key) or '—')}</td>" for key, _ in columns)
        body.append(f"<tr>{cells}<td>{esc(locations(row))}</td></tr>")
    return f"<div class='table-scroll'><table><thead><tr>{header}<th scope='col'>Источник</th></tr></thead><tbody>{''.join(body)}</tbody></table></div>"


def build(saved_run: Path, output: Path) -> dict:
    fixture = REPO / "tests/fixtures/whole_specification"
    pair = prepare(fixture / "manifest.json")["pairs"][0]
    extracted = {}
    for side in ("old", "new"):
        source = pair[side]
        matches = []
        for answer_path in sorted((saved_run / "calls").glob("*/answer.json")):
            saved_source = json.loads((answer_path.parent / "source.json").read_text(encoding="utf-8"))
            if (saved_source.get("source_sha256") == source["source_sha256"]
                    and saved_source.get("blocks") == source["blocks"]):
                checked = validate_extraction(source, json.loads(answer_path.read_text(encoding="utf-8")))
                if checked["complete"]:
                    matches.append(checked)
        if len(matches) != 1:
            raise ValueError(f"{side}: нужен один полный сохранённый ответ на точный синтетический MD, найдено {len(matches)}")
        extracted[side] = matches[0]
    result = compare(extracted["old"], extracted["new"])
    if not result["complete"] or result["unresolved"]:
        raise ValueError("Пример не прошёл проверку полного сравнения")
    entries = change_entries(result)
    markdown = render_markdown(result)
    changes = "".join(f"<li>{esc(html.unescape(render_entry(entry)))}</li>" for entry in entries)
    unchanged = []
    for row in result["pairs"]:
        if row["unchanged"]:
            fields = row["new"]["fields"]
            unchanged.append(f"<li><strong>{esc(fields['name'])}</strong> · {esc(fields['qty'])} {esc(fields['unit'])}"
                             f"<span>{esc(locations(row['old']))} → {esc(locations(row['new']))}</span></li>")
    template = (REPO / "scripts/templates/whole_specification_preview.html").read_text(encoding="utf-8")
    tokens = {"CHANGES": changes, "COUNT": str(len(entries)), "UNCHANGED": "".join(unchanged),
              "UNCHANGED_COUNT": str(len(unchanged)), "MARKDOWN": esc(markdown)}
    for side in ("old", "new"):
        tokens[side.upper() + "_TABLE"] = source_table(extracted[side]["rows"])
        tokens[side.upper() + "_MD"] = esc((fixture / f"{side}_results.md").read_text(encoding="utf-8"))
        tokens[side.upper() + "_PAGES"] = ", ".join(map(str, pair[side]["selection"]["pages"]))
    for token, value in tokens.items():
        template = template.replace("@@" + token + "@@", value)
    output.mkdir(parents=True, exist_ok=True)
    (output / "index.html").write_text(template, encoding="utf-8")
    (output / "report.md").write_text(markdown, encoding="utf-8")
    return {"output": str(output), "changes": len(entries), "unchanged": len(unchanged), "model_calls": 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--saved-run", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(args.saved_run, args.output), ensure_ascii=False))
