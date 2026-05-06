#!/usr/bin/env python3
"""Build a local HTML QA report for Qwen block enrichment output."""

from __future__ import annotations

import argparse
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BLOCK_RE = re.compile(r"^### BLOCK \[(?P<block_type>[A-Z]+)\]: (?P<block_id>[A-Z0-9-]+)\s*$", re.M)
ENRICHED_RE = re.compile(r"^\*\*\[ENRICHED (?P<label>.+?)\]\*\*\s*$", re.M)

REASONING_MARKERS = (
    "Self-Correction",
    "Output Generation",
    "Schema check",
    "Final Check",
    "Proceeds",
    "I will",
    "Wait,",
    "[Done]",
)


@dataclass
class BlockMd:
    original: str
    enriched: str
    enriched_label: str


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def find_document_md(project_dir: Path, summary: dict[str, Any]) -> Path:
    md_path = summary.get("md_path")
    if md_path:
        candidate = Path(md_path)
        if candidate.exists():
            return candidate
        candidate = project_dir / md_path
        if candidate.exists():
            return candidate
    matches = sorted(project_dir.glob("*_document.md"))
    if not matches:
        raise FileNotFoundError(f"*_document.md not found in {project_dir}")
    return matches[0]


def parse_document_md(md_path: Path) -> dict[str, BlockMd]:
    text = md_path.read_text(encoding="utf-8")
    matches = list(BLOCK_RE.finditer(text))
    result: dict[str, BlockMd] = {}

    for idx, match in enumerate(matches):
        if match.group("block_type") != "IMAGE":
            continue
        block_id = match.group("block_id")
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        section = text[start:end].strip()
        enriched_match = ENRICHED_RE.search(section)
        if enriched_match:
            original = section[: enriched_match.start()].strip()
            enriched = section[enriched_match.start() :].strip()
            label = enriched_match.group("label").strip()
        else:
            original = section
            enriched = ""
            label = ""
        result[block_id] = BlockMd(original=original, enriched=enriched, enriched_label=label)
    return result


def index_by_block(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    data = load_json(path)
    blocks = data.get("blocks") or []
    return {str(item.get("block_id")): item for item in blocks if isinstance(item, dict)}


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def risk_flags(block: dict[str, Any], md: BlockMd | None, truncated_ids: set[str]) -> list[str]:
    block_id = str(block.get("block_id", ""))
    enriched = md.enriched if md else ""
    warnings = set(block.get("warnings") or [])
    flags: list[str] = []

    if block_id in truncated_ids:
        flags.append("truncated")
    if block.get("high_detail_status") == "skipped_large_block":
        flags.append("base_only_large")
    if block.get("base_response_source") in {"reasoning_tail", "reasoning_content"}:
        flags.append(f"base_{block.get('base_response_source')}")
    hd_source = block.get("high_detail_response_source")
    if hd_source in {"reasoning_tail", "reasoning_content"}:
        flags.append(f"hd_{hd_source}")
    if "base_failed" in warnings:
        flags.append("base_failed")
    if "base_too_short" in warnings:
        flags.append("base_too_short")
    if any(marker in enriched for marker in REASONING_MARKERS):
        flags.append("reasoning_leak")
    if "[Done]" in enriched:
        flags.append("done_placeholder")
    if len(enriched) < 220:
        flags.append("very_short")
    if re.search(r"(?:\b200\b[\s,;]*){8,}", enriched) or enriched.count("200") >= 35:
        flags.append("numeric_repetition")
    if "Изображение пустое" in enriched:
        flags.append("claims_blank_image")

    return flags


def risk_score(flags: list[str]) -> int:
    weights = {
        "reasoning_leak": 10,
        "numeric_repetition": 8,
        "claims_blank_image": 7,
        "done_placeholder": 6,
        "truncated": 5,
        "base_failed": 5,
        "base_too_short": 5,
        "base_only_large": 2,
        "very_short": 2,
    }
    return sum(weights.get(flag, 1) for flag in flags)


def rel_image(output_dir: Path, profile_dir: str, block_id: str, index: dict[str, dict[str, Any]]) -> str:
    item = index.get(block_id)
    file_name = item.get("file") if item else f"block_{block_id}.png"
    path = output_dir / profile_dir / str(file_name)
    return f"{profile_dir}/{html.escape(str(file_name), quote=True)}" if path.exists() else ""


def md_pre(text: str, max_chars: int = 7000) -> str:
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "\n\n...[обрезано в отчете]..."
    return html.escape(text)


def badge(text: str, css: str = "") -> str:
    return f'<span class="badge {css}">{html.escape(text)}</span>'


def status_badge(value: Any) -> str:
    text = as_text(value) or "n/a"
    css = "ok" if text == "ok" else "warn" if "skip" in text or "partial" in text else "bad"
    return badge(text, css)


def choose_blocks(
    blocks: list[dict[str, Any]],
    md_sections: dict[str, BlockMd],
    truncated_ids: set[str],
    limit: int,
) -> list[tuple[str, dict[str, Any], list[str]]]:
    enriched: list[tuple[dict[str, Any], list[str], int]] = []
    for block in blocks:
        block_id = str(block.get("block_id", ""))
        flags = risk_flags(block, md_sections.get(block_id), truncated_ids)
        enriched.append((block, flags, risk_score(flags)))

    picked: list[tuple[str, dict[str, Any], list[str]]] = []
    seen: set[str] = set()

    def add(group: str, candidates: list[tuple[dict[str, Any], list[str], int]], count: int) -> None:
        for block, flags, _score in candidates:
            block_id = str(block.get("block_id", ""))
            if block_id in seen:
                continue
            picked.append((group, block, flags))
            seen.add(block_id)
            if sum(1 for item in picked if item[0] == group) >= count:
                break

    red_flags = sorted((item for item in enriched if item[2] > 0), key=lambda item: item[2], reverse=True)
    high_detail = [item for item in enriched if item[0].get("final_profile") == "qwen_300_high_detail"]
    base_large = [item for item in enriched if item[0].get("high_detail_status") == "skipped_large_block"]
    unusual_ok = [
        item
        for item in enriched
        if item[0].get("high_detail_status") == "ok"
        and item[0].get("final_profile") != "qwen_300_high_detail"
    ]

    add("Красные флаги", red_flags, min(16, limit))
    add("High-detail 300 DPI", high_detail, 12)
    add("Base-only 100 DPI", base_large, 12)
    add("Нестандартный статус", unusual_ok, 4)

    if len(picked) < limit:
        add("Остальные", enriched, limit - len(picked))
    return picked[:limit]


def size_line(item: dict[str, Any] | None) -> str:
    if not item:
        return "нет"
    size = item.get("render_size") or []
    size_text = f"{size[0]}x{size[1]} px" if len(size) == 2 else "unknown px"
    return f"{size_text}, {item.get('size_kb', '?')} KB, p={item.get('page', '?')}"


def build_html(project_dir: Path, output_path: Path, sample_limit: int) -> str:
    output_dir = project_dir / "_output"
    summary = load_json(output_dir / "qwen_enrichment_summary.json")
    md_path = find_document_md(project_dir, summary)
    md_sections = parse_document_md(md_path)
    index100 = index_by_block(output_dir / "blocks_qwen_100" / "index.json")
    index300 = index_by_block(output_dir / "blocks_qwen_300" / "index.json")

    blocks = [item for item in summary.get("blocks", []) if isinstance(item, dict)]
    truncated_raw = summary.get("truncated") or []
    truncated_ids = {
        str(item.get("block_id"))
        for item in truncated_raw
        if isinstance(item, dict) and item.get("block_id")
    }
    if not truncated_ids and summary.get("blocks_truncated"):
        truncated_ids = {str(item.get("block_id")) for item in blocks if item.get("base_finish_reason") == "length"}

    selected = choose_blocks(blocks, md_sections, truncated_ids, sample_limit)

    counts = {
        "blocks": len(blocks),
        "ok": summary.get("blocks_ok"),
        "failed": summary.get("blocks_failed"),
        "coverage": summary.get("coverage_ratio"),
        "high_detail_ok": sum(1 for block in blocks if block.get("final_profile") == "qwen_300_high_detail"),
        "base_only": sum(1 for block in blocks if block.get("high_detail_status") == "skipped_large_block"),
        "risk_selected": sum(1 for _group, _block, flags in selected if flags),
    }

    rows = []
    for n, (group, block, flags) in enumerate(selected, start=1):
        block_id = str(block.get("block_id", ""))
        md = md_sections.get(block_id)
        base_item = index100.get(block_id)
        hd_item = index300.get(block_id)
        final_profile = str(block.get("final_profile") or "")
        primary_profile = "blocks_qwen_300" if final_profile == "qwen_300_high_detail" and hd_item else "blocks_qwen_100"
        primary_src = rel_image(output_dir, primary_profile, block_id, index300 if primary_profile.endswith("300") else index100)
        src100 = rel_image(output_dir, "blocks_qwen_100", block_id, index100)
        src300 = rel_image(output_dir, "blocks_qwen_300", block_id, index300)
        page = (base_item or hd_item or {}).get("page", "?")
        warnings = block.get("warnings") or []
        safety = block.get("high_detail_safety") or {}

        image_html = (
            f'<img class="main-img" src="{primary_src}" alt="{html.escape(block_id)}">'
            if primary_src
            else '<div class="missing-img">нет изображения</div>'
        )
        thumb100 = f'<a href="{src100}" target="_blank">100 DPI</a>' if src100 else "100 DPI нет"
        thumb300 = f'<a href="{src300}" target="_blank">300 DPI</a>' if src300 else "300 DPI нет"
        flags_html = " ".join(badge(flag, "risk") for flag in flags) or badge("без авто-флагов", "ok")
        warnings_html = " ".join(badge(str(w), "warn") for w in warnings) or badge("нет", "ok")

        controls = "".join(
            f"""
            <label>{label}
              <select data-score="{html.escape(key)}">
                <option value="">-</option>
                <option value="0">0 плохо</option>
                <option value="1">1 частично</option>
                <option value="2">2 хорошо</option>
              </select>
            </label>
            """
            for key, label in (
                ("type", "тип"),
                ("text", "текст"),
                ("dims", "размеры/оси/марки"),
                ("hallucination", "без галлюцинаций"),
                ("useful", "полезно"),
            )
        )

        rows.append(
            f"""
            <article class="card" data-block-id="{html.escape(block_id)}">
              <header>
                <div>
                  <div class="eyebrow">{html.escape(group)} · #{n} · page {html.escape(str(page))}</div>
                  <h2>{html.escape(block_id)}</h2>
                </div>
                <div class="status-line">
                  {status_badge(block.get("coverage_status"))}
                  {badge(final_profile or "n/a", "profile")}
                </div>
              </header>
              <div class="grid">
                <section class="image-pane">
                  {image_html}
                  <div class="links">{thumb100} · {thumb300}</div>
                  <dl>
                    <dt>100 DPI</dt><dd>{html.escape(size_line(base_item))}</dd>
                    <dt>300 DPI</dt><dd>{html.escape(size_line(hd_item))}</dd>
                    <dt>Base source</dt><dd>{html.escape(as_text(block.get("base_response_source")) or "n/a")}</dd>
                    <dt>HD source</dt><dd>{html.escape(as_text(block.get("high_detail_response_source")) or "n/a")}</dd>
                    <dt>HD safety</dt><dd>{html.escape(as_text(safety) or "n/a")}</dd>
                  </dl>
                </section>
                <section class="text-pane">
                  <div class="flags">
                    <strong>Флаги:</strong> {flags_html}
                  </div>
                  <div class="flags">
                    <strong>Warnings:</strong> {warnings_html}
                  </div>
                  <details open>
                    <summary>Qwen enriched</summary>
                    <pre>{md_pre(md.enriched if md else "")}</pre>
                  </details>
                  <details>
                    <summary>Исходное описание OCR</summary>
                    <pre>{md_pre(md.original if md else "", 3500)}</pre>
                  </details>
                </section>
              </div>
              <section class="scorebox">
                <div class="score-controls">{controls}</div>
                <textarea data-score="note" placeholder="заметка ревьюера"></textarea>
              </section>
            </article>
            """
        )

    selected_ids = [block.get("block_id") for _group, block, _flags in selected]
    payload = {
        "project_dir": str(project_dir),
        "summary_path": str(output_dir / "qwen_enrichment_summary.json"),
        "document_path": str(md_path),
        "selected_block_ids": selected_ids,
    }

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Qwen QA report · {html.escape(project_dir.name)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f3f5f8;
      --panel: #ffffff;
      --text: #1d2733;
      --muted: #68758a;
      --line: #d9e0ea;
      --ok: #0f8a5f;
      --warn: #a66a00;
      --bad: #b42318;
      --profile: #2457a6;
      --risk-bg: #fff1f0;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: rgba(243, 245, 248, 0.96);
      border-bottom: 1px solid var(--line);
      padding: 14px 22px;
      backdrop-filter: blur(8px);
    }}
    h1 {{ margin: 0 0 8px; font-size: 22px; }}
    h2 {{ margin: 2px 0 0; font-size: 18px; }}
    .summary {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }}
    .actions {{
      margin-top: 10px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }}
    button {{
      border: 1px solid #0b7b61;
      border-radius: 6px;
      background: #008060;
      color: white;
      padding: 8px 12px;
      font-weight: 700;
      cursor: pointer;
    }}
    main {{ padding: 18px 22px 36px; }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      margin: 0 0 18px;
      overflow: hidden;
      box-shadow: 0 1px 2px rgba(20, 32, 48, 0.04);
    }}
    .card > header {{
      display: flex;
      justify-content: space-between;
      gap: 14px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }}
    .status-line, .flags, .links {{ display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }}
    .grid {{
      display: grid;
      grid-template-columns: minmax(320px, 48%) minmax(320px, 1fr);
      gap: 0;
    }}
    .image-pane, .text-pane {{ padding: 14px 16px; }}
    .image-pane {{ border-right: 1px solid var(--line); }}
    .main-img {{
      width: 100%;
      max-height: 720px;
      object-fit: contain;
      background: #eef2f7;
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    .missing-img {{
      min-height: 240px;
      display: grid;
      place-items: center;
      color: var(--muted);
      background: #eef2f7;
      border: 1px solid var(--line);
      border-radius: 6px;
    }}
    dl {{
      display: grid;
      grid-template-columns: 90px 1fr;
      gap: 5px 10px;
      margin: 12px 0 0;
      color: var(--muted);
      overflow-wrap: anywhere;
    }}
    dt {{ font-weight: 700; color: #40516a; }}
    dd {{ margin: 0; }}
    details {{
      border-top: 1px solid var(--line);
      margin-top: 10px;
      padding-top: 10px;
    }}
    summary {{ cursor: pointer; font-weight: 800; }}
    pre {{
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      margin: 8px 0 0;
      padding: 12px;
      background: #f8fafc;
      border: 1px solid #e4eaf2;
      border-radius: 6px;
      max-height: 420px;
      overflow: auto;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 3px 7px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #f7f9fc;
      color: #3e4f67;
      font-size: 12px;
      font-weight: 700;
    }}
    .badge.ok {{ color: var(--ok); border-color: #b7dfce; background: #effaf5; }}
    .badge.warn {{ color: var(--warn); border-color: #f2d38d; background: #fff8e8; }}
    .badge.bad, .badge.risk {{ color: var(--bad); border-color: #f2b8b5; background: var(--risk-bg); }}
    .badge.profile {{ color: var(--profile); border-color: #bdd0f3; background: #f0f5ff; }}
    .scorebox {{
      border-top: 1px solid var(--line);
      padding: 12px 16px 14px;
      background: #fbfcfe;
    }}
    .score-controls {{
      display: grid;
      grid-template-columns: repeat(5, minmax(130px, 1fr));
      gap: 8px;
    }}
    label {{
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }}
    select, textarea {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: white;
      color: var(--text);
      padding: 7px 8px;
      font: inherit;
    }}
    textarea {{
      margin-top: 8px;
      min-height: 56px;
      resize: vertical;
    }}
    a {{ color: #075fa8; }}
    @media (max-width: 980px) {{
      .grid {{ grid-template-columns: 1fr; }}
      .image-pane {{ border-right: 0; border-bottom: 1px solid var(--line); }}
      .score-controls {{ grid-template-columns: repeat(2, minmax(130px, 1fr)); }}
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <h1>Qwen QA report</h1>
    <div class="summary">
      {badge(f"blocks {counts['blocks']}")}
      {badge(f"ok {counts['ok']}", "ok")}
      {badge(f"failed {counts['failed']}", "ok" if counts["failed"] == 0 else "bad")}
      {badge(f"coverage {counts['coverage']}")}
      {badge(f"HD 300 {counts['high_detail_ok']}", "profile")}
      {badge(f"base-only {counts['base_only']}", "warn")}
      {badge(f"sample {len(selected)}")}
      {badge(f"risk cards {counts['risk_selected']}", "risk")}
    </div>
    <div class="actions">
      <button id="export">Экспорт оценок JSON</button>
      <button id="clear">Очистить оценки</button>
    </div>
  </div>
  <main>
    {''.join(rows)}
  </main>
  <script>
    const meta = {json.dumps(payload, ensure_ascii=False)};
    const keyPrefix = "qwen-qa:" + meta.summary_path + ":";

    function cardData(card) {{
      const data = {{ block_id: card.dataset.blockId }};
      card.querySelectorAll("[data-score]").forEach((el) => {{
        data[el.dataset.score] = el.value;
      }});
      return data;
    }}

    function saveCard(card) {{
      localStorage.setItem(keyPrefix + card.dataset.blockId, JSON.stringify(cardData(card)));
    }}

    function loadCard(card) {{
      const raw = localStorage.getItem(keyPrefix + card.dataset.blockId);
      if (!raw) return;
      const data = JSON.parse(raw);
      card.querySelectorAll("[data-score]").forEach((el) => {{
        if (Object.prototype.hasOwnProperty.call(data, el.dataset.score)) {{
          el.value = data[el.dataset.score] || "";
        }}
      }});
    }}

    document.querySelectorAll(".card").forEach((card) => {{
      loadCard(card);
      card.querySelectorAll("[data-score]").forEach((el) => {{
        el.addEventListener("change", () => saveCard(card));
        el.addEventListener("input", () => saveCard(card));
      }});
    }});

    document.getElementById("export").addEventListener("click", () => {{
      const scores = Array.from(document.querySelectorAll(".card")).map(cardData);
      const payload = {{ ...meta, exported_at: new Date().toISOString(), scores }};
      const blob = new Blob([JSON.stringify(payload, null, 2)], {{ type: "application/json" }});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "qwen_qa_scores.json";
      a.click();
      URL.revokeObjectURL(url);
    }});

    document.getElementById("clear").addEventListener("click", () => {{
      document.querySelectorAll(".card").forEach((card) => {{
        localStorage.removeItem(keyPrefix + card.dataset.blockId);
        card.querySelectorAll("[data-score]").forEach((el) => el.value = "");
      }});
    }});
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project_dir", type=Path)
    parser.add_argument("--sample-limit", type=int, default=40)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    project_dir = args.project_dir.resolve()
    if not project_dir.exists():
        raise FileNotFoundError(project_dir)
    output_path = args.output or project_dir / "_output" / "qwen_qa_report.html"
    html_text = build_html(project_dir, output_path, args.sample_limit)
    output_path.write_text(html_text, encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
