"""One source registry; dense local indices, no per-reference document constants."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import hashlib
import json

from . import frozen_v1 as v1

PRODUCER_VERSION = "semantic-foundation-v3.0.0"


@dataclass
class LineLedger:
    document: dict
    pages: dict
    blocks: list[dict]
    lines: list
    columns: dict[str, list]
    page_lines: dict[int, list[int]]
    clean: list[str]

    @classmethod
    def read(cls, document: dict):
        pages, _ = v1.parse_markdown(Path(document["artifacts"]["work_md"]["path"]))
        raw = json.loads(Path(document["artifacts"]["blocks"]["path"]).read_text())
        for page in raw.get("pages", []):
            number = int(page["page_index"]) + 1
            pages.setdefault(number, v1.ParsedPage(number))
        raw_index = defaultdict(list)
        for index, block in enumerate(raw.get("blocks", [])):
            number = int(block.get("page_index", -1)) + 1
            raw_index[(number, str(block.get("block_id", "")))].append(index)
        columns = {k: [] for k in ("block_ref", "markdown_line", "within_block_line",
                                   "normalized_text", "kind", "owner", "evidence_codes")}
        blocks, lines, clean, page_lines, represented = [], [], [], defaultdict(list), set()
        for number, page in sorted(pages.items()):
            for block in page.blocks:
                indices = raw_index.get((number, block.block_id), [])
                represented.update(indices)
                ref = len(blocks)
                blocks.append({"page": number, "block_id": block.block_id,
                               "block_type": block.block_type, "header_line": block.header_line,
                               "raw_block_indices": indices, "stamp": block.stamp,
                               "empty_lines": sum(not x.text.strip() for x in block.lines),
                               "state": "SOURCE_BLOCK"})
                for line in block.lines:
                    if not line.text.strip():
                        continue
                    i = len(lines)
                    lines.append(line)
                    clean.append(v1.strip_markup(line.text))
                    page_lines[number].append(i)
                    values = (ref, line.markdown_line, line.within_block_line,
                              v1.normalize(clean[-1]), "UNKNOWN", "UNOWNED", [])
                    for key, value in zip(columns, values):
                        columns[key].append(value)
        # Blocks without exported text remain explicit source evidence, including images.
        for index, block in enumerate(raw.get("blocks", [])):
            if index not in represented:
                blocks.append({"page": int(block.get("page_index", -1)) + 1,
                               "block_id": str(block.get("block_id", "")),
                               "block_type": block.get("block_type", "unknown"),
                               "raw_block_indices": [index], "empty_lines": 0,
                               "state": "NO_MARKDOWN_CONTENT"})
        return cls(document, pages, blocks, lines, columns, dict(page_lines), clean), raw

    def claim(self, line_id: int, owner: int | str):
        if self.columns["owner"][line_id] != "UNOWNED":
            raise ValueError(f"Duplicate ownership at line {line_id}")
        self.columns["owner"][line_id] = owner

    def anchor(self, line_id: int, edge: str) -> dict:
        """External DEV/scorer locator. Constants belong to the enclosing case."""
        line = self.lines[line_id]
        return {"line_id": line_id, "page": line.page, "block_id": line.block_id,
                "markdown_line": line.markdown_line, "within_block_line": line.within_block_line,
                "line_sha256": hashlib.sha256(line.text.encode()).hexdigest(), "edge": edge}

    def artifact(self) -> dict:
        return {"schema": "line-ledger.v3", "producer_version": PRODUCER_VERSION,
                "document_version": self.document["document_version"],
                "document_code": self.document["document_code"],
                "sources": self.document["artifacts"], "blocks": self.blocks,
                "columns": self.columns,
                "index_contract": "line_id = ordinal = zero-based column index; raw text = sources.work_md[markdown_line]"}

    def audit(self, units: list[dict], raw: dict) -> dict:
        source = [(x.page, x.block_id, x.markdown_line) for p in self.pages.values()
                  for b in p.blocks for x in b.lines if x.text.strip()]
        actual = [(x.page, x.block_id, x.markdown_line) for x in self.lines]
        raw_indices = {i for b in self.blocks for i in b["raw_block_indices"]}
        owners = self.columns["owner"]
        bad = [i for i, owner in enumerate(owners)
               if (isinstance(owner, int) and not 0 <= owner < len(units))
               or (isinstance(owner, str) and owner not in {"FURNITURE", "EXCLUDED", "REVIEW", "UNOWNED"})]
        unexplained = [i for i, owner in enumerate(owners)
                       if owner == "UNOWNED" or (isinstance(owner, str) and not self.columns["evidence_codes"][i])]
        return {"nonempty_lines": len(actual), "empty_lines": sum(b["empty_lines"] for b in self.blocks),
                "duplicate_semantic_lines": len(actual) - len(set(actual)),
                "unowned_unexplained_lines": len(unexplained), "invalid_owners": len(bad),
                "lost_source_lines": len(set(source) - set(actual)),
                "lost_source_blocks": len(set(range(len(raw.get("blocks", [])))) - raw_indices),
                "markdown_source_blocks": sum(len(p.blocks) for p in self.pages.values()),
                "raw_source_blocks": len(raw.get("blocks", [])),
                "registered_blocks": len(self.blocks)}
