#!/usr/bin/env python3
"""Second-pass cleanup after remove_critic_v2_portal.py."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent


def _delete_block(lines: list[str], start_pat: str, until_pat: str) -> list[str]:
    i = 0
    while i < len(lines):
        if start_pat in lines[i]:
            j = i + 1
            while j < len(lines) and until_pat not in lines[j]:
                j += 1
            lines = lines[:i] + lines[j:]
            continue
        i += 1
    return lines


def patch_app_js(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    lines = _delete_block(lines, "critic-v2-disagreements", "} else if (hash.match(/^\\/project\\/(.+)\\/log$/))")
    lines = _delete_block(lines, "/critic-v2$/", "} else if (hash.match(/^\\/project\\/(.+)\\/log$/))")
    text = "".join(lines)
    text = re.sub(r"            // Critic v2.*\n", "", text)
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def patch_index_html(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        r"\n\s*@click=\"toggleCv2Sort\(\)\".*?</th>\n",
        "\n",
        text,
        flags=re.DOTALL,
        count=1,
    )
    path.write_text(text, encoding="utf-8")
    print("patched", path)


def main() -> None:
    patch_app_js(ROOT / "frontend/static/js/app.js")
    patch_index_html(ROOT / "frontend/index.html")
    print("done")


if __name__ == "__main__":
    main()
