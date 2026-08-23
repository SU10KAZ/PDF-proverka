# -*- coding: utf-8 -*-
"""`hyb` — независимый пиксельный арбитр на ТЕХ ЖЕ изображениях, что видела рука B.

Считается доля краски одной стороны, не объяснённая краской другой в пределах 1 px
(то же определение, что у предфильтра гейта). Нужен, чтобы отделить «подмена есть в
манифесте» от «подмена видна на картинке»: часть контрфактов и часть реальных пар
не двигают ни одного пикселя (loc L3: 47 % промахов реестра — подмены, не меняющие
ни одного пикселя).
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H          # noqa: E402


def main():
    views = H.load("hyb_map.json")["views"]
    rows = []
    for v in views:
        a = H.VIEW_DIR / f"{v['view_id']}_1.png"
        b = H.VIEW_DIR / f"{v['view_id']}_2.png"
        px = v["px"]
        same_dims = px[0] and px[1] and px[0] == px[1]
        st = H.structural_diff(a, b) if same_dims else None
        rows.append({"case_id": v["case_id"], "view_id": v["view_id"], "source": v["source"],
                     "truth": v["truth"], "px": px, "same_dims": bool(same_dims),
                     "structural": round(st, 6) if st is not None else None})
        print(v["case_id"][:40], same_dims, rows[-1]["structural"])
    H.dump({"note": "structural = доля краски, не объяснённой другой стороной в пределах 1 px; "
                    "None -> кадры разного размера в пикселях, сравнение неприменимо",
            "rows": rows}, "hyb_arbiter.json")


if __name__ == "__main__":
    main()
