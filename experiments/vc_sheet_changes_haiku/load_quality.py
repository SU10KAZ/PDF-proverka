"""Качество ответов Luna по ступеням параллельности (load_test.sh): точные значения, полнота, брак.

    python experiments/vc_sheet_changes_haiku/load_quality.py <каталог load>
"""
import json
import re
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

NAMES = {"r03abbdc826": "л.9", "r251568b0fd": "л.3", "r3e8b9d8b2d": "л.6", "r684f40d4a4": "Разр",
         "r8f36a78306": "Тит", "raaa888156f": "л.13", "rd10411fdc6": "л.1", "rdf760c07b1": "л.11",
         "ref10b9722d": "л.2", "rf5fc5cd16a": "л.10", "rfe1ee8c4ae": "л.8"}
# л.13: 17 пар «было → стало», совпавших у Sol, Haiku и Luna.
L13 = [("3383,1", "3388,7"), ("7912,3", "7923,5"), ("2569,0", "2569,1"), ("1602,6", "1608,1"),
       ("4186,1", "4191,7"), ("3570,0", "3575,6"), ("441,7", "441,8"), ("838,5", "839,5"), ("614,6", "615,4"),
       ("245,7", "245,8"), ("26,1", "26,0"), ("41,57", "52,05"), ("941,09", "945,70"), ("447,39", "433,83"),
       ("379,20", "376,13"), ("58,59", "499,11"), ("432,84", "499,11")]


def text_of(r: dict) -> str:
    return json.dumps({"c": r["changes"], "u": r["unresolved"]}, ensure_ascii=False)


def pairs_found(r: dict) -> int:
    t = text_of(r)
    return sum(1 for a, b in L13 if a in t and b in t)


def l2_elev(r: dict) -> str:
    t = text_of(r)
    if "7,585" in t and "7,640" in t:
        return "верно"
    if "7,676" in t and "7,640" in t:
        return "-7,676 (ошибка)"
    return "нет"


def main() -> None:
    root = Path(sys.argv[1])
    levels = sorted((int(d.name[1:]), d) for d in root.glob("p*") if d.is_dir() and d.name[1:].isdigit())
    for n, d in levels:
        per = defaultdict(list)
        calls, ins, outs, bad = [], [], [], 0
        for f in sorted(d.glob("sheets/*/sheet_changes.json")):
            r = json.loads(f.read_text(encoding="utf-8"))
            spid = re.sub(r"^\d+_", "", f.parent.name)
            per[spid].append(r)
            for c in r["diagnostics"]["calls"]:
                calls.append(c["seconds"])
                ins.append(c["usage"].get("input_tokens", 0))
                outs.append(c["usage"].get("output_tokens", 0))
                bad += 0 if c["ok"] else 1
        print(f"\n=== N={n}: прогонов листов {sum(map(len, per.values()))}, обращений {len(calls)}, неудачных {bad}, "
              f"медиана обращения {st.median(calls) if calls else 0:.0f} с, выход/обращ. {st.mean(outs) if outs else 0:.0f}")
        for spid, rs in sorted(per.items(), key=lambda x: NAMES.get(x[0], x[0])):
            ch = [len(r["changes"]) for r in rs]
            un = [len(r["unresolved"]) for r in rs]
            extra = ""
            if spid == "raaa888156f":
                extra = " | л.13 пар верно: " + ",".join(str(pairs_found(r)) for r in rs) + f" из {len(L13)}"
            if spid == "ref10b9722d":
                extra = " | отметка: " + ",".join(l2_elev(r) for r in rs)
            states = ",".join(sorted({r["state"] for r in rs}))
            print(f"  {NAMES.get(spid, spid):5} x{len(rs)}  изм {ch}  вопр {un}  [{states}]{extra}")


if __name__ == "__main__":
    main()
