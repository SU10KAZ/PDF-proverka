"""Эксперимент 3: Vision Analyst читает штамп листа.

Зачем: сопоставление листов по штампу было удалено из системы 17.08. Текстовый
слой на этих листах даёт заголовки ТАБЛИЦ («Экспликация помещений квартир
3 этажа»), а не идентификацию листа. Штамп в правом нижнем углу содержит
настоящую идентификацию («Корпус 1, 2. План 3 этажа. М 1:200»), и её видно
только на изображении.

Проверяем: восстанавливает ли зрение эту идентификацию и рассудит ли оно спор
между production-матчингом и матчингом по тексту.

Только чтение. Изображения рендерятся из PDF во временный каталог.
"""
from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from gateway import call_codex, call_claude

IMG_DIR = Path("/tmp/ai_sup_vision")
RESULTS = Path(__file__).parent / "results"

STAMP_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["stamp_readable", "sheet_title", "building", "floor", "sheet_kind", "confidence"],
    "properties": {
        "stamp_readable": {"type": "boolean"},
        "sheet_title": {"type": "string", "description": "дословно строка названия листа из штампа"},
        "building": {"type": "string", "description": "корпус(а), как указано; пусто если нет"},
        "floor": {"type": "string", "description": "этаж, как указано; пусто если нет"},
        "sheet_kind": {
            "type": "string",
            "enum": ["ПЛАН_ЭТАЖА", "ПЛАН_КРОВЛИ", "РАЗРЕЗ", "ФАСАД", "УЗЕЛ", "ВЕДОМОСТЬ", "ДРУГОЕ"],
        },
        "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
    },
}

PROMPT = (
    "На изображении — лист рабочей документации. В правом нижнем углу находится "
    "штамп (основная надпись). Прочитай в нём НАЗВАНИЕ ЛИСТА — строку вида "
    "«Корпус N. План M этажа. М 1:200». Верни ровно то, что написано, без домыслов. "
    "Если штамп не читается — stamp_readable=false и пустые поля."
)

# страницы, участвующие в 12 парах production и 3 парах, на которых сошлись модели
PAGES = [("L", p) for p in (24, 25, 26, 28, 29, 30, 33, 34, 39, 40, 43, 44)] + \
        [("R", p) for p in (3, 8, 9, 10, 11, 12, 14, 15, 16, 18, 21, 23)]

PRODUCTION_PAIRS = [(24, 16), (25, 21), (26, 11), (28, 9), (29, 15), (30, 8),
                    (33, 12), (34, 3), (39, 18), (40, 14), (43, 10), (44, 23)]
AI_TEXT_PAIRS = [(29, 8), (30, 9), (24, 3)]


def read_stamp(side_page):
    side, page = side_page
    img = IMG_DIR / f"{side}{page}.png"
    if not img.exists():
        return side_page, None, "нет изображения"
    r = call_codex(PROMPT, model="gpt-5.6-sol", schema=STAMP_SCHEMA,
                   effort="low", images=[str(img)], timeout_s=420)
    return side_page, (r.parsed if r.ok else None), (r.error[:200] if not r.ok else "")


def main() -> None:
    t0 = time.time()
    stamps: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for (side, page), parsed, err in ex.map(read_stamp, PAGES):
            key = f"{side}{page}"
            stamps[key] = parsed or {"error": err}
            title = (parsed or {}).get("sheet_title", "") or f"ОШИБКА: {err}"
            print(f"  {key:>4}: {title[:70]}")

    print(f"\nпрочитано за {time.time() - t0:.0f}s\n")

    def key_of(k: str) -> tuple:
        s = stamps.get(k) or {}
        return (s.get("sheet_kind"), (s.get("building") or "").strip(),
                (s.get("floor") or "").strip())

    print("=== АРБИТРАЖ ПО ШТАМПАМ ===")
    print("(совпадение = один тип листа, тот же корпус, тот же этаж)\n")

    def judge(pairs, label):
        agree = 0
        print(f"{label}:")
        for lp, rp in pairs:
            a, b = key_of(f"L{lp}"), key_of(f"R{rp}")
            ok = a == b and all(a)
            agree += ok
            mark = "СОВПАЛО " if ok else "НЕ СОВП."
            print(f"  {mark} L{lp:>3} -> R{rp:<3} | {a} vs {b}")
        print(f"  итого совпало: {agree}/{len(pairs)}\n")
        return agree

    prod = judge(PRODUCTION_PAIRS, "A) 12 пар production-матчера")
    ai = judge(AI_TEXT_PAIRS, "B) 3 пары, на которых сошлись 5 конфигураций моделей по тексту")

    RESULTS.mkdir(exist_ok=True)
    (RESULTS / "vision_experiment.json").write_text(json.dumps({
        "stamps": stamps,
        "production_pairs_agreed": prod, "production_pairs_total": len(PRODUCTION_PAIRS),
        "ai_text_pairs_agreed": ai, "ai_text_pairs_total": len(AI_TEXT_PAIRS),
    }, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"записано: {RESULTS / 'vision_experiment.json'}")


if __name__ == "__main__":
    main()
