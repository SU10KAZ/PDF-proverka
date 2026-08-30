#!/usr/bin/env python3
"""Прогнать боевое сравнение одной пары документов из командной строки.

До этого скрипта единственным способом выполнить полный боевой прогон был
HTTP-запрос к порталу. Для приёмки это неудобно: она обязана быть
воспроизводимой и изолированной, а не зависеть от поднятого бэкенда и от
того, что в этот момент делает очередь.

Режим ИИ по умолчанию — FAST, то есть ноль обращений к модели. Это
осознанное умолчание: приёмка детерминированного слоя не должна маскировать
его пробелы работой модели.

Использование:
    python scripts/stage_comparison_run_pair.py <session_id> <pair_id> \\
        --left-page 1 --right-page 1 \\
        --left-block blk_... --right-block blk_...

    # область сравнения из уже сохранённого прогона
    python scripts/stage_comparison_run_pair.py <session_id> <pair_id> --reuse-selection
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_id")
    parser.add_argument("pair_id")
    parser.add_argument("--input-mode", choices=("PAGE", "DOCUMENT"), default="PAGE")
    parser.add_argument("--left-page", type=int, action="append", default=[])
    parser.add_argument("--right-page", type=int, action="append", default=[])
    parser.add_argument("--left-block", action="append", default=[])
    parser.add_argument("--right-block", action="append", default=[])
    parser.add_argument(
        "--ai-mode",
        choices=("FAST", "STANDARD", "DEEP"),
        default="FAST",
        help="FAST (по умолчанию) — ноль обращений к модели",
    )
    parser.add_argument(
        "--reuse-selection",
        action="store_true",
        help="взять страницы и блоки из сохранённого состояния прогона",
    )
    args = parser.parse_args()

    # Режим объявляется до импорта: настройки читают окружение при загрузке.
    if args.ai_mode == "FAST":
        os.environ.setdefault("STAGE_COMPARISON_AI_MODE", "OFF")

    from backend.app.services.stage_comparison import (
        production_orchestrator,
        production_store,
    )

    left_pages = list(args.left_page)
    right_pages = list(args.right_page)
    left_blocks = list(args.left_block)
    right_blocks = list(args.right_block)
    if args.reuse_selection:
        state = production_store.load_artifact(args.session_id, args.pair_id, "state")
        selection = (state or {}).get("selection") or {}
        if not selection:
            print("Сохранённого выбора нет — укажите страницы и блоки явно.", file=sys.stderr)
            return 2
        left_pages = left_pages or list(selection.get("left_pages") or [])
        right_pages = right_pages or list(selection.get("right_pages") or [])
        left_blocks = left_blocks or list(selection.get("left_block_ids") or [])
        right_blocks = right_blocks or list(selection.get("right_block_ids") or [])

    started = time.perf_counter()
    state = production_orchestrator.run_production_comparison(
        args.session_id,
        args.pair_id,
        input_mode=args.input_mode,
        left_pages=left_pages,
        right_pages=right_pages,
        left_block_ids=left_blocks,
        right_block_ids=right_blocks,
        ai_mode=args.ai_mode,
    )
    stages = state.get("stages") or {}
    print(json.dumps({
        "статус": state.get("status"),
        "длительность_мс": state.get("duration_ms"),
        "секунд_по_часам": round(time.perf_counter() - started, 2),
        "режим_входа": state.get("input_mode"),
        "обращений_к_модели": (stages.get("ai_resolution") or {}).get("calls", 0),
        "изменений": (stages.get("unified_synthesis") or {}).get("changes"),
        "на_проверку": (stages.get("unified_synthesis") or {}).get("review_items"),
        "противоречий_документа": (stages.get("graphic") or {}).get(
            "document_inconsistencies"
        ),
    }, ensure_ascii=False, indent=1))
    return 0 if state.get("status") == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
