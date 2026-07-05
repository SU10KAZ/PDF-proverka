#!/usr/bin/env python3
"""
mark_stale_legacy_version_groups.py
-----------------------------------
Пометить УСТАРЕВШИЕ legacy `version_group.json`, которые отстают от авторитетного
манифеста projects_v2 (`document.json`).

Контекст: система на projects_v2-primary. Список версий берётся из projects_v2, а
legacy-контейнеры `projects/**/<...>(main)/version_group.json` заморожены на момент
cutover. Если после cutover в проект добавили новую версию (напр. V3), legacy-файл
показывает лишь V1/V2 и ВВОДИТ В ЗАБЛУЖДЕНИЕ при ручном разборе (именно это сбило
при диагностике графика 13АВ-РД-АР1.2-К4).

Скрипт находит legacy version_group.json, у которых projects_v2 знает БОЛЬШЕ версий,
и добавляет НЕразрушающую пометку-хлебную-крошку:
    "legacy_superseded": true,
    "authoritative_storage": "projects_v2",
    "legacy_note": "<пояснение>",
    "legacy_marked_at": "<ISO>"

Парсер version_group (`version_service._normalize_manifest`) читает только известные
ключи, поэтому доп. поля игнорируются и парсинг не ломается. Даты/версии/папки НЕ
трогаются, файлы НЕ удаляются (весь projects/ под 30-дневной защитой после cutover).

Безопасность:
  * АДДИТИВНО — только добавляет поля; ничего не удаляет и не меняет по сути;
  * ЦЕЛЕВО — только legacy, отставшие от projects_v2 (реальные footgun'ы);
  * ИДЕМПОТЕНТНО — уже помеченные (legacy_superseded) пропускаются;
  * БЭКАП — каждый файл копируется в *.bak-legacy-mark-<ts> перед правкой;
  * DRY-RUN по умолчанию — без --apply только печатает план;
  * ПОСТ-ВЕРИФИКАЦИЯ — после записи проверяет, что version_service всё ещё
    нормализует манифест с теми же versions/latest.

Запуск:
    python scripts/mark_stale_legacy_version_groups.py            # dry-run
    python scripts/mark_stale_legacy_version_groups.py --apply    # запись
"""
from __future__ import annotations

import argparse
import glob
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import backend.app.services.common.version_service as version_service  # noqa: E402

_NOTE = (
    "LEGACY-контейнер, заморожен на cutover projects_v2. НЕ авторитетен: актуальный "
    "список версий — в projects_v2/objects/.../documents/<code>/versions, читать через "
    "API /api/projects/<id>/versions. Файл оставлен как страховка до планового удаления "
    "projects/."
)


def _load(path: str) -> dict | None:
    try:
        d = json.load(open(path, encoding="utf-8"))
        return d if isinstance(d, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _build_v2_index() -> dict[str, int]:
    """document_code → число версий в projects_v2."""
    idx: dict[str, int] = {}
    for p in glob.glob("projects_v2/objects/**/documents/*/document.json", recursive=True):
        d = _load(p)
        if not d:
            continue
        code = d.get("document_code") or Path(p).parent.name
        idx[code] = len(d.get("versions", []) or [])
    return idx


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="записать (по умолчанию dry-run)")
    args = ap.parse_args()

    v2n = _build_v2_index()
    now = datetime.now().isoformat(timespec="seconds")

    to_mark: list[tuple[str, str, int, int]] = []  # (path, code, legacy_n, v2_n)
    already, no_v2_pair, up_to_date = 0, 0, 0
    for lp in glob.glob("projects/**/version_group.json", recursive=True):
        lg = _load(lp)
        if not lg:
            continue
        code = lg.get("logical_project_id") or ""
        ln = len(lg.get("versions", []) or [])
        if lg.get("legacy_superseded"):
            already += 1
            continue
        if code not in v2n:
            no_v2_pair += 1
            continue
        if v2n[code] <= ln:
            up_to_date += 1
            continue
        to_mark.append((lp, code, ln, v2n[code]))

    print(f"legacy version_group.json: помечаем отставшие от projects_v2")
    print(f"  уже помечено (пропуск): {already}")
    print(f"  без v2-пары (не мигрирован, не трогаем): {no_v2_pair}")
    print(f"  согласованы с v2 (не footgun): {up_to_date}")
    print(f"  ОТСТАЮТ → к пометке: {len(to_mark)}")
    print()
    for lp, code, ln, v2 in sorted(to_mark, key=lambda x: x[1]):
        print(f"  [{ln}→v2:{v2}] {code}")
        print(f"          {lp}")

    if not to_mark:
        print("\nНечего помечать.")
        return 0
    if not args.apply:
        print("\nDRY-RUN. Для записи: --apply")
        return 0

    marked, failed = 0, 0
    for lp, code, ln, v2 in to_mark:
        p = Path(lp)
        lg = _load(lp)
        if not lg:
            failed += 1
            continue
        # 1. бэкап
        bak = p.with_name(p.name + f".bak-legacy-mark-{now.replace(':', '').replace('-', '')}")
        shutil.copy2(p, bak)
        # 2. аддитивная пометка
        lg["legacy_superseded"] = True
        lg["authoritative_storage"] = "projects_v2"
        lg["legacy_note"] = _NOTE
        lg["legacy_marked_at"] = now
        tmp = p.with_name(p.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(lg, f, ensure_ascii=False, indent=2)
        tmp.replace(p)
        # 3. пост-верификация: version_service нормализует тот же набор версий/latest
        try:
            raw = version_service._read_group_manifest_raw(p.parent)
            norm = version_service._normalize_manifest(raw, code)
            ok = (len(norm["versions"]) == ln and bool(norm["latest_version_id"]))
        except Exception as e:  # noqa: BLE001
            ok = False
            print(f"  ВЕРИФИКАЦИЯ ПАДАЕТ {code}: {e}")
        if ok:
            marked += 1
        else:
            # откат из бэкапа
            shutil.copy2(bak, p)
            failed += 1
            print(f"  ОТКАТ {code}: нормализация изменилась → вернул из бэкапа")

    print(f"\nПомечено: {marked}, сбоев/откатов: {failed}")
    print("Бэкапы: *.bak-legacy-mark-* рядом с каждым файлом.")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
