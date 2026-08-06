#!/usr/bin/env python3
"""
dedupe_project_id_forms.py
--------------------------
Схлопнуть записи, задвоенные из-за двух форм project_id.

Один документ попадал в decisions_log под двумя написаниями — с префиксом папки
(`PS/ПД-00542664-ПС-1_V1`, так пишет загрузка решений из Excel) и без него
(`ПД-00542664-ПС-1_V1`, так пишет сохранение из интерфейса). Дедуп журнала
ключуется на `(source_project, item_id)`, поэтому каждое решение эксперта
заводило ДВЕ записи: 6409 лишних из 20433 на 117 проектах (06.08.2026). То же
в графике: 110 лишних отметок дня завершения.

Источник закрыт в коде (`canonical_source_project`, коммит 9d63a4cc), этот
скрипт разбирает накопленное.

Какую запись оставляем в группе:
  1. ту, на чей `DEC-id` уже ссылаются patterns.json / evidence_golden_set.json —
     иначе осиротим 7021 ссылку;
  2. иначе — более позднюю по `expert_date`: это последнее слово эксперта
     (в 3 группах решения прямо противоположны, в 4 расходятся причины);
  3. при равенстве — с меньшим номером DEC.

Запуск:
    python scripts/dedupe_project_id_forms.py            # холостой прогон
    python scripts/dedupe_project_id_forms.py --apply    # запись + бэкапы
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

KB_DIR = _ROOT / "knowledge_base"
DECISIONS = KB_DIR / "decisions_log.json"
COMPLETIONS = KB_DIR / "schedule_completion.json"
REFERENCING = [KB_DIR / "patterns.json", KB_DIR / "evidence_golden_set.json"]

_PREFIX_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,5}")
_DEC_RE = re.compile(r"DEC-\d+")


def canon(project_id) -> str:
    pid = str(project_id or "").strip()
    if "/" not in pid:
        return pid
    head, _, tail = pid.partition("/")
    return tail.strip() if tail and _PREFIX_RE.fullmatch(head) else pid


def referenced_ids() -> set[str]:
    """DEC-id, на которые ссылаются паттерны и эталонная выборка."""
    ids: set[str] = set()
    for path in REFERENCING:
        if path.exists():
            ids.update(_DEC_RE.findall(path.read_text(encoding="utf-8")))
    return ids


def pick(group: list[dict], refs: set[str]) -> tuple[dict, str]:
    """Какую запись оставить и по какой причине."""
    linked = [e for e in group if str(e.get("id")) in refs]
    if len(linked) == 1:
        return linked[0], "на неё ссылаются паттерны/эталон"
    pool = linked or group
    by_date = sorted(pool, key=lambda e: (str(e.get("expert_date") or ""), str(e.get("id") or "")))
    return by_date[-1], "последняя по дате решения"


def dedupe_decisions(refs: set[str], apply: bool) -> dict:
    data = json.loads(DECISIONS.read_text(encoding="utf-8"))
    entries = data.get("entries", [])
    groups: dict[tuple, list[dict]] = collections.defaultdict(list)
    for e in entries:
        groups[(e.get("object_id"), canon(e.get("source_project")), e.get("item_id"), e.get("item_type"))].append(e)

    keep_ids: set[int] = set()
    dropped: list[dict] = []
    conflicts: list[dict] = []
    orphaned_refs = 0

    for key, group in groups.items():
        if len(group) == 1:
            keep_ids.add(id(group[0]))
            continue
        winner, reason = pick(group, refs)
        keep_ids.add(id(winner))
        decisions = {str(e.get("expert_decision") or "") for e in group}
        if len(decisions) > 1:
            conflicts.append({
                "project": key[1], "item": key[2],
                "kept": f"{winner.get('id')} = {winner.get('expert_decision')}",
                "dropped": [f"{e.get('id')} = {e.get('expert_decision')}" for e in group if e is not winner],
                "reason": reason,
            })
        for e in group:
            if e is winner:
                continue
            dropped.append(e)
            if str(e.get("id")) in refs:
                orphaned_refs += 1

    kept = [e for e in entries if id(e) in keep_ids]
    # Карта «удаляемый DEC-id → оставшийся»: ссылки в паттернах и эталонной
    # выборке надо перенаправить, иначе 201 привязка повиснет в пустоте.
    remap: dict[str, str] = {}
    for key, group in groups.items():
        if len(group) < 2:
            continue
        winner, _ = pick(group, refs)
        for e in group:
            if e is not winner and str(e.get("id")) in refs:
                remap[str(e.get("id"))] = str(winner.get("id"))
    # Форму имени приводим к канону и у оставшихся — иначе следующая запись из
    # Excel снова разойдётся с интерфейсом.
    renamed = 0
    for e in kept:
        c = canon(e.get("source_project"))
        if c != e.get("source_project"):
            e["source_project"] = c
            renamed += 1

    if apply and dropped:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(DECISIONS, DECISIONS.with_suffix(f".json.before_dedupe_{stamp}"))
        data["entries"] = kept
        DECISIONS.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        retarget_references(remap, stamp)

    return {
        "было": len(entries), "останется": len(kept), "удалим": len(dropped),
        "имя_приведено_к_канону": renamed,
        "конфликты_решений": conflicts,
        "удаляемых_со_ссылками": orphaned_refs,
        "ссылок_перенаправим": len(remap),
    }


def retarget_references(remap: dict[str, str], stamp: str) -> None:
    """Перевести ссылки паттернов и эталона на оставшиеся записи."""
    if not remap:
        return
    pattern = re.compile("|".join(re.escape(k) for k in sorted(remap, key=len, reverse=True)))
    for path in REFERENCING:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        new_text, n = pattern.subn(lambda m: remap[m.group(0)], text)
        if n:
            shutil.copy2(path, path.with_suffix(f".json.before_dedupe_{stamp}"))
            path.write_text(new_text, encoding="utf-8")
            print(f"   ссылки в {path.name}: перенаправлено {n}")


def dedupe_completions(apply: bool) -> dict:
    data = json.loads(COMPLETIONS.read_text(encoding="utf-8"))
    comps = data.get("completions", [])
    groups: dict[tuple, list[dict]] = collections.defaultdict(list)
    for c in comps:
        groups[(c.get("object_id"), canon(c.get("source_project")), c.get("date"),
                c.get("reviewer"), str(c.get("version_id") or ""))].append(c)

    kept, dropped = [], 0
    for group in groups.values():
        # Отметка дня — факт без содержания, поэтому берём первую (самую раннюю
        # по set_at): именно она зафиксировала завершение.
        winner = sorted(group, key=lambda c: str(c.get("set_at") or ""))[0]
        winner["source_project"] = canon(winner.get("source_project"))
        kept.append(winner)
        dropped += len(group) - 1

    if apply and dropped:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        shutil.copy2(COMPLETIONS, COMPLETIONS.with_suffix(f".json.before_dedupe_{stamp}"))
        data["completions"] = kept
        COMPLETIONS.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"было": len(comps), "останется": len(kept), "удалим": dropped}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="записать изменения (по умолчанию — холостой прогон)")
    args = ap.parse_args()

    refs = referenced_ids()
    print(f"ссылок на DEC-id в паттернах и эталонной выборке: {len(refs)}\n")

    dec = dedupe_decisions(refs, args.apply)
    print("── Журнал решений ──")
    for k in ("было", "останется", "удалим", "имя_приведено_к_канону", "удаляемых_со_ссылками", "ссылок_перенаправим"):
        print(f"   {k}: {dec[k]}")
    if dec["конфликты_решений"]:
        print(f"\n   ⚠ группы с РАЗНЫМИ вердиктами: {len(dec['конфликты_решений'])}")
        for c in dec["конфликты_решений"]:
            print(f"      {c['project']} {c['item']}: оставляем {c['kept']} ({c['reason']}), "
                  f"убираем {', '.join(c['dropped'])}")

    comp = dedupe_completions(args.apply)
    print("\n── Отметки дня завершения ──")
    for k, v in comp.items():
        print(f"   {k}: {v}")

    print("\n" + ("ИЗМЕНЕНИЯ ЗАПИСАНЫ (бэкапы рядом с файлами)" if args.apply
                  else "холостой прогон — ничего не записано; для записи: --apply"))


if __name__ == "__main__":
    main()
