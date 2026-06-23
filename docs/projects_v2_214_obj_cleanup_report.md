# 214_Obj cleanup safety report

Дата: 2026-06-23
Путь: `projects_v2/objects/214_Obj`
Режим: read-only анализ. Итоговое состояние после проверки: `214_Obj` на месте, stale ledger-запись на месте. Реальная чистка не оставлена выполненной.

## Вывод

`214_Obj` выглядит как тестовый остаток, а не реальный объект:

- `object.json`: `display_name = "214. Obj"`, `legacy_path = /tmp/pytest-of-coder/.../214. Obj`.
- Документ: `KM/1232-ЧМ-КМ-1`, `legacy_project_path = /tmp/pytest-of-coder/.../214. Obj/KM/1232-ЧМ-КМ-1`.
- `version.json`: `analysis_status = partial`, нет исходных PDF/MD/OCR/result в `input_manifest.json` (`present: false`).
- `project_info.json`: минимальный синтетический объект `{"project_id":"1232-ЧМ-КМ-1","name":"1232-ЧМ-КМ-1","section":"KM"}`.
- Поиск по содержимому не показал признаков реального объекта Alia/Mosfilm/King и т.п.; совпадения - `/tmp/pytest-of-coder/...` и синтетические findings.

Внутри есть не реальные, а синтетические audit artifacts:

```text
files: 67
dirs: 47
03_findings.json files: 34
findings items: 34, все вида F-001 / problem: x
expert_review.json files: 27
review decisions: 27, все для F-001 с decision=accepted
```

Важно: в `projects_v2/_system/old_to_new_map.json` есть две записи для `1232-ЧМ-КМ-1`:

- каноническая реальная: `objects/214_Alia_ASTERUS/...`, legacy path `/projects/214. Alia (ASTERUS)/KM/1232-ЧМ-КМ-1`;
- тестовая stale: `objects/214_Obj/...`, legacy path `/tmp/pytest-of-coder/.../214. Obj/KM/1232-ЧМ-КМ-1`.

Поэтому безопасная ручная чистка должна быть не только move папки, но и удаление ровно stale ledger-записи `v2_document_dir contains /objects/214_Obj/`, с бэкапом ledger. В этом задании итоговая чистка не выполнялась.

## Перечень верхнего содержимого

```text
projects_v2/objects/214_Obj/object.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/document.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/current_version.txt
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/version.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/01_input/input_manifest.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/01_input/project_info.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/03_analysis/latest/03_findings.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/04_review/expert_review.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/03_analysis/runs/run_*/03_findings.json
projects_v2/objects/214_Obj/disciplines/KM/documents/1232-ЧМ-КМ-1/versions/v001/03_analysis/runs/run_*/expert_review.json
```

## Рекомендуемая ручная процедура, не выполнялась

Команды ниже должен выполнять человек в maintenance-окно после pre-flight idle-check. Они делают tar-бэкап, бэкап ledger, обратимый move в `_trash` и удаляют только stale ledger-запись `214_Obj`, не трогая канонический `214_Alia_ASTERUS`.

```bash
ps aux | grep -E "process_project|blocks.py|gemma|claude -p|comparison|uvicorn" | grep -v grep
python3 -c 'import json; from pathlib import Path;
for path in [Path("backend/app/data/prepare_queue.json"), Path("backend/app/data/batch_queue.json")]:
    print(path, json.loads(path.read_text(encoding="utf-8")) if path.exists() else "missing")'

TS=$(date +%Y%m%d-%H%M%S)
cp projects_v2/_system/old_to_new_map.json "/tmp/old_to_new_map.214_Obj_cleanup.$TS.json"
tar -C projects_v2/objects -czf "/tmp/214_Obj.$TS.tgz" 214_Obj
mkdir -p projects_v2/_system/_trash
mv projects_v2/objects/214_Obj "projects_v2/_system/_trash/214_Obj.$TS"
python3 - <<'PY_LEDGER_214_OBJ'
import json
from pathlib import Path
p = Path('projects_v2/_system/old_to_new_map.json')
data = json.loads(p.read_text(encoding='utf-8'))
items = data.get('migrations', [])
kept = []
removed = []
for item in items:
    if '/objects/214_Obj/' in str(item.get('v2_document_dir', '')):
        removed.append(item)
    else:
        kept.append(item)
if len(removed) != 1:
    raise SystemExit(f'expected exactly 1 stale 214_Obj ledger entry, got {len(removed)}')
data['migrations'] = kept
p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('removed stale ledger entries:', len(removed))
PY_LEDGER_214_OBJ
```

Rollback после такой процедуры:

```bash
mv "projects_v2/_system/_trash/214_Obj.$TS" projects_v2/objects/214_Obj
cp "/tmp/old_to_new_map.214_Obj_cleanup.$TS.json" projects_v2/_system/old_to_new_map.json
```

## Риски

- Папка содержит synthetic findings/reviews, поэтому формально она не пустая. Решение о чистке должен подтвердить человек.
- Ledger cleanup нужен, иначе stale registry entry останется указывать на перемещённую папку.
- Команды выше намеренно не используют `--force` и не трогают legacy `projects/`.
