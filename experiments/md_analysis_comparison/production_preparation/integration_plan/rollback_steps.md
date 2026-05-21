# Rollback Steps

**Дата:** 2026-05-20

Стратегия отката для Phase 0 / Phase 1 — поэтапная, без потери данных.

---

## Phase 0 rollback

### Quick rollback (1 минута, без deploy)
```bash
# В env-конфигурации (например, .env или systemd-environment):
STAGE01_DEDUP_ENABLED=false
```

Перезапуск backend → следующий pipeline-run **пропускает** dedup-проход.
Это уже default; явный rollback — установить `false`.

**Эффект:**
- Существующие `03_findings.json` не трогаются.
- Новые `03_findings.json` не содержат `meta.dedup_report`.
- Никаких миграций данных.

### Code rollback (1 PR)
Revert merge commit Phase 0. Никаких миграций; checklists/data директории
ещё не созданы на Phase 0; даже если есть — они аддитивные.

### Per-project rollback
Существующий version_service в production уже умеет хранить версии
`03_findings.json` ([decisions_log.json](../../../knowledge_base/decisions_log.json)
зафиксирован). Чтобы откатить конкретный проект — выбрать предыдущую версию.

### Состояние после Phase 0 rollback
- ✅ Все pre-Phase-0 проекты работают без изменений.
- ✅ Новые проекты идут без dedup.
- ✅ Никаких stale данных.

---

## Phase 1 rollback

### Quick rollback (1 минута, без deploy)
```bash
STAGE01_COMPLETENESS_LENS_ENABLED=false
# document_type detector продолжает работать (это безопасно), но lens не вызывается.
```

Альтернатива (более полный rollback):
```bash
STAGE01_COMPLETENESS_LENS_ENABLED=false
STAGE01_COMPLETENESS_BY_DOC_TYPE=""  # пустой → lens skip на любом doc_type
```

Перезапуск → следующий pipeline-run использует только Opus current_method
(но с **новым prompt'ом** `text_analysis_task.md` — see "Prompt-specific rollback"
ниже).

**Эффект:**
- Pipeline сводится к "current_method only", но с новым prompt'ом.
- `meta.document_type` всё ещё пишется (но не используется для routing).
- `meta.completeness_applied=false`.

### Prompt-specific rollback (если новый prompt сам по себе делает хуже)

Этот сценарий — наибольший risk (R1.4 в [estimated_risk.md](estimated_risk.md)).

**Опция A: env-toggleable prompt path (рекомендованный design)**

При имплементации Phase 1 рекомендуется добавить:

```python
# backend/app/core/config.py
TEXT_ANALYSIS_TASK_TEMPLATE_LEGACY = _PIPELINE_RU / "text_analysis_task_legacy.md"
TEXT_ANALYSIS_TASK_TEMPLATE = (
    TEXT_ANALYSIS_TASK_TEMPLATE_LEGACY
    if os.getenv("STAGE01_USE_LEGACY_PROMPT", "false").lower() == "true"
    else _PIPELINE_RU / "text_analysis_task.md"
)
```

При первом merge'е Phase 1:
- старый prompt сохраняется как `text_analysis_task_legacy.md` (рядом);
- новый prompt — `text_analysis_task.md`;
- env `STAGE01_USE_LEGACY_PROMPT=true` → quick rollback к старому prompt'у.

**Опция B: git revert (полный rollback кода)**

```bash
git revert <phase1-merge-commit>
```

Останутся:
- discipline checklists в `backend/app/data/discipline_checklists/` (additive).
- `STAGE01_*` env vars в config (additive, no-op без модулей).
- 03_findings.json с `meta.schema_version=2` — не блокирует читателей.

Удалять или нет — на выбор. Безопасно оставить как мёртвый код до подтверждения
"never going back".

### Per-project rollback
Через существующий version_service: переключить активную версию
`01_text_analysis.json` и `03_findings.json` на предыдущую (pre-Phase-1).

### Состояние после Phase 1 rollback
- ✅ Все pre-Phase-1 проекты работают.
- ✅ Phase-1-вышедшие проекты сохраняют `schema_version=2` outputs, но frontend
  / Excel-экспорт игнорируют новые поля.
- ✅ Re-audit'ы (если нужны) идут под legacy prompt + только Opus.

---

## Полный rollback (Phase 0 + Phase 1)

```bash
STAGE01_DEDUP_ENABLED=false
STAGE01_COMPLETENESS_LENS_ENABLED=false
STAGE01_USE_LEGACY_PROMPT=true   # если опция A была введена
```

Перезапуск backend → pipeline ведёт себя как до Phase 0/1 deploy.

Затем — git revert обоих merge commits (Phase 1 first, потом Phase 0).

---

## Data cleanup

### НЕ требуется
- Существующие `01_text_analysis.json` / `03_findings.json` остаются как есть.
- `meta.dedup_report`, `meta.document_type`, `meta.completeness_applied` —
  optional fields, безопасно остаются в данных.
- Checklists в `backend/app/data/discipline_checklists/` — read-only data,
  никакого вреда.

### Опционально (для чистоты)
Чтобы убрать новые поля из существующих outputs:
```python
# tools/strip_phase1_fields.py (не нужен на production, для чистоты dev)
for path in glob("projects/**/_output/03_findings.json", recursive=True):
    data = json.load(open(path))
    if data.get("meta", {}).get("schema_version") == 2:
        data["meta"].pop("dedup_report", None)
        data["meta"].pop("document_type", None)
        data["meta"].pop("completeness_applied", None)
        data["meta"]["schema_version"] = 1
        # findings: оставить или зачистить class_key/source_agents/...
```

Этого делать не обязательно; data остаётся валидным.

---

## Checklist отката (production playbook)

1. [ ] Сменить env vars в orchestrator (systemd/docker-compose).
2. [ ] `systemctl restart audit-backend` (или эквивалент).
3. [ ] Сверить через `/api/health` что backend поднялся.
4. [ ] Запустить smoke pipeline на 1 проекте — убедиться, что
       `meta.completeness_applied` отсутствует / `false`.
5. [ ] Если проблема серьёзная — git revert merge commit + redeploy.
6. [ ] Уведомить пользователей (если был user-visible regression).
7. [ ] Открыть incident ticket с диагнозом, чтобы не повторить.

---

## Сценарии rollback'а — таблица решений

| Симптом | Что делать |
|---|---|
| `meta.dedup_report.same_class_drops` > 5 на A0 проектах | `STAGE01_DEDUP_FUZZY_THRESHOLD=0.85` (tighten) или `STAGE01_DEDUP_ENABLED=false` |
| Sonnet lens rate-limit → весь pipeline timeout'ит | `STAGE01_COMPLETENESS_LENS_ENABLED=false` (graceful fallback должен сам это поймать; но env-flag — гарантированный путь) |
| FP взрыв на full_rd | `STAGE01_COMPLETENESS_BY_DOC_TYPE="audit_comparison"` (route off для full_rd) |
| Opus current_method начал писать «отсутствует пояснительная записка» на specification_only | `STAGE01_USE_LEGACY_PROMPT=true` (rollback prompt) |
| Schema-v2 outputs ломают frontend | НЕ должно случиться (verified pre-deploy); если случилось — `STAGE01_USE_LEGACY_PROMPT=true` + frontend hotfix |
| Полный коллапс — pipeline падает на старте | `STAGE01_DEDUP_ENABLED=false STAGE01_COMPLETENESS_LENS_ENABLED=false STAGE01_USE_LEGACY_PROMPT=true` + restart |

---

## Что НЕ может быть rollback'нуто quick'ом

- Уже записанные `03_findings.json` с `schema_version=2`. Чтобы их обновить —
  re-audit через version_service. Это on-demand, не blocking.
- Новые поля в `frontend/static/js/app.js`, если будут добавлены в follow-up
  PR — отдельный rollback.

Quick rollback **не возвращает** уже сделанные LLM-вызовы — paid_cost остаётся
в логе (см. MEMORY paid_api_guard). Это норма.
