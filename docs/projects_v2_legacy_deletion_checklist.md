# projects_v2 — legacy deletion-readiness checklist

> Обновление 2026-07-13: production уже работает в
> `projects_v2` / `projects_v2_primary`; coverage backlog и drift равны нулю,
> ledger validation и 558 профильных тестов зелёные. Для явно выбранного
> ускоренного сценария после побайтово проверенного внешнего бэкапа подготовлен
> защищённый workflow:
> `docs/projects_v2_legacy_retirement_after_backup.md`. Он не отменяет риски
> literal parity и требует их отдельного явного подтверждения.

**Дата:** 2026-06-18
**Статус:** КРИТЕРИИ. Это финальный gate, после которого вообще можно удалять
legacy. На момент написания — **НЕ выполнен** (write-cutover ещё не делался).

## Жёсткое правило

```
projects/ НЕЛЬЗЯ удалять напрямую.
Сначала — ТОЛЬКО quarantine archive (mv projects projects_legacy_archive_<date>).
Удаление архива — ТОЛЬКО после периода наблюдения и этого checklist.
```

Последовательность: **cutover → наблюдение → quarantine archive → наблюдение →
удаление архива**. Перепрыгивать этапы нельзя.

## Предпосылки (должны быть выполнены ДО старта таймера наблюдения)

- [ ] write-cutover выполнен (`AUDIT_STORAGE_BACKEND=projects_v2`,
      `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`) по
      `projects_v2_write_cutover_playbook`;
- [ ] все открытые блокеры playbook закрыты (source-reading из v2, promotion
      runs→latest, export rewrite, destructive contract, prepare/batch,
      `production_uses_v2()` стал True);
- [ ] legacy переведён в quarantine archive (`plan_legacy_quarantine` →
      ручной `mv`), а НЕ удалён.

## Критерии готовности к удалению архива (ВСЕ обязательны)

- [ ] **30 дней** стабильного `projects_v2_primary` в проде;
- [ ] **0 critical errors** за период (нет 500 на read/audit/export, нет
      v2-write errors, `dual_write_shadow_errors.jsonl` не растёт);
- [ ] **все active проекты** прошли полный v2-primary lifecycle (upload →
      audit → findings → review → export) хотя бы раз;
- [ ] v2-only тесты зелёные:
      `pytest tests -q -k "projects_v2 or v2_primary or write_facade or migration_coverage"`;
- [ ] зелёные smoke: export / clean / rename / audit / prepare / batch / resume;
- [ ] **external backup** legacy верифицирован (tar восстанавливается, checksum
      сходится, лежит вне основного диска);
- [ ] **quarantine archive не использовался 30 дней** (нет обращений к
      `projects_legacy_archive_*`, никакой код/endpoint не резолвит в него);
- [ ] verifier покрытия чист: `verify_migration_coverage.py` →
      `missing_v2_real_backlog=0`, нет проектов, существующих только в legacy;
- [ ] **rollback path validated** — проверено, что из backup можно вернуть
      legacy и переключить `AUDIT_STORAGE_BACKEND=legacy` /
      `WRITE_MODE=dual_write_shadow` (учения отката проведены);
- [ ] **два инженера** независимо подтвердили готовность (sign-off).

## Что проверить перед самим `rm` архива

- [ ] grep по коду: ни один путь не строит `projects/` или
      `projects_legacy_archive_*` как обязательный источник
      (`resolve_project_dir`, `PROJECTS_DIR`, hardcodes);
- [ ] `DualReadService` / canary не обращаются к legacy
      (`legacy_project_path` в v2 `document.json` либо обновлены, либо
      больше не читаются как обязательные);
- [ ] decisions_log / KB целостность подтверждена по v2 project_id;
- [ ] нет running audit/batch/prepare на момент удаления.

## Порядок удаления (только после ВСЕХ галочек)

1. финальный external backup архива;
2. ещё раз verifier + smoke;
3. удаление в maintenance-окне;
4. наблюдение 1–2 дня; при любой проблеме — restore из backup.

## Текущий вывод

```
Можно ли удалять projects/ сейчас: НЕТ
Причина: write-cutover не выполнялся; prod = legacy-read + dual_write_shadow;
         блокеры playbook открыты; период наблюдения не начинался.
```

Связанные документы:
- `docs/projects_v2_write_cutover_playbook.md`
- `scripts/projects_v2/plan_legacy_quarantine.py`
- `scripts/projects_v2/verify_migration_coverage.py`
