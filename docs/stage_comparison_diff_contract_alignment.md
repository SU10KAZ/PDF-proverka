# Stage Comparison — контракт вывода, выравнивание и доменная схема (r5 / r4 / r1)

**Дата:** 2026-06-05

Три улучшения качества сравнения чертежей (Qwen→enriched MD→Opus), целящие в
один системный риск: **«Qwen не распознал поле» неотличимо от «инженер убрал»**.
Реализованы вместе с r6 self-check (см.
[stage_comparison_main_path_selfcheck.md](stage_comparison_main_path_selfcheck.md))
и проактивным тайлингом r2 (см.
[stage_comparison_qwen_problem_block_retry.md](stage_comparison_qwen_problem_block_retry.md)).

## r5 — контракт вывода Opus (always-on, prompt-only)

**Модуль:** [enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py).

`SYSTEM_PROMPT` дополнен правилом «три состояния + две цитаты»:

- для типов «изменилось» (`changed`/`material_changed`/…/`section_changed`)
  Opus ОБЯЗАН процитировать ОБА значения (`evidence_left` И `evidence_right`);
- если факт виден только с одной стороны и отсутствие неоднозначно (возможен
  пропуск распознавания), Opus использует новый тип **`present_one_side`** и
  пишет в отсутствующую сторону «не описано (возможно, не распознано)» вместо
  `removed`/`added`;
- однозначное появление/исчезновение по-прежнему оформляется `added`/`removed`;
- новое поле **`disputed`** (bool) — спорная/неуверенная дельта.

`_normalize_change`:
- `present_one_side` добавлен в `_ALLOWED_TYPE`;
- `disputed` прокидывается;
- `present_one_side` ИЛИ `disputed=true` → принудительно `requires_human_review=true`.

`v2_review.derive_quality_label`: `disputed=true` → `questionable` (приоритет у
`requires_human_review` → `needs_human_review`).

Always-on: это prompt/normalization-улучшение без потери данных, действует на
следующих прогонах сравнения. Откат — git revert.

## r4 — выравнивание по потребителю + словарь синонимов (always-on)

**Модуль:** [enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py),
данные: [backend/app/data/consumer_synonyms.json](../backend/app/data/consumer_synonyms.json).

`SYSTEM_PROMPT` дополнен правилом: сопоставлять отходящие линии/потребители по
ИМЕНИ потребителя (ВРУ1, ШУ-ХЦ, ЩР-1а), а НЕ по позиции и НЕ по обозначению
аппарата (`1QF8` в двух стадиях может питать разные нагрузки).

`build_user_prompt` всегда инжектит тег `<CONSUMER_SYNONYMS>` из
`consumer_synonyms.json` (`load_consumer_synonyms` / `build_consumer_synonyms_context`):
группы эквивалентных обозначений (`ШУ-ХЦ = ВРУ-ХЦ = шкаф управления хладоцентром`)
— Opus трактует их как одного потребителя, переименование внутри группы не
считается изменением.

Путь файла переопределяется env `STAGE_COMPARISON_CONSUMER_SYNONYMS_FILE`.
Пустой/отсутствующий файл → тег не добавляется (fail-soft, поведение как раньше).
Оператор редактирует JSON под объект.

## r1 — фиксированная доменная JSON-схема Qwen (flag-gated, default OFF)

**Модуль:** [md_image_enrichment.py](../backend/app/services/stage_comparison/md_image_enrichment.py).

Generic-схема Qwen переменной длины: отсутствующее поле молча выпадает, и Opus
не отличает «поля нет» от «не описано». Доменный слой навязывает фиксированный
набор слотов с явным «не указано» для отсутствующих, ОДИНАКОВЫЙ для обеих сторон.

| Флаг | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED` | `false` | включить доменные поля для схем |

Когда ВКЛ:
- схемный prompt (`scheme`/`dense_scheme`) получает суффикс с требованием вернуть
  объект `domain_fields` со ВСЕМИ слотами (`feeders`, `main_breakers`,
  `sectional`, `metering`, `compensation`, `cts`, `busbars`, `earthing`, `notes`),
  отсутствующий слот = «не указано» (не выдумывать, не достраивать ряды);
- `prompt_version` блока становится `v6_scheme_domain_fields` → `compute_image_cache_key`
  даёт другой ключ, старый v5-кеш не задевается; **после включения нужен
  re-enrichment пары** (cache miss), иначе доменных полей в описаниях не будет;
- `_coerce_domain_fields` детерминированно добивает недостающие/пустые слоты
  «не указано» (не доверяя полноте модели) — это и есть гарантия «одинаковая
  схема для обеих сторон»;
- `_format_qwen_description_md` рендерит секцию `DOMAIN_FIELDS` ВСЕГДА (включая
  «не указано»), чтобы Opus видел явное отсутствие, а не пропуск.

Когда ВЫКЛ (default): prompt/версия/кеш/рендер идентичны прежним (no-op).

`DOMAIN_FIXED_SLOTS` сейчас покрывает электрические однолинейные схемы;
расширяется добавлением ключей по block_type.

## Деплой

uvicorn без `--reload` держит модуль в памяти — после правок нужен рестарт:

```bash
pkill -f "uvicorn backend.app.main"
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
```

r5/r4 действуют сразу на новых прогонах сравнения. Для r1 после рестарта надо
выставить `STAGE_COMPARISON_DOMAIN_FIELDS_ENABLED=true` и перезапустить
md-enrichment пары (`force=true`), затем сравнение.

## Тесты

- [tests/test_stage_comparison_unified_analysis.py](../tests/test_stage_comparison_unified_analysis.py) — r5 (`_normalize_change` present_one_side/disputed) + r4 (синонимы, инъекция, правило, env-override, fail-soft);
- [tests/test_stage_comparison_v2_review.py](../tests/test_stage_comparison_v2_review.py) — `derive_quality_label` disputed→questionable;
- [tests/test_stage_comparison_domain_fields.py](../tests/test_stage_comparison_domain_fields.py) — r1 (flag off/on, coerce, рендер, cache-key, не-схемный no-op).
