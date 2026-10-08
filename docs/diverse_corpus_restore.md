# Корпус «блоки разных дисциплин»: чем он является и как его пересобрать

`experiments/блоки разных дисциплин` выглядит как исследовательский архив — на нём
даже стоит маркер `DELETE_CANDIDATE.md` («не удалять автоматически»). Но по факту
это **фикстура 17 тестов**: девяти профильных тестов геометрии (АР, КЖ, КМ, ТХ,
ГП, ОВ, ЭОМ, ВК, СС) и восьми смежных — `test_ar_ceiling_lighting_profile`,
`test_low_voltage_geometry`, `test_structural_access_geometry`,
`test_block_reference_catalog`, `test_build_vector_graph_gallery`,
`test_vectograf_eom_profiles`, `test_vector_path_graph`,
`backend/tests/test_water_supply_geometry`.

Каталог **не отслеживается git**: в индексе лежат только 36 файлов (README,
`*_PROFILES.md`, несколько скриптов и эталонных графов), а ~1100 PDF-вырезок,
их графы и манифесты живут только на диске. Поэтому `git clean -xdf experiments`
уносит фикстуру целиком, а `git checkout` её не возвращает.

## Что делать, если каталог пропал

```bash
python scripts/restore_diverse_corpus.py index --force   # индекс block_id → документ
python scripts/restore_diverse_corpus.py build --all     # вырезки + графы + манифесты
python scripts/restore_diverse_corpus.py coverage --all  # отчёты семантического покрытия
python scripts/restore_diverse_corpus.py ss              # оба корпуса СС (ALIA)
```

Обращений к моделям нет — вся геометрия детерминированная.

## Почему восстановление точное

Уцелели два независимых перечня состава корпуса:

* `backend/app/pipeline/stages/block_context/reference_catalog/disciplines/*.json` —
  1133 записи (`block_id`, `profile_id`, `subtype`, `source_page`), собранные
  компилятором каталога из этого же корпуса (версия `2026.07.13-1`);
* `docs/graphic_anchors/пробы/corpus_results.jsonl` — 1187 записей с точными
  именами файлов вырезок.

Сами блоки живы в `projects_v2`: у каждого документа есть `02_work/result.json`
(страницы, блоки, `coords_norm`/`polygon_points_norm`) и `02_work/document.pdf`.
Вырезка и граф строятся теми же функциями `build_<дисциплина>_graph_from_source`,
что работают в бою, поэтому результат не приближает исходный, а совпадает с ним:
проходят и жёсткие проверки поимённых блоков (`physical_line_segments_total > 100`
у `7DU7-346V-DN6`), и round-trip от исходного полигона.

## Три места, где восстановление легко ошибается

1. **Один `block_id` живёт в нескольких документах.** Среди них есть служебные
   вроде «ВЕКТОГРАФ — ТХ», где `document.pdf` подменён заглушкой на 1838 байт, а
   `result.json` описывает 7 страниц с `coords_norm = [0,0,1,1]`. Индекс поэтому
   мультизначный, а `_pick_version` берёт документ, где страница существует и
   блок не занимает лист целиком.
2. **Профиль `legend` — надведомственный.** У «Условных обозначений» собственный
   билдер и собственный гейт; если строить их дисциплинарным билдером, граф
   получит верный `profile_id`, но не наполнится, и `evaluate_legend_gate` вернёт
   `use: False`.
3. **СС требует имён, а не манифеста.** `test_low_voltage_geometry` и
   `test_structural_access_geometry` открывают вырезки по жёстким путям
   (`02_13АВ-РД-АПЗ.АПС-К3_V1__6W3K-9C4Y-VPY.pdf`), поэтому по СС
   восстанавливаются все вырезки перечня зондов, а не только каталожные.

## Семантическое покрытие

`*_SEMANTIC_COVERAGE.json` пересобирается сверкой: каждая подпись PDF-вырезки
ищется в `semantic_ledger` графа — плоском реестре всех текстовых строк с
координатами. Пропуски считаются по данным, а не проставляются нулями: если
реестр окажется неполным, отчёт покажет ненулевой `pdf_misses_total`, и тест
покрытия честно упадёт.

Формат совместим с компилятором каталога (`build_catalog.py` читает
`records[].categories.{covered,missed,source}` и `source_layer_state`).
