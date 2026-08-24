# Cleanup plan: Vector Architecture research archive

Дата inventory: 2026-08-24. Область: только
`experiments/stage_comparison_vector_architecture_opus/`.

Цель очистки — оставить архив знаний и воспроизводимый исследовательский код для будущего G2,
удалив результаты отдельных прогонов, изображения, дампы и кэши. Production, Stage 5.3,
Graphic G1 и production-Вектограф находятся вне области этой операции.

## Pre-cleanup inventory

До удаления в каталоге находилось 1 579 файлов общим фактическим размером 205 995 045 байт
(204 458 308 байт удаляемых данных и 1 536 737 байт сохраняемых данных; `du -sh`: 201M).
Git показывал 1 изменённый tracked-файл, 300 untracked-файлов и 644 ignored-файла;
staged-файлов не было.

| Категория | Файлов | Размер, байт | Типы | Действие |
|---|---:|---:|---|---|
| A. Знание и контракты | 11 | 233 885 | 8 Markdown, 3 JSON | сохранить |
| B. Исследовательский код и его статическая конфигурация | 196 | 1 302 852 | 194 Python, 1 JSON, 1 gitignore | сохранить |
| C. Промежуточные результаты и кэши | 1 372 | 204 458 308 | 629 JSON, 480 PNG, 162 PYC, 46 GZ, 43 Markdown, 10 TXT, 2 LOG | удалить |
| **Итого** | **1 579** | **205 995 045** |  |  |

Классификатор покрывает все 1 579 файлов; неклассифицированных файлов нет.

## Decision table

| file/path | type | action | reason |
|---|---|---|---|
| `README.md` | A: knowledge | keep | карта исследования и инструкции воспроизведения |
| `BRIEF.md` | A: knowledge | keep | исходный исследовательский бриф |
| `OPUS_VECTOR_ARCHITECTURE_REPORT.md` | A: knowledge | keep | финальный архитектурный отчёт |
| `artifacts/orchestrator_findings.md` | A: knowledge | keep | итоговые измерения оркестратора |
| `artifacts/pair_analysis.md` | A: knowledge | keep | финальный разбор пар |
| `artifacts/failure_modes.md` | A: knowledge | keep | каталог подтверждённых режимов отказа |
| `artifacts/proposed_contract.json` | A: contract | keep | предлагаемый контракт VectorBlockDescription v0.2 |
| `artifacts/comparison_example.json` | A: contract example | keep | пример журнала изменений L3 |
| `artifacts/ai_payload_examples.json` | A: contract example | keep | примеры минимального AI-payload |
| `artifacts/architecture_diagram.md` | A: architecture | keep | схема слоёв и ответственности |
| `artifacts/hybrid_TASK_CONTRACT.md` | A: contract | keep | контракт точечной Vision-задачи |
| `.gitignore` | B: support | keep | не допускает повторного добавления Python-кэшей |
| `__init__.py` | B: source | keep | Python package marker |
| `poc/**/*.py` (2 файла) | B: source | keep | PoC формы контракта и package marker |
| `probes/**/*.py` (191 файл) | B: source | keep | object grouping, invisible ink, descriptors, relation graph, hybrid routing, dimensions и verification tools |
| `probes/dim_blocks.json` | B: static research config | keep | входная конфигурация dimension probes, не результат прогона |
| `artifacts/*`, кроме восьми явно сохранённых файлов (269 файлов) | C: generated results/docs | delete | JSON/GZ/TXT/LOG, benchmark dumps и промежуточные FINDINGS/VERIFY |
| `artifacts/**/` (941 файл) | C: generated artifacts | delete | изображения, crops, overlays, diagnostics, descriptions и raw run records |
| `**/__pycache__/*.pyc` (162 файла) | C: cache | delete | регенерируемый Python bytecode |

`poc/` сохранён как исследовательский исходный код: его удаление противоречило бы требованию
сохранить research source code, хотя в целевой схеме каталога этот небольшой пакет отдельно не
показан.

## Generated artifact directory inventory

Все перечисленные ниже каталоги относятся к категории C и удаляются целиком после сохранения
этого плана.

| file/path | files | bytes | type | action | reason |
|---|---:|---:|---|---|---|
| `artifacts/dim_evidence/` | 10 | 2 577 495 | render evidence | delete | промежуточные изображения dimension probe |
| `artifacts/falsify_cases/` | 75 | 4 578 162 | JSON cases | delete | результаты прогонов |
| `artifacts/falsify_crops/` | 72 | 1 029 374 | crops | delete | регенерируемые изображения |
| `artifacts/falsify_visual/` | 32 | 2 750 924 | diagnostics | delete | регенерируемые изображения |
| `artifacts/fmc_crops/` | 42 | 10 165 060 | crops | delete | регенерируемые изображения |
| `artifacts/fmc_descriptions/` | 63 | 12 089 342 | dumps | delete | промежуточные описания корпуса |
| `artifacts/hatchnoise/` | 60 | 12 321 870 | results/renders | delete | результаты hatch/noise probe |
| `artifacts/hybrid_crops/` | 12 | 939 460 | crops | delete | регенерируемые изображения |
| `artifacts/obj_overlays/` | 20 | 9 352 917 | overlays | delete | диагностические изображения |
| `artifacts/obj_sections/` | 5 | 1 160 982 | renders | delete | диагностические изображения |
| `artifacts/obj_zoom/` | 25 | 4 659 689 | zoom renders | delete | диагностические изображения |
| `artifacts/p02v/` | 3 | 137 426 | controls | delete | промежуточные проверки |
| `artifacts/ptn/` | 53 | 7 150 996 | results/renders | delete | результаты pattern probe |
| `artifacts/relgraph_rotation_crops/` | 4 | 105 281 | crops | delete | регенерируемые изображения |
| `artifacts/tbl_crops/` | 27 | 3 732 901 | crops | delete | регенерируемые изображения |
| `artifacts/tcf_crops/` | 39 | 1 357 980 | crops | delete | регенерируемые изображения |
| `artifacts/txgeo_crops/` | 7 | 1 049 288 | crops | delete | регенерируемые изображения |
| `artifacts/txgeo_fresh_descriptions/` | 11 | 5 434 707 | dumps | delete | промежуточные описания |
| `artifacts/txgeo_relations/` | 60 | 13 009 394 | relation dumps | delete | результаты прогонов |
| `artifacts/v02/` | 9 | 645 265 | prototype results | delete | промежуточные результаты PoC |
| `artifacts/vv_crops/` | 1 | 88 859 | crop | delete | регенерируемое изображение |
| `artifacts/vv_verify/` | 2 | 16 818 | raw verification | delete | промежуточные результаты Vision |
| `artifacts/vvb_crops/` | 28 | 4 039 171 | crops | delete | регенерируемые изображения |
| `artifacts/vvb_runs/` | 96 | 464 821 | run records | delete | raw multimodal calls |
| `artifacts/vvd_crops/` | 11 | 3 566 761 | crops | delete | регенерируемые изображения |
| `artifacts/vvd_runs/` | 66 | 563 910 | run records | delete | raw Vision verification calls |
| `artifacts/vvg_fresh/` | 56 | 82 760 885 | extracted blocks | delete | тяжёлые промежуточные JSON/PNG |
| `artifacts/vvg_runs/` | 14 | 165 458 | run records | delete | raw gate verification calls |
| `artifacts/vvg_runs_rep2/` | 8 | 60 552 | repeated runs | delete | raw repeated calls |
| `artifacts/vvp_verify/` | 20 | 158 833 | verification records | delete | промежуточные результаты pipeline scoring |
| `artifacts/vvp_vision/` | 10 | 77 051 | Vision records | delete | промежуточные результаты pipeline scoring |

## Post-cleanup checks

1. В `artifacts/` остаются ровно восемь whitelisted-файлов.
2. В `probes/` и `poc/` остаётся исследовательский исходный код; `__pycache__` отсутствуют.
3. Поиск импортов и runtime-чтений удалённых файлов отделяет ожидаемые reproduction inputs от
   сломанных Python-импортов.
4. `git diff` и `git status` содержат только
   `experiments/stage_comparison_vector_architecture_opus/`.
5. Все оставшиеся Markdown и JSON открываются/парсятся.
6. Фиксируются итоговые число файлов и размер каталога.

## Execution result

Очистка выполнена 2026-08-24 в соответствии с таблицей решений:

- 1 372 файла категории C (204 458 308 байт) удалены из рабочей копии;
- recovery-копия на время проверки: `/tmp/pdf-proverka-vector-cleanup.hxQ2eC`; после успешных
  проверок и фиксации результата она удалена;
- осталось 208 файлов общим размером содержимого 1 546 154 байта (`du -sh`: 2.0M);
- размер содержимого каталога уменьшен примерно на 99.25%;
- в `artifacts/` осталось ровно 8 whitelisted-файлов;
- сохранены 194 Python-файла, 4 валидных JSON и 9 UTF-8 Markdown-файлов;
- PNG, JPG, JPEG, PYC, raw run records и diagnostic directories отсутствуют;
- удалённый набор содержит 0 Python source files;
- AST-проверка всех 194 Python-файлов и import smoke-check 8 ключевых модулей успешны;
- один hybrid-зонд по-прежнему требует внешнюю optional-зависимость `tiktoken`; это не ссылка
  на удалённый файл и не результат очистки;
- Git diff за пределами `experiments/stage_comparison_vector_architecture_opus/` пуст.
