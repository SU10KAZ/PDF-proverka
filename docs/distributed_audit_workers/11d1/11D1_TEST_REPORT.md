# 11D.1 — отчёт по тестам

Ни один тест 11D.1 не обращается к модели. Промпты собираются офлайн; подставной
CLI здесь даже не нужен — проверяется вход, а не вызов.

## 1. Новый набор

`tests/test_11d1_text_analysis_semantic_equivalence.py` — **28 тестов, все
зелёные** (0,88 с).

Базой сравнения взята **ветка B** (`task_builder.prepare_text_analysis_task`,
Claude CLI с файловыми инструментами) — то есть буквально тот путь, который 11D
заменил. Это отличается от 11D, где `semantic_preservation_report` сверяется с
API-промптом ветки OpenRouter: тот уже прошёл `_clean_template_for_api`, поэтому
разницу между CLI-промптом и API-промптом он по определению не видит.

## 2. Покрытие требований §21

| § | требование | тесты |
|---|---|---|
| A | тот же MD → тот же payload | `test_a_provider_inlines_the_whole_md_verbatim`, `test_a_document_is_not_touched_by_path_cleanup`, `test_legacy_delivers_document_by_path_not_inline` |
| B | та же дисциплина → те же инженерные правила | `test_b_discipline_profile_reaches_both_paths` |
| C | сохранение инструкций severity | `test_c_severity_rules_identical_in_both`, `test_c_severity_semantics_carried_into_provider_prompt`, `test_c_severity_semantics_is_symmetric`, `test_c_severity_semantics_has_no_project_specific_content` |
| D | сохранение стража отсутствия | `test_d_absence_guard_reaches_both_paths_when_enabled` |
| E | сохранение md_prescan | `test_e_md_prescan_reaches_both_paths` |
| F | сохранение нормативного входа | `test_f_normative_reference_is_not_weaker_in_provider` |
| G | сохранение JSON-схемы | `test_g_output_schema_preserved` |
| H | транспортные инструкции сняты корректно | `test_h_tool_instructions_present_in_legacy_absent_in_provider` |
| I | **ни одно инженерное правило не удалено вместе с оболочкой Read/Write** | `test_i_every_engineering_line_of_legacy_survives` |
| J | нет зависимости от файловой системы проекта | `test_j_no_filesystem_dependency_in_provider_instructions` |
| K | нет требования Write | `test_k_no_write_requirement` |
| L | нет требования инструментов | `test_l_no_tool_requirement`, `test_l_tool_restriction_does_not_silence_absence_findings` |
| M | документ доставлен целиком | `test_m_document_chars_match_user_message` + тесты A |
| N | порядок секций | `test_n_section_order_matches_legacy`, `test_n_document_and_transport_contract_are_last` |
| O | отпечаток промпта | `test_o_prompt_is_deterministic`, `test_o_engineering_markers_survive` |
| P | прежний путь не изменён | `test_p_legacy_path_unchanged_by_11d1`, `test_p_shared_template_untouched` |
| Q | личный контекст остаётся исключённым | `test_q_personal_context_stays_excluded` |
| R | в операционных отчётах нет содержимого | `test_r_build_map_carries_no_content`, `test_r_soft_contract_report_carries_no_content` |

Непокрытых пунктов A..R нет.

## 3. Тест I — что он на самом деле проверяет

Из legacy-промпта выбрасываются строки, содержащие транспортные маркеры (`Read
tool`, `Write tool`, `DO NOT output to chat` и ещё шесть), а каждая оставшаяся
непустая строка ищется в provider-промпте после нормализации (абсолютные пути →
плейсхолдер, схлопывание пробелов).

Замер на реальном документе 133-23-ГК-ЭС:

| | инженерных строк legacy | не найдено в provider |
|---|---|---|
| provider как отгружен на 11D | 181 | **0** |
| provider после правок 11D.1 | 181 | **0** |

Тест не вырожденный: на синтетической фикстуре проверяется 181 строка (профиль
ЭОМ + шаблон), список маркеров дублируется в тесте намеренно — если фильтр
очистки расширят и он начнёт уносить инженерный текст, тест упадёт, а не молча
согласится с новым фильтром.

## 4. Регресс

| набор | результат |
|---|---|
| `tests/test_11d1_text_analysis_semantic_equivalence.py` | **28 passed** |
| `tests/test_distributed_workers_text_analysis_provider.py` (набор 11D) | **86 passed** |
| выборка `-k "provider or text_analysis or claude_runner or distributed or prompt or prescan or absence"` | 1741 passed, 1 failed, 7 skipped |
| **полный прогон** `tests` + `backend/tests` | **7705 passed**, 78 failed, 110 skipped, 33 errors |

### Атрибуция падений — новых нет

Тот же полный набор прогнан на **нетронутом worktree базового коммита**
`537c08a5` (`.claude/worktrees/distributed-audit-workers-text-analysis-provider`):

```
база (537c08a5):  78 failed, 7677 passed, 110 skipped, 33 errors
11D.1:            78 failed, 7705 passed, 110 skipped, 33 errors
```

Списки `FAILED`/`ERROR` сопоставлены построчно: **111 позиций в обоих, пересечение
полное**.

```
только у 11D.1 (новые падения):  — пусто —
только в базе (что-то починилось): — пусто —
разница по passed: +28 = ровно новый набор 11D.1
```

Это известный долг платформы (UI-тесты фронта, индекс норм, codex-раннер,
stage_comparison) плюс дрейф окружения worktree; к правкам 11D.1 отношения не
имеет.

Дополнительно исключены из прогона 9 файлов geometry-наборов
(`test_*_geometry.py`): они падают **на сборе** из-за отсутствия корпусных данных
в worktree — то же самое зафиксировано в `11D_TEST_REPORT.md` и воспроизводится на
базовом коммите.

## 5. Чего тесты НЕ доказывают

1. **Что правки улучшают результат.** Проверен только вход. Влияние
   `SEVERITY_SEMANTICS` и уточнённого транспортного контракта на выход модели не
   измерялось — §1 задания запрещает реальные вызовы.
2. **Что legacy-модель действительно читала `CLAUDE.md`.** Доказано, что канал
   был открыт (cwd = корень репозитория, `--setting-sources` не передаётся,
   собственный комментарий репозитория описывает эффект), и что она сделала 5
   ходов. Что именно попало в её контекст — из артефактов не восстановить.
3. **Что тема «тип системы заземления» пропала не из-за промпта.** Тесты
   показывают лишь, что входные условия для неё не ухудшились.
4. **Поведение на большом документе.** Все замеры — на одном документе в 11,4 К
   символов. Ветка нарезки промпта в provider-режиме отсутствует по-прежнему
   (вместо неё отказ по потолку 600 000 символов).
