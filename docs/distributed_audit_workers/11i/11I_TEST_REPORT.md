# Этап 11I — отчёт о тестах

Обращений к модели за весь этап: **0**.
Claude runtime calls = 0, Codex runtime calls = 0, OpenRouter calls = 0.
Всё, что запускалось, — разбор, сериализация, детерминированные проверки и
поддельные CLI (bash-скрипты, ведущие журнал argv/stdin).

---

## 1. Новые тесты этапа: 81

| Файл | Тестов | О чём |
|---|---|---|
| `tests/test_audit_routing_plan.py` | 49 | доменная часть: компиляция, хэш, валидатор, бюджет, требования |
| `tests/test_audit_routing_plan_contract.py` | 23 | контракт задания: заморозка, журнал, совместимость, fail closed |
| `tests/test_audit_routing_plan_ensemble_e2e.py` | 9 | ансамбль на поддельных провайдерах |

---

## 2. Покрытие обязательного списка задания (§57)

| # | Требование | Тест | Статус |
|---|---|---|---|
| A | compile Claude+GPT+Codex | `test_a_compile_claude_gpt_codex` | ✅ |
| B | compile Full Codex | `test_b_compile_full_codex` | ✅ |
| C | canonical serialization | `test_c_canonical_serialization_is_stable` | ✅ |
| D | stable hash | `test_d_hash_is_stable_and_content_bound` | ✅ |
| E | unknown provider reject | `test_e_unknown_provider_rejected` | ✅ |
| F | unknown capability reject | `test_f_unknown_capability_rejected` | ✅ |
| G | duplicate action reject | `test_g_duplicate_action_rejected` | ✅ |
| H | dependency cycle reject | `test_h_dependency_cycle_rejected` | ✅ |
| I | invalid effort reject | `test_i_invalid_effort_rejected` | ✅ |
| J | deterministic action без провайдера | `test_j_deterministic_action_cannot_have_provider` | ✅ |
| K | model action обязан назвать провайдера | `test_k_model_action_requires_provider_and_capability` | ✅ |
| L | no credentials | `test_l_plan_carries_no_credentials` | ✅ |
| M | no center exact-model selection | `test_m_no_exact_model_selection_from_center` | ✅ |
| N | third-leg flag snapshot | `test_n_third_leg_flag_snapshot` | ✅ |
| O | deterministic optimization-fix snapshot | `test_o_deterministic_optimization_fix_snapshot` | ✅ |
| P | смена пресета не меняет задание | `test_p_global_preset_change_does_not_touch_created_job` | ✅ |
| Q | старое задание совместимо | `test_q_old_job_without_routing_plan_still_parses` | ✅ |
| R | новое задание без плана — fail closed | `test_r_new_job_with_inference_without_plan_fails_closed` | ✅ |
| S | multi-provider requirements | `test_s_multi_provider_requirements_extraction` | ✅ |
| T | воркер без OpenRouter отклонён | `test_t_worker_missing_openrouter_is_refused_before_dispatch` | ✅ |
| U | воркер без Claude отклонён (даже Full Codex) | `test_u_worker_without_claude_rejected_even_for_full_codex` | ✅ |
| V | воркер без Codex отклонён | `test_v_worker_without_codex_rejected` | ✅ |
| W | воркер со всем принят | `test_w_worker_with_all_capabilities_accepted` | ✅ |
| X | маршрут на действие в привязке | `test_x_binding_carries_route_per_action` | ✅ |
| Y | журнал различает ноги | `test_y_ledger_key_separates_ensemble_legs` | ✅ |
| Z | три детектора — одна параллельная группа | `test_z_block_three_detectors_are_one_parallel_group` | ✅ |
| AA | судья после детекторов | `test_aa_block_judge_depends_on_detectors` | ✅ |
| AB/AC | 4 и 3 вызова на блок | `test_ab_ac_block_call_count_third_leg_on_off` | ✅ |
| AD/AE | две ноги 05 ‖ + детерминированный мерж | `test_ad_ae_optimization_dual_leg_and_deterministic_merge` | ✅ |
| AF/AG | критик 05 по пресету | `test_af_ag_optimization_critic_provider_per_preset` | ✅ |
| AH/AI | F OPT Fix и Верификатор = 0 вызовов | `test_ah_ai_zero_model_calls_for_deterministic_stages` | ✅ |
| AJ | Full Codex: страж = Claude | `test_aj_full_codex_absence_guard_still_claude` | ✅ |
| AK/AL | текст по пресету | `test_ak_al_text_provider_per_preset` | ✅ |
| AM/AN | targeted только на Codex-пути | `test_am_an_targeted_merge_passes_only_on_codex_path` | ✅ |
| AO/AP | нормы — центральная область | `test_ao_ap_norm_actions_are_center_scope` | ✅ |
| AQ | effort xhigh сохранён | `test_aq_reasoning_effort_xhigh_preserved` | ✅ |
| AR | B=40 → ~160 вызовов этапа 01 | `test_ar_budget_b40_reflects_topology` | ✅ |
| AS | разбивка по провайдерам | `test_as_provider_specific_budget_breakdown` | ✅ |
| AT/AU | локальный след = эталон (оба пресета) | `test_action_trace_matches_reference_matrix` + артефакты `11I_FAKE_LOCAL_*.json` | ✅ |
| AV/AW | сетевой след = эталон | частично — см. §4 | ⚠️ |
| AX | хэш совпадает на всём круге | `test_ax_routing_hash_survives_the_whole_roundtrip` | ✅ |
| AY | правка stage_models после выдачи не влияет | `test_ay_changing_stage_models_after_dispatch_does_not_alter_execution` | ✅ |
| AZ | нет молчаливой деградации | `test_az_no_silent_degradation_on_missing_provider`, `test_worker_without_a_route_never_degrades_silently` | ✅ |

---

## 3. Доказательство ансамбля на поддельных провайдерах

`tests/test_audit_routing_plan_ensemble_e2e.py` поднимает полную обстановку
попытки: замороженный план, привязка с четырьмя маршрутами, два поддельных CLI.

Проверяется не наш счётчик, а **журнал, который ведут сами подпроцессы** —
только он может засвидетельствовать, что вызов состоялся и с какой моделью.

Установлено:

* две codex-ноги с ОДИНАКОВЫМ промптом и ОДИНАКОВОЙ картинкой дают два
  ФАКТИЧЕСКИХ запуска подпроцесса и два разных ключа журнала (`performed=True` у
  обоих). До 11I их ключи совпадали побайтово;
* каждая нога уходит своей моделью локальной политики (`fake-codex-standard`,
  `fake-codex-strong`, `fake-codex-judge`, `fake-claude-strong`);
* судья — отдельное, четвёртое обращение;
* повтор ТОЙ ЖЕ ноги читается из журнала и не оплачивается (`performed=False`,
  подпроцесс не запускался);
* обе ноги этапа 05 идут разными провайдерами в одной попытке;
* отсутствие маршрута OpenRouter даёт отказ, а не выполнение ансамбля без ноги,
  и при этом НИ ОДНОГО обращения к другому провайдеру «вместо» не происходит.

---

## 4. Сетевой E2E: что сделано и что нет

**Сделано.** `tests/test_distributed_workers_network_e2e_11g.py` (24 теста)
прогоняет настоящий `RemoteWorkerExecutionBackend` с настоящим окружением
центра и настоящей БД: план компилируется, попадает в `create_audit_job`,
переживает JSON-круг через `logical_jobs.payload`, разбирается воркером
(`audit_runner.validate_params`) и сверяется по хэшу.

**Не сделано.** Прогон через ЖИВОЙ HTTPS-транспорт с отдельным процессом агента
(как в smoke-стенде 11G). Причина: такой стенд поднимает uvicorn и агента
процессами и на этой машине занимает единицы минут на прогон; в рамках 11I он
не добавил бы утверждений сверх уже проверенных — сериализация, хэш,
совместимость и разбор проверены на реальных объектах, а транспорт с 11G не
менялся.

**Что это значит для вердикта.** Пункты AV/AW списка §57 закрыты частично:
след действий сверен с эталоном ЛОКАЛЬНО (`11I_FAKE_LOCAL_*.json`), контракт
«центр → задание → воркер» проверен на настоящих объектах, но не через живой
сокет. Это зафиксировано как ограничение, а не как выполненный пункт.

---

## 5. Регресс

Прогон распределённого набора (`tests/test_distributed_workers_*.py` +
три файла 11I): **1252 пройдено, 3 упало**.

Все три падения воспроизводятся на базе 11H (`de2f84f2`) в отдельном worktree,
то есть к изменениям 11I отношения не имеют:

| Тест | Проверено на базе |
|---|---|
| `test_distributed_workers_executor.py::test_killing_agent_does_not_stop_the_audit` | падает на `de2f84f2` |
| `test_distributed_workers_prepipeline_gate.py::test_two_real_processes_overlap_and_third_waits` | падает на `de2f84f2` |
| `test_distributed_workers_prepipeline_gate.py::test_agent_restart_keeps_two_processes_and_creates_no_duplicates` | падает на `de2f84f2` |

Все три — тесты, поднимающие настоящие подпроцессы с таймаутами.

### Тесты прежних этапов, обновлённые СОЗНАТЕЛЬНО

Обновлены там, где 11I меняет контракт, а не там, где что-то сломалось:

1. `test_distributed_workers_network_e2e_11g.py`
   * `_REAL_WORKER_CAPS` — воркер обязан объявить `routing_plan_v1` и шесть
     способностей трёх провайдеров;
   * бюджет проверяется как СВОЙСТВО («покрывает ансамбль»), а не как
     константа `11`;
   * фикстура версии получила `project_info.json` с дисциплиной: от неё
     зависит targeted-проход, и версия без дисциплины — это версия, для
     которой правильного маршрута не существует;
   * `test_g` дополнен обеими сторонами: задание с планом принимается, то же
     задание без плана отвергается.
2. `test_distributed_workers_worker_slice_11f.py`
   * `assert resolver.MAX_INFERENCES_CEILING == 64` → сверка трёх валидаторов
     между собой. Утверждение теста прежнее и единственно важное («три
     валидатора одного поля обязаны иметь ОДИН потолок»), изменилось значение.
