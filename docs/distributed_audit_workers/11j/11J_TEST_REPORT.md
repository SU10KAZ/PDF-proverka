# 11J — что проверено

## 1. Обращений к настоящим моделям — ноль

| Провайдер | Реальных вызовов |
|---|---|
| Claude | 0 |
| Codex | 0 |
| OpenRouter | 0 |

Ни один тест этого этапа не ходил в интернет. Поддельны ровно две внешние
точки: CLI (подпроцесс со скриптом) и шлюз (локальный HTTP-сервер). Транспорт
до них — настоящий: настоящий `subprocess`, настоящий сокет, настоящий httpx,
настоящий заголовок `Authorization`.

## 2. Новые наборы

| Файл | Тестов | Предмет |
|---|---|---|
| `tests/test_openrouter_worker_provider_11j.py` | 57 | §37 и §38: провайдер, секрет, топология |
| `tests/test_multiprovider_bridge_execution_11j.py` | 7 | исполнение ансамбля тремя провайдерами |
| **Итого 11J** | **64** | |

Плюс наборы 11I остаются зелёными: `test_audit_routing_plan.py` (49),
`test_audit_routing_plan_contract.py` (23), `test_audit_routing_plan_network_e2e.py` (14).

## 3. §37 — обязательные проверки OpenRouter

| Пункт | Тест | Итог |
|---|---|---|
| A. провайдер распознаётся | `test_a_provider_recognized_by_both_registries` | ✅ |
| A2. реестры центра и воркера согласованы | `test_a2_center_registry_knows_provider_and_all_capabilities` | ✅ |
| B. способность объявляется только при ключе | `test_b_capability_advertised_only_when_key_configured` | ✅ |
| C. без шлюза воркер несовместим | `test_c_missing_openrouter_makes_worker_incompatible` | ✅ |
| C2. без Claude «Full Codex» тоже несовместим | `test_c2_missing_claude_also_makes_full_codex_incompatible` | ✅ |
| D. нет тихой деградации | `test_d_no_silent_degradation_route_missing_is_refused` | ✅ |
| E. визуальный вход шлюза | `test_e_visual_input_reaches_the_gateway` | ✅ |
| F+G+H. одинаковый вход у ног ансамбля | `test_f_g_h_same_semantic_input_across_detector_legs` | ✅ |
| I+J. секрета нет в задании и пакете | `test_i_j_secret_absent_from_job_and_package` | ✅ |
| K. секрета нет в привязке | `test_k_secret_absent_from_binding_written_to_disk` | ✅ |
| L. секрета нет в результате и логах | `test_l_secret_absent_from_provider_result_and_logs` | ✅ |
| M. секрета нет в heartbeat/EventOutbox | `test_m_secret_absent_from_heartbeat_and_center_payload` | ✅ |
| N. секрета нет в журнале шлюза | `test_n_secret_absent_from_stub_call_log` | ✅ |
| O. ошибка авторизации классифицирована | `test_o_p_q_errors_are_classified[auth_error]` | ✅ |
| P. лимит классифицирован | `[rate_limit]` + `test_p2_status_map_is_explicit` | ✅ |
| Q. таймаут классифицирован | `[timeout]` | ✅ |
| R. нормализация расхода | `test_r_usage_is_normalized_to_the_common_shape` | ✅ |
| S. журнал различает провайдера и действие | `test_s_ledger_entry_carries_provider_and_action` | ✅ |

## 4. §38 — обязательные проверки исполнения маршрута

| Пункт | Тест | Итог |
|---|---|---|
| T+U. блок = ровно четыре действия (оба пресета) | `test_t_u_block_stage_is_exactly_four_model_actions` | ✅ |
| V+W. три детектора ‖, судья после барьера | `test_v_w_three_detectors_are_one_parallel_group_judge_after` | ✅ |
| X. судья — не центральное действие | `test_x_judge_is_not_a_center_action` | ✅ |
| Y. targeted-проходы «Full Codex» исполняются | `test_y_full_codex_targeted_merge_is_planned_and_executable` | ✅ |
| Z. на пресете A их нет | `test_z_claude_preset_has_no_targeted_passes` | ✅ |
| AA. страж отсутствия — worker Claude | `test_aa_full_codex_absence_guard_is_worker_claude` | ✅ |
| AB+AC. оптимизация двумя провайдерами, объединение детерминированное | `test_ab_ac_optimization_is_dual_provider_on_the_worker` | ✅ |
| AB2. `xhigh` доезжает до argv | `test_ab2_visual_leg_effort_actually_reaches_the_cli` | ✅ |
| AD+AE. критик по пресету | `test_ad_ae_optimization_critic_follows_the_preset` | ✅ |
| AF+AG. детерминированные этапы = 0 вызовов | `test_af_ag_deterministic_stages_make_zero_model_calls` | ✅ |
| AH. нормативный этап остаётся центру | `test_ah_norm_stage_stays_central` | ✅ |
| AH2. нормативная база не уезжает в пакет | `test_ah2_norm_database_never_enters_the_worker_package` | ✅ |
| AI. хвост читает замороженный план | `test_ai_central_norm_tail_reads_the_frozen_plan` | ✅ |
| AJ. смена пресета не меняет идущее задание | `test_aj_global_preset_switch_cannot_change_a_running_job` | ✅ |
| AJ2. планы двух задач изолированы | `test_aj2_bound_plan_is_isolated_between_concurrent_tasks` | ✅ |
| AK+AL. хэш переживает сериализацию | `test_ak_al_plan_hash_survives_serialization` | ✅ |

## 5. §39 — сетевые проверки

| Пункт | Итог |
|---|---|
| AM. живой HTTPS + поддельные провайдеры, пресет A | **НЕ ВЫПОЛНЕН** (KI-11J-1) |
| AN. то же для «Full Codex» | **НЕ ВЫПОЛНЕН** (KI-11J-1) |
| AO–AV. poll / claim / гранты / пакет / выгрузка / ACK / retention / импорт | не выполнялись в рамках 11J; транспорт не менялся с 11G, где они зелёные |
| AW. нормативная граница центра | ✅ на настоящих объектах (`test_ah`, `test_ah2`, `test_ai`) |
| AX. нет SSH-транспорта задания | ✅ унаследовано с 11G, код не менялся |
| AY. нет входящего порта на воркере | ✅ унаследовано с 11G, код не менялся |

Что ВЫПОЛНЕНО на настоящем сокете взамен: `test_multiprovider_bridge_execution_11j.py`
— один блок, четыре действия, три канала (HTTP + два подпроцесса), настоящие
мост, привязка, журнал и exactly-once.

## 6. Регресс

| Набор | Итог |
|---|---|
| Ключевые наборы распределённых воркеров (5 файлов) | 318 passed, 1 skipped |
| Широкая выборка `-k "distributed or provider or routing or worker"` | 1333 passed, 3 failed, 2 skipped |

Три падения — `test_killing_agent_does_not_stop_the_audit` и два
`test_distributed_workers_prepipeline_gate`. **Все три воспроизведены на базе
11I (`58584de6`) тем же прогоном** — то есть существовали до этапа и к
изменениям отношения не имеют.

Девять ошибок СБОРА в наборах геометрического корпуса — отсутствующие файлы
данных, дрейф окружения, зафиксированный ещё 11I.

## 7. Отдельно о том, чего тесты НЕ доказывают

Тест доказывает, что код УМЕЕТ вести себя правильно. Он не доказывает, что так
поведёт себя боевой прогон на настоящих моделях: у настоящего шлюза другая
задержка, другие тексты ошибок и другое поведение под нагрузкой. Про это —
следующий этап; 11J намеренно останавливается на исполнимом маршруте с
поддельными провайдерами (§35 и §46 задания).
