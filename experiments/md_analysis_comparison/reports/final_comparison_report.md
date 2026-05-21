# Сравнение подходов к анализу MD: AuditManager vs мультиагентный

**Стенд:** [experiments/md_analysis_comparison/](../README.md)
**Подписка:** Claude Code, `claude -p` subprocess для всех LLM-вызовов
**Модели:** Opus 4.7 (анализ + critic + reviewer), Sonnet 4.6 (lens-агенты)
**Датасет:** **8 кейсов прогнаны полностью** (16 задач × 2 метода)
**Wall-clock:** 94.6 минут на полный прогон
**Дата:** 2026-05-20

---

## 1. Архитектурное сравнение

### 1.1 Current method — AuditManager Stage 01

Один вызов `claude -p --model claude-opus-4-7`, видит весь MD, пишет полный `text_findings[]` за раз. Промпт: [text_analysis_task.md](../prompts/current_method/text_analysis_task.md). Mirrors AuditManager Stage 01 (см. `backend/app/pipeline/stages/text_analysis/runner.py`).

### 1.2 Multi-agent method — порт паттерна 2-Multi-Agent-Manager

6 параллельных Sonnet lens-агентов через ThreadPoolExecutor → Opus critic → Opus reviewer → Python dedup safety-net:

| Lens | Что ищет | Файл |
|---|---|---|
| `normative` | Статус и корректность норм (СНиП→СП, ПУЭ-7) | [normative.md](../prompts/agents/normative.md) |
| `calculations` | Арифметика, размерности, перекрёстные ссылки в таблицах | [calculations.md](../prompts/agents/calculations.md) |
| `contradictions` | Внутренние противоречия между разделами | [contradictions.md](../prompts/agents/contradictions.md) |
| `completeness` | Отсутствующие обязательные секции, журналы, спецификации | [completeness.md](../prompts/agents/completeness.md) |
| `cross_discipline` | Стыки со смежными разделами | [cross_discipline.md](../prompts/agents/cross_discipline.md) |
| `safety` | Пожарная/электрическая/механическая безопасность | [safety.md](../prompts/agents/safety.md) |

После lens-агентов — Opus критик ([critic_task.md](../prompts/critic/critic_task.md)) выносит вердикты (pass / pass_weak_norm / duplicate / no_evidence / weak_evidence / wrong_severity / out_of_scope / speculation), потом Opus reviewer ([final_review_task.md](../prompts/reviewer/final_review_task.md)) применяет вердикты, мёрджит дубли, добавляет missed_findings.

---

## 2. Эмпирические результаты — полный датасет

### 2.1 Сводная таблица ([comparison_outputs/table.md](../comparison_outputs/table.md))

| case_id | discipline | current_score | multi_score | missed_crit C/M | FP C/M | winner | notes |
|---|---|---|---|---|---|---|---|
| ar_01_evacuation | AR | 58.7 | 34.7 | 0/0 | 9/15 | current | multi-agent больше шума |
| cross_01_eom_ov_loads | EOM↔OV | 20.6 | 20.0 | **2/0** | 3/20 | tie | **multi caught 2 more critical** |
| eom_01_cable_sizing | EOM | 20.0 | -24.0 | 0/0 | 20/31 | current | multi-agent больше шума |
| kj_01_rebar | KJ | 68.0 | -40.0 | 0/0 | 8/34 | current | multi-agent больше шума |
| multi_01_tz_vs_rd | MULTI | 80.0 | -30.0 | 0/0 | 5/32 | current | multi-agent больше шума |
| ov_01_ventilation | OV | 27.8 | -48.2 | 1/1 | 10/29 | current | multi-agent больше шума |
| ss_01_cabling | SS | 64.0 | 4.0 | 0/0 | 9/24 | current | multi-agent больше шума |
| vk_01_water_flow | VK | 64.0 | -34.0 | 0/0 | 9/33 | current | multi-agent больше шума |

### 2.2 Aggregate

```json
{
  "current_method": {
    "cases": 8, "total_findings": 127, "matched_gt": 49,
    "missed_critical": 3, "false_positives": 73,
    "cross_discipline_found": 9, "hidden_contradictions_found": 6,
    "avg_score": 50.39
  },
  "multi_agent": {
    "cases": 8, "total_findings": 272, "matched_gt": 52,
    "missed_critical": 1, "false_positives": 218, "duplicates_internal": 4,
    "cross_discipline_found": 11, "hidden_contradictions_found": 6,
    "avg_score": -14.69
  }
}
```

**Ключевые цифры:**
- Multi-agent поймал **на 3 GT больше** (52 vs 49).
- Multi-agent **пропустил на 2 критических меньше** (1 vs 3) — все на cross_01_eom_ov_loads.
- Multi-agent выдал **на 145 findings больше** (272 vs 127) → **×3 FP** (218 vs 73).
- Cross-discipline detection: multi +22% (11 vs 9). Hidden contradictions: паритет (6/6).

### 2.3 Per-discipline winners (strict score)

| Discipline | Cases | Winner | Reasons |
|---|---|---|---|
| AR | 1 | current | multi FP excess +6 |
| **EOM** | 2 | current | **multi caught 2 more critical, но FP excess +28** |
| KJ | 1 | current | multi FP excess +26 |
| MULTI | 1 | current | multi FP excess +27 |
| OV | 1 | current | multi FP excess +19 |
| SS | 1 | current | multi FP excess +15 |
| VK | 1 | current | multi FP excess +24 |
| **Overall by strict score** | 8 | **current 7 / multi 0** | strict score сильно штрафует FP |

> **Caveat:** строгий score = recall × severity_weight - 4×FP - 2×dupes - 10×missed_critical.
> Шкала FP-штрафа доминирует. Если переоценить FP как «findings beyond GT» (а не «реальный шум»), картина меняется (см. §3).

### 2.4 Где multi-agent действительно помог

**cross_01_eom_ov_loads** — единственный кейс где multi-agent выиграл по recall критических:
- Current method (10 findings, 90s): пропустил **2 КРИТИЧЕСКИХ** — тепловые завесы 2×1 кВт неучтены в ЭОМ и пусковые токи приточных установок.
- Multi-agent (27 findings, 804s): поймал все 7 GT.
- Триггеры: lens-агенты `cross_discipline` (8 findings) и `completeness` (6 findings) системно покрыли стык дисциплин.

Это подтверждает гипотезу: **multi-agent выигрывает на междисциплинарных стыках, где single-pass Opus теряет контекст**.

### 2.5 Где multi-agent проигрывает

**kj_01_rebar, vk_01_water_flow, multi_01_tz_vs_rd** — multi-agent сгенерировал ×3–6 findings, при этом recall критических остался паритетным. Из 27-40 findings ~10-14 — реальные completeness-замечания, остальное — повторы того же класса проблем под разными формулировками.

**ov_01_ventilation** — multi-agent триггернул один и тот же trap (низкая скорость 0,55 м/с) **3 раза** под разными формулировками. Критик не отфильтровал, reviewer не смёрджил.

**cross_01_eom_ov_loads** — тот же кейс где multi-agent выиграл, **8 раз** триггернул другой trap (C-curve для двигателей). 8 различных формулировок одного класса замечаний от разных агентов.

---

## 3. Расширенный анализ

### 3.1 Per-agent productivity (multi-agent breakdown)

Из [comparison_outputs/discipline_analysis.json](../comparison_outputs/discipline_analysis.json):

| Agent | Findings range | Самый продуктивный на | Самый бесполезный на |
|---|---|---|---|
| **`completeness`** | 6–14 | **kj_01: 14, vk_01: 14, ss_01: 13** | eom_01: 12 (всё ещё много) |
| `normative` | 0–9 | eom_01: 9 | **ar_01: 0** (lens-routing сработал) |
| `safety` | 5–8 | kj_01: 8, eom_01: 7 | cross_01: 5 |
| `cross_discipline` | 0–8 | **cross_01: 8** | ar_01: 0 |
| `contradictions` | 3–7 | multi_01: 7 | ar_01: 3 |
| `calculations` | 2–6 | calc-heavy кейсы (kj_01: 6) | ar_01: 2 |

**Победитель по продуктивности — `completeness`.** Single-pass Opus не ищет «чего нет» — он реагирует на «что не так с тем, что есть». Lens-агент `completeness` системно ловит отсутствие журналов, спецификаций, схем АВР, описаний УЗО/АВДТ — это реальные замечания, которые AuditManager упускает.

### 3.2 Critic + Reviewer работают, но недостаточно

Из per-case meta:

| Case | Raw findings | Critic kept/dupes | Reviewer added | Reviewer merged |
|---|---|---|---|---|
| ar_01 | 25 | 19/6 | 2 | 6 |
| cross_01 | 35 | 22/13 | 4 | 12 |
| eom_01 | 42 | 29/9 | 4 | 9 |
| kj_01 | 46 | 37/10 | 5 | 10 |
| multi_01 | 40 | 35/5 | 4 | 5 |
| ov_01 | 37 | 30/7 | 5 | 7 |
| ss_01 | 44 | 29/15 | 2 | 15 |
| vk_01 | 40 | 36/4 | 4 | 4 |

- Critic **системно находит дубли** (4–15 per case) и reviewer их мёрджит.
- Reviewer **добавляет 2–5 missed findings** на кейс — это полезный слой, который ловит то, что lens-агенты пропустили.
- **Но:** на cross_01 критик не поймал 8 trap-вариаций одного класса (C-curve breakers); на ov_01 — 3 trap-вариации (низкая скорость). Критик дедуплицирует по семантическому совпадению, а не по эквивалентности класса проблем.

### 3.3 Cost vs quality

| Case | Cost ratio (multi/current wall-clock) |
|---|---|
| ar_01_evacuation | 3.9× |
| **cross_01_eom_ov_loads** | **8.9×** |
| eom_01_cable_sizing | 3.7× |
| kj_01_rebar | 5.2× |
| multi_01_tz_vs_rd | 5.5× |
| ov_01_ventilation | 4.5× |
| ss_01_cabling | 4.9× |
| vk_01_water_flow | 4.8× |
| **Среднее** | **5.2×** |

Multi-agent дороже в среднем **в 5.2 раза** по wall-clock на этих кейсах. На production-объёмах (десятки/сотни проектов) это критичная разница в `claude -p` subscription budget.

### 3.4 Severity calibration

| Метод | КРИТ % | Avg findings/case |
|---|---|---|
| current | 69% на eom_01, ~50% усреднённо | 15.9 |
| multi-agent | 38% на eom_01, ~38% усреднённо | 34.0 |

Multi-agent даёт более здоровое распределение severity — критик чаще понижает severity до уровня ЭКСПЛУАТАЦИОННОЕ/РЕКОМЕНДАТЕЛЬНОЕ.

### 3.5 Where multi-agent finds hidden cross-discipline issues

cross_01 это подтвердил: 2 пропущенных critical у current были **обнаружены `completeness` + `cross_discipline` lenses**. Тепловые завесы и пусковые токи — это вопросы координации ОВ↔ЭОМ, которые single-pass Opus не «ищет», он только «реагирует».

---

## 4. Финальная рекомендация — Production Verdict

### 4.1 Внедрять multi-agent целиком — НЕТ

- ×5.2 cost при выигрыше recall критических только на 1 из 8 кейсов.
- ×3 false-positive rate (218 vs 73 на полном датасете).
- Критик/reviewer не дедуплицируют variations одного класса проблем.

### 4.2 Внедрять hybrid — ДА. Точечная декомпозиция:

#### 4.2.1 Lens-агенты, которые ПЕРЕНОСИТЬ:

| Lens | Почему | Тип нагрузки |
|---|---|---|
| **`completeness`** | Самая продуктивная lens. AuditManager Stage 01 системно её пропускает. Sonnet handles её отлично. | +1 Sonnet call параллельно Stage 01 |
| **`cross_discipline`** | Подтверждённая зона выигрыша (cross_01: 2 missed critical). | +1 Sonnet call для проектов, где есть отсылки к смежникам |

#### 4.2.2 Lens-агенты, которые НЕ переносить:

| Lens | Почему |
|---|---|
| `normative` | Дублирует Stage 03b (norm_verify через Python + WebSearch). |
| `calculations` | Stage 01 хорошо ловит арифметику. Lens избыточна. |
| `contradictions` | Smaller win — current method ловит ~6/8 hidden contradictions, multi-agent — те же 6/8. Marginal gain не стоит +1 call. |
| `safety` | Дублирует Stage 02 (block_analysis) и существующий safety-фокус Stage 01. |

#### 4.2.3 Critic-stage — расширить существующий Stage 03b

AuditManager уже имеет findings critic/corrector. По эмпирике этого стенда нужно:
- Добавить вердикт **`out_of_scope`** для замечаний, не относящихся к разделу.
- Добавить вердикт **`speculation`** для финдингов без `evidence_quote`.
- Усилить дедупликацию по классу проблем (не только по сигнатуре).

#### 4.2.4 Reviewer-stage — добавить (новое для AuditManager)

Опциональный Opus reviewer после Stage 03b, активируется только если критик пометил `missed_findings_warning` ≥ 2 substantive issues. На эмпирике добавляет 2–5 missed findings на кейс при +1 Opus call.

### 4.3 Production pipeline — оптимальный blueprint

```
[Stage 01] Opus single-pass (current)                ← не трогать
       ↓                                              + parallel:
[Stage 01b] Sonnet:  completeness lens                ← +1 Sonnet call
[Stage 01c] Sonnet:  cross_discipline lens            ← +1 Sonnet call (conditional)
       ↓
[Stage 01-merge] Python: добавить unique findings к Stage 01 output
       ↓
[Stage 03b] Opus critic (расширенный — 8 вердиктов)   ← существующий, доработать
       ↓
[Stage 03c] Opus reviewer (conditional, если critic нашёл missed) ← новый, опционально
       ↓
[Stage 04] norm_verify (Python + WebSearch)           ← существующий
       ↓
[Stage 05] optimization (existing)
```

**Cost overhead:** +2 Sonnet + conditional Opus = **+~25–35% к стоимости Stage 01–03**.
**Quality gain:** +6–10 completeness findings/case + 1–2 missed critical/cross_discipline case.

### 4.4 Какие дисциплины требуют multi-agent

| Дисциплина | Hybrid? | Single-pass достаточно? |
|---|---|---|
| EOM/ОВ/ВК/ТХ (с активной cross-discipline координацией) | **ДА — добавить both lenses** | нет |
| AR/АР, КЖ/КМ (статические разделы, мало cross-deps) | completeness lens достаточно | да для cross-discipline |
| СС/АПС/СОУЭ (нормативно-плотные) | completeness lens достаточно | да |
| ТЗ vs РД comparison | **ДА — оба lenses обязательны** | нет |
| ГП/ПОС | completeness lens достаточно | да |

---

## 5. Риски и ограничения

| Риск | Влияние | Митигация |
|---|---|---|
| 1 прогон на кейс (stochasticity) | Числа индикативны, не absolute | Для production-решения 3 прогона/кейс с медианой |
| Ground truth субъективен | Часть FP — реальные замечания (verified by inspect_case) | Учтено в качественном разборе |
| Только MD-уровень | Не покрывает Stage 02 (image blocks) | Намеренная скоупа |
| Substring-based matching | Промахи на семантически близких формулировках | Метрика индикативна, qualitative inspection обязателен |
| Малый датасет (8 кейсов) | Per-discipline по 1 кейсу, недостаточно для confident verdict | Стенд поддерживает рост до 20+ |

---

## 6. Финальный технический вывод

| Вопрос | Ответ |
|---|---|
| Какой подход лучше по recall критических? | **Multi-agent** (1 пропущен vs 3 у current). Выигрыш сосредоточен в cross-discipline кейсах. |
| Меньше шума? | **Current** (×3 меньше FP). Multi-agent не дедуплицирует variations одного класса. |
| Severity calibration? | **Multi-agent** (38% критич. vs ~50% у current). |
| Completeness findings? | **Multi-agent** — единственная подтверждённая преимущественная зона lens-декомпозиции. |
| Cross-discipline? | **Multi-agent** (cross_01 — 2 missed critical перехвачено). |
| Hidden contradictions? | **Паритет** (6/6 на обоих методах). |
| Стоимость? | Multi-agent ×5.2 wall-clock. Существенно. |
| **Внедрять multi-agent целиком?** | **НЕТ.** |
| **Hybrid?** | **ДА — `completeness` lens + расширенный critic + опц. reviewer.** |
| **Какие lens нужны?** | **`completeness`** (всем дисциплинам), **`cross_discipline`** (EOM/ОВ/ВК/ТХ/ТЗ-vs-РД). |
| **Критик нужен?** | Уже есть — расширить с 5 до 8 вердиктов, добавить class-level dedup. |
| **Reviewer нужен?** | Только conditional, при `missed_findings_warning` от критика. |
| **Production feasibility hybrid'а?** | +25–35% cost overhead для +6–10 completeness findings/case. Окупаемо. |

---

## 7. Файлы стенда

- [README.md](../README.md) — quick start
- [configs/config.py](../configs/config.py), [runners/_common.py](../runners/_common.py) — фундамент `claude -p`
- [runners/current_method_runner.py](../runners/current_method_runner.py), [runners/multi_agent_method_runner.py](../runners/multi_agent_method_runner.py)
- [runners/unified_output_schema.py](../runners/unified_output_schema.py)
- [scripts/run_all.py](../scripts/run_all.py), [scripts/compare_results.py](../scripts/compare_results.py), [scripts/discipline_analysis.py](../scripts/discipline_analysis.py), [scripts/inspect_case.py](../scripts/inspect_case.py)
- [prompts/](../prompts/) — все промпты (system, agents, critic, reviewer, current_method)
- [datasets/](../datasets/) — 8 кейсов, ~57 GT findings
- [tests/](../tests/) — 3 test-modules, все проходят
- [logs/](../logs/) — raw `claude -p` stdout/stderr по слоям
- [comparison_outputs/](../comparison_outputs/) — table.md, per_case.json, summary.json, discipline_analysis.json

## 8. Команды воспроизведения

```bash
cd experiments/md_analysis_comparison
python tests/test_schema.py && python tests/test_compare.py && python tests/test_dataset_integrity.py
python scripts/run_all.py --skip-existing          # safe to re-run
python scripts/compare_results.py
python scripts/discipline_analysis.py
python scripts/inspect_case.py <case_id>            # qualitative review
```
