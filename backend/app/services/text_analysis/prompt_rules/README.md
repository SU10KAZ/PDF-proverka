# Prompt rule blocks — P0 safety layer

**Дата:** 2026-05-21
**Источник:** `experiments/md_analysis_comparison/normative_checklist_research/recommendations/prompt_rules_update.md`
**Статус:** **НЕ wired**. Эти файлы — заготовка для будущего
`completeness_runner`, которого ещё не существует.

## Что это

Каждый `.md` в этой папке — самостоятельный prompt-блок, который будущий
runner будет вклеивать в системную часть completeness-lens prompt'а.
Сейчас они не подгружаются ни одним runtime-сервисом.

Файлы хранятся в `backend/app/services/text_analysis/prompt_rules/`,
**не** в `backend/app/data/prompts/`, чтобы случайный `prompt_loader.load_all()`
не подцепил их в production без отдельного решения.

## Состав

| Файл | Что запрещает / требует |
|---|---|
| `stage_gate_rules.md` | ПД-only items нельзя флагать в РД-марке; downgrade в unknown stage |
| `document_type_rules.md` | spec_only не требует ПЗ/расчётов; audit_comparison не требует full_rd |
| `object_signal_rules.md` | условные items требуют сигнал в MD (таблица сигналов) |
| `cross_section_rules.md` | cross-section findings в single-MD pipeline запрещены |
| `anti_hallucination_rules.md` | запрет на «вероятно», «следует уточнить» без конкретики |
| `anti_phantom_clause_rules.md` | запрет на неподтверждённые подпункты норм; параллель ПУЭ→СП |
| `coordination_rules.md` | coordination artifacts не могут быть `missing findings` |
| `README.md` | этот файл |

## Как НЕ использовать

- Не импортировать через `prompt_loader.load(...)`.
- Не упоминать в runner'е до того, как соответствующий sub-task разрешит wiring.
- Не считать эти rules уже применёнными к текущему production prompt'у
  `prompts/pipeline/ru/phase1/completeness_lens_production_prompt.md` —
  они будут вмержены отдельным sub-task'ом с human review.

## Как должен использовать runner (будущее)

Конкатенировать блоки в фиксированном порядке после системной части prompt'а:

1. `stage_gate_rules.md`
2. `document_type_rules.md`
3. `object_signal_rules.md`
4. `cross_section_rules.md`
5. `coordination_rules.md`
6. `anti_phantom_clause_rules.md`
7. `anti_hallucination_rules.md`

После каждой склейки runner должен валидировать grounding (см.
`docs/critic_corrector.md`).

## Контекст для каждого правила

Каждый блок написан так, чтобы быть само-достаточным: LLM, читающая его в
изоляции, всё ещё понимает контекст. Это даёт runner'у свободу включать
только подмножество правил (например, если document_type уже известен,
блок `document_type_rules.md` всё равно остаётся информативным).
