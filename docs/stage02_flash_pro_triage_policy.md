# Stage 02 Flash -> Pro Triage Policy

Зафиксировано: 23.04.2026

## Цель

Снизить стоимость stage 02 без возврата к Pro multi-block batching, который на сложных KJ-чертежах показал деградацию качества.

Практический принцип:

```text
Flash дешево смотрит все блоки single-block.
Gemini Pro смотрит только выбранные рискованные блоки single-block.
```

## Почему не Pro b2/b4/b6

По экспериментам на KJ:

- Малый KJ жил до `b6` только потому, что фактическая плотность блоков была ниже.
- Большой KJ ломался даже на малых batch-размерах по quality-preservation gate.
- Главный риск не completeness, а потеря деталей: generic summary, KV collapse/inflation, пропуски локальных замечаний.

Поэтому Pro должен получать один сложный чертежный блок за раз.

## Алгоритм

1. Запустить `google/gemini-2.5-flash` на всех блоках в `single-block` режиме.
2. Построить escalation set по Flash-результату и metadata блока.
3. Запустить `google/gemini-3.1-pro-preview` только на escalation set, тоже строго `single-block`.
4. Смержить результат: для escalated blocks успешный Pro заменяет Flash, иначе остается Flash fallback с пометкой.

## Default escalation rules

Pro запускается, если выполняется хотя бы одно:

- сложный/risky блок имеет Flash findings;
- сложный/risky блок у Flash упал, пропал или помечен unreadable;
- блок с finding имеет слабое/неуверенное Flash-чтение;
- finding имеет high-value severity: critical, operational, economic или cross-section check.

Сложный/risky блок:

- `risk=heavy` или `risk=normal`;
- full-page, merged, quadrant;
- большой размер/плотный OCR по эвристикам runner'а.

## Cost guardrail

Simple/light blocks with Flash findings остаются Flash-only по умолчанию.

Если нужен recall-first режим, можно явно включить:

```bash
--include-simple-findings
```

Если нужно ограничить spend Pro:

```bash
--max-pro-cost-usd 8
--max-pro-blocks 80
```

## Command

Dry-run без API:

```bash
python scripts/run_stage02_flash_pro_triage.py \
  --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \
  --dry-run
```

Реальный OpenRouter-прогон:

```bash
python scripts/run_stage02_flash_pro_triage.py \
  --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \
  --parallelism-flash 3 \
  --parallelism-pro 2 \
  --max-pro-cost-usd 8
```

## UI

В конфигурации моделей строка `02 Блоки` поддерживает pair-mode:

```text
Gemini 2.5 Flash + Gemini 3.1 Pro
```

В UI это выглядит как два выбранных кружка в одной строке `02 Блоки`.
Такой выбор сохраняется как специальный backend profile:

```text
pair/gemini-2.5-flash+gemini-3.1-pro
```

После сохранения обычный запуск аудита использует этот профиль только для
этапа `02 Блоки`: стандартная multi-block упаковка пропускается, запускается
Flash -> Pro triage, затем pipeline продолжает следующие этапы от
`_output/02_blocks_analysis.json`.

Также оставлен отдельный служебный endpoint для ручного stage-02 запуска:

```text
POST /api/audit/{project_id}/flash-pro-triage
```

Direct Gemini API варианты скрыты из UI, пока доступ через Google API key
нестабилен по региону/IP.

UI-режим записывает итог в:

```text
_output/02_blocks_analysis.json
```

Если файл уже был, runner сохраняет backup-копию:

```text
_output/02_blocks_analysis.before_flash_pro_triage.<timestamp>.json
```

## Constraints

- Не менять production defaults.
- Не трогать Flash defaults в webapp.
- Не использовать Claude comparison в этом режиме.
- Не делать recrop/rebuild blocks.
- Не отправлять Pro несколько блоков в одном prompt.
- Не использовать findings count как единственный критерий: selection учитывает risk, unreadable, weak summary, KV, severity.

## Рекомендация

Для сложных KJ-чертежей это текущий practical path:

```text
Flash full single-block triage
+ Pro selected single-block verification
```

Если Google Gemini Batch API станет доступен, тот же алгоритм можно запускать дешевле как provider-side batch из независимых single-block requests.
