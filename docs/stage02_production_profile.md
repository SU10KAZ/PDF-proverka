# Stage 02 block_batch — Production Profile

**Зафиксировано:** 20–21.04.2026  
**Документ:** КЖ5.17 (13АВ-РД-КЖ5.17-23.1-К2), 215 блоков  
**Источник в коде:** `blocks.STAGE02_PRODUCTION_PROFILE`, `blocks.get_stage02_production_profile()`

---

## Production defaults

| Параметр | Значение | Источник в коде |
|----------|----------|-----------------|
| render_profile | `r800` | `MIN_LONG_SIDE_PX = 800` в `blocks.py` |
| min_long_side_px | 800 | `blocks.MIN_LONG_SIDE_PX` |
| target_dpi | 100 | `blocks.TARGET_DPI` |
| claude_batch_profile | `baseline` | `blocks._CLAUDE_RISK_TARGETS` |
| heavy target/max | 5 / 6 | `_CLAUDE_RISK_TARGETS["heavy"]` |
| normal target/max | 8 / 8 | `_CLAUDE_RISK_TARGETS["normal"]` |
| light target/max | 10 / 10 | `_CLAUDE_RISK_TARGETS["light"]` |
| claude_hard_cap | 12 | `blocks.CLAUDE_HARD_CAP` |
| parallelism default | 3 | `webapp/config.CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT` |
| parallelism cap | 3 | `webapp/config.CLAUDE_BLOCK_BATCH_PARALLELISM_CAP` |
| model | claude-opus-4-7 | `webapp/config.STAGE_MODEL_CONFIG["block_batch"]` |
| safe_fallback | r800 + baseline_p2 | parallelism=2 при rate-limit проблемах |

---

## Как запустить production stage 02

```bash
# 1. Crop блоков (использует production r800 по умолчанию)
python blocks.py crop projects/<path>

# 2. Генерация батчей (production baseline profile автоматически)
python blocks.py batches projects/<path>

# 3. Запуск через webapp (parallelism=3 + Opus 4.7 по умолчанию)
cd webapp && python main.py
# → дашборд → проект → запустить этап 02 Блоки
```

**Модель:** `claude-opus-4-7` — установлена в `STAGE_MODEL_CONFIG["block_batch"]` через `webapp/data/stage_models.json`.  
Проверить/изменить: `GET /api/audit/model/stages` или UI → Settings → Stage Models.

---

## Решения экспериментов

### Batching: baseline_p3 (winner), aggressive_p3 (не принят)

**Метод:** `--final-comparison` (4 рана: 2 full + 2 subset на 60 фиксированных блоках, seed=42)

| Профиль | Findings (full) | Coverage | bwf subset | elapsed |
|---------|-----------------|----------|------------|---------|
| baseline_p3 | 168 | 100% | **45**/60 | 33.0 мин |
| aggressive_p3 | 186 (+10.7%) | 100% | **41**/60 | 29.8 мин |

**Gate 3 (quality subset):** aggressive не прошёл `blocks_with_findings ≥ 95% baseline`  
(41/45 = 91.1% < 95%). Разрыв мал (4 блока из 45), но gate консервативный.

**Артефакты:**
```
projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/
  _experiments/block_batch_final/20260420_155609/   ← full-runs
  _experiments/block_batch_ab/20260420_221123/      ← валидный subset, финальный gate
    winner_recommendation.md
    gate_report.json
    subset_side_by_side.md
```

**Почему aggressive НЕ принят:**  
Aggressive нашёл больше замечаний на полном прогоне (+10.7%), но на контрольном subset  
пропустил больше блоков (bwf 91.1% < порога 95%). При небольшой выборке это может быть  
дисперсия, но gate требует доказательства — aggressive его не дал.

### Resolution: r800 (winner), r1000/r1200 (не приняты)

**Метод:** `run_block_resolution_matrix.py` — Phase A (subset single-block)

r1000/r1200 не прошли gate по надёжности/coverage или росту batch-cost.  
r800 остаётся production default: стабильный, предсказуемый размер блоков.

---

## Safe fallback

**r800 + baseline_p2 (parallelism=2)**  
Использовать если:
- Наблюдаются систематические rate-limit ошибки при parallelism=3
- 5-часовой лимит Claude близок к исчерпанию перед запуском

Как переключить: `CLAUDE_BLOCK_BATCH_PARALLELISM=2 python blocks.py batches ...`  
или через ENV в webapp systemd/docker конфиге.

---

## Experimental profiles (не для production)

| Профиль | Где определён | Назначение |
|---------|---------------|------------|
| aggressive_p3 | `scripts/run_claude_block_batch_matrix.PROFILES["aggressive"]` | Повторный тест на другом документе |
| r1000, r1200 | `scripts/run_block_resolution_matrix.RESOLUTION_PROFILES` | Resolution A/B эксперименты |
| conservative | `scripts/run_claude_block_batch_matrix.PROFILES["conservative"]` | Крайне осторожный fallback |

ENV overrides для экспериментов: `CLAUDE_BATCH_{HEAVY,NORMAL,LIGHT}_{TARGET,MAX}`,  
`CLAUDE_BLOCK_BATCH_PARALLELISM`, `BLOCK_RENDER_MIN_LONG_SIDE`.

---

## Изменение production defaults

Если будущий эксперимент покажет другого победителя:
1. Обновить `_CLAUDE_RISK_TARGETS` в `blocks.py` (batching)
2. Обновить `STAGE02_PRODUCTION_PROFILE` в `blocks.py`
3. Обновить `CLAUDE_BLOCK_BATCH_PARALLELISM_DEFAULT` в `webapp/config.py`
4. Обновить этот документ
5. Обновить `CLAUDE.md` → раздел "Пакетный анализ блоков"
