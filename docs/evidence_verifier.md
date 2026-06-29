# Evidence Verifier — документация

Проект: `/home/coder/projects/PDF-proverka`  
Связанный слой 1: [kb_agent.md](kb_agent.md)  
Обновлено: июнь 2026

---

## Содержание

1. [Зачем нужен](#1-зачем-нужен)
2. [Два слоя: KB vs EV](#2-два-слоя-kb-vs-ev)
3. [Схема](#3-схема)
4. [Карта файлов](#4-карта-файлов)
5. [Маршрутизация путей](#5-маршрутизация-путей)
6. [Связка с KB-агентом](#6-связка-с-kb-агентом)
7. [Golden dataset](#7-golden-dataset)
8. [Бенчмарк моделей](#8-бенчмарк-моделей)
9. [API и CLI](#9-api-и-cli)
10. [UI](#10-ui)
11. [Переменные окружения](#11-переменные-окружения)
12. [Формат evidence_validation.json](#12-формат-evidence_validationjson)

---

## 1. Зачем нужен

**KB-агент** отвечает: *«похоже ли замечание на уже разобранные экспертами случаи?»* — быстро, по тексту и metadata, без чтения документа.

**Evidence Verifier (EV)** отвечает: *«подтверждается ли замечание фактами в документе и на графическом блоке?»* — медленнее, с доступом к MD, OCR/Gemma-enrichment и crop изображений блоков через локальные vision-модели (ngrok/LM Studio).

EV запускается **выборочно** (спорные после KB, graphic evidence, `grounded_strong`), не в основном пайплайне.

---

## 2. Два слоя: KB vs EV

| | KB-агент | Evidence Verifier |
|---|----------|-------------------|
| Вопрос | Похоже на прошлые решения? | Подтверждается документом? |
| Вход | Текст finding + KB examples | MD, crops, Gemma OCR, blocks |
| Скорость | Минуты (Claude CLI batch) | Долго (vision per block) |
| Выход | `kb_validation.json` | `evidence_validation.json` |
| UI колонка | KB-Agent | EV (Проверка) |

---

## 3. Схема

```
03_findings.json ──┬──► KB-Agent ──► kb_validation.json
                   │
                   └──► Evidence Verifier
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
              context_loader  router   kb_routing
                    │         │
         graphic path    text path
                    │         │
              vision LLM   Claude CLI
                    │         │
                    └────┬────┘
                         ▼
              evidence_validation.json
                         ▼
              GET/POST /api/findings/.../evidence-validation
                         ▼
                   UI: колонка EV
```

---

## 4. Карта файлов

```
backend/app/pipeline/stages/findings_review/evidence_verifier/
  __init__.py              # EvidenceVerifier export
  classifier.py            # graphic_confirmed / graphic_rejected / text_only / ...
  context_loader.py        # blocks, PNG paths, gemma OCR, MD excerpt
  router.py                # graphic | text | mixed | weak
  kb_routing.py            # should_run_evidence_verifier()
  graphic_verifier.py      # vision LLM (graphic_llm_local)
  text_verifier.py         # Claude CLI text path
  parse.py                 # EVDecision, safety post-processing
  engine.py                # EvidenceVerifier orchestration
  golden_set.py            # build/load evidence_golden_set.json
  prompts/
    verify_graphic.ru.md
    verify_text.ru.md

backend/app/services/findings/evidence_validation_service.py
backend/app/api/routers/findings.py   # GET/POST evidence-validation
scripts/build_evidence_golden_set.py
scripts/benchmark_evidence_models.py
scripts/validate_findings_evidence.py
knowledge_base/evidence_golden_set.json
benchmarks/evidence_verify/report_*.json
```

---

## 5. Маршрутизация путей

`router.route_verification_path()` по контексту finding:

| Путь | Условие |
|------|---------|
| `graphic` | image evidence / source_block_ids, есть PNG crop |
| `text` | только text evidence / evidence_text_refs |
| `mixed` | image + text refs |
| `weak` | ungrounded, нет crops → `needs_human` |

Вердикт: `accept` / `reject` / `borderline` / `needs_human` + `verification_path` + `block_ids_used`.

---

## 6. Связка с KB-агентом

`kb_routing.should_run_evidence_verifier()`:

| Сценарий | EV |
|----------|-----|
| KB `borderline` | всегда |
| KB `reject` + graphic evidence | обязателен |
| KB `reject` (text) | запуск |
| KB `needs_human` | запуск |
| KB `accept` + Critic v2 ≥ 85 | пропуск |
| KB `accept` (иначе) | пропуск |
| `grounded_strong` + image | запуск |

Параметр `force=true` в API/CLI игнорирует маршрутизацию.

---

## 7. Golden dataset

Скрипт join `decisions_log.json` ↔ `03_findings.json` по `(source_project, item_id)`:

```bash
cd /home/coder/projects/PDF-proverka
python3 scripts/build_evidence_golden_set.py
```

Классы: `graphic_confirmed`, `graphic_rejected`, `text_only`, `text_mixed`, `ungrounded_dispute`, `ungrounded_other`.

Выход: `knowledge_base/evidence_golden_set.json`.

Новые экспертные решения обогащаются полями `grounding_level`, `primary_block_ids`, `evidence_types` в `KnowledgeBaseEntry` (`expert_review.py`).

---

## 8. Бенчмарк моделей

```bash
python3 scripts/benchmark_evidence_models.py --limit 20
python3 scripts/benchmark_evidence_models.py --models qwen/qwen3.6-35b-a3b google/gemma-4-26b-a4b
```

Метрики: `accuracy`, `false_reject_rate`, `avg_latency_sec`.  
Отчёт: `benchmarks/evidence_verify/report_YYYYMMDD_HHMMSS.json`.

Требует доступный ngrok/LM Studio (`CHANDRA_BASE_URL`).

---

## 9. API и CLI

### API

```
GET  /api/findings/{project_id}/evidence-validation
POST /api/findings/{project_id}/evidence-validation/run?section=TX&force=false
```

Query: `version_id`, `graphic_model`, `text_model`, `force`.

### CLI

```bash
python3 scripts/validate_findings_evidence.py "13АВ-РД-ТХ1.2-ПА V1" --section TX
python3 scripts/validate_findings_evidence.py "PROJECT_ID" --force
```

---

## 10. UI

Колонка **EV** в таблице замечаний (рядом с KB-Agent):

- Бейдж: принять / отклонить / под вопросом / эксперт
- Tooltip: путь (`графика` / `текст` / `mixed`), блоки, explanation
- Кнопка **«Запустить EV»** в панели замечаний — POST (тяжёлый прогон, не автозагрузка)

---

## 11. Переменные окружения

| Переменная | Назначение |
|------------|------------|
| `CHANDRA_BASE_URL` | ngrok/LM Studio endpoint |
| `NGROK_AUTH_USER` / `NGROK_AUTH_PASS` | Basic auth |
| `STAGE_COMPARISON_GRAPHIC_LLM_MODEL` | default graphic model |
| `EV_GRAPHIC_MODEL` | override для EV |
| `EV_TEXT_MODEL` | Claude model для text path (default `sonnet`) |

---

## 12. Формат evidence_validation.json

```json
{
  "generated_at": "2026-06-27T12:00:00",
  "project_id": "...",
  "section": "TX",
  "graphic_model": "qwen/qwen3.6-35b-a3b",
  "text_model": "sonnet",
  "total_findings": 68,
  "total_processed": 42,
  "skipped_count": 26,
  "errors_count": 0,
  "decisions": [
    {
      "finding_id": "F-001",
      "llm_decision": "reject",
      "human_taxonomy_reason": null,
      "confidence": 0.82,
      "explanation": "...",
      "verification_path": "graphic",
      "block_ids_used": ["7NHH-UUGK-64M"],
      "evidence_checked": true,
      "model_used": "qwen/qwen3.6-35b-a3b"
    }
  ]
}
```

`verification_path: "skipped"` — EV не запускался (KB routing).
