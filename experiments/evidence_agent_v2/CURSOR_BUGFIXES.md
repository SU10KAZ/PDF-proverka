# Список фиксов для Cursor — пакет evidence_verifier

Найдено независимой сверкой (EV2). **3 аварийных бага → графический путь
`evidence_verifier` сейчас не запускается вообще.** Ниже точные «было/стало».
Проверено против реальных сигнатур в `findings_service.py` и `graphic_llm_local.py`.

---

## АВАРИЙНЫЕ (без них графика не работает)

### Баг 1 — `context_loader.py:195` — TypeError на каждом замечании

`compute_text_evidence` принимает `(graph, ocr_index, findings)`, а вызывается с
двумя позиционными аргументами не на тех местах.

```python
# было (строка 195):
    text_map = compute_text_evidence(items, block_info)

# стало:
    text_map = compute_text_evidence(index_data or {}, {}, items)
```
(`index_data` — это document_graph, уже загружен выше в той же функции; OCR-индекс
можно передать пустым `{}` — внутри есть fallback.)

### Баг 2 — `graphic_verifier.py:67` — AttributeError на первом vision-ответе

У `DescribeResult` НЕТ поля `raw_text`. Сырой текст — в `full_raw_response`.

```python
# было (строка 67):
    raw_text = result.raw_text or ""

# стало:
    raw_text = (result.full_raw_response or result.raw_response_excerpt or "").strip()
```

### Баг 3 — `graphic_verifier.py:71` — валидный ответ выбрасывается как «ошибка»

`describe_image_local` парсит ответ под diff-схему stage-comparison. Ответ
верификатора (массив `[{finding_id,...}]`) под неё не подходит → `status="invalid_json"`,
хотя текст корректен и лежит в `full_raw_response`. Текущая проверка принимает только
`done/partial` (причём `partial` вообще не существует в статусах DescribeResult:
done|error|provider_unavailable|invalid_json|timeout) и теряет валидный ответ.

```python
# было (строка 71):
    if result.status not in ("done", "partial") or not raw_text.strip():

# стало (принять invalid_json, если есть сырой текст; падать только на транспорте):
    if not raw_text or result.status in ("error", "provider_unavailable", "timeout"):
```
Готовый референс рабочей логики — `experiments/evidence_agent_v2/extract.py`
(функция `perceive_async`, конец).

---

## МЕЛКИЕ (не валят, но логика мёртвая/неверная)

| Файл:строка | Проблема | Фикс |
|-------------|----------|------|
| `benchmark_evidence_models.py:29` | `DEFAULT_MODELS` нереальны (`gemma/gemma3.6-*` не существует) | `["qwen/qwen3.6-35b-a3b","qwen/qwen3.6-27b","google/gemma-4-12b","google/gemma-4-26b-a4b","google/gemma-4-31b"]` |
| `evidence_validation_service.py:92` | `critic_map` не строится → ветка `kb_accept_high_critic` мертва | грузить оценки critic_v2 из `03_findings_review.json` и передать `critic_map=` в `verify_batch`, либо убрать параметр |
| `router.py:22` | `if has_text or ctx.md_excerpt:` — мёртвая часть условия | `if has_text: return PATH_TEXT`; для «нет картинки и нет текста» → `PATH_WEAK` |
| `text_verifier.py:60` | `RuntimeError` при `returncode!=0` даже если в stdout валидный JSON; нет LANG/LC_ALL | ошибка только при `returncode!=0 and not stdout.strip()`; добавить `LANG`/`LC_ALL` в env |
| `context_loader.py:180` | недостижимый fallback по `runs/` (двойная `is_file()`) | обходить `runs/` при `index_data is None`, без лишней проверки файла |
| `engine.py:70` | MIXED-путь не помечает `verification_path="mixed"` при graphic-вердикте accept/reject | всегда ставить `PATH_MIXED` в mixed-ветке перед возвратом |
| `graphic_verifier.py:78` | рассинхрон формата `block_id` (с/без `block_`) обнуляет `block_ids_used` | нормализовать обе стороны через `.replace("block_","")` перед сравнением |

---

## Рекомендация по существу (не только баги)

Даже после фикса 3 аварийных багов остаётся слабость дизайна: один vision-вызов
просит модель сразу вынести вердикт `accept/reject` — самый «галлюциногенный» для
VLM режим. По бенчмарку (EV2, qwen3.6-35b) такой подход даёт `false_reject=0.4`
(на 40% реальных замечаний модель ошибочно «опровергает»). Помогает схема EV2:
модель только ЧИТАЕТ чертёж (`contradicts: yes/no/cannot_tell`), вердикт выносит
Python-политика; промпт presence-gate (см. `extract.py` `_PROMPT_C`) убирает
ложные «yes». Детали и цифры — `experiments/evidence_agent_v2/FINDINGS.md`.

Модель для графики — **`qwen/qwen3.6-35b-a3b`** (единственная быстрая и держащая
JSON; 27b и gemma уходят в CoT и в разы медленнее).
