# Port-spec: перенос схемы EV2 (восприятие⊥суждение) в Cursor `graphic_verifier.py`

Цель: заменить «один vision-вызов → модель сама даёт accept/reject» на схему EV2:
модель ТОЛЬКО читает чертёж (`contradicts_finding: yes/no/cannot_tell`), вердикт
выносит Python. Это убирает `false_reject≈0.4` (модель ошибочно «опровергает»
реальные замечания).

Меняются ДВА файла Cursor:
- `evidence_verifier/prompts/verify_graphic.ru.md` (текст промпта)
- `evidence_verifier/graphic_verifier.py` (парс + политика)

`parse.py` НЕ трогаем (он нужен текстовому пути). Сначала примени 3 аварийных
фикса из `CURSOR_BUGFIXES.md` — без них графика не доходит до вердикта.

---

## Шаг 1 — заменить промпт `prompts/verify_graphic.ru.md` целиком

Сохрани плейсхолдеры Cursor (`{{FINDING}}`, `{{GEMMA_TEXT}}`). Новый текст:

```markdown
Ты эксперт по строительной проектной документации. Перед тобой ОДИН графический блок чертежа (изображение) и OCR-текст этого блока. КЛЮЧЕВОЕ: ты видишь только ОДИН блок из большого комплекта. Если предмет замечания не на нём — это нормально, и это НЕ значит, что замечание ложное.

Замечание, сформированное другим ИИ:
{{FINDING}}

OCR/enrichment блока:
{{GEMMA_TEXT}}

Твоя роль — НЕ выносить вердикт «верно/неверно». Ты только читаешь, что показано на ЭТОМ блоке, и сравниваешь с предметом замечания.

1) disputed_subject — одной фразой: какой именно элемент/значение/подпись оспаривает замечание.
2) Тест присутствия (ПЕРЕД вердиктом): виден ли на ПОКАЗАННОМ блоке тот самый элемент?
   • НЕТ (другой узел/таблица/лист, обрезано) → value_on_drawing="предмет не на блоке"; evidence_quote=""; region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА, но нечитаемо → region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА и читаемо → продолжай.
3) value_on_drawing — что фактически показано. evidence_quote — ДОСЛОВНАЯ надпись/число с чертежа или OCR (копируй точно).
4) contradicts_finding:
   "yes" — РАЗРЕШЕН только если: предмет присутствует и читаем; evidence_quote непустой и относится к предмету; видимое ПРЯМО опровергает замечание. Иначе запрещён.
   "no"  — видимое на блоке ПОДТВЕРЖДАЕТ проблему из замечания.
   "cannot_tell" — по умолчанию: предмет не на блоке, нечитаем, нет цитаты, либо надпись не противоречит напрямую.

ЗАПРЕТЫ: «не вижу / нет на блоке» = "cannot_tell", НИКОГДА не "yes". "yes" с пустым evidence_quote недопустим. При сомнении → "cannot_tell".

Ответь ТОЛЬКО одним JSON-объектом, без markdown:
{"disputed_subject":"...","value_on_drawing":"...","evidence_quote":"...","region_legible":true,"contradicts_finding":"yes|no|cannot_tell","note":"одно предложение"}
```

---

## Шаг 2 — в `graphic_verifier.py` добавить парс восприятия + политику

Добавь две функции (взяты из EV2 `extract.py`/`verify.py`, адаптированы под EVDecision):

```python
import json, re

_CONTRA = {"yes", "no", "cannot_tell"}

def _parse_perception(text: str) -> dict | None:
    text = (text or "").strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict): return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict): return obj[0]
    except json.JSONDecodeError:
        pass
    dec = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                obj, _ = dec.raw_decode(text[i:])
                if isinstance(obj, dict): return obj
            except json.JSONDecodeError:
                continue
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try: return json.loads(m.group(0))
        except json.JSONDecodeError: return None
    return None

def _perception_to_decision(finding_id: str, obj: dict | None, block_ids: list) -> EVDecision:
    if not obj:
        return missing_decision({"id": finding_id}, verification_path="graphic")
    contradicts = str(obj.get("contradicts_finding", "cannot_tell")).strip().lower()
    if contradicts not in _CONTRA:
        contradicts = "cannot_tell"
    quote = str(obj.get("evidence_quote", "")).strip()
    # GUARD: «yes» без дословной цитаты = рассуждательный yes без якоря → cannot_tell
    if contradicts == "yes" and not quote:
        contradicts = "cannot_tell"

    # ПОЛИТИКА (K=1). Семантика: yes = чертёж ОПРОВЕРГАЕТ замечание → reject;
    #   no = чертёж ПОДТВЕРЖДАЕТ → accept; cannot_tell → эксперт.
    if contradicts == "yes":
        decision, conf, reason = "reject", 0.8, "Чертёж опровергает замечание (есть цитата)."
    elif contradicts == "no":
        decision, conf, reason = "accept", 0.7, "Чертёж подтверждает проблему."
    else:
        decision, conf, reason = "needs_human", 0.4, "По блоку нельзя проверить."

    return EVDecision(
        finding_id=finding_id,
        llm_decision=decision,
        human_taxonomy_reason="visual_or_ocr_misread" if decision == "reject" else None,
        explanation=f"{reason} {str(obj.get('value_on_drawing',''))[:200]}",
        confidence=conf,
        verification_path="graphic",
        block_ids_used=block_ids,
        evidence_checked=True,
    )
```

В `verify_graphic_async` заменить хвост (после получения `raw_text`):

```python
    # было: parse_verification_response(raw_text, ...) -> parsed[0]
    # стало:
    obj = _parse_perception(raw_text)
    d = _perception_to_decision(str(ctx.finding.get("id", "")), obj,
                                [b.block_id for b in ctx.blocks])
    d.model_used = result.model_used or model
    return d
```

---

## Шаг 3 — настройка «строгости» (precision/recall)

`_perception_to_decision` выше — **полезный режим** (yes→reject). Если важнее
максимальная безопасность (никогда не резать реальное):

```python
    if contradicts == "yes":
        decision, conf = "borderline", 0.6   # вместо reject — мягко
```

Эмпирика EV2 (qwen3.6-35b, 10+10): строгий вариант (borderline) → `false_reject=0`,
но `recall=0`; полезный (reject с quote-guard) ловит часть ошибок. **Рекомендую
начать с reject + quote-guard** (guard уже страхует от пустых «yes»), потом смотреть
на реальных данных.

## Шаг 4 — проверка

1. Прогнать F-001 на `13АВ-РД-ТХ1.2-ПА V1` — должно дойти до вердикта (как сейчас).
2. Прогнать 5-10 замечаний, где эксперт ОТКЛОНИЛ как «AI неверно прочитал» —
   проверить, что часть ловится как `reject` с непустым `evidence_quote`.
3. Прогнать 5-10 ПОДТВЕРЖДЁННЫХ — убедиться, что `reject` среди них почти нет.

Модель — `qwen/qwen3.6-35b-a3b`. На общей нейросети со мной одновременно не гонять.
