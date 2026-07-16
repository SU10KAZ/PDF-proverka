"""EV2 perception step — модель ЧИТАЕТ чертёж, НЕ выносит вердикт accept/reject.

Отличие от подхода Cursor: модель не решает «верно ли замечание», а отвечает на
узкий, проверяемый вопрос — «что фактически показано на чертеже по предмету
замечания и противоречит ли это утверждению замечания». Это её сильная сторона
(прочитать значение) и слабая у Cursor-подхода (целостное суждение → галлюцинации).
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .context import Context

CONTRADICTS = {"yes", "no", "cannot_tell"}

# Базовый промпт (v1). Слабость (подтверждена бенчмарком: false_contradict=0.3-0.4):
# определяет yes/no РАНЬШЕ проверки, виден ли вообще предмет на блоке → модели
# показывают ПЕРВЫЙ блок, в нём часто нет предмета замечания, и она говорит «yes».
_PROMPT_BASELINE = """Ты эксперт по строительной проектной документации. Перед тобой ОДин графический блок чертежа (изображение) и текст автоматического OCR этого блока.

Замечание, сформированное другим ИИ по этому чертежу:
{finding}

OCR/enrichment блока:
{ocr}

Твоя задача — НЕ решать, верное ли замечание. Только посмотреть на чертёж и сообщить ФАКТЫ:
1. Что именно оспаривает замечание (предмет).
2. Что РЕАЛЬНО показано на чертеже по этому предмету (значение/подпись/наличие). Если не видно или этого нет на блоке — так и напиши.
3. Противоречит ли увиденное на чертеже утверждению замечания.

Правила:
- Не выдумывай значения, которых нет на изображении или в OCR.
- Если нужного фрагмента не видно/нечитаемо — region_legible=false и contradicts_finding="cannot_tell".
- contradicts_finding:
    "yes"        — то, что видно на чертеже, ОПРОВЕРГАЕТ замечание (замечание похоже на ошибку ИИ);
    "no"         — то, что видно, ПОДТВЕРЖДАЕТ проблему из замечания (замечание похоже на верное);
    "cannot_tell"— по этому блоку нельзя определить.

Ответь ТОЛЬКО одним JSON-объектом, без markdown и текста вокруг:
{{"disputed_subject":"...","value_on_drawing":"...","evidence_quote":"...","region_legible":true,"contradicts_finding":"yes|no|cannot_tell","note":"одно предложение"}}
"""

# Вариант C (presence-gate + quote-gate + анти-флип). Рекомендован анализом для
# снижения false_contradict: СНАЧАЛА тест присутствия предмета на блоке (нет →
# cannot_tell), «yes» разрешён ТОЛЬКО при непустой дословной цитате, «не вижу» ≠
# «опровергнуто», при сомнении → cannot_tell. Контракт вывода тот же.
_PROMPT_C = """Ты эксперт по строительной проектной документации. Перед тобой ОДИН графический блок чертежа (изображение) и OCR-текст этого блока. КЛЮЧЕВОЕ: ты видишь только ОДИН блок из большого комплекта. Если предмет замечания не на нём — это нормально, и это НЕ значит, что замечание ложное.

Замечание, сформированное другим ИИ:
{finding}

OCR/enrichment блока:
{ocr}

Твоя роль — НЕ выносить вердикт «верно/неверно». Ты только читаешь, что показано на ЭТОМ блоке, и сравниваешь с предметом замечания. Заполни поля строго по правилам ниже.

1) disputed_subject — одной фразой: какой именно элемент/значение/подпись оспаривает замечание.

2) Тест присутствия (сделай ПЕРЕД вердиктом): виден ли на ПОКАЗАННОМ блоке тот самый элемент, к которому относится замечание?
   • НЕТ (другой узел/таблица/лист, обрезано, лишь фрагмент) → value_on_drawing="предмет замечания не присутствует на показанном блоке"; evidence_quote=""; region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА, но мелко/смазано/нечитаемо → region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА и читаемо → продолжай.

3) value_on_drawing — что фактически показано по предмету. evidence_quote — ДОСЛОВНАЯ надпись/число с чертежа или OCR (копируй точно, не достраивай).

4) contradicts_finding:
   "yes" — РАЗРЕШЕН только если ОДНОВРЕМЕННО: предмет присутствует и читаем; evidence_quote непустой и относится к предмету; видимое значение ПРЯМО опровергает замечание. Иначе "yes" запрещён.
   "no"  — видимое на блоке подтверждает проблему из замечания.
   "cannot_tell" — значение по умолчанию: предмет не на блоке, нечитаем, нет дословной цитаты, либо надпись не противоречит замечанию напрямую.

ДВА ЗАПРЕТА (нарушение = неверный ответ):
  — «не вижу / не нашёл / нет на блоке» ≠ «опровергнуто». Это всегда "cannot_tell".
  — contradicts_finding="yes" с пустым evidence_quote недопустим.

Если сомневаешься между "yes" и "cannot_tell" — выбирай "cannot_tell". Лучше честно сказать «по блоку не определить», чем ошибочно объявить замечание опровергнутым.

Ответь ТОЛЬКО одним JSON-объектом, без markdown и текста вокруг:
{{"disputed_subject":"...","value_on_drawing":"...","evidence_quote":"...","region_legible":true,"contradicts_finding":"yes|no|cannot_tell","note":"одно предложение"}}
"""

# Вариант B (средняя точка). Сохраняет presence-gate + quote-gate из C (чтобы
# держать false_reject низким), но СНИМАЕТ анти-флип-правило «при сомнении →
# cannot_tell», из-за которого C ни разу не говорил «yes» и обнулил recall.
# Цель: вернуть НЕНУЛЕВОЙ recall при по-прежнему низком false_reject — рабочая
# точка фронтира между A (permissive) и C (глухо-консервативный).
_PROMPT_B = """Ты эксперт по строительной проектной документации. Перед тобой ОДИН графический блок чертежа (изображение) и OCR-текст этого блока. КЛЮЧЕВОЕ: ты видишь только ОДИН блок из большого комплекта. Если предмет замечания не на нём — это нормально, и это НЕ значит, что замечание ложное.

Замечание, сформированное другим ИИ:
{finding}

OCR/enrichment блока:
{ocr}

Твоя роль — НЕ выносить вердикт «верно/неверно». Ты читаешь, что показано на ЭТОМ блоке, и сравниваешь с предметом замечания. Заполни поля строго по правилам.

1) disputed_subject — одной фразой: какой именно элемент/значение/подпись оспаривает замечание.

2) Тест присутствия (сделай ПЕРЕД вердиктом): виден ли на ПОКАЗАННОМ блоке тот самый элемент, к которому относится замечание?
   • НЕТ (другой узел/таблица/лист, обрезано, лишь фрагмент) → value_on_drawing="предмет замечания не присутствует на показанном блоке"; evidence_quote=""; region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА, но мелко/смазано/нечитаемо → region_legible=false; contradicts_finding="cannot_tell". Готово.
   • ДА и читаемо → продолжай.

3) value_on_drawing — что фактически показано по предмету. evidence_quote — ДОСЛОВНАЯ надпись/число с чертежа или OCR (копируй точно, не достраивай).

4) contradicts_finding:
   "yes" — предмет присутствует и читаем, evidence_quote непустой и относится к предмету, и видимое значение ПРЯМО опровергает замечание. Если это условие выполнено — ставь "yes" СМЕЛО, не занижай в cannot_tell.
   "no"  — видимое на блоке подтверждает проблему из замечания.
   "cannot_tell" — предмет не на блоке, нечитаем, либо нет дословной цитаты.

ДВА ЗАПРЕТА (нарушение = неверный ответ):
  — «не вижу / не нашёл / нет на блоке» ≠ «опровергнуто». Это всегда "cannot_tell".
  — contradicts_finding="yes" с пустым evidence_quote недопустим.

Ответь ТОЛЬКО одним JSON-объектом, без markdown и текста вокруг:
{{"disputed_subject":"...","value_on_drawing":"...","evidence_quote":"...","region_legible":true,"contradicts_finding":"yes|no|cannot_tell","note":"одно предложение"}}
"""

_PROMPTS = {"baseline": _PROMPT_BASELINE, "b": _PROMPT_B, "c": _PROMPT_C}


def _select_prompt() -> str:
    return _PROMPTS.get(os.environ.get("EV2_PROMPT", "c").strip().lower(), _PROMPT_C)


@dataclass
class Perception:
    contradicts: str          # yes|no|cannot_tell
    region_legible: bool
    value_on_drawing: str
    disputed_subject: str
    evidence_quote: str
    note: str
    raw: Optional[dict] = None
    error: str = ""
    model_used: str = ""

    @property
    def ok(self) -> bool:
        return not self.error and self.contradicts in CONTRADICTS


def _finding_text(finding: dict, section: str) -> str:
    parts = [
        f"ID: {finding.get('id', '?')}",
        f"Раздел: {section or finding.get('section', '?')}",
        f"Лист: {finding.get('sheet', '?')}",
        f"Критичность: {finding.get('severity', '?')}",
        f"Суть: {finding.get('problem') or finding.get('description') or finding.get('summary', '')}",
        f"Норма: {finding.get('norm', '')}",
        f"Рекомендация: {finding.get('solution') or finding.get('recommendation', '')}",
    ]
    return "\n".join(p for p in parts if p.split(': ', 1)[-1].strip())


def _parse(text: str) -> Optional[dict]:
    text = (text or "").strip()
    # Reasoning-модель без серверного split кладёт мысли в `content` как
    # <think>…</think>{json}. Финальный JSON — ПОСЛЕ последнего </think>.
    # (При серверном split content уже чист — no-op.)
    if "</think>" in text:
        text = text.rsplit("</think>", 1)[-1].strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj[0]
    except json.JSONDecodeError:
        pass
    # вытащить первый {...}
    dec = json.JSONDecoder()
    for i, ch in enumerate(text):
        if ch == "{":
            try:
                obj, _ = dec.raw_decode(text[i:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                continue
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            return None
    return None


def _to_perception(obj: Optional[dict], model_used: str, error: str = "") -> Perception:
    if not obj:
        return Perception("cannot_tell", False, "", "", "", "", None,
                          error or "no_json", model_used)
    contradicts = str(obj.get("contradicts_finding", "cannot_tell")).strip().lower()
    if contradicts not in CONTRADICTS:
        contradicts = "cannot_tell"
    legible = obj.get("region_legible")
    legible = bool(legible) if isinstance(legible, bool) else str(legible).lower() in ("true", "1", "yes")
    quote = str(obj.get("evidence_quote", "")).strip()
    # Детерминированный guard (реализует машинно-проверяемое правило варианта C):
    # «yes» без непустой дословной цитаты с чертежа недопустим — это «рассуждательный»
    # yes без визуального якоря, главный источник false_contradict. Понижаем до
    # cannot_tell независимо от того, послушалась ли модель промпта.
    if contradicts == "yes" and not quote:
        contradicts = "cannot_tell"
    return Perception(
        contradicts=contradicts,
        region_legible=legible,
        value_on_drawing=str(obj.get("value_on_drawing", ""))[:400],
        disputed_subject=str(obj.get("disputed_subject", ""))[:300],
        evidence_quote=quote[:300],
        note=str(obj.get("note", ""))[:300],
        raw=obj,
        model_used=model_used,
    )


def _montage(primary: Path, extras: list, out_path: Path, long_side: int = 1000,
             max_total: int = 2000) -> Path:
    """Вертикальный коллаж: основной блок + блоки-кандидаты (с подписями) в ОДНУ картинку.

    describe_image_local принимает один image_path, поэтому несколько блоков подаём
    коллажем — модель видит и основной чертёж, и связанную ведомость/спецификацию.
    """
    from PIL import Image, ImageDraw

    paths = [primary] + [p for p in extras if p and Path(p).is_file()]
    imgs = []
    for i, p in enumerate(paths):
        try:
            im = Image.open(p).convert("RGB")
        except Exception:
            continue
        if im.width > long_side:
            im = im.resize((long_side, int(im.height * long_side / im.width)))
        label_h = 22
        canvas = Image.new("RGB", (im.width, im.height + label_h), "white")
        ImageDraw.Draw(canvas).text((4, 4), "ОСНОВНОЙ БЛОК" if i == 0 else f"СВЯЗАННЫЙ БЛОК {i}",
                                    fill="black")
        canvas.paste(im, (0, label_h))
        imgs.append(canvas)
    if not imgs:
        return primary
    w = max(im.width for im in imgs)
    total_h = sum(im.height for im in imgs) + 6 * (len(imgs) - 1)
    out = Image.new("RGB", (w, total_h), "white")
    y = 0
    for im in imgs:
        out.paste(im, (0, y))
        y += im.height + 6
    # страховка от гигантской высокой картинки (модель захлёбывается/висит)
    if max(out.width, out.height) > max_total:
        scale = max_total / max(out.width, out.height)
        out = out.resize((max(1, int(out.width * scale)), max(1, int(out.height * scale))))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.save(out_path)
    return out_path


async def _describe_direct(png_path, prompt: str, model: str, scale: Optional[float],
                           *, long_side_abs: Optional[int] = None,
                           enable_thinking: bool = False,
                           max_tokens_override: Optional[int] = None) -> str:
    """Прямой вызов /v1/chat/completions c управляемым chat_template_kwargs.enable_thinking.

    Возвращает ПОЛЕ `content`. При серверном reasoning-split мысли уходят в
    `reasoning_content` (игнор), в `content` — чистый JSON. Если split'а нет и
    reasoning утёк в content как <think>…</think>{json} — его срежет `_parse`.
    long_side_abs задаёт АБСОЛЮТНОЕ разрешение (для high-res рендера); иначе
    long_side = cfg.image_long_side*scale (возмущение масштабом, baseline-режим).
    Переиспользует прод-конфиг/auth/энкод. Fail-soft → '' (кейс уйдёт в needs_human).
    """
    import httpx
    from backend.app.services.stage_comparison.graphic_llm_local import (
        load_local_graphic_llm_config, _build_headers,
        _resize_png_to_long_side, _png_bytes_to_data_url,
    )
    cfg = load_local_graphic_llm_config()
    long_side = int(long_side_abs) if long_side_abs else int(cfg.image_long_side * (scale or 1.0))
    long_side = max(64, long_side)
    try:
        url = _png_bytes_to_data_url(_resize_png_to_long_side(Path(png_path), long_side))
    except Exception:
        return ""
    payload = {
        "model": model,
        "max_tokens": int(max_tokens_override or cfg.max_tokens),
        "temperature": float(cfg.temperature),
        "chat_template_kwargs": {"enable_thinking": bool(enable_thinking)},
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": prompt},
            {"type": "image_url", "image_url": {"url": url}},
        ]}],
    }
    try:
        async with httpx.AsyncClient(timeout=cfg.timeout_sec) as client:
            r = await client.post(
                f"{cfg.base_url}/v1/chat/completions",
                headers=_build_headers(cfg), json=payload,
            )
        data = r.json()
        return (data["choices"][0]["message"].get("content") or "").strip()
    except Exception:
        return ""


async def perceive_async(
    ctx: Context, *, model: str,
    extra_pngs: Optional[list] = None, extra_context: str = "",
    scale: Optional[float] = None,
    png_override: Optional[Path] = None,
    long_side_abs: Optional[int] = None,
    enable_thinking: Optional[bool] = None,
    max_tokens_override: Optional[int] = None,
) -> Perception:
    from backend.app.services.stage_comparison.graphic_llm_local import describe_image_local

    # png_override — подать high-res рендер вместо gemma-кропа (эксперимент high-res).
    png = png_override or ctx.primary_png
    if not png:
        return Perception("cannot_tell", False, "", "", "", "", None, "no_png", model)

    # multi-image: коллаж основного + связанных блоков (Фаза 4)
    send_png = png
    if extra_pngs:
        try:
            send_png = _montage(Path(png), list(extra_pngs),
                                ctx.output_dir / "_ev2_montage" / f"{ctx.finding.get('id','x')}.png")
        except Exception:
            send_png = png

    ocr = "\n\n".join(
        f"### {b.block_id}\n{b.gemma_text[:2500]}" for b in ctx.blocks if b.gemma_text
    ) or "(нет OCR)"
    if extra_context:
        ocr += f"\n\n### Связанные блоки (другие листы)\n{extra_context[:2000]}"
    prompt = _select_prompt().format(finding=_finding_text(ctx.finding, ctx.section), ocr=ocr)
    # Qwen3-семейство: soft-switch `/no_think` ПРЕФИКСОМ гасит chain-of-thought
    # (endpoint без серверного non-reasoning-режима иначе выдаёт рассуждения → нет JSON).
    # Гейт EV2_NO_THINK (default off) — на vibe (JSON-режим) не нужен.
    if os.environ.get("EV2_NO_THINK", "").strip().lower() in ("1", "true", "yes", "on"):
        prompt = "/no_think\n" + prompt

    # Прямой путь для reasoning-endpoint'а (EV2_DIRECT_NOTHINK): chat_template_kwargs
    # enable_thinking=false → чистый JSON в поле `content` (мысли уходят в reasoning_content,
    # которое describe_image_local ошибочно тянул в raw и ломал парсинг). Возмущение
    # масштабом здесь РЕАЛЬНОЕ (long_side*scale), т.к. мы не даём прод-нормализацию сгладить K.
    if os.environ.get("EV2_DIRECT_NOTHINK", "").strip().lower() in ("1", "true", "yes", "on"):
        # enable_thinking: явный параметр > env EV2_ENABLE_THINKING > False.
        if enable_thinking is None:
            enable_thinking = os.environ.get("EV2_ENABLE_THINKING", "").strip().lower() \
                in ("1", "true", "yes", "on")
        raw_text = await _describe_direct(
            send_png, prompt, model, scale,
            long_side_abs=long_side_abs, enable_thinking=enable_thinking,
            max_tokens_override=max_tokens_override)
        if not raw_text:
            return _to_perception(None, model, error="direct:empty_content")
        return _to_perception(_parse(raw_text), model)

    try:
        result = await describe_image_local(Path(send_png), prompt, model=model)
    except Exception as exc:
        return _to_perception(None, model, error=f"call_error:{exc}")

    # ВАЖНО: describe_image_local парсит ответ как stage-comparison DIFF JSON.
    # Наш ответ — не diff (объект contradicts_finding), поэтому status часто
    # "invalid_json", но сырой текст лежит в full_raw_response. Берём его.
    raw_text = (result.full_raw_response or result.raw_response_excerpt or "").strip()
    if result.parsed and not raw_text:
        raw_text = json.dumps(result.parsed, ensure_ascii=False)
    if not raw_text:
        return _to_perception(None, result.model_used or model,
                              error=f"llm:{result.error or result.status}")
    return _to_perception(_parse(raw_text), result.model_used or model)
