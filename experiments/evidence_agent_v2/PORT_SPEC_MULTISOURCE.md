# Port-spec: многоисточниковый верификатор (норма + кросс-блок + зрение) → Cursor

Перенос схемы EV2 (валидирована офлайн) в production `backend/.../evidence_verifier/`.
Референс-реализация целиком — `experiments/evidence_agent_v2/`:
`norm_check.py`, `cross_block.py`, `fusion.py`, `verify.py:verify_finding_multi`,
`extract.py:_montage`. Все офлайн-части покрыты тестами (41/41).

Предусловие: применены 3 аварийных фикса (`CURSOR_BUGFIXES.md`) и схема восприятия
(`PORT_SPEC.md`) — графический путь даёт `contradicts yes/no/cannot_tell`.

ГЛАВНЫЙ ИНВАРИАНТ (обязателен): `reject` достижим ТОЛЬКО из сильного визуала
(decision==reject, ≥2 «yes» с цитатой) или из cross_block `xref_refutes` + согласного
визуала. **Норм-проверка НИКОГДА не reject'ит** — максимум accept-с-пометкой / needs_human.

---

## Шаг 1 — норм-путь (офлайн, без нейросети)

Новый `evidence_verifier/norm_check.py` — портировать из EV2 `norm_check.py` 1:1.
Переиспользует существующее (НЕ дублировать):
```python
from norms.external_provider import resolve_norm_status   # статус по «грязному» коду
from norms._core import extract_norms_from_text            # извлечение кодов regex
from norms_api import get_paragraph                        # опц. цитата пункта (sys.path += norms/tools)
```
Маппинг статуса → сигнал (таблица в EV2 `_classify_status`):
`active`→neutral; `replaced`/`cancelled`/`outdated_edition`→**accept_with_flag** (+flags);
`not_in_index`→soft_human. `decision_hint` типизирован так, что reject недостижим.

## Шаг 2 — кросс-блок (офлайн)

Новый `evidence_verifier/cross_block.py` — портировать из EV2. Переиспользует уже
существующие в твоём дереве ретриверы:
```python
from backend.app.pipeline.stages.findings_review.critic_v2.context.neighbor_blocks import get_neighbor_blocks
from backend.app.pipeline.stages.findings_review.critic_v2.context.cross_references import get_cross_references
```
Эвристика: замечание про отсутствие/неполноту + искомая марка найдена у соседа →
`xref_refutes` (decision_hint=reject_candidate, НЕ reject сам). Иначе — `xref_context_only`
+ `candidate_block_ids` (для multi-image). Нужен `document_graph` (у тебя грузится в context_loader).

## Шаг 3 — слияние

Новый `evidence_verifier/fusion.py` — портировать `fuse(visual, norm_signal, cross_block)`
(правила F1–F9 из EV2). Обязателен **property-тест инварианта**: при `visual=accept` любой
`norm_signal` → результат ≠ reject (см. EV2 `test_fusion_policy.py`).

## Шаг 4 — EVDecision + роутинг

`parse.py:EVDecision` — добавить поля:
```python
source: str = ""                 # visual_strong|cross_block_strong|norm_flag|conflict|...
norm_flags: list = field(default_factory=list)
requires_human_review: bool = False
```
`engine.py:verify_finding` — роутер с ранними выходами (EV2 `verify_finding_multi`):
```
norm_signal = run_norm_check(finding)          # офлайн
cross = run_cross_block(finding, graph)         # офлайн
если norm.accept_with_flag и cross != xref_refutes:  return fuse(None, norm, cross)   # без vision
если cross.xref_supports:                            return fuse(None, norm, cross)   # без vision
если нет картинки:                                   return fuse(None, norm, cross)
visual = verify_graphic(... extra_block_ids=cross.candidate_block_ids)  # vision
return fuse(visual, norm, cross)
```
Это экономит дорогой 35B: норма заменена/отменена и кросс-подтверждение закрываются офлайн.

## Шаг 5 — multi-image (Фаза 4)

`graphic_verifier.py` — при наличии `candidate_block_ids` склеить основной + кандидатные
блоки в ОДИН коллаж (EV2 `extract.py:_montage`, PIL, вертикальный стек с подписями) и
послать его в `describe_image_local` (один image_path). Текст кандидатов — в `{{GEMMA_TEXT}}`.

## Шаг 6 — сервис

`evidence_validation_service.run_evidence_validation` — `decisions[]` дополнить полями
`source`, `norm_flags`, `requires_human_review`. Ничего не удалять, только обогащать.

---

## Проверка после переноса
1. Офлайн-тесты норм/кросс-блок/fusion зелёные (перенести из EV2).
2. `multi --offline-only` на 50 findings: `false_reject=0`, появляются `norm_flag` accept и
   `norm_not_indexed` needs_human (как в EV2: 0.0 / 48 needs_human / 2 accept).
3. На нейросети (окно без EV2/меня): нормативный кейс с заменённой нормой → accept-с-пометкой
   без vision; absence-кейс (F-008) → vision с коллажем ведомости → возможный reject через F2.

## Что НЕ автоматизируется (честно)
- Инженерные суждения (~6/24 отклонений) → остаются needs_human (нужен эксперт).
- Норм-индекс неполон (~58% цитируемых норм не в индексе на тест-выборке) → много soft_human;
  это безопасно (не вывод), но снижает «решённость» норм-пути. Пополнение индекса — отдельная задача.
