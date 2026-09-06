# Entity Matcher v3 — сжатие хранимых кандидатов и самотождество

**Дата:** 2026-09-06. **Статус:** кандидат, отдельный коммит, в прод не выкатывался.
Флага нет: это исправление дефектов, а не новая функция; откат — прежний релиз.

## Два доказанных дефекта v2 (пара АР1 объекта 272, FAST)

1. **Самотождество.** `entity_records_from_atoms` чеканит субъект с
   `identity_conflict=True`, если в источнике один субъект получил два
   `project_entity_ref`. v2 считал этот конфликт опровергающим сигналом:
   субъект X объявлялся `DIFFERENT_ENTITY` против самого себя с `HIGH`
   (50 субъектов АР1). Конфликт идентичности — дефект производителя текстовых
   атомов, он ничего не доказывает. v3: такая пара → `UNKNOWN`, не actionable
   (`review_required=false`), не хранится; вопрос «20.2 и 01.1 — один объект?»
   инженеру не задаётся. DIFFERENT остаётся только по содержательным
   опровержениям (конфликт роли, явный `different_entity_ref`, разные явные
   `project_entity_ref`).
2. **Декартово произведение.** v2 хранил по строке на каждую из 245 500 пар
   (338 МБ `entity_relations` + 360 МБ `review_application`, 134 с/пару), из
   них 245 072 — `DIFFERENT_ENTITY` «два разных явных project ref, ни одного
   общего факта». Ни один потребитель их не читает: алиасы — из
   `SAME_ENTITY`, вопросы — из `POSSIBLE`/`UNKNOWN` с `review_required`. v3
   считает такие строки в `relation_counts`, но не пишет
   (`suppressed_trivial_relations`, `different_entity_is_exhaustive=false`).
   Тривиальный `UNKNOWN` (не лучший кандидат своего LEFT, ни одного общего
   факта) тоже не хранится. DIFFERENT с любым положительным сигналом
   (опровергнутый двойник) хранится — это доказательство.

## Что НЕ меняется

`SAME_ENTITY` (≥3 независимых сильных сигнала + единственность), `POSSIBLE_ENTITY`,
`review_required` у POSSIBLE/лучшего UNKNOWN, relation_id хранимых строк, порядок
и содержание вопросов, синтез, отчёт. Числа АР1: POSSIBLE 428→428, review 428→428,
SAME 0→0, хранимых строк 245 500→621, JSON 255,8 МБ→0,6 МБ, self-pair DIFFERENT 50→0.

## Контракт артефакта

- `algorithm_version` = `production-entity-matcher-v3`; `input_signature`
  включает версию → существующие пары считаются устаревшими и
  пересчитываются при следующем запуске. Исторические сессии не трогаются.
- `diagnostics`: `relations_persisted`, `suppressed_trivial_relations`,
  `identity_conflict_subjects{LEFT,RIGHT}`, `different_entity_is_exhaustive=false`;
  `relation_counts` по-прежнему считает ВСЕ оценённые пары.
- `entity_relations.relations` больше не исчерпывающая таблица: потребитель,
  которому нужен полный перечень DIFFERENT, должен пересчитать сам.

## Регресс

`corpus-audits/20260906_v002_clean_integration/reports/` — полный replay 23 пар
V002 c v3 при флаге Sheet Matcher OFF: вопросы/изменения/review = Golden
Baseline, хранимых объектных строк 245 738 → ~637, время пары АР1 134 с → ~35 с.
