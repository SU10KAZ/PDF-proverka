# G2.2 — dense_sectioned_board production profile

## Результат

Профиль построен без production CASES: `dense_sectioned_board.py` принимает только
`VectorEvidence`, не открывает PDF и не содержит block/page ids или заранее заданных
обозначений оборудования и числа секций. Корпусные ids живут только в этом
воспроизводимом evaluation runner.

## 1. Получился ли профиль без CASES?

Да. Detection использует плотность аппаратов, Y/X clustering, повторяемость колонок,
префиксы, горизонтальную геометрию шин и межсекционный разрыв. Недостаточный набор
сигналов возвращает `UNKNOWN`; `profile_confidence` сохраняется в графе.

## 2. Что распознаётся

| Блок | SOURCE | INPUT_DEVICE | BUS_SECTION | SECTION_DEVICE | OUTGOING_DEVICE |
|---|---|---|---:|---|---:|
| П, стр. 21 | ТП2, ТП1 | QF2, QF1 | 2 | QF3 | 30 |
| РД, стр. 52 | Т1, Т2 | QF1, QF2 | 2 | QS1 | 27 |

На обязательной паре автоматически восстановлены цепочки
`SOURCE → INPUT_DEVICE → BUS_SECTION → OUTGOING_DEVICE → LOAD/UNKNOWN_NODE`.
У `SOURCE` отдельно сохранены `source_role` и `source_representation`.

## 3. Что осталось unknown

- `blk_039909ec039649a1b8209f059c95167b`: 6 unknown nodes; labels: QF4, QF5, ГРЩ1-РП1-11, ГРЩ1-РП1-14, ГРЩ1-РП2-11, ГРЩ1-РП2-14; identity coverage 0.867.
- `blk_2d72a6705eaf4d8c9ee1d6ff459b15a6`: 2 unknown nodes; labels: UNLABELED; identity coverage 0.926.

`UNKNOWN_NODE` создаётся для ветвей без надёжно привязанной идентичности нагрузки и
для неразрешённых аппаратов вводной зоны. Он не блокирует восстановленный backbone.

## 4. Переносимость и coverage

Исследовательский корпус: 4/4 блоков detected,
4/4 валидны по `system-graph.v1`; failures: нет.
Проверены две стадии и два способа представления листа, включая поворот 270°.
Это подтверждает переносимость между четырьмя доступными dense-блоками, но корпус
происходит из одного проектного комплекта; до универсального профиля всех ГРЩ нужна
дальнейшая межпроектная выборка. Близкие, но недостаточно плотные щиты остаются на
classic/других профилях или честно деградируют в `UNKNOWN`/raw-vector.

## 5. Classic path и Stage Comparison

Classic Vectograf остаётся первым в router cascade; существующий builder и gate не
изменены. Общая offset-привязка колонок перенесена в geometry helper с совместимым
classic wrapper; на 2000 детерминированных случайных раскладках результат старой и
новой реализаций совпал полностью.

- G2.2/profile + block-context/source-kind integration: `56 passed`.
- classic Vectograf/singleline/common evidence: `57 passed, 23 skipped`;
  skips — отсутствующие локальные PDF-корпусы, как в G2.1.
- Stage Comparison: `300 passed`.

## 6. Готовность к comparator

Да, после принятия G2.2 можно переходить к отдельному этапу comparator: оба графа
имеют единый контракт, provenance, grounded nodes/edges и раздельные honesty metrics.
Comparator, сравнение П↔РД и GraphicChangeLedger в G2.2 намеренно не реализованы.
Identity coverage ниже 1.0 должна учитываться будущим comparator как неопределённость,
а не как доказанное изменение.

## Воспроизведение

```bash
python experiments/g2_dense_sectioned_board/run_corpus.py
```
