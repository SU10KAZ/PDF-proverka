# Function Lineage v2.6 — журнал фаз

Ветка `main`, чекаут `/home/coder/projects/PDF-proverka`.
Точка старта сессии — `bb755def` (v2.5 записана). Прод-линия на момент
работы: `origin/main` = `4d489bf9033ad40c40099fe5e1436493bc56c0ed`,
релиз `ui-real-4d489bf9`, shadow OFF, материализация OFF.

> Обновлено после решения по NO-GO: добавлены Фазы A–E.

## Число коммитов

На момент первой сверки — **7**, не 8. В отчёте по Фазе 6 стояло «8 isolated commits» при списке из
семи — это была ошибка в числе, список был полным. Проверка:
`git rev-list --count bb755def..HEAD` = `7`.

## Классификация

| Класс | Смысл |
|---|---|
| `PRODUCTION REQUIRED` | код, без которого боевая интеграция Function Lineage не работает |
| `RESEARCH ONLY` | стенды, замороженные корпуса, артефакты измерений; в прод не переносятся |
| `TEST INFRA ONLY` | инфраструктура тестов; в прод-интеграцию Function Lineage НЕ входит |

## Ledger

### 1. `b0763f2b` — криминалистика конфликтов ёмкости (Фаза 1)

* **Класс:** RESEARCH ONLY
* **Файлы:** `experiments/function_lineage_v2/capacity_forensics.py`,
  `tests/test_function_lineage_v2_capacity_forensics.py`,
  `comparison/.../20260903_function_lineage_v2_6_capacity_forensics/`
  (`capacity_conflict_forensics.json`, `report.md`)
* **Тесты:** 8 (`test_function_lineage_v2_capacity_forensics.py`)
* **Вердикт:** 9 из 9 конфликтов v2.5 — истинные. Гипотеза мастер-задачи
  о двойном потреблении ёмкости родителем и ребёнком НЕ подтвердилась.
  Найдена латентная ложно-конфликтная поверхность.

### 2. `8fb78e65` — ёмкость по владению линией (Фаза 2)

* **Класс:** PRODUCTION REQUIRED
* **Файлы:** `backend/app/services/stage_comparison/function_lineage_shadow.py`,
  `tests/test_function_lineage_capacity_ownership.py`
* **Тесты:** 11 (`test_function_lineage_capacity_ownership.py`) +
  49 существующих (`test_stage_comparison_function_lineage_shadow.py`,
  `test_function_lineage_v1.py`)
* **Вердикт:** 117 ложных конфликтов снято, все истинные сохранены.
  Классификация производности воспроизводит замороженный аудит ровно
  (439 групп, 0 расхождений).

### 3. `027790bd` — детерминированный регресс корпуса (Фазы 3–4)

* **Класс:** RESEARCH ONLY, кроме `transport.py` (стенд) —
  прод-кода не содержит
* **Файлы:** `experiments/function_lineage_v2/regression.py`,
  `experiments/function_lineage_v2/transport.py`,
  `tests/test_function_lineage_v2_regression.py`,
  `comparison/.../20260903_function_lineage_v2_6_deterministic_regression/`
* **Тесты:** 10
* **Вердикт:** 17/17 воротец. Кандидаты пересобираются байт-в-байт,
  полнота не сдвинулась, 9/9 истинных конфликтов по-прежнему отвергнуты.

### 4. `4335972a` — holdout-выборка и внешний шлюз (Фаза 5)

* **Класс:** RESEARCH ONLY
* **Файлы:** `experiments/function_lineage_v2/holdout.py`,
  `tests/test_function_lineage_v2_holdout.py`,
  `comparison/.../20260904_function_lineage_v2_6_holdout_evaluation/`
  (замороженные входы, манифест, раскрытие)
* **Тесты:** 9
* **Вердикт:** 36 задач из 170 допустимых, все доступные страты покрыты,
  промпты сентинелов побайтово совпали с v2.5.

### 5. `39648cac` — заслон пакета `experiments`

* **Класс:** TEST INFRA ONLY — **в прод-интеграцию Function Lineage НЕ входит**
* **Файлы:** `tests/conftest.py`
* **Тесты:** сборка 7342 → 7486 тестов
* **Вердикт:** починен доэкзистентный дефект, из-за которого 14 модулей
  тестов не собирались при полном прогоне. К Function Lineage отношения
  не имеет, кроме того что разблокировал его тесты.

### 6. `664ccce1` — разметка частичного пересечения владельцев

* **Класс:** RESEARCH ONLY (правка классификатора, не правила ёмкости)
* **Файлы:** `experiments/function_lineage_v2/capacity_forensics.py`,
  `tests/test_function_lineage_v2_capacity_forensics.py`, артефакты Фаз 1 и 3
* **Тесты:** 9
* **Вердикт:** поведение отказа не изменилось ни в одном случае. Класс A
  теперь точно совпадает с множеством отказов правила ёмкости (16792),
  сумма лицензий — с 473.

### 7. `336decd0` — независимая holdout-оценка (Фаза 6)

* **Класс:** RESEARCH ONLY
* **Файлы:** `experiments/function_lineage_v2/holdout.py` (раннер),
  `experiments/function_lineage_v2/holdout_metrics.py`,
  `tests/test_function_lineage_v2_holdout{,_metrics}.py`,
  результаты прогона
* **Тесты:** 17 + 9
* **Вердикт:** 110/110 вызовов успешны; безопасность чиста;
  воспроизводимость 0,444 при пороге 0,90 → **NOT READY**.

### 8. `52a11e47` — этот журнал

* **Класс:** RESEARCH ONLY (документация)

### 9. `d300e15f` — глобальное разрешение ёмкости (Фаза A)

* **Класс:** PRODUCTION REQUIRED
* **Файлы:** `backend/app/services/stage_comparison/function_lineage_shadow.py`,
  `experiments/function_lineage_v2/regression.py`,
  `tests/test_function_lineage_capacity_composability.py`
* **Тесты:** 16 composability + 60 существующих + 17/17 воротец регресса
* **Вердикт:** исход задачи больше не зависит от вывода соседней задачи и
  от состава партии. Ёмкость считается один раз, после консенсуса, как
  чистая функция множества устойчивых заявок.

### 10. `1e6e3c00` — диагностическая переигровка и тиры (Фазы B–C)

* **Класс:** смешанный — тиры публикации PRODUCTION REQUIRED,
  `holdout_replay.py` RESEARCH ONLY
* **Файлы:** `backend/app/services/stage_comparison/function_lineage_shadow.py`
  (гейт публикации), `experiments/function_lineage_v2/holdout_replay.py`,
  `tests/test_function_lineage_publication_tiers.py`
* **Тесты:** 6 + 16
* **Вердикт:** переигровка 16→20 из 36, конфликты 12→3 (все истинные);
  `AUTO_ELIGIBLE_RELATIONS` пуст, всё уходит в REVIEW по умолчанию.

### 11. `d2e92ec4` — тированная приёмочная выборка (Фазы D–E)

* **Класс:** RESEARCH ONLY
* **Файлы:** `experiments/function_lineage_v2/acceptance.py`,
  `tests/test_function_lineage_v2_acceptance.py`, замороженные артефакты
* **Тесты:** 12
* **Вердикт:** 99 задач в трёх тирах, оба решающих тира взяты целиком,
  194 запланированных запроса, остановка на шлюзе согласия.

## Что уйдёт в боевую интеграцию

Production-required на сегодня:

* `8fb78e65` — ёмкость по владению линией;
* `d300e15f` — глобальное разрешение ёмкости после консенсуса;
* `1e6e3c00` — гейт публикации и тиры (только часть в
  `function_lineage_shadow.py`).

Всё остальное — стенды, артефакты и инфраструктура тестов. Отдельно
подтверждаю: `39648cac` — TEST INFRA ONLY, в интеграцию не входит.
