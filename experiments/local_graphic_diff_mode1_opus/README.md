# MODE 1 — local graphic diff подготовленных графических блоков

Исследовательский трек (Claude Opus 5). **Production не меняется**, ничего из этой
папки не подключено к конвейеру.

## Что это

Режим сравнения для случая, когда два графических блока **достаточно близки** и
проектировщик внёс локальные правки:

```
prepared block LEFT ─┐
                     ├─ deterministic registration
prepared block RIGHT ┘        ↓
                     local ink/vector difference в общем физическом кадре
                              ↓
                     change regions → graphic events
                              ↓
                     object layer — только адрес/имя, не источник события
```

Сильно перестроенный П→РД — **не** задача MODE 1; он обязан сам сказать
`MODE_2_REQUIRED`.

## Контракт входа

Блоки **уже подготовлены** upstream-конвейером и не ищутся заново:
`document_graph.json → pages[].image_blocks[] → {id, page_index, coords_norm}`.
`coords_norm` — bbox в **визуальном** пространстве страницы (том же, в котором
живут `page.rect` и `get_pixmap(clip=…)`).

Вся геометрия один раз переводится в **визуальные точки PDF**
(`pt = data_pt * page.rotation_matrix`) и дальше живёт только в них: никаких
`x/w, y/h` — анизотропная нормировка (дефект O10 прошлого аудита) невозможна
по построению.

## Состав

```
m1/core.py      извлечение видимой геометрии: page cache, /Rotate, clip-path,
                фильтр невидимой краски, заливки (even-odd), растеризация в
                физическую сетку
m1/register.py  регистрация: uniform scale + translation (+ опц. поворот),
                голосование по дескрипторам, МНК по инлайерам, phase-correlation
                как запасной путь; residual/anchors/confidence/failure_reason
m1/quality.py   двойная метрика извлечения: precision И recall против рендера
m1/diff.py      локальная карта различий, change regions, граница блока,
                события, объектный слой, маршрутизация
probes/         зонды: добыча пар, сигналы, разметка, прогоны, контроли
artifacts/      все измерения
```

## Как воспроизвести

```bash
P=experiments/local_graphic_diff_mode1_opus/probes
python $P/mine_pairs.py            # добыть пары подготовленных блоков из ревизий корпуса
python $P/scan_signals.py          # дешёвые сигналы по всем 1431 кандидатам
python $P/select_benchmark.py      # стратифицированный бенчмарк
python $P/add_negative_controls.py # text-only и table-only контроли
python $P/gt_tool.py               # рендеры для разметки
python $P/gt_assist.py             # растровые признаки по каждому кластеру
python $P/gt_sheet.py              # контактные листы для глаза
PYTHONPATH=. python $P/make_gt.py  # human_ground_truth.json
python $P/run_benchmark.py         # прогон MODE 1
python $P/evaluate.py              # метрики против истины
python $P/extraction_quality.py    # двойная метрика извлечения
python $P/controls.py all          # разбавление, дрейф рамки, перепаковка
python $P/prefilter.py ; python $P/prefilter.py sweep
python $P/vision_payloads.py       # точечные vision-пейлоады
python $P/calibrate.py             # развёртка порогов
python $P/assemble.py              # финальные артефакты
```

Полные рендеры для разметки (`artifacts/gt_evidence/`, ~74 МБ PNG) в репозиторий не
кладутся — они восстанавливаются `gt_tool.py`. В коммите остаются контактные листы
24 пар, размеченных глазами, и кропы vision-пейлоадов.

Отчёт — `MODE1_LOCAL_GRAPHIC_DIFF_REPORT.md`.
