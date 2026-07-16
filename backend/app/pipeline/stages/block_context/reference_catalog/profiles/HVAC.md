# Профили дисциплины ОВ


# Девять логических грамматик ОВ

Машинные идентификаторы профилей и полей используются только внутри JSON. Человекочитаемые
описания в `hvac_out/*.structure.md` полностью русские и построены в стиле эталона
Вектографа: назначение, краткий итог, состав, связи и ограничения.

Общая форма результата:

```text
hvac block
├── source: PDF, страница, block_id
├── containers: виды / разрезы / физические сборки
├── nodes: системы, стояки, аппараты, отметки, размеры
├── networks: CAD-компоненты или семантические группы
├── edges: только доказанные или явно помеченные пространственные связи
├── validation: профильные метрики
└── warnings: границы доказательности
```

## 1. План ОВ — `hvac_floor_plan`

```text
plan
├── grid: вертикальные и горизонтальные оси
├── systems / risers / equipment / sizes
├── colored CAD route segments
├── endpoint-connected route components
└── branches and attached labels
```

Связность строится по совпадающим концам CAD-отрезков. Простое X-пересечение без конца
линии не считается тройником. На листах без читаемых марок сохраняется полноценный
геометрический граф трасс, но система остаётся неизвестной.

## 2. Аксонометрия отопления — `heating_axonometry`

```text
heat source
└── pipe system T11/T12/T21/T22
    └── riser / branch
        ├── level or floor
        ├── heating device
        └── valve
```

Источник, система, стояки, уровни и приборы образуют иерархию. Внутренний порядок
аппаратов в многоточечной CAD-компоненте не домысливается.

## 3. Аксонометрия вентиляции — `ventilation_axonometry`

```text
air system В*/П*/ДУ*
├── damper
├── terminal / fan / grille
├── fire rating
├── duct sizes and levels
└── CAD air-route inventory
```

Аппарат относится только к ближайшей системе. Пересечение линий само по себе не создаёт
ответвление воздуховода.

## 4. Гидравлическая схема — `hydronic_principle`

```text
hydronic assembly
├── pump / heat exchanger / collector
├── valve / filter / instrument
├── numbered apparatus callouts
├── pipe sizes
└── CAD path components
```

Если одна CAD-компонента соединяет ровно два аппарата, создаётся подтверждённая пара.
Многоточечная цепь остаётся гиперсетью. Если символы имеют разрывы, создаётся только
`spatial_inventory`: состав схемы известен, попарные рёбра не выдумываются.

## 5. Монтажный узел — `hvac_installation_detail`

```text
installation detail
├── drawing views / sections
├── assembly parts
├── dimensions
└── physical line geometry
```

Это граф физического состава и видов, а не поток среды.

## 6. Разрез — `hvac_section_layout`

```text
section sheet
├── views: Разрез … or 7-7 / 8-8 / 9-9
├── elevations
├── duct and pipe sizes
└── equipment in section
```

## 7. Чертёж оборудования — `hvac_equipment_drawing`

```text
equipment
├── model
├── modules / sections
├── ports
├── dimensions
└── geometry-only physical parts
```

Профиль работает и с оборудованием без текстового слоя: физические части сохраняются как
`geometry_part`, но им не присваиваются вымышленные названия.

## 8. Рабочая характеристика — `hvac_performance_chart`

```text
performance sheet
├── model
├── axes and numeric values
├── vector curve paths
└── embedded raster chart regions
```

Растровая кривая явно отмечается `embedded_raster`. Её точки не выдаются за извлечённые
векторные значения.

## 9. Площадочная схема — `hvac_site_overview`

```text
heat source / ITP
└── site heat distribution
    ├── building K1 … K6
    └── underground parking zones
```

## Что означает `complete`

`complete` означает, что извлечены обязательные элементы грамматики данного типа блока.
Это не разрешение дорисовывать неизвестную топологию. Ограниченные случаи остаются
`spatial_inventory`, `multi_apparatus_hydronic_path`, `geometry_only` или
`embedded_raster`, и это состояние записано в JSON и Markdown.

## Уровни доказательности

Каждое русское описание теперь явно показывает один из уровней:

- инженерный граф — есть рёбра, подтверждённые непрерывной CAD-геометрией;
- предметная иерархия — узлы определены, но часть связей только пространственная;
- физическая иерархия — виды, разрезы и составные части распределены по группам;
- геометрический инвентарь — трассы извлечены, но марки отсутствуют или не читаются;
- пространственный инвентарь — состав известен, порядок соединения не подтверждён;
- аналитическая геометрия — параметры и векторные пути рабочего графика;
- растровый инвентарь — данные изображения сохраняются без ложной векторизации.
