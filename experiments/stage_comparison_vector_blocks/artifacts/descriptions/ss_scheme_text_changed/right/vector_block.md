# Vector block `blk_11f1168f77f84b9283dec8a7bc14e9bb`

- Источник: `/home/coder/projects/PDF-proverka/projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v003/02_work/document.pdf`, страница 6
- Качество vector layer: **GOOD**
- Bbox PDF: `[49.8691, 15.5752, 1668.37688, 450.50262]`

## Общая структура

- Примитивов: 159 (filled_polygon: 120, line: 2, path: 36, polyline: 1)
- Сегментов: 1190
- Замкнутых paths: 121
- Компонентов: 446
- Конечных точек: 818; ветвлений: 388
- T-соединений: 113; X-пересечений без подтверждённого junction: 22

## Текст и значения

- Text spans: 97; привязано к ближайшей геометрии: 97
- Инженерные значения-кандидаты: нет
- Основные подписи: ОСПД, этаж, этаж, Корпус, ВК, 1.1.1.1, ВК, 1.1.1.2, ВК, 1.1.1.3, Помещение СС, (1., Т, .4), ОСПД, Помещение СС, (5., Т, .1), ОСПД, Пристройка К, 3-, К, ВК, 4.1.1.8, ВК, 4.1.1.9, ВК, 4.1.1.10, ВК, 4.1.1.11, ВК, 4.1.1.12, ВК, 4.1.1.13, Корпус, Контроль периметра здания, Контроль периметра, здания, Корпус …

## Повторяющиеся геометрические мотивы

- `pattern_51a88d1cfb2b`: 36 × filled_polygon (4 сегм.)
- `pattern_0fe264c46020`: 26 × filled_polygon (4 сегм.)
- `pattern_72facc24ae3d`: 15 × path (8 сегм.)
- `pattern_a19f76151fcd`: 15 × filled_polygon (4 сегм.)
- `pattern_f78075fcb9ad`: 14 × filled_polygon (4 сегм.)
- `pattern_a20595ee6ee4`: 10 × filled_polygon (4 сегм.)
- `pattern_1fdc30195319`: 7 × filled_polygon (4 сегм.)
- `pattern_ddcb6a4dc1b2`: 4 × filled_polygon (4 сегм.)
- `pattern_5c5f11279bb1`: 3 × filled_polygon (4 сегм.)
- `pattern_df9ae95d2e9e`: 3 × filled_polygon (4 сегм.)
- `pattern_c56acd98ae96`: 2 × filled_polygon (4 сегм.)

## Hatch-like candidates

- `hatch_0_-11`: 500 сегм., угол 0°
- `hatch_90_-8`: 241 сегм., угол 90°
- `hatch_90_-9`: 69 сегм., угол 90°
- `hatch_0_-9`: 67 сегм., угол 0°
- `hatch_0_-13`: 54 сегм., угол 0°
- `hatch_0_-8`: 50 сегм., угол 0°
- `hatch_90_-7`: 43 сегм., угол 90°
- `hatch_0_-10`: 20 сегм., угол 0°

## Многоуровневый размер

- Level 0 raw: 97743 байт (~24436 токенов)
- Level 1 normalized: 89837 байт (~22460 токенов)
- Level 2 groups/topology: 40906 байт (~10147 токенов)
- Level 3 compact: 3701 байт (~846 токенов)

## Неоднозначности

- X-crossings are recorded but not treated as connected without a junction marker.
- Text-to-geometry anchors are proximity candidates, not semantic assertions.
- Repeated patterns identify geometric motifs, not discipline-specific object classes.
- Hatch-like structures are parallel-segment candidates and may also represent grids or repeated linework.
- Polygon clipping keeps segments with an endpoint or midpoint inside the polygon; boundary-only intersections remain approximate.
