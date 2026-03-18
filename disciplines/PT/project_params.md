## Что искать в тексте (дисциплина ВПВ/АУПТ)

- Нормативные ссылки -> проверить актуальность по нормативной базе дисциплины
- Расчётные параметры ВПВ (число струй, расход, напор, время работы)
- Параметры АУПТ (группа помещений, интенсивность, площадь орошения)
- Характеристики насосов (подача, напор, мощность)
- Объём пожарного резервуара
- Диаметры трубопроводов из таблиц и схем
- Противоречия между гидравлическим расчётом и принятыми решениями

## JSON-шаблон project_params

```json
{
  "vpv_required": true,
  "vpv_streams_count": 2,
  "vpv_flow_rate_lps": 2.5,
  "vpv_free_head_m": 4,
  "vpv_duration_hours": 3,
  "sprinkler_required": false,
  "sprinkler_group": "",
  "sprinkler_area_m2": 0,
  "sprinkler_intensity_lps_m2": 0,
  "drencher_required": false,
  "drencher_flow_lps_per_m": 0,
  "fire_pump_main_flow_lps": 0,
  "fire_pump_head_m": 0,
  "fire_pump_reserve": "100%",
  "jockey_pump": true,
  "fire_reservoir_volume_m3": 0,
  "building_height_m": 0,
  "building_floors": 0,
  "fire_hazard_category": "",
  "fire_resistance_degree": ""
}
```

## Описание полей

| Поле | Описание | Пример |
|---|---|---|
| vpv_required | Требуется ли ВПВ | true, false |
| vpv_streams_count | Количество пожарных струй | 1, 2, 3, 4 |
| vpv_flow_rate_lps | Расход одной струи, л/с | 2.5, 5.0 |
| vpv_free_head_m | Свободный напор у ПК, м | 4, 6, 8 |
| vpv_duration_hours | Время работы ВПВ, ч | 3 |
| sprinkler_required | Требуется ли спринклерная АУПТ | true, false |
| sprinkler_group | Группа помещений по СП 485 | "1", "2", "3", "4.1", "4.2" |
| sprinkler_area_m2 | Площадь орошения, м² | 120, 180, 240, 360 |
| sprinkler_intensity_lps_m2 | Интенсивность орошения, л/(с·м²) | 0.08, 0.12, 0.24 |
| drencher_required | Требуется ли дренчерная АУПТ | true, false |
| drencher_flow_lps_per_m | Расход дренчерной завесы, л/(с·м) | 0.5, 1.0 |
| fire_pump_main_flow_lps | Подача основного пожарного насоса, л/с | 10, 20, 40 |
| fire_pump_head_m | Напор пожарного насоса, м | 40, 60, 80 |
| fire_pump_reserve | Резерв насоса | "100%" |
| jockey_pump | Наличие жокей-насоса | true, false |
| fire_reservoir_volume_m3 | Объём пожарного резервуара, м³ | 50, 100, 200 |
| building_height_m | Высота здания, м | 28, 50, 75 |
| building_floors | Число этажей | 9, 17, 25 |
| fire_hazard_category | Функциональная пожарная опасность | "Ф1.3", "Ф1.2", "Ф3.1" |
| fire_resistance_degree | Степень огнестойкости здания | "I", "II", "III" |
