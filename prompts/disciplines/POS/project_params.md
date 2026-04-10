## Что искать в тексте (дисциплина ПОС)
- Нормативные ссылки (СП 48.13330, СП 49.13330, СНиП 12-03, СНиП 12-04 и др.)
- Площадь строительной площадки
- Продолжительность строительства (месяцы)
- Количество и типы башенных кранов (марка, грузоподъёмность, вылет стрелы)
- Тип и высота ограждения строительной площадки
- Характеристики временных дорог (ширина, покрытие)
- Максимальная численность рабочих
- Мощность временной трансформаторной подстанции
- Потребность в воде на строительной площадке
- Объём земляных работ, глубина котлована
- Наличие сноса/демонтажа существующих зданий

```json
{
  "building_type": "МКД",
  "site_area_m2": 0,
  "construction_duration_months": 0,
  "tower_cranes_count": 0,
  "tower_crane_model": "",
  "tower_crane_capacity_t": 0,
  "tower_crane_reach_m": 0,
  "fence_type": "",
  "fence_height_m": 0,
  "temp_road_width_m": 0,
  "temp_road_surface": "",
  "max_workforce": 0,
  "temp_power_kVA": 0,
  "temp_water_demand_m3day": 0,
  "excavation_depth_m": 0,
  "excavation_volume_m3": 0,
  "demolition_required": false,
  "winter_construction": false
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_type | Тип здания | "МКД 25 этажей" |
| site_area_m2 | Площадь строительной площадки, м2 | 8500 |
| construction_duration_months | Продолжительность строительства, мес | 24 |
| tower_cranes_count | Количество башенных кранов | 2 |
| tower_crane_model | Марка башенного крана | "Liebherr 172 EC-B 8" |
| tower_crane_capacity_t | Грузоподъёмность на макс. вылете, т | 2.4 |
| tower_crane_reach_m | Максимальный вылет стрелы, м | 55 |
| fence_type | Тип ограждения площадки | "сплошное защитно-охранное" |
| fence_height_m | Высота ограждения, м | 2.0 |
| temp_road_width_m | Ширина временных дорог, м | 3.5 |
| temp_road_surface | Покрытие временных дорог | "ж/б плиты" |
| max_workforce | Максимальная численность рабочих | 150 |
| temp_power_kVA | Мощность временной ТП, кВА | 400 |
| temp_water_demand_m3day | Потребность в воде, м3/сут | 25 |
| excavation_depth_m | Глубина котлована, м | 9.0 |
| excavation_volume_m3 | Объём земляных работ, м3 | 45000 |
| demolition_required | Наличие сноса существующих зданий | true |
| winter_construction | Строительство в зимний период | true |
