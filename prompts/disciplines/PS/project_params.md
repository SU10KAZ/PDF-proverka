## Что искать в тексте (дисциплина ПС)
- Технико-экономические показатели (ТЭП) здания
- Площади (участка, застройки, общая, жилая, нежилая)
- Этажность (надземная/подземная), высота здания
- Количество и типология квартир
- Строительный объём (надземный/подземный)
- Класс энергоэффективности
- Степень огнестойкости, класс конструктивной пожарной опасности
- Описание конструктивных решений
- Описание инженерных систем
- Мероприятия по доступности для МГН
- Нормативные ссылки (ПП РФ №87, СП 54, ФЗ-384, ФЗ-123 и др.)

```json
{
  "building_type": "МКД",
  "site_area_m2": 0,
  "building_footprint_m2": 0,
  "total_area_m2": 0,
  "residential_area_m2": 0,
  "non_residential_area_m2": 0,
  "above_ground_floors": 0,
  "underground_floors": 0,
  "building_height_m": 0,
  "apartments_total": 0,
  "apartments_by_type": {},
  "residents_estimated": 0,
  "parking_spaces": 0,
  "building_volume_m3": 0,
  "building_volume_above_m3": 0,
  "building_volume_below_m3": 0,
  "energy_class": "",
  "fire_resistance_degree": "",
  "structural_fire_hazard_class": "",
  "functional_fire_hazard_class": "",
  "responsibility_class": "",
  "structural_system": "",
  "foundation_type": ""
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_type | Тип здания | "МКД" |
| site_area_m2 | Площадь земельного участка, м2 | 12500 |
| building_footprint_m2 | Площадь застройки, м2 | 3200 |
| total_area_m2 | Общая площадь здания, м2 | 45000 |
| residential_area_m2 | Жилая площадь, м2 | 28000 |
| non_residential_area_m2 | Площадь нежилых помещений, м2 | 5000 |
| above_ground_floors | Количество надземных этажей | 25 |
| underground_floors | Количество подземных этажей | 2 |
| building_height_m | Высота здания, м | 75.0 |
| apartments_total | Общее количество квартир | 350 |
| apartments_by_type | Квартиры по типам | {"студия": 50, "1к": 120, "2к": 100, "3к": 60, "4к": 20} |
| residents_estimated | Расчётное количество жителей | 900 |
| parking_spaces | Количество машиномест | 180 |
| building_volume_m3 | Строительный объём здания, м3 | 135000 |
| building_volume_above_m3 | Строительный объём надземной части, м3 | 110000 |
| building_volume_below_m3 | Строительный объём подземной части, м3 | 25000 |
| energy_class | Класс энергоэффективности | "B" |
| fire_resistance_degree | Степень огнестойкости | "II" |
| structural_fire_hazard_class | Класс конструктивной пожарной опасности | "С0" |
| functional_fire_hazard_class | Класс функциональной пожарной опасности | "Ф1.3" |
| responsibility_class | Класс ответственности | "КС-2" |
| structural_system | Конструктивная система | "монолитная каркасная" |
| foundation_type | Тип фундамента | "свайный с монолитным ростверком" |
