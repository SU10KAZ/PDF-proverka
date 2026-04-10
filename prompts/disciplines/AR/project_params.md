## Что искать в тексте (дисциплина АР)
- Нормативные ссылки (СП 54, СП 59, СП 118, СП 1, ГОСТ 21.101 и др.)
- Этажность, высота здания, отметки
- Площади квартир (общая, жилая, приведённая)
- Конструктивная схема (монолитный каркас, кирпич, панель)
- Степень огнестойкости, класс конструктивной пожарной опасности
- Категория ответственности здания
- Климатический район, ветровой и снеговой район
- Класс энергоэффективности
- Данные по инсоляции и естественному освещению
- Материалы наружных стен, утеплителя, отделки фасада

```json
{
  "building_floors": 0,
  "building_height_m": 0,
  "building_class": "Ф1.3",
  "fire_resistance_degree": "",
  "structural_fire_hazard_class": "",
  "structural_system": "",
  "climate_zone": "",
  "wind_region": "",
  "snow_region": "",
  "energy_class": "",
  "apartment_count": 0,
  "total_area_m2": 0,
  "living_area_m2": 0,
  "wall_construction": "",
  "insulation_material": "",
  "insulation_thickness_mm": 0,
  "facade_type": "",
  "roof_type": "",
  "mgn_apartments_count": 0
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_floors | Количество этажей (надземных) | 25 |
| building_height_m | Высота здания, м | 75.6 |
| building_class | Класс функциональной пожарной опасности | "Ф1.3" |
| fire_resistance_degree | Степень огнестойкости | "II" |
| structural_fire_hazard_class | Класс конструктивной пожарной опасности | "С0" |
| structural_system | Конструктивная схема | "монолитный каркас" |
| climate_zone | Климатический район | "IIВ" |
| wind_region | Ветровой район | "I" |
| snow_region | Снеговой район | "III" |
| energy_class | Класс энергоэффективности | "B+" |
| apartment_count | Количество квартир | 150 |
| total_area_m2 | Общая площадь здания | 12500 |
| living_area_m2 | Жилая площадь | 8200 |
| wall_construction | Конструкция наружных стен | "кирпич 120 + утеплитель 150 + вентфасад" |
| insulation_material | Утеплитель | "минераловатная плита" |
| insulation_thickness_mm | Толщина утеплителя, мм | 150 |
| facade_type | Тип фасада | "вентилируемый навесной" |
| roof_type | Тип кровли | "плоская неэксплуатируемая" |
| mgn_apartments_count | Количество квартир для МГН | 8 |
