## Что искать в тексте (дисциплина КМ)
- Нормативные ссылки (СП 16.13330, ГОСТ 27772, ГОСТ 23118 и др.)
- Марки стали и их категории
- Тип конструктивной схемы каркаса (рамный, связевой, рамно-связевой)
- Пролёты, шаг колонн, высоты этажей
- Нагрузки (постоянные, временные, снеговые, ветровые)
- Класс ответственности здания, коэффициент надёжности по назначению
- Требуемый предел огнестойкости несущих конструкций
- Класс агрессивности среды
- Способы сварки, марки электродов
- Класс прочности болтов

```json
{
  "building_type": "МКД",
  "structural_system": "",
  "steel_grade_primary": "",
  "steel_grade_secondary": "",
  "span_max_m": 0,
  "column_spacing_m": 0,
  "floor_height_m": 0,
  "floors_count": 0,
  "load_dead_kPa": 0,
  "load_live_kPa": 0,
  "snow_region": "",
  "wind_region": "",
  "seismic_intensity": 0,
  "fire_resistance_class": "",
  "fire_rating_required": "",
  "fire_protection_type": "",
  "fire_protection_group": "",
  "corrosion_class": "",
  "coating_system": "",
  "welding_method": "",
  "electrode_grade": "",
  "bolt_class": "",
  "responsibility_class": "",
  "reliability_factor": 0
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_type | Тип здания | "МКД" |
| structural_system | Тип конструктивной схемы | "рамно-связевой каркас" |
| steel_grade_primary | Основная марка стали | "С345" |
| steel_grade_secondary | Марка стали для второстепенных элементов | "С245" |
| span_max_m | Максимальный пролёт, м | 12.0 |
| column_spacing_m | Шаг колонн, м | 6.0 |
| floor_height_m | Высота этажа, м | 3.3 |
| floors_count | Количество этажей | 25 |
| load_dead_kPa | Постоянная нагрузка на перекрытие, кПа | 5.0 |
| load_live_kPa | Временная нагрузка на перекрытие, кПа | 2.0 |
| snow_region | Снеговой район | "III" |
| wind_region | Ветровой район | "II" |
| seismic_intensity | Сейсмичность, баллы | 0 |
| fire_resistance_class | Класс огнестойкости здания | "II" |
| fire_rating_required | Требуемый предел огнестойкости несущих | "R90" |
| fire_protection_type | Тип огнезащиты | "тонкослойная вспучивающаяся" |
| fire_protection_group | Группа огнезащитной эффективности | "4-я группа" |
| corrosion_class | Класс агрессивности среды | "слабоагрессивная" |
| coating_system | Система антикоррозийного покрытия | "грунт ГФ-021 + эмаль ПФ-115, 2 слоя" |
| welding_method | Способ сварки | "механизированная в CO2" |
| electrode_grade | Марка электрода/проволоки | "Св-08Г2С" |
| bolt_class | Класс прочности болтов | "8.8" |
| responsibility_class | Класс ответственности | "КС-2 (повышенный)" |
| reliability_factor | Коэффициент надёжности по назначению | 1.0 |
