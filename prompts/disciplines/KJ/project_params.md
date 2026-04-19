## Что искать в тексте (дисциплина КЖ)
- Нормативные ссылки (СП 63.13330, СП 70.13330, СП 20.13330, СП 22.13330, СП 28.13330, ГОСТ 34028-2016, ГОСТ 26633-2015 и др.)
- Классы бетона по прочности (В20, В25, В30, В35, В40), морозостойкости (F100-F300), водонепроницаемости (W4-W12)
- Классы и марки арматуры (A500С, A240, B500C; устаревшие — А-II, А-III, ГОСТ 5781-82)
- Величины защитного слоя бетона (мм) для разных элементов
- Требуемый предел огнестойкости несущих конструкций (REI)
- Класс агрессивности среды (СП 28.13330.2017)
- Класс ответственности здания (КС-1, КС-2, КС-3) и коэффициент надёжности
- Сейсмичность района (в баллах)
- Тип фундамента (плитный, свайно-ростверковый, ленточный, столбчатый)
- Толщины плит перекрытия и стен
- Конструктивная схема (монолитный каркас, монолитно-стеновая, смешанная)
- Шифр комплекта (например, 13АВ-РД-КЖx.y-Kn) и организация-разработчик
- Длины анкеровки, нахлёста (lan, llap)
- Рабочие швы бетонирования, распалубочная прочность
- Категория требований к трещиностойкости

```json
{
  "building_type": "МКД",
  "structural_system": "",
  "foundation_type": "",
  "concrete_class_primary": "",
  "concrete_class_foundation": "",
  "concrete_class_columns": "",
  "concrete_class_walls": "",
  "concrete_class_slab": "",
  "concrete_frost_resistance": "",
  "concrete_waterproofness": "",
  "rebar_class_primary": "",
  "rebar_class_secondary": "",
  "rebar_gost": "",
  "cover_thickness_slab_mm": 0,
  "cover_thickness_columns_mm": 0,
  "cover_thickness_walls_mm": 0,
  "cover_thickness_foundation_mm": 0,
  "slab_thickness_typical_mm": 0,
  "wall_thickness_typical_mm": 0,
  "floor_height_m": 0,
  "floors_count": 0,
  "floors_below_grade": 0,
  "load_dead_kPa": 0,
  "load_live_kPa": 0,
  "snow_region": "",
  "wind_region": "",
  "seismic_intensity": 0,
  "fire_resistance_class": "",
  "fire_rating_required": "",
  "corrosion_class": "",
  "anchorage_length_mm": 0,
  "lap_length_mm": 0,
  "responsibility_class": "",
  "reliability_factor": 0,
  "progressive_collapse_required": false,
  "document_code": "",
  "designer_org": "",
  "client_org": ""
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_type | Тип здания | "МКД" |
| structural_system | Конструктивная схема | "монолитный железобетонный каркас" |
| foundation_type | Тип фундамента | "свайно-ростверковый", "плитный" |
| concrete_class_primary | Основной класс бетона | "В30" |
| concrete_class_foundation | Класс бетона фундамента/ростверка | "В25" |
| concrete_class_columns | Класс бетона колонн | "В40" |
| concrete_class_walls | Класс бетона стен | "В30" |
| concrete_class_slab | Класс бетона плиты перекрытия | "В30" |
| concrete_frost_resistance | Морозостойкость | "F150" |
| concrete_waterproofness | Водонепроницаемость | "W8" |
| rebar_class_primary | Основная рабочая арматура | "А500С" |
| rebar_class_secondary | Конструктивная арматура / хомуты | "А240" |
| rebar_gost | ГОСТ арматуры | "ГОСТ 34028-2016" |
| cover_thickness_slab_mm | Защитный слой в плите, мм | 25 |
| cover_thickness_columns_mm | Защитный слой в колоннах, мм | 30 |
| cover_thickness_walls_mm | Защитный слой в стенах, мм | 25 |
| cover_thickness_foundation_mm | Защитный слой в фундаменте, мм | 40 |
| slab_thickness_typical_mm | Типовая толщина плиты перекрытия, мм | 200 |
| wall_thickness_typical_mm | Типовая толщина стены, мм | 200 |
| floor_height_m | Высота этажа, м | 3.3 |
| floors_count | Количество этажей (надземных) | 38 |
| floors_below_grade | Количество подземных этажей | 2 |
| load_dead_kPa | Постоянная нагрузка на перекрытие, кПа | 5.0 |
| load_live_kPa | Временная нагрузка на перекрытие, кПа | 2.0 |
| snow_region | Снеговой район | "III" |
| wind_region | Ветровой район | "II" |
| seismic_intensity | Сейсмичность, баллы | 0 |
| fire_resistance_class | Класс огнестойкости здания | "II" |
| fire_rating_required | Требуемый предел огнестойкости несущих | "REI 120" |
| corrosion_class | Класс агрессивности среды | "слабоагрессивная" |
| anchorage_length_mm | Типовая длина анкеровки, мм | 500 |
| lap_length_mm | Типовая длина нахлёста, мм | 700 |
| responsibility_class | Класс ответственности | "КС-2 (повышенный)" |
| reliability_factor | Коэффициент надёжности по назначению | 1.0 |
| progressive_collapse_required | Проверка на прогрессирующее обрушение | true |
| document_code | Шифр комплекта РД | "13АВ-РД-КЖ5.1-К1К2" |
| designer_org | Организация-разработчик | "ООО «ГК ОЛИМППРОЕКТ»" |
| client_org | Заказчик | "ООО «АСТЕРУС ДЕВЕЛОПМЕНТ»" |
