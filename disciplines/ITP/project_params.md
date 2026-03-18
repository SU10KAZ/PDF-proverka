## Что искать в тексте (дисциплина ИТП)
- Нормативные ссылки (СП 124.13330, СП 60.13330, ГОСТ 34011, Постановление №1034)
- Тепловые нагрузки по контурам (отопление, ГВС, вентиляция)
- Температурный график теплоснабжения (первичный/вторичный контур)
- Параметры теплоносителя (давление, температура)
- Тип схемы присоединения (зависимая/независимая)
- Схема приготовления ГВС (одноступенчатая/двухступенчатая)
- Характеристики теплообменников (мощность, марка, тип)
- Характеристики насосов (марка, расход, напор, мощность)
- Параметры УУТЭ (тип теплосчётчика, диаметры расходомеров)
- Технические условия теплоснабжающей организации

```json
{
  "building_type": "МКД",
  "heat_load_heating_kW": 0,
  "heat_load_dhw_kW": 0,
  "heat_load_ventilation_kW": 0,
  "heat_load_total_kW": 0,
  "temp_graph_primary": "130/70",
  "temp_graph_secondary_heating": "95/70",
  "temp_graph_secondary_dhw": "60/5",
  "pressure_primary_MPa": 0,
  "pressure_secondary_MPa": 0,
  "connection_type": "независимая",
  "dhw_scheme": "двухступенчатая",
  "heat_exchanger_brand": "",
  "heat_exchanger_type": "пластинчатый",
  "pump_brand": "",
  "pump_heating_flow_m3h": 0,
  "pump_heating_head_m": 0,
  "pump_dhw_flow_m3h": 0,
  "pump_dhw_head_m": 0,
  "metering_device": "",
  "metering_flowmeter_dn": 0,
  "heat_source": "",
  "power_supply_category": "II"
}
```

## Описание полей
| Поле | Описание | Пример |
|---|---|---|
| building_type | Тип здания | "МКД" |
| heat_load_heating_kW | Тепловая нагрузка на отопление, кВт | 850 |
| heat_load_dhw_kW | Тепловая нагрузка на ГВС, кВт | 420 |
| heat_load_ventilation_kW | Тепловая нагрузка на вентиляцию, кВт | 180 |
| heat_load_total_kW | Суммарная тепловая нагрузка, кВт | 1450 |
| temp_graph_primary | Температурный график первичного контура | "130/70" |
| temp_graph_secondary_heating | Температурный график вторичного контура отопления | "95/70" |
| temp_graph_secondary_dhw | Температура ГВС / ХВС на входе | "60/5" |
| pressure_primary_MPa | Давление в первичном контуре, МПа | 1.6 |
| pressure_secondary_MPa | Давление во вторичном контуре, МПа | 0.6 |
| connection_type | Тип присоединения к тепловой сети | "независимая" |
| dhw_scheme | Схема приготовления ГВС | "двухступенчатая" |
| heat_exchanger_brand | Марка теплообменника | "Alfa Laval M6-MFG" |
| heat_exchanger_type | Тип теплообменника | "пластинчатый" |
| pump_brand | Марка насосов | "Grundfos / Wilo" |
| pump_heating_flow_m3h | Расход насоса отопления, м3/ч | 25 |
| pump_heating_head_m | Напор насоса отопления, м | 12 |
| pump_dhw_flow_m3h | Расход насоса ГВС, м3/ч | 8 |
| pump_dhw_head_m | Напор насоса ГВС, м | 20 |
| metering_device | Марка теплосчётчика | "Карат-Компакт-2" |
| metering_flowmeter_dn | Диаметр расходомера УУТЭ, мм | 80 |
| heat_source | Источник теплоснабжения | "ТЭЦ / котельная" |
| power_supply_category | Категория электроснабжения ИТП | "II" |
