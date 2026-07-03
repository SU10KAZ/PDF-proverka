# LLM-граф однолинейной схемы ВРУ

> Топология (QF↔линия↔панель) восстановлена геометрией чертежа (PDF стр.7: X-колонки + монотонная привязка по панелям, верифицировано визуально РП1/РП2/РП3); электрические параметры — из валидированной расчётной таблицы (физпроверки P_расч≈P_уст·Kc и I≈P/(√3·U·cosφ) = 100%).

## Источник
- PDF: 13АВ-РД-ЭМ-К1 (document.pdf), **страница PDF 7** (внимание: Chandra page_index=8 — расхождение нумерации, реальная страница 7)
- Лист: ЭМ — Внутреннее электроснабжение. Корпус 1
- Название схемы: Однолинейная расчётная схема ВРУ-К1.1 (начало)
- Источник топологии: get_text('words')+get_drawings стр.7; параметров: вектор-таблица блока 7HYD

## Неоднозначности
- Исходящих QF на листе: **68**; активных с полными параметрами: **59**; резерв (свободные ячейки): **5**; спорных (requires_review): **4**. Плюс вводных QF (ВА-305, нижний ряд): 2.
- Повтор маркировки QF: QF2.3 — одна метка на разных ячейках (сохранены обе).
- Маркировка ЩМкв повторяется (квартирные щиты) — разные потребители с одинаковой подписью.
- requires_review (колонка без кода): QF1.3, QF2.3, QF2.3, QF3.44.
- РП5 упоминается в подписях, но отдельных QF5.x на этом листе нет (лист-продолжение / потребитель).

## Граф питания

### Edge
ID: E-INPUT
From: Внешняя сеть (ПАО «Мосэнергосбыт»)
Via: ВП1 (ВА-305 320А 35кА / ВР-101-250) + ВП2 (ВР-101-630)
To: ВРУ-К1.1.Шины
Type: input
Status: active

### Edge
ID: E-AVR
From: ВП1 / ВП2
Via: АВР-301 (АВР1, 3P, Iн=40А) + QS1/QS2 (ВР-101-63 3P 63А)
To: РП4 (АВР).Шины
Type: avr_transfer
Status: active

### Edge
ID: E-VRU-РП1
From: ВРУ-К1.1.Шины
Via: секционная связь
To: РП1.Шины
Type: bus_feed
Status: active

### Edge
ID: E-VRU-РП2
From: ВРУ-К1.1.Шины
Via: секционная связь
To: РП2.Шины
Type: bus_feed
Status: active

### Edge
ID: E-VRU-РП3
From: ВРУ-К1.1.Шины
Via: секционная связь
To: РП3.Шины
Type: bus_feed
Status: active

### Edge
ID: E-VRU-РП4
From: ВРУ-К1.1.Шины
Via: секционная связь
To: РП4 (АВР).Шины
Type: bus_feed
Status: active

### Edge
ID: E-QF1.1
From: РП1.Шины
Via: QF1.1 ВА-333А
To: Стояк квартир - 36 квартир, 1стояк - 2-12эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF1.2
From: РП1.Шины
Via: QF1.2 ВА-333А
To: Стояк квартир - 40 квартир, 2 стояк - 2-12эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF1.3
From: РП1.Шины
Via: QF1.3 ВА-333А
To: не указано на листе
Type: outgoing_line
Status: ambiguous

### Edge
ID: E-QF2.1
From: РП2.Шины
Via: QF2.1 ВА-333А
To: Стояк квартир - 40 квартир, 1 стояк - 13-20эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF2.2
From: РП2.Шины
Via: QF2.2 ВА-333А
To: Стояк квартир - 27 квартир, 2стояк -13-20эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF2.3
From: РП2.Шины
Via: QF2.3 ВА-333А
To: не указано на листе
Type: outgoing_line
Status: ambiguous

### Edge
ID: E-QF2.3
From: РП2.Шины
Via: QF2.3 ВА-333А
To: не указано на листе
Type: outgoing_line
Status: ambiguous

### Edge
ID: E-QF3.1
From: РП3.Шины
Via: QF3.1 ВА-300
To: Освещение помещения электрощитовой жилья
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.2
From: РП3.Шины
Via: QF3.2 ВА-300
To: Освещение технич. помещений -1 этажа
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.3
From: РП3.Шины
Via: QF3.3 ВА-300
To: Освещение помещений ревизии -1-20 этажи (+антресоль)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.4
From: РП3.Шины
Via: QF3.4 ВА-300
To: Освещение МОП 1 эт. (п.1.МОП.2-1.МОП.5,1.МОП.12,1.МОП.13)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.5
From: РП3.Шины
Via: QF3.5 ВА-300
To: Освещение МОП 1 эт. (LED-лента, настен.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.6
From: РП3.Шины
Via: QF3.6 ВА-300
To: Освещение МОП 1 эт. (п.1.МОП.6,1.МОП.7,1.МОП.10,1.МОП.11)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.7
From: РП3.Шины
Via: QF3.7 ВА-332А
To: Щ0-Лобби
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.8
From: РП3.Шины
Via: QF3.8 ВА-300
To: Освещение ЛК1.3 -1 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.9
From: РП3.Шины
Via: QF3.9 ВА-300
To: Освещение ЛК1.1 1-12 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.10
From: РП3.Шины
Via: QF3.10 ВА-300
To: Розетки для уборочной техники 2-12 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.11
From: РП3.Шины
Via: QF3.11 ВА-300
To: Освещение ЛК1.2 1-12 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.12
From: РП3.Шины
Via: QF3.12 ВА-300
To: Освещение ЛК1.1 13-20 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.13
From: РП3.Шины
Via: QF3.13 ВА-300
To: Освещение ЛК1.2 13-20 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.14
From: РП3.Шины
Via: QF3.14 ВА-300
To: Освещение межкварт. коридора 2-12 эт. (потол.) (левый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.15
From: РП3.Шины
Via: QF3.15 ВА-300
To: Освещение межкварт. коридора 2-12 эт. (настен.) (левый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.16
From: РП3.Шины
Via: QF3.16 ВА-300
To: Освещение межкварт. коридора 2-12 эт. (потол.) (правый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.17
From: РП3.Шины
Via: QF3.17 ВА-300
To: Освещение межкварт. коридора 2-12 эт. (настен.) (правый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.18
From: РП3.Шины
Via: QF3.18 ВА-300
To: Освещение межкварт. коридора 13-20 эт. (потол.) (левый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.19
From: РП3.Шины
Via: QF3.19 ВА-300
To: Освещение межкварт. коридора 13-20 эт. (настен.) (левый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.20
From: РП3.Шины
Via: QF3.20 ВА-300
To: Освещение межкварт. коридора 13-20 эт. (потол.) (правый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.21
From: РП3.Шины
Via: QF3.21 ВА-300
To: Освещение межкварт. коридора 13-20 эт. (настен.) (правый)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.22
From: РП3.Шины
Via: QF3.22 ВА-300
To: Освещение лифтового холла -1, 2-12 эт. (потол.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.23
From: РП3.Шины
Via: QF3.23 ВА-300
To: Освещение лифтового холла -1, 2-12 эт. (настен.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.24
From: РП3.Шины
Via: QF3.24 ВА-300
To: Освещение лифтового холла 13-20 эт. (потол.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.25
From: РП3.Шины
Via: QF3.25 ВА-300
To: Освещение лифтового холла 13-20 эт. (настен.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.26
From: РП3.Шины
Via: QF3.26 ВА-300
To: Освещение тамбур-шлюза (LED-лента) 2-12 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.27
From: РП3.Шины
Via: QF3.27 ВА-332А
To: Освещение тамбур-шлюза (LED-лента) 13-20 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.28
From: РП3.Шины
Via: QF3.28 ВА-332А
To: Тепловая завеса У1.1 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.29
From: РП3.Шины
Via: QF3.29 ВА-332А
To: Тепловая завеса У1.2 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.30
From: РП3.Шины
Via: QF3.30 ВА-332А
To: Тепловая завеса У1.3 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.31
From: РП3.Шины
Via: QF3.31 ВА-332А
To: Тепловая завеса У1.4 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.32
From: РП3.Шины
Via: QF3.32 ВА-332А
To: Тепловая завеса У1.5 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.33
From: РП3.Шины
Via: QF3.33 ВА-332А
To: Тепловая завеса У1.6 (1 эт, гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.34
From: РП3.Шины
Via: QF3.34 ВА-332А
To: Тепловая завеса 2.1 (1 эт, вестибюль)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.35
From: РП3.Шины
Via: QF3.35 ВА-300
To: Тепловая завеса 2.2 (1 эт, вестибюль)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.36
From: РП3.Шины
Via: QF3.36 ВА-300
To: Внутрипольные конвекторы (гранд-лобби)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.37
From: РП3.Шины
Via: QF3.37 ВА-300
To: Розетка для электроинструмента в электрощитовой
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.38
From: РП3.Шины
Via: QF3.38 ВА-300
To: Розетка для конвектора в электрощитовой
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.40
From: РП3.Шины
Via: QF3.40 ВА-300
To: Розетки для уборочной техники 13-20 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.41
From: РП3.Шины
Via: QF3.41 ВА-332А
To: ЩАУВ6-К1 (12 эт.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.42
From: РП3.Шины
Via: QF3.42 ВА-332А
To: ЩАУВ7-К1 (1 эт., ввод 2 кат.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.43
From: РП3.Шины
Via: QF3.43 ВА-332А
To: ЩАУВ8-К1 (1 эт., ввод 2 кат.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF3.44
From: РП3.Шины
Via: QF3.44 ВА-332А
To: не указано на листе
Type: outgoing_line
Status: ambiguous

### Edge
ID: E-QF4.1
From: РП4 (АВР).Шины
Via: QF4.1 ВА-300
To: ЩАУВ7-К1 (1 эт., ввод 1 кат.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.2
From: РП4 (АВР).Шины
Via: QF4.2 ВА-300
To: ЩАУВ8-К1 (1 эт., ввод 1 кат.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.3
From: РП4 (АВР).Шины
Via: QF4.3 ВА-300
To: ЩД-АСУД.И.1.1 (-1 эт.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.4
From: РП4 (АВР).Шины
Via: QF4.4 ВА-332А
To: Блок управления дренажным насосм Д1
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.5
From: РП4 (АВР).Шины
Via: QF4.5 ВА-332А
To: Блок управления дренажным насосм Д2
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.6
From: РП4 (АВР).Шины
Via: QF4.6 ВА-332А
To: Блок управления дренажным насосм Д3
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.7
From: РП4 (АВР).Шины
Via: QF4.7 ВА-332А
To: Блок управления дренажным насосм Д4
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.8
From: РП4 (АВР).Шины
Via: QF4.8 ВА-300
To: ЩД-АСКУВТ1.1 (-1 эт.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.9
From: РП4 (АВР).Шины
Via: QF4.9 ВА-300
To: ОСПД1.4 (СОТ), 13 эт.
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.10
From: РП4 (АВР).Шины
Via: QF4.10 ВА-300
To: STR1.8, STR1.19 (СКУД, 13 эт.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.11
From: РП4 (АВР).Шины
Via: QF4.11 ВА-300
To: STR1.17, STR1.2..1.7 (СКУД, 1 эт.)
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.12
From: РП4 (АВР).Шины
Via: QF4.12 ВА-300
To: Кондиционер помещения СС (-1 эт.) раб.
Type: outgoing_line
Status: active

### Edge
ID: E-QF4.13
From: РП4 (АВР).Шины
Via: QF4.13 ВА-300
To: Кондиционер помещения СС (-1 эт.) рез.
Type: outgoing_line
Status: active

## Панели

### Node
ID: PANEL_VRU-K1.1
Type: input_panel
Name: ВРУ-К1.1 (вводное устройство)
Fed_from:
- Внешняя сеть через ВП1 (ВА-305 320А 35кА)
- Внешняя сеть через ВП2 (ВР-101-630)
Parameters:
  АВР: АВР-301 3P, Iн=40А; QS1/QS2 ВР-101-63 3P 63А
  Icn_автоматов: ВА-300=15кА, ВА-332А=35кА, ВА-333А=35кА (прим.11)
  Условие: Iкз(3) < Icn

### Node
ID: PANEL_РП1
Type: panel
Name: РП1
Fed_from:
- ВРУ-К1.1.Шины
Parameters:
  Iкз3=13.20кА; Iу=18.66кА; Iкз1=7.84кА; Ру=995.00кВт; Кс=0.172
  feeder_count: 3 (active 2, reserve 0)

### Node
ID: PANEL_РП2
Type: panel
Name: РП2
Fed_from:
- ВРУ-К1.1.Шины
Parameters:
  Iкз3=13.20кА; Iу=18.66кА; Iкз1=7.84кА; Ру=876.00кВт; Кс=0.177
  feeder_count: 4 (active 2, reserve 0)

### Node
ID: PANEL_РП3
Type: panel
Name: РП3
Fed_from:
- ВРУ-К1.1.Шины
Parameters:
  ОДН; Ру=156.81кВт; Кс=0.17
  feeder_count: 43 (active 42, reserve 0)

### Node
ID: PANEL_РП4
Type: panel
Name: РП4 (АВР)
Fed_from:
- ВП1/ВП2 через АВР1
Parameters:
  Ру=18.28кВт (рабочий/пожарный режим); питание через АВР1
  feeder_count: 18 (active 13, reserve 5)

## Отходящие линии

### Line
ID: QF1.1
Panel: РП1
From: РП1.Шины
To: Стояк квартир - 36 квартир, 1стояк - 2-12эт.
Consumer: Стояк квартир - 36 квартир, 1стояк - 2-12эт.
Location: 2-12эт
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 200А
Load:
  LineCode: К1.1.1С1
  P_inst: 475.0 кВт
  Ks: 0.21
  CosPhi: 0.93
  P_calc: 99.75 кВт
  Ip: 162.52 А
Cable:
  Type: ППГнг(A)-HF 4х(1х70)+(1х50)
  Length: 90.0 м
  VoltageDrop: 1.85 %
  Ikz1: 3.128 кА
Route: Лоток-90м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF1.2
Panel: РП1
From: РП1.Шины
To: Стояк квартир - 40 квартир, 2 стояк - 2-12эт.
Consumer: Стояк квартир - 40 квартир, 2 стояк - 2-12эт.
Location: 2-12эт
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 200А
Load:
  LineCode: К1.1.2С1
  P_inst: 520.0 кВт
  Ks: 0.2
  CosPhi: 0.93
  P_calc: 104.0 кВт
  Ip: 169.44 А
Cable:
  Type: ППГнг(A)-HF 4х(1х70)+(1х50)
  Length: 90.0 м
  VoltageDrop: 1.93 %
  Ikz1: 3.128 кА
Route: Лоток-90м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF1.3
Panel: РП1
From: РП1.Шины
To: не указано на листе
Consumer: не указано на листе
Location: не указано на листе
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 200А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: ambiguous
Review: активная колонка без сопоставленного кода — requires_review (визуальная сверка)

### Line
ID: QF2.1
Panel: РП2
From: РП2.Шины
To: Стояк квартир - 40 квартир, 1 стояк - 13-20эт.
Consumer: Стояк квартир - 40 квартир, 1 стояк - 13-20эт.
Location: 13-20эт
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 200А
Load:
  LineCode: К1.1.1С2
  P_inst: 520.0 кВт
  Ks: 0.2
  CosPhi: 0.93
  P_calc: 104.0 кВт
  Ip: 169.44 А
Cable:
  Type: ППГнг(A)-HF 4х(1х70)+(1х50)
  Length: 110.0 м
  VoltageDrop: 2.36 %
  Ikz1: 2.757 кА
Route: Лоток-110м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF2.2
Panel: РП2
From: РП2.Шины
To: Стояк квартир - 27 квартир, 2стояк -13-20эт.
Consumer: Стояк квартир - 27 квартир, 2стояк -13-20эт.
Location: -13-20эт
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 160А
Load:
  LineCode: К1.1.2С2
  P_inst: 356.0 кВт
  Ks: 0.233
  CosPhi: 0.93
  P_calc: 82.948 кВт
  Ip: 135.14 А
Cable:
  Type: ППГнг(A)-HF 4х(1х70)+(1х50)
  Length: 110.0 м
  VoltageDrop: 1.88 %
  Ikz1: 2.757 кА
Route: Лоток-110м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF2.3
Panel: РП2
From: РП2.Шины
To: не указано на листе
Consumer: не указано на листе
Location: не указано на листе
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 125А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: ambiguous
Review: активная колонка без сопоставленного кода — requires_review (визуальная сверка); метка QF2.3 повторяется на листе (2×) — разные ячейки

### Line
ID: QF2.3
Panel: РП2
From: РП2.Шины
To: не указано на листе
Consumer: не указано на листе
Location: не указано на листе
Breaker:
  Type: ВА-333А
  Poles: 3P
  Icn: 35кА
  In: 125А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: ambiguous
Review: активная колонка без сопоставленного кода — requires_review (визуальная сверка); метка QF2.3 повторяется на листе (2×) — разные ячейки

### Line
ID: QF3.1
Panel: РП3
From: РП3.Шины
To: Освещение помещения электрощитовой жилья
Consumer: Освещение помещения электрощитовой жилья
Location: электрощитов
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.1-1
  P_inst: 0.25 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.25 кВт
  Ip: 1.2 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 40.0 м
  VoltageDrop: 0.1 %
  Ikz1: 1.041 кА
Route: Пг.20-40м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.2
Panel: РП3
From: РП3.Шины
To: Освещение технич. помещений -1 этажа
Consumer: Освещение технич. помещений -1 этажа
Location: -1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.1-2
  P_inst: 0.85 кВт
  Ks: 0.6
  CosPhi: 0.95
  P_calc: 0.51 кВт
  Ip: 2.45 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 195.0 м
  VoltageDrop: 0.77 %
  Ikz1: 0.288 кА
Route: Лоток-35м; Пг.20-160м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.3
Panel: РП3
From: РП3.Шины
To: Освещение помещений ревизии -1-20 этажи (+антресоль)
Consumer: Освещение помещений ревизии -1-20 этажи (+антресоль)
Location: -1-20 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.2
  P_inst: 0.125 кВт
  Ks: 0.6
  CosPhi: 0.95
  P_calc: 0.075 кВт
  Ip: 0.36 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 115.0 м
  VoltageDrop: 0.15 %
  Ikz1: 0.224 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.4
Panel: РП3
From: РП3.Шины
To: Освещение МОП 1 эт. (п.1.МОП.2-1.МОП.5,1.МОП.12,1.МОП.13)
Consumer: Освещение МОП 1 эт. (п.1.МОП.2-1.МОП.5,1.МОП.12,1.МОП.13)
Location: МОП
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.3-1
  P_inst: 0.38 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.38 кВт
  Ip: 1.82 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 145.0 м
  VoltageDrop: 0.77 %
  Ikz1: 0.218 кА
Route: Лоток-35м; Каб.несущие констр.-10м; Пг.20-100м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.5
Panel: РП3
From: РП3.Шины
To: Освещение МОП 1 эт. (LED-лента, настен.)
Consumer: Освещение МОП 1 эт. (LED-лента, настен.)
Location: МОП
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.3-2
  P_inst: 0.52 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.52 кВт
  Ip: 2.49 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 125.0 м
  VoltageDrop: 1.05 %
  Ikz1: 0.218 кА
Route: Лоток-35м; Каб.несущие констр.-10м; Пг.20-80м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.6
Panel: РП3
From: РП3.Шины
To: Освещение МОП 1 эт. (п.1.МОП.6,1.МОП.7,1.МОП.10,1.МОП.11)
Consumer: Освещение МОП 1 эт. (п.1.МОП.6,1.МОП.7,1.МОП.10,1.МОП.11)
Location: МОП
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.4
  P_inst: 0.12 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.12 кВт
  Ip: 0.58 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 125.0 м
  VoltageDrop: 0.25 %
  Ikz1: 0.218 кА
Route: Лоток-35м; Каб.несущие констр.-10м; Пг.20-80м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.7
Panel: РП3
From: РП3.Шины
To: Щ0-Лобби
Consumer: Щ0-Лобби
Location: Лобби
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 20А
Load:
  LineCode: К1.1.5
  P_inst: 1.49 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 1.49 кВт
  Ip: 2.38 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 60.0 м
  VoltageDrop: 0.63 %
  Ikz1: 0.345 кА
Route: Пг.25-60м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.8
Panel: РП3
From: РП3.Шины
To: Освещение ЛК1.3 -1 эт.
Consumer: Освещение ЛК1.3 -1 эт.
Location: ЛК1
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.6-1
  P_inst: 0.04 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.04 кВт
  Ip: 0.2 А
Cable:
  Type: ППГнг(A)-HF 3х1.5
  Length: 55.0 м
  VoltageDrop: 0.06 %
  Ikz1: 0.343 кА
Route: Лоток-20м; Пг.20-25м; Пг.20-10м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.9
Panel: РП3
From: РП3.Шины
To: Освещение ЛК1.1 1-12 эт.
Consumer: Освещение ЛК1.1 1-12 эт.
Location: ЛК1
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.6-2
  P_inst: 0.12 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.12 кВт
  Ip: 0.58 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 110.0 м
  VoltageDrop: 0.19 %
  Ikz1: 0.278 кА
Route: Лоток-35м; Пг.25-25м; Пг.25-50м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.10
Panel: РП3
From: РП3.Шины
To: Розетки для уборочной техники 2-12 эт.
Consumer: Розетки для уборочной техники 2-12 эт.
Location: 2-12 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.25
  P_inst: 2.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 2.0 кВт
  Ip: 11.37 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 90.0 м
  VoltageDrop: 1.78 %
  Ikz1: 0.479 кА
Route: Пг.25-90м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.11
Panel: РП3
From: РП3.Шины
To: Освещение ЛК1.2 1-12 эт.
Consumer: Освещение ЛК1.2 1-12 эт.
Location: ЛК1
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.6-3
  P_inst: 0.12 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.12 кВт
  Ip: 0.58 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 105.0 м
  VoltageDrop: 0.17 %
  Ikz1: 0.308 кА
Route: Лоток-35м; Каб.несущие констр.-20м; Пг.25-50м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.12
Panel: РП3
From: РП3.Шины
To: Освещение ЛК1.1 13-20 эт.
Consumer: Освещение ЛК1.1 13-20 эт.
Location: ЛК1
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.6-4
  P_inst: 0.08 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.08 кВт
  Ip: 0.39 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 145.0 м
  VoltageDrop: 0.19 %
  Ikz1: 0.194 кА
Route: Лоток-80м; Пг.25-30м; Пг.25-35м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.13
Panel: РП3
From: РП3.Шины
To: Освещение ЛК1.2 13-20 эт.
Consumer: Освещение ЛК1.2 13-20 эт.
Location: ЛК1
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.6-5
  P_inst: 0.08 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.08 кВт
  Ip: 0.39 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 130.0 м
  VoltageDrop: 0.17 %
  Ikz1: 0.216 кА
Route: Лоток-80м; Пг.25-15м; Пг.25-35м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.14
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 2-12 эт. (потол.) (левый)
Consumer: Освещение межкварт. коридора 2-12 эт. (потол.) (левый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.7-1
  P_inst: 0.66 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.66 кВт
  Ip: 3.16 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.89 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.15
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 2-12 эт. (настен.) (левый)
Consumer: Освещение межкварт. коридора 2-12 эт. (настен.) (левый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.7-2
  P_inst: 0.396 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.396 кВт
  Ip: 1.9 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.53 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.16
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 2-12 эт. (потол.) (правый)
Consumer: Освещение межкварт. коридора 2-12 эт. (потол.) (правый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.8-1
  P_inst: 0.66 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.66 кВт
  Ip: 3.16 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.89 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.17
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 2-12 эт. (настен.) (правый)
Consumer: Освещение межкварт. коридора 2-12 эт. (настен.) (правый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.8-2
  P_inst: 0.198 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.198 кВт
  Ip: 0.95 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.27 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.18
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 13-20 эт. (потол.) (левый)
Consumer: Освещение межкварт. коридора 13-20 эт. (потол.) (левый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.9-1
  P_inst: 0.48 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.48 кВт
  Ip: 2.3 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.61 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.19
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 13-20 эт. (настен.) (левый)
Consumer: Освещение межкварт. коридора 13-20 эт. (настен.) (левый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.9-2
  P_inst: 0.288 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.288 кВт
  Ip: 1.38 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.37 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.20
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 13-20 эт. (потол.) (правый)
Consumer: Освещение межкварт. коридора 13-20 эт. (потол.) (правый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.10-1
  P_inst: 0.48 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.48 кВт
  Ip: 2.3 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.61 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.21
Panel: РП3
From: РП3.Шины
To: Освещение межкварт. коридора 13-20 эт. (настен.) (правый)
Consumer: Освещение межкварт. коридора 13-20 эт. (настен.) (правый)
Location: коридор
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.10-2
  P_inst: 0.144 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.144 кВт
  Ip: 0.69 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.19 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.22
Panel: РП3
From: РП3.Шины
To: Освещение лифтового холла -1, 2-12 эт. (потол.)
Consumer: Освещение лифтового холла -1, 2-12 эт. (потол.)
Location: 2-12 эт
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.11-1
  P_inst: 0.98 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.98 кВт
  Ip: 4.69 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 1.31 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.23
Panel: РП3
From: РП3.Шины
To: Освещение лифтового холла -1, 2-12 эт. (настен.)
Consumer: Освещение лифтового холла -1, 2-12 эт. (настен.)
Location: 2-12 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.11-2
  P_inst: 0.6685 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.6685 кВт
  Ip: 3.2 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.9 %
  Ikz1: 0.326 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.24
Panel: РП3
From: РП3.Шины
To: Освещение лифтового холла 13-20 эт. (потол.)
Consumer: Освещение лифтового холла 13-20 эт. (потол.)
Location: 13-20 эт
Breaker:
  Type: ВА-300
  Poles: 2Р
  Icn: 15кА
  In: 10А
Load:
  LineCode: К1.1.12-1
  P_inst: 0.64 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.64 кВт
  Ip: 3.07 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.82 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.25
Panel: РП3
From: РП3.Шины
To: Освещение лифтового холла 13-20 эт. (настен.)
Consumer: Освещение лифтового холла 13-20 эт. (настен.)
Location: 13-20 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.12-2
  P_inst: 0.32 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.32 кВт
  Ip: 1.54 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.41 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.26
Panel: РП3
From: РП3.Шины
To: Освещение тамбур-шлюза (LED-лента) 2-12 эт.
Consumer: Освещение тамбур-шлюза (LED-лента) 2-12 эт.
Location: 2-12 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.13-1
  P_inst: 0.319 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.319 кВт
  Ip: 1.53 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 85.0 м
  VoltageDrop: 0.66 %
  Ikz1: 0.216 кА
Route: Лоток-35м; Каб.несущие констр.-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.27
Panel: РП3
From: РП3.Шины
To: Освещение тамбур-шлюза (LED-лента) 13-20 эт.
Consumer: Освещение тамбур-шлюза (LED-лента) 13-20 эт.
Location: 13-20 эт
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 25А
Load:
  LineCode: К1.1.13-2
  P_inst: 0.261 кВт
  Ks: 1.0
  CosPhi: 0.95
  P_calc: 0.261 кВт
  Ip: 1.25 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 115.0 м
  VoltageDrop: 0.34 %
  Ikz1: 0.338 кА
Route: Лоток-35м; Каб.несущие констр.-80м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.28
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.1 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.1 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 25А
Load:
  LineCode: К1.1.14
  P_inst: 12.22 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 12.22 кВт
  Ip: 18.9 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 1.88 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.29
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.2 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.2 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.15
  P_inst: 12.22 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 12.22 кВт
  Ip: 18.9 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 1.88 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.30
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.3 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.3 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.16
  P_inst: 15.2 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 15.2 кВт
  Ip: 23.51 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 2.34 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.31
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.4 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.4 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.17
  P_inst: 15.2 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 15.2 кВт
  Ip: 23.51 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 2.34 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.32
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.5 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.5 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.18
  P_inst: 15.2 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 15.2 кВт
  Ip: 23.51 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 2.34 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.33
Panel: РП3
From: РП3.Шины
To: Тепловая завеса У1.6 (1 эт, гранд-лобби)
Consumer: Тепловая завеса У1.6 (1 эт, гранд-лобби)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.19
  P_inst: 15.2 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 15.2 кВт
  Ip: 23.51 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 120.0 м
  VoltageDrop: 2.34 %
  Ikz1: 0.773 кА
Route: Пг.32-120м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.34
Panel: РП3
From: РП3.Шины
To: Тепловая завеса 2.1 (1 эт, вестибюль)
Consumer: Тепловая завеса 2.1 (1 эт, вестибюль)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 32А
Load:
  LineCode: К1.1.20
  P_inst: 18.53 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 18.53 кВт
  Ip: 28.65 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 85.0 м
  VoltageDrop: 2.02 %
  Ikz1: 1.047 кА
Route: Пг.32-85м;
Control: ПС, АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.35
Panel: РП3
From: РП3.Шины
To: Тепловая завеса 2.2 (1 эт, вестибюль)
Consumer: Тепловая завеса 2.2 (1 эт, вестибюль)
Location: 1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.21
  P_inst: 18.53 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 18.53 кВт
  Ip: 28.65 А
Cable:
  Type: ППГнг(A)-HF 5х10
  Length: 95.0 м
  VoltageDrop: 2.26 %
  Ikz1: 0.951 кА
Route: Пг.32-95м;
Control: ПС, АСУД
Status: active
Review: номинал 16А < тока 28.65А — проверить

### Line
ID: QF3.36
Panel: РП3
From: РП3.Шины
To: Внутрипольные конвекторы (гранд-лобби)
Consumer: Внутрипольные конвекторы (гранд-лобби)
Location: лобби
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.22
  P_inst: 0.5 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 0.5 кВт
  Ip: 2.32 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 130.0 м
  VoltageDrop: 1.02 %
  Ikz1: 0.216 кА
Route: Пг.25-130м;
Control: ПС
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.37
Panel: РП3
From: РП3.Шины
To: Розетка для электроинструмента в электрощитовой
Consumer: Розетка для электроинструмента в электрощитовой
Location: электрощитов
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.23
  P_inst: 2.0 кВт
  Ks: 0.8
  CosPhi: 0.8
  P_calc: 1.6 кВт
  Ip: 9.1 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 20.0 м
  VoltageDrop: 0.51 %
  Ikz1: 1.221 кА
Route: Пг.25-20м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.38
Panel: РП3
From: РП3.Шины
To: Розетка для конвектора в электрощитовой
Consumer: Розетка для конвектора в электрощитовой
Location: электрощитов
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.24
  P_inst: 0.5 кВт
  Ks: 1.0
  CosPhi: 0.98
  P_calc: 0.5 кВт
  Ip: 2.32 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 20.0 м
  VoltageDrop: 0.16 %
  Ikz1: 1.221 кА
Route: Пг.25-20м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.40
Panel: РП3
From: РП3.Шины
To: Розетки для уборочной техники 13-20 эт.
Consumer: Розетки для уборочной техники 13-20 эт.
Location: 13-20 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.26
  P_inst: 2.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 2.0 кВт
  Ip: 11.37 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 120.0 м
  VoltageDrop: 2.37 %
  Ikz1: 0.365 кА
Route: Пг.25-120м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.41
Panel: РП3
From: РП3.Шины
To: ЩАУВ6-К1 (12 эт.)
Consumer: ЩАУВ6-К1 (12 эт.)
Location: 12 эт
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.27
  P_inst: 0.2 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 0.2 кВт
  Ip: 1.14 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 110.0 м
  VoltageDrop: 0.35 %
  Ikz1: 0.254 кА
Route: Лоток-90м; Пг.25-20м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.42
Panel: РП3
From: РП3.Шины
To: ЩАУВ7-К1 (1 эт., ввод 2 кат.)
Consumer: ЩАУВ7-К1 (1 эт., ввод 2 кат.)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 25А
Load:
  LineCode: К1.1.28
  P_inst: 2.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 2.0 кВт
  Ip: 3.79 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 60.0 м
  VoltageDrop: 0.64 %
  Ikz1: 0.453 кА
Route: Лоток-60м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF3.43
Panel: РП3
From: РП3.Шины
To: ЩАУВ8-К1 (1 эт., ввод 2 кат.)
Consumer: ЩАУВ8-К1 (1 эт., ввод 2 кат.)
Location: 1 эт
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.29
  P_inst: 10.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 10.0 кВт
  Ip: 18.94 А
Cable:
  Type: ППГнг(A)-HF 5х6
  Length: 60.0 м
  VoltageDrop: 1.33 %
  Ikz1: 0.957 кА
Route: Лоток-60м;
Control: не указано на листе
Status: active
Review: номинал 16А < тока 18.94А — проверить

### Line
ID: QF3.44
Panel: РП3
From: РП3.Шины
To: не указано на листе
Consumer: не указано на листе
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 25А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: ambiguous
Review: активная колонка без сопоставленного кода — requires_review (визуальная сверка)

### Line
ID: QF4.1
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: ЩАУВ7-К1 (1 эт., ввод 1 кат.)
Consumer: ЩАУВ7-К1 (1 эт., ввод 1 кат.)
Location: 1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-1
  P_inst: 0.3 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 0.3 кВт
  Ip: 1.71 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 60.0 м
  VoltageDrop: 0.29 %
  Ikz1: 0.453 кА
Route: Лоток-60м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.2
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: ЩАУВ8-К1 (1 эт., ввод 1 кат.)
Consumer: ЩАУВ8-К1 (1 эт., ввод 1 кат.)
Location: 1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-2
  P_inst: 0.5 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 0.5 кВт
  Ip: 2.85 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 60.0 м
  VoltageDrop: 0.48 %
  Ikz1: 0.453 кА
Route: Лоток-60м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.3
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: ЩД-АСУД.И.1.1 (-1 эт.)
Consumer: ЩД-АСУД.И.1.1 (-1 эт.)
Location: -1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-3
  P_inst: 0.5 кВт
  Ks: 1.0
  CosPhi: 0.9
  P_calc: 0.5 кВт
  Ip: 2.53 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 30.0 м
  VoltageDrop: 0.24 %
  Ikz1: 0.858 кА
Route: Лоток-30м;
Control: АСУД
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.4
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Блок управления дренажным насосм Д1
Consumer: Блок управления дренажным насосм Д1
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.3п-4
  P_inst: 1.1 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 1.1 кВт
  Ip: 2.09 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 50.0 м
  VoltageDrop: 0.3 %
  Ikz1: 0.538 кА
Route: Пг.25-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.5
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Блок управления дренажным насосм Д2
Consumer: Блок управления дренажным насосм Д2
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.3п-5
  P_inst: 1.1 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 1.1 кВт
  Ip: 2.09 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 70.0 м
  VoltageDrop: 0.41 %
  Ikz1: 0.392 кА
Route: Пг.25-70м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.6
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Блок управления дренажным насосм Д3
Consumer: Блок управления дренажным насосм Д3
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.3п-6
  P_inst: 1.1 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 1.1 кВт
  Ip: 2.09 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 120.0 м
  VoltageDrop: 0.7 %
  Ikz1: 0.233 кА
Route: Пг.25-120м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.7
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Блок управления дренажным насосм Д4
Consumer: Блок управления дренажным насосм Д4
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 16А
Load:
  LineCode: К1.1.3п-7
  P_inst: 1.1 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 1.1 кВт
  Ip: 2.09 А
Cable:
  Type: ППГнг(A)-HF 5х2.5
  Length: 120.0 м
  VoltageDrop: 0.7 %
  Ikz1: 0.233 кА
Route: Пг.25-120м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.8
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: ЩД-АСКУВТ1.1 (-1 эт.)
Consumer: ЩД-АСКУВТ1.1 (-1 эт.)
Location: -1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-8
  P_inst: 0.5 кВт
  Ks: 1.0
  CosPhi: 0.9
  P_calc: 0.5 кВт
  Ip: 2.53 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 50.0 м
  VoltageDrop: 0.4 %
  Ikz1: 0.538 кА
Route: Лоток-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.9
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: ОСПД1.4 (СОТ), 13 эт.
Consumer: ОСПД1.4 (СОТ), 13 эт.
Location: 13 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 20А
Load:
  LineCode: К1.1.3п-9
  P_inst: 3.0 кВт
  Ks: 1.0
  CosPhi: 0.9
  P_calc: 3.0 кВт
  Ip: 15.16 А
Cable:
  Type: ППГнг(A)-HF 3х6
  Length: 90.0 м
  VoltageDrop: 1.77 %
  Ikz1: 0.665 кА
Route: Лоток-90м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.10
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: STR1.8, STR1.19 (СКУД, 13 эт.)
Consumer: STR1.8, STR1.19 (СКУД, 13 эт.)
Location: 13 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-10
  P_inst: 0.24 кВт
  Ks: 1.0
  CosPhi: 0.9
  P_calc: 0.24 кВт
  Ip: 1.22 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 140.0 м
  VoltageDrop: 0.53 %
  Ikz1: 0.201 кА
Route: Лоток-140м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.11
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: STR1.17, STR1.2..1.7 (СКУД, 1 эт.)
Consumer: STR1.17, STR1.2..1.7 (СКУД, 1 эт.)
Location: 1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: К1.1.3п-11
  P_inst: 0.84 кВт
  Ks: 1.0
  CosPhi: 0.9
  P_calc: 0.84 кВт
  Ip: 4.25 А
Cable:
  Type: ППГнг(A)-HF 3х2.5
  Length: 150.0 м
  VoltageDrop: 1.99 %
  Ikz1: 0.188 кА
Route: Пг.25-100м; Лоток-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.12
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Кондиционер помещения СС (-1 эт.) раб.
Consumer: Кондиционер помещения СС (-1 эт.) раб.
Location: -1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 32А
Load:
  LineCode: К1.1.3п-12
  P_inst: 4.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 4.0 кВт
  Ip: 22.73 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 50.0 м
  VoltageDrop: 1.98 %
  Ikz1: 0.823 кА
Route: Лоток-50м;
Control: не указано на листе
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.13
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Кондиционер помещения СС (-1 эт.) рез.
Consumer: Кондиционер помещения СС (-1 эт.) рез.
Location: -1 эт
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 32А
Load:
  LineCode: К1.1.3п-13
  P_inst: 4.0 кВт
  Ks: 1.0
  CosPhi: 0.8
  P_calc: 4.0 кВт
  Ip: 22.73 А
Cable:
  Type: ППГнг(A)-HF 3х4
  Length: 50.0 м
  VoltageDrop: 1.98 %
  Ikz1: 0.823 кА
Route: Лоток-50м;
Control: ПС
Status: active
Review: данные извлечены уверенно

### Line
ID: QF4.14
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Резерв (свободная ячейка)
Consumer: Резерв (свободная ячейка)
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 16А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: ПС
Status: reserve
Review: данные извлечены уверенно

### Line
ID: QF4.15
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Резерв (свободная ячейка)
Consumer: Резерв (свободная ячейка)
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 3P
  Icn: 35кА
  In: 16А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: reserve
Review: данные извлечены уверенно

### Line
ID: QF4.16
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Резерв (свободная ячейка)
Consumer: Резерв (свободная ячейка)
Location: не указано на листе
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: reserve
Review: данные извлечены уверенно

### Line
ID: QF4.17
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Резерв (свободная ячейка)
Consumer: Резерв (свободная ячейка)
Location: не указано на листе
Breaker:
  Type: ВА-332А
  Poles: 1Р
  Icn: 35кА
  In: 16А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: reserve
Review: данные извлечены уверенно

### Line
ID: QF4.18
Panel: РП4 (АВР)
From: РП4 (АВР).Шины
To: Резерв (свободная ячейка)
Consumer: Резерв (свободная ячейка)
Location: не указано на листе
Breaker:
  Type: ВА-300
  Poles: 1Р
  Icn: 15кА
  In: 16А
Load:
  LineCode: не указано на листе
  P_inst: не указано на листе
  Ks: не указано на листе
  CosPhi: не указано на листе
  P_calc: не указано на листе
  Ip: не указано на листе
Cable:
  Type: не указано на листе
  Length: не указано на листе
  VoltageDrop: не указано на листе
  Ikz1: не указано на листе
Route: не указано на листе
Control: не указано на листе
Status: reserve
Review: данные извлечены уверенно

## Контрольные замечания

### Извлечено уверенно
- 59/68 исходящих линий: QF + автомат + потребитель + расчётные параметры (физика power=59/59, current=59/59 = 100%).
- Автомат (ВА-…/уставка) по геометрии колонки: 68/68.
- Панели определены по префиксу QF: РП1=3, РП2=4, РП3=43, РП4 (АВР)=18.
- Привязка QF↔линия проверена визуально (рендер) на РП1/РП2/РП3 — монотонный X-порядок подтверждён.

### Требует ручной проверки
- Линий с флагами: 6 из 68.
  - QF1.3 (код?): активная колонка без сопоставленного кода — requires_review (визуальная сверка)
  - QF2.3 (код?): активная колонка без сопоставленного кода — requires_review (визуальная сверка); метка QF2.3 повторяется на листе (2×) — разные ячейки
  - QF2.3 (код?): активная колонка без сопоставленного кода — requires_review (визуальная сверка); метка QF2.3 повторяется на листе (2×) — разные ячейки
  - QF3.35 (К1.1.21): номинал 16А < тока 28.65А — проверить
  - QF3.43 (К1.1.29): номинал 16А < тока 18.94А — проверить
  - QF3.44 (код?): активная колонка без сопоставленного кода — requires_review (визуальная сверка)

### Самопроверка покрытия QF
- Исходящих QF: 68 → QF1.1 QF1.2 QF1.3 QF2.1 QF2.2 QF2.3 QF2.3 QF3.1 QF3.2 QF3.3 QF3.4 QF3.5 QF3.6 QF3.7 QF3.8 QF3.9 QF3.10 QF3.11 QF3.12 QF3.13 QF3.14 QF3.15 QF3.16 QF3.17 QF3.18 QF3.19 QF3.20 QF3.21 QF3.22 QF3.23 QF3.24 QF3.25 QF3.26 QF3.27 QF3.28 QF3.29 QF3.30 QF3.31 QF3.32 QF3.33 QF3.34 QF3.35 QF3.36 QF3.37 QF3.38 QF3.40 QF3.41 QF3.42 QF3.43 QF3.44 QF4.1 QF4.2 QF4.3 QF4.4 QF4.5 QF4.6 QF4.7 QF4.8 QF4.9 QF4.10 QF4.11 QF4.12 QF4.13 QF4.14 QF4.15 QF4.16 QF4.17 QF4.18
- Активных (с параметрами): 59 | резерв: 5 (QF4.14 QF4.15 QF4.16 QF4.17 QF4.18) | спорных: 4 (QF1.3 QF2.3 QF2.3 QF3.44)
- Вводных QF (ВА-305, нижний ряд): 2
- Коды таблицы без привязки к QF: 0 → —
- Линий без потребителя/автомата (кроме резерва): 4