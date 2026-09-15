# TWO_PAIR_F1_F4_F2_REGRESSION

STATUS: **COMPLETE_LOCAL_REGRESSION**

BRANCH: `research/projectchange-f1-f4-f2` — отдельный clean worktree.

| Этап | IMPLEMENTED | REGRESSION / TESTS |
| --- | --- | --- |
| F1 | YES | PASS, обе DEV-пары |
| F4 | YES | PASS |
| F2 | YES | PASS, обе DEV-пары |

**117 локальных тестов: PASS. 29 source-audit cases. MODEL CALLS: 0.**

Полное покрытие объявленных требований есть у **16 из 29** пакетов. У остальных
13 сохранены честные PARTIAL / TRUNCATED / MISSING. PASS означает отсутствие
ложного COMPLETE, а не полноту recall и не результат нового inference.

Статусы требований: `{'COMPLETE': 220, 'TRUNCATED': 34, 'PARTIAL': 14, 'MISSING': 5}`. Геометрия и хеши 86 фактически передаваемых растров проверены; расхождений нет. Максимум 54962 символов вместе с инструкцией, максимум 8 изображений. Байты запросов подготовлены только локально, ничего не отправлено.

## Cases checked

PAIR 5: C01, C05, C07, C08, C15, C16, C17, C18, C19, C20, C22.

PAIR 7: R01, R02, R03, R05, R08, R09, R10, R11, R12, R13, R16, R18, R19, R21, R23, R25, Q04, S_FP01.

F1: все 19 обязательных случаев. F2: все 12 обязательных случаев, дополнительные
случаи и четыре отрицательных варианта. Для R11 проверено как подтверждённое ядро
переноса вытяжки паркинга, так и недопустимое сопоставление с жилой вытяжкой.
R19 типизирован как критерий жилья 60→100 W/m²; общие суммы и МОП не подменяют его.
R25 сохраняет признак обнаружения после outputs и ограниченную область требования.

## Known regressions / limits

KNOWN REGRESSIONS: не обнаружены в локальных проверках.

NEW ROOT CAUSES: новых архитектурных причин по источникам не заявлено.

- Retrieval проверен по явной карте требований из source-first аудитов. Автономное определение семантической области не доказано.
- В C22 связанные анкеты вне двух выбранных пар остаются MISSING. Исторические страницы, исключённые DEV policy, не открывались.
- GRAPHIC witnesses нового контракта требуют visual_locator, bbox и binding_reason. Исторические результаты и исходные witnesses не переписывались. Прохождение F4 не устанавливает истинность engineering event.
- COMPLETE относится к доставке указанной аудированной области. Таблицы, примечания и схемы подтверждаются полным растром; OCR сам по себе не даёт сертификат полноты.
- Новые контракты подключены к формату semantic packet и model view. F2 возвращает диагностику отдельного claim; admission/grouping semantics и F3 не перестроены.

VALIDATION OPENED: **NO**. FINAL HOLDOUT OPENED: **NO**.

OTHER PROJECTS USED: **NO**. PRODUCTION CHANGED: **NO**.

PUSH: **NO**. DEPLOY: **NO**. SOURCE-FIRST TRUTH FILES CHANGED: **NO**.

NEXT ACTION: остановиться. Небольшой controlled inference на двух DEV-парах
возможен только после отдельного решения пользователя. Автоматического запуска нет.

## Detailed matrix

Each row links a complete machine-readable requirement receipt. COMPLETE means delivery of an audited region, not engineering truth.

| Pair / case | Source truth | Before | After | OLD / NEW / counter / table-section / graphic |
| --- | --- | --- | --- | --- |
| 5 / C01 | REAL CHANGE | Неуверенность в принадлежности Т4.1/Т3.2 распространена на независимый факт двух зон. Неизвестный состав потребителей важен для сравнения расходов, но не отменяет явно изменённую топологию. В V2-пакете нет OLD 13/19 и NEW 22. Это не пропуск зоны V1; это потеря подтверждённого ядра при V2 admission. | [{'COMPLETE': 8, 'TRUNCATED': 1}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C01/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / NOT_APPLICABLE / PARTIAL_OR_MISSING / COMPLETE |
| 5 / C05 | NOT A CHANGE | Шапка общей таблицы перенесена на частный пожарный компонент. Локальное изменение существует в C04, дополнительного общедомового события этот материал не доказывает. | [{'COMPLETE': 6}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C05/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / COMPLETE |
| 5 / C07 | NOT A CHANGE | 0,89+0,15=1,04; 0,46+0,09=0,55; 0,43+0,06=0,49. Совпадение названий строк не доказывает одинаковый состав. Арифметика объясняет новый итог прежними потребителями; явного авторского пояснения перегруппировки нет, поэтому не изобретена новая организационная принадлежность персонала. | [{'COMPLETE': 7}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C07/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / NOT_APPLICABLE |
| 5 / C08 | REVIEW | Максимумы не складываются как суточный баланс: нельзя доказать их равенство суммой двух старых пиков. Режим и состав сравнения различаются; самостоятельный материальный результат не установлен. | [{'COMPLETE': 5}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C08/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 5 / C15 | REAL CHANGE | OLD 13 значится в пакетном покрытии, но переданы только ранние native-блоки со штампом/автоматикой, а нижний абзац объединённой ГВС не передан. Это потеря инженерного контекста до сравнения. | [{'COMPLETE': 9, 'PARTIAL': 1, 'TRUNCATED': 2}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C15/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / COMPLETE / PARTIAL_OR_MISSING |
| 5 / C16 | REAL CHANGE | Сопоставимые старые основные схемы не вошли в primary-пакеты. Нельзя заменять это утверждением «добавлены коллекторы»: они уже упомянуты OLD. | [{'COMPLETE': 12, 'TRUNCATED': 1}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C16/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / COMPLETE / PARTIAL_OR_MISSING |
| 5 / C17 | REAL CHANGE | Для сопоставления требуется преобразование одной системы в несколько ветвей. Отказ от неверной замены один-к-одному обоснован, но pipeline не восстанавливает родительскую переразбивку. | [{'COMPLETE': 8, 'PARTIAL': 2}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C17/EVIDENCE_COVERAGE.json) | PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / NOT_APPLICABLE / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING |
| 5 / C18 | REAL CHANGE | Положительная старая конфигурация и явный отрицательный пункт не соединены с новой функциональной сетью. NEW 21 содержит сохранённую фразу об отсутствии мер качества: это внутреннее противоречие текста; новая сеть независимо подтверждена основными схемами. Опциональное дозирование BWT не объявлено принятым. | [{'COMPLETE': 14, 'PARTIAL': 3, 'TRUNCATED': 10}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C18/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING |
| 5 / C19 | REAL CHANGE | Потеря сравнения полных старых/новых схем. Растр подтверждает именно «автополив», не «автостоянку». Утверждение ограничено двумя ветвями на схемах, не всем учётом объекта. | [{'COMPLETE': 13}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C19/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / COMPLETE |
| 5 / C20 | REAL CHANGE | В primary-пакетах отсутствовала сопоставимая OLD основная схема узла 21. Совпадающая функция ввода позволяет сравнить перепроектирование; изменение осей само по себе не доказывает другой независимый объект. | [{'COMPLETE': 7}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C20/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / COMPLETE |
| 5 / C22 | NOT A CHANGE | Выбрана единственная удобная OLD-анкета, проигнорировано same-version OLD контрсвидетельство 36. Доказано исправление/расхождение записи; физическое повышение гарантии сети не доказано. Старое противоречие не устраняется голосованием документов. | [{'COMPLETE': 8, 'MISSING': 2}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair5_C22/EVIDENCE_COVERAGE.json) | PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / COMPLETE / PARTIAL_OR_MISSING / NOT_APPLICABLE |
| 7 / R01 | REAL | OLD 21 + NEW 26 полными растрами в primary и closure: 2/4 трубы и межсезонное отопление доказаны. В V2 добавлен полный NEW 25. OLD 22 в closure — штамп; OLD 62 не дан, поэтому весь демонтаж сезонного переключения не прослежен. | [{'COMPLETE': 8, 'TRUNCATED': 6, 'PARTIAL': 1}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R01/EVIDENCE_COVERAGE.json) | PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / NOT_APPLICABLE / COMPLETE / PARTIAL_OR_MISSING |
| 7 / R02 | REAL | Целевой coldloop: OLD 8 об отоплении + NEW 41 схема отопления. NEW 46 действительно есть полным растром в cold46, но OLD 3 — оглавление. OLD 61 в sumclose содержит список коллектора/счётчиков без схемы, NEW 27 — штамп. | [{'MISSING': 1, 'COMPLETE': 10}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R02/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / PARTIAL_OR_MISSING / COMPLETE |
| 7 / R03 | REAL | meter: OLD 6 заканчивается незавершённым перечнем потребителей, OLD 7 отсутствует в этом пакете; NEW 42 ссылается на узлы 7/8, но узел технического учёта 7 на NEW 41 не вложен. В другом heat41 OLD — титулы. | [{'TRUNCATED': 11, 'COMPLETE': 16}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R03/EVIDENCE_COVERAGE.json) | PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING |
| 7 / R05 | REAL | meter содержит полный OLD 6 с 95/70 для паркинга и вентиляции и полный NEW 42, где смесительный узел тепловых завес подписан Т1=90/Т2=65. Это достаточное совместное свидетельство хотя бы компонента ВТЗ; NEW 11 для всей области не дан. | [{'COMPLETE': 8}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R05/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / COMPLETE |
| 7 / R08 | REAL | NEW 48 содержит 8 водяных завес: 4 растровых тайла, строки и характеристики действительно присутствуют. OLD 43 с 2 завесами отсутствует; в tab48 OLD 1/2. Целевой curtain вообще сравнивает OLD 9 с NEW 12. | [{'MISSING': 1, 'COMPLETE': 8}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R08/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / PARTIAL_OR_MISSING / COMPLETE |
| 7 / R09 | REAL | NEW 48 строки 13+5 входных завес присутствуют растром; OLD 43 с 9 завесами отсутствует, вместо него титулы. | [{'COMPLETE': 3}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R09/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 7 / R10 | REAL | NEW 48 строки А1–А6 / А7–А12 и NEW 42 доступны. OLD 43 с 19 агрегатами, 1400 м³/ч, 14 кВт отсутствует. | [{'COMPLETE': 5}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R10/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / COMPLETE |
| 7 / R11 | REAL | Целевой roof содержит OLD 9 и NEW 19; нужные OLD 10/58 и NEW 22/44 туда не собраны. OLD 58 отдельно встречается с NEW 24 в garbage/kns, NEW 44 — с OLD 20. | [{'MISSING': 1, 'COMPLETE': 5}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R11/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / NOT_APPLICABLE / PARTIAL_OR_MISSING / COMPLETE |
| 7 / R12 | REAL | NEW 23 полный, включая постоянные 30%. OLD 10 формально есть в ductclose, но последняя цитата обрывается на «работают в», до старого режима; растра OLD 10 нет. | [{'COMPLETE': 2}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R12/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 7 / R13 | REAL | ss: NEW 23 заканчивается заголовком СС; содержание на NEW 24 отсутствует в целевом пакете. vent44: полный NEW 44 + OLD 20, но нет OLD 37/39/52/54/57 и полной привязки групп. | [{'PARTIAL': 6, 'TRUNCATED': 3, 'COMPLETE': 14}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R13/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / COMPLETE / PARTIAL_OR_MISSING / PARTIAL_OR_MISSING |
| 7 / R16 | REAL | OLD 6 и 9 содержат старые нормы. NEW 20 есть лишь штампом в ductclose, без условной нормы 3/30 и санузла 50. | [{'COMPLETE': 3}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R16/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 7 / R18 | REAL | NEW 21 с разрешением фасадного выброса представлен только штампом. NEW 44 растр есть, OLD 53 коммерческой схемы в V1 нет; OLD 20 содержит лишь общее описание кровельного выброса. | [{'COMPLETE': 8}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R18/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / COMPLETE |
| 7 / R19 | REAL | V1 closure: OLD 21 полный со строкой 60; NEW 25 имеет явную цитату 100, не только штамп. МОП 60 не передан. V2 criterion получил OLD 21 и полные NEW 25/26 с текстом/растром. | [{'COMPLETE': 4}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R19/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / COMPLETE |
| 7 / R21 | REAL | NEW 49 подбор присутствует растром, но с OLD 3. OLD 42 старого подбора отсутствует; OLD 53 контрсвидетельство размещения в V1 тоже отсутствует. | [{'COMPLETE': 7}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R21/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / COMPLETE |
| 7 / R23 | REAL | NEW 48/49 полностью покрыты четырьмя тайлами каждого листа. OLD 37 в closure — только штамп; OLD 37–42 таблицы не даны; первичные OLD — титулы/оглавление. | [{'COMPLETE': 8, 'PARTIAL': 1}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R23/EVIDENCE_COVERAGE.json) | COMPLETE / PARTIAL_OR_MISSING / NOT_APPLICABLE / PARTIAL_OR_MISSING / COMPLETE |
| 7 / Q04 | REVIEW | Обе суммы переданы. Closure получил строку жилья 100, но не строку МОП 60; старый состав и новый состав не сведены по категориям. Теплосвод OLD 23 / NEW 28 не дан. | [{'COMPLETE': 5}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_Q04/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 7 / R25 | REAL | OLD 9 и NEW 19 полными растрами дают ядро. В ductclose NEW 21 только штамп, NEW 22 отсутствует; строка ≥0,8 с NEW 22 встречается лишь в чужом dispatchclose. V2 имеет только OLD 9/NEW 19. | [{'COMPLETE': 5}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_R25/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / NOT_APPLICABLE / COMPLETE / NOT_APPLICABLE |
| 7 / S_FP01 | FALSE_AS_UNQUALIFIED_CHANGE / OLD_SOURCE_CONFLICT | V1 OLD 20/NEW 23 полны; OLD 53 ни в primary, ни в closure нет. V2 пакет уже содержит OLD 53 полным растром, плюс OLD 20/NEW 23. Timeout receipt подтверждает 3 изображения, output_bytes=0. | [{'COMPLETE': 4}](/home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/final_candidate/packages/pair7_S_FP01/EVIDENCE_COVERAGE.json) | COMPLETE / COMPLETE / COMPLETE / COMPLETE / COMPLETE |

## Typed state / comparability / materiality

F4: **PASS** (6 local raster tests). F2 uses source-audit inputs; no new inference was run.

| Pair / case | Role and scope | Mapping | Applicable conditions | Comparability | Materiality | Expected semantic disposition |
| --- | --- | --- | --- | --- | --- | --- |
| 5 / C01 | StateRole.TOPOLOGY: Water supply parent network | 1→N | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL CHANGE |
| 5 / C05 | StateRole.CAPACITY: Residential local fire-water | 1→1 | engineering_subject, scope, state_role, consumer_composition, local_or_global, component_or_total, operating_mode, stage_phase, physical_quantity, unit, functional_role, branch_identity | INCOMPARABLE | REVIEW: Comparable typed states required | NOT A CHANGE |
| 5 / C07 | StateRole.CALCULATED_RESULT: Nonresidential daily demand row | 1→1 | engineering_subject, scope, state_role, consumer_composition, local_or_global, component_or_total, operating_mode, stage_phase, physical_quantity, unit, functional_role, branch_identity, calculation_basis | INCOMPARABLE | REVIEW: Comparable typed states required | NOT A CHANGE |
| 5 / C08 | StateRole.CALCULATED_RESULT: Peak nonresidential demand | 1→1 | engineering_subject, scope, state_role, consumer_composition, local_or_global, component_or_total, operating_mode, stage_phase, physical_quantity, unit, functional_role, branch_identity, calculation_basis | INCOMPARABLE | REVIEW: Comparable typed states required | REVIEW |
| 5 / C15 | StateRole.TOPOLOGY: Hot water distribution by consumer group | 1→N | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL CHANGE |
| 5 / C16 | StateRole.ROUTING: Floor collector to apartments in common corridors | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported ROUTING | REAL CHANGE |
| 5 / C17 | StateRole.SELECTED_EQUIPMENT: Parent pumping and zonal distribution | 1→N | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL CHANGE |
| 5 / C18 | StateRole.TOPOLOGY: Dedicated improved drinking-water network | 1→N | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL CHANGE |
| 5 / C19 | StateRole.TOPOLOGY: Irrigation and automatic-irrigation metering on depicted branches | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL CHANGE |
| 5 / C20 | StateRole.TOPOLOGY: Incoming water assembly | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL CHANGE |
| 5 / C22 | StateRole.INPUT_CRITERION: External guaranteed water head | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | REVIEW | REVIEW: Comparable typed states required | NOT A CHANGE |
| 7 / R01 | StateRole.TOPOLOGY: Housing fan-coil hydronic network | 1→N | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL |
| 7 / R02 | StateRole.ROUTING: Apartment cooling distribution and metering | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported ROUTING | REAL |
| 7 / R03 | StateRole.TOPOLOGY: Technical heat metering at section level | 1→N | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL |
| 7 / R05 | StateRole.REQUIREMENT: Parking, service rooms and ventilation heating circuits | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | COMPARABLE | MATERIAL: Changed source-supported REQUIREMENT | REAL |
| 7 / R08 | StateRole.SELECTED_EQUIPMENT: Water air curtains at parking gates | 1→N | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL |
| 7 / R09 | StateRole.SELECTED_EQUIPMENT: Electrical entrance air curtains | 1→N | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL |
| 7 / R10 | StateRole.SELECTED_EQUIPMENT: Parking air-heating units across both levels | N→M | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL |
| 7 / R11 | StateRole.ROUTING: Parking general exhaust parent route | N→M | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported ROUTING | REAL |
| 7 / R12 | StateRole.OPERATING_MODE: Parking and ramp general ventilation | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported OPERATING_MODE | REAL |
| 7 / R13 | StateRole.TOPOLOGY: Underground communications and electrical-room ventilation | N→M | engineering_subject, scope, state_role, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported TOPOLOGY | REAL |
| 7 / R16 | StateRole.REQUIREMENT: Apartment ventilation rule by room and occupancy category | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | COMPARABLE | MATERIAL: Changed source-supported REQUIREMENT | REAL |
| 7 / R18 | StateRole.REQUIREMENT: Clean commercial exhaust without harmful emissions or smells | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | COMPARABLE | MATERIAL: Changed source-supported REQUIREMENT | REAL |
| 7 / R19 | StateRole.INPUT_CRITERION: Housing cooling criterion per square metre | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | COMPARABLE | MATERIAL: Changed source-supported INPUT_CRITERION | REAL |
| 7 / R21 | StateRole.SELECTED_EQUIPMENT: Dispatcher-room unit for 43.36 m2 | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role, branch_identity | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL |
| 7 / R23 | StateRole.SELECTED_EQUIPMENT: Parent set of housing and parking air-handling equipment | N→M | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, functional_role | COMPARABLE | MATERIAL: Changed source-supported SELECTED_EQUIPMENT | REAL |
| 7 / Q04 | StateRole.CALCULATED_RESULT: Total calculated cooling demand | 1→1 | engineering_subject, scope, state_role, consumer_composition, local_or_global, component_or_total, operating_mode, stage_phase, physical_quantity, unit, functional_role, branch_identity, calculation_basis | INCOMPARABLE | REVIEW: Comparable typed states required | REVIEW |
| 7 / R25 | StateRole.REQUIREMENT: Housing/commercial intersection, non-fire-rated duct class | 1→1 | engineering_subject, scope, state_role, local_or_global, component_or_total, stage_phase, physical_quantity, unit, functional_role | COMPARABLE | MATERIAL: Changed source-supported REQUIREMENT | REAL |
| 7 / S_FP01 | StateRole.ROUTING: Dispatcher fan location | 1→1 | engineering_subject, scope, state_role, stage_phase, functional_role | REVIEW | REVIEW: Comparable typed states required | FALSE_AS_UNQUALIFIED_CHANGE / OLD_SOURCE_CONFLICT |

Full OLD/NEW typed values, all NOT_APPLICABLE conditions, adversarial variants, source truth and receipts are in the sibling JSON.

These are claim-level diagnostics. They do not change ProjectChange grouping or admission, and do not reinterpret frozen model results.

## Limits

- Bounded source-audit retrieval, not autonomous subject discovery.
- PASS tests honest delivery status, not complete recall or inference accuracy.
- Linked evidence outside the two selected pairs remains MISSING.
- R25 was added after outputs; prior exposure is preserved.
