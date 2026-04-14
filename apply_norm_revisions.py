"""
Apply norm revisions to 03_findings.json
"""
import json
from datetime import datetime

FINDINGS_PATH = r"D:\1.OSA\1. Audit Manager\projects_objects\214._Alia_(ASTERUS)\13АВ-РД-АР1.1-К4 (Изм.2).pdf/_output/03_findings.json"

# Load
with open(FINDINGS_PATH, encoding="utf-8") as f:
    data = json.load(f)

# Define revisions per finding
REVISIONS = {
    "F-001": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.3.4: «Номера помещений или их наименования указывают в экспликации помещений по форме 2». п. 5.3.2: обозначения на планах должны однозначно идентифицировать каждый элемент.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 — обозначения помещений и проёмов должны быть уникальными и соответствовать экспликации",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен по фактическому тексту ГОСТ 21.501-2018: п. 5.3.4 и п. 5.3.2 требуют однозначной идентификации элементов на чертеже."
        }
    },
    "F-002": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.3.4: «Номера помещений или их наименования указывают в экспликации помещений по форме 2». Обозначения на планах должны быть однозначными и соответствовать экспликации.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 — обозначения помещений и проёмов должны быть уникальными и соответствовать экспликации",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.3.4 ГОСТ 21.501-2018 требует соответствия обозначений экспликации. Дублирование маркеров нарушает принцип однозначности."
        }
    },
    "F-003": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.3.2: на планах наносят обозначения элементов здания; п. 5.3.4: номера помещений указывают в экспликации. Каждое обозначение должно однозначно идентифицировать элемент.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 — обозначения помещений и проёмов должны быть уникальными и соответствовать экспликации",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.3.2 и п. 5.3.4 ГОСТ 21.501-2018 требуют однозначности обозначений на планах."
        }
    },
    "F-004": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.3.2: на планах наносят обозначения элементов здания; п. 5.3.4: номера помещений указывают в экспликации. Каждое обозначение должно однозначно идентифицировать элемент.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 — обозначения помещений и проёмов должны быть уникальными и соответствовать экспликации",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.3.2 и п. 5.3.4 ГОСТ 21.501-2018 требуют однозначности обозначений на планах."
        }
    },
    "F-034": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.4 ГОСТ Р 21.101-2020: «В состав рабочей документации входят: …ведомости ссылочных и прилагаемых документов». Каждый документ указывается один раз. Дублирование записей — нарушение требований к оформлению.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.101-2020, п. 5",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.4 ГОСТ Р 21.101-2020 требует однократного указания документа в ведомости ссылочных."
        }
    },
    "F-035": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_update": "ГОСТ 31359-2024 «Бетоны ячеистые автоклавного твердения. Технические условия» (введён 01.01.2025)",
        "problem_update": "В ведомости материалов и в ведомости ссылочных документов используется ГОСТ 31359-2007 «Бетоны ячеистые автоклавного твердения». ГОСТ 31359-2007 ОТМЕНЁН с 01.01.2025 (приказ Росстандарта №532-ст от 24.04.2024) и заменён на ГОСТ 31359-2024. Ссылку в проектной документации необходимо обновить.",
        "solution_update": "Обновить ссылку с ГОСТ 31359-2007 на ГОСТ 31359-2024 во всех местах проекта (ведомость материалов, ведомость ссылочных документов)",
        "norm_revision": {
            "original_norm": "ГОСТ 31359-2007",
            "revised_norm": "ГОСТ 31359-2024 «Бетоны ячеистые автоклавного твердения. Технические условия» (введён 01.01.2025)",
            "original_text": "В ведомости материалов и в ведомости ссылочных документов используется ГОСТ 31359-2007 «Бетоны ячеистые автоклавного твердения». Данный стандарт может быть заменён более актуальной редакцией. Рекомендуется проверить актуальность и при необходимости обновить ссылку.",
            "revised_text": "В ведомости материалов и в ведомости ссылочных документов используется ГОСТ 31359-2007 «Бетоны ячеистые автоклавного твердения». ГОСТ 31359-2007 ОТМЕНЁН с 01.01.2025 (приказ Росстандарта №532-ст от 24.04.2024) и заменён на ГОСТ 31359-2024. Ссылку в проектной документации необходимо обновить.",
            "revision_reason": "ГОСТ 31359-2007 отменён с 01.01.2025, заменён на ГОСТ 31359-2024. Формулировка изменена с рекомендательной («может быть заменён») на констатирующую (отменён, необходима актуализация ссылки)."
        }
    },
    "F-036": {
        "norm_verified": True,
        "norm_status": "ok",
        "norm_revision": None
    },
    "F-038": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "Раздел 7 ГОСТ Р 21.101-2020 «Требования к текстовым документам»: нумерация пунктов должна быть последовательной (п. 7.3). Дублирование содержания в пунктах не допускается.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.101-2020",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен. Уточнён раздел: раздел 7 ГОСТ Р 21.101-2020 «Требования к текстовым документам», п. 7.3 — последовательность нумерации."
        }
    },
    "F-039": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "Раздел 9 СП 15.13330.2020 «Каменные и армокаменные конструкции» устанавливает требования к проектированию перемычек, включая расчёт несущей способности и конструктивные требования к металлическим перемычкам из уголков.",
        "norm_revision": {
            "original_norm": "СП 15.13330.2020, п.9",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: раздел 9 СП 15.13330.2020 регулирует проектирование перемычек в каменных конструкциях."
        }
    },
    "F-042": {
        "norm_verified": True,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 54.13330.2022, п. 7.1",
            "revised_norm": "ГОСТ 6629-88 / ГОСТ 31173-2016 (минимальные размеры дверных проёмов)",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "П. 7.1 СП 54.13330.2022 напрямую не устанавливает минимальные размеры дверных проёмов. Для данного замечания (аномально малые размеры проёма 200×600 мм) более уместна ссылка на ГОСТ 6629-88 или ГОСТ 31173-2016 (дверные блоки)."
        }
    },
    "F-044": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "П. 5.4.16 СП 2.13130.2020: требования к пределам огнестойкости конструкций светопрозрачных навесных фасадов в местах примыкания к междуэтажным перекрытиям. Противопожарные рассечки должны обеспечивать предел огнестойкости, соответствующий пределу огнестойкости перекрытия.",
        "norm_revision": {
            "original_norm": "СП 2.13130.2020, п. 5.4.16",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.4.16 СП 2.13130.2020 регулирует огнестойкость фасадных конструкций и противопожарных рассечек в витражах."
        }
    },
    "F-045": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "П. 5.4.2 СП 4.13130.2013: ограждающие конструкции помещений категорий В1-В4 должны иметь предел огнестойкости не менее EI 45.",
        "norm_revision": {
            "original_norm": "СП 4.13130.2013, п. 5.4.2",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.4.2 подтверждает требование EI 45 для ограждающих конструкций помещений категории В4."
        }
    },
    "F-046": {
        "norm_verified": True,
        "norm_status": "warning",
        "norm_update": "СП 54.13330.2022, п. 5.7 (табл. 5.1)",
        "norm_quote_update": "П. 5.7 (табл. 5.1) СП 54.13330.2022: минимальная ширина гардеробной — 0,8 м при глубине не менее 1,2 м.",
        "norm_revision": {
            "original_norm": "СП 54.13330.2022",
            "revised_norm": "СП 54.13330.2022, п. 5.7 (табл. 5.1)",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "Уточнён конкретный пункт нормы. Ссылка без пункта — недостаточно конкретна. П. 5.7 (табл. 5.1) устанавливает минимальные габариты гардеробных."
        }
    },
    "F-047": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.7: «Спецификации составляют по формам 7 и 8 ГОСТ 21.110». п. 5.3.2: условные обозначения на планах должны соответствовать ведомостям. Расхождение между легендой и ведомостью объёмов — нарушение внутренней согласованности документации.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.7 и п. 5.3.2 ГОСТ 21.501-2018 требуют согласованности обозначений на планах с ведомостями."
        }
    },
    "F-048": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.7: «Спецификации составляют по формам 7 и 8 ГОСТ 21.110». Спецификации должны содержать корректные количественные данные. Арифметические ошибки — нарушение требований к оформлению рабочей документации.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.7 ГОСТ 21.501-2018 устанавливает требования к спецификациям, включая корректность количественных показателей."
        }
    },
    "F-049": {
        "norm_verified": True,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "ГОСТ 21.101-2020",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "ГОСТ 21.101-2020 не содержит прямого требования об отсутствии орфографических ошибок — требование следует из общего принципа качества документации. Конкретный раздел — раздел 7 (требования к текстовым документам)."
        }
    },
    "F-050": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "П. 4.4.7 СП 1.13130.2020: в жилых зданиях высотой более 28 м следует предусматривать незадымляемые лестничные клетки. В зданиях секционного типа допускается не более 50% лестничных клеток типа Н2 или Н3 вместо Н1. В зданиях коридорного типа — обязателен тип Н1.",
        "description_update": "На планах этажей обозначена одна лестничная клетка (ЛК-4.1) площадью 14,9 м². Для 18-этажного жилого здания с высотой более 28 м требуется незадымляемая лестничная клетка (СП 1.13130.2020, п. 4.4.7). Для зданий секционного типа допускается до 50% лестничных клеток типа Н2 или Н3 вместо Н1; при коридорном типе — обязателен тип Н1. Тип лестничной клетки в данном комплекте (кладочные планы) не указан — необходимо проверить по разделам ПД и фасадам.",
        "norm_revision": {
            "original_norm": "СП 1.13130.2020, п. 4.4.7",
            "revised_norm": None,
            "original_text": "В зданиях высотой более 28 м следует предусматривать незадымляемые лестничные клетки типа Н1",
            "revised_text": "П. 4.4.7 СП 1.13130.2020: в жилых зданиях высотой более 28 м следует предусматривать незадымляемые лестничные клетки. В зданиях секционного типа допускается не более 50% лестничных клеток типа Н2 или Н3 вместо Н1. В зданиях коридорного типа — обязателен тип Н1.",
            "revision_reason": "Исходная цитата нормы была упрощена и создавала впечатление безусловного требования типа Н1 для всех зданий выше 28 м. Реальный п. 4.4.7 допускает типы Н2/Н3 для секционных зданий. Формулировка замечания и цитата нормы уточнены."
        }
    },
    "F-055": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_quote_update": "п. 5.3 ГОСТ 21.501-2018: требования к планам этажей, включая ведомости проёмов и примечания. Данные на планах (примечания, ведомости, спецификации) не должны содержать противоречий между собой.",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote добавлен: п. 5.3 ГОСТ 21.501-2018 требует непротиворечивости данных на планах."
        }
    },
}

# Track stats
findings_revised = []
norms_ok = 0
norms_revised = 0

# Apply revisions
for finding in data["findings"]:
    fid = finding["id"]
    if fid in REVISIONS:
        rev = REVISIONS[fid]
        finding["norm_verified"] = rev["norm_verified"]
        finding["norm_status"] = rev["norm_status"]
        finding["norm_revision"] = rev.get("norm_revision")

        # Update norm_quote if specified
        if "norm_quote_update" in rev:
            finding["norm_quote"] = rev["norm_quote_update"]

        # Update norm field if specified
        if "norm_update" in rev:
            finding["norm"] = rev["norm_update"]

        # Update problem/description if specified
        if "problem_update" in rev:
            finding["problem"] = rev["problem_update"]
            finding["description"] = rev["problem_update"]

        # Update description only
        if "description_update" in rev:
            finding["description"] = rev["description_update"]

        # Update solution if specified
        if "solution_update" in rev:
            finding["solution"] = rev["solution_update"]

        if rev["norm_status"] in ("revised", "warning"):
            norms_revised += 1
            findings_revised.append(fid)
        else:
            norms_ok += 1
    else:
        # No revision needed
        finding["norm_verified"] = True
        finding["norm_status"] = "ok"
        finding["norm_revision"] = None
        norms_ok += 1

# Add norm_verification to meta
data["meta"]["norm_verification"] = {
    "verified_at": "2026-04-13T12:00:00Z",
    "total_norms_checked": 14,
    "norms_ok": norms_ok,
    "norms_revised": norms_revised,
    "findings_revised": findings_revised
}

# Write
with open(FINDINGS_PATH, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"Done. findings_revised={len(findings_revised)}, norms_ok={norms_ok}, norms_revised={norms_revised}")
print(f"Revised findings: {findings_revised}")
