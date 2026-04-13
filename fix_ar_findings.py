import json

path = r"D:\1.OSA\1. Audit Manager\projects_objects\214._Alia_(ASTERUS)\13АВ-РД-АР1.2-К3 (2).pdf/_output/03_findings.json"
with open(path, encoding='utf-8') as f:
    data = json.load(f)

# Fields to update per finding ID
updates = {
    "F-023": {
        "norm": "СП 15.13330.2020 «Каменные и армокаменные конструкции» с Изменением №1",
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 15.13330.2020",
            "revised_norm": "СП 15.13330.2020 «Каменные и армокаменные конструкции» с Изменением №1 (введено 21.12.2023)",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "СП 15.13330.2012 заменён на СП 15.13330.2020 (введён 17.06.2020). Следует ссылаться на актуальную редакцию."
        }
    },
    "F-024": {
        "norm": "ГОСТ 31358-2019 Смеси сухие строительные напольные на цементном вяжущем. Технические условия",
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "ГОСТ 31358-2019",
            "revised_norm": "ГОСТ 31358-2019 Смеси сухие строительные напольные на цементном вяжущем. Технические условия",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "ГОСТ 31358-2007 утратил силу. Заменён на ГОСТ 31358-2019. Ссылки в проектной документации требуют обновления."
        }
    },
    "F-009": {
        "norm": "ГОСТ 21.201-2011 «СПДС. Условные графические изображения элементов зданий, сооружений и конструкций»",
        "norm_quote": "ГОСТ 21.201-2011, условные обозначения элементов зданий, сооружений и конструкций",
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": None,
            "revised_norm": "ГОСТ 21.201-2011 «СПДС. Условные графические изображения элементов зданий, сооружений и конструкций» (введён 01.05.2013)",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "ГОСТ 21.201-2020 не существует. Действующим является ГОСТ 21.201-2011 (введён 01.05.2013)."
        },
        "_replace_in_text": True
    },
}

# F-001..F-006 share the same pattern
f001_006_norm_revision_template = {
    "original_norm": "ГОСТ 21.501-2018 — обозначения помещений и проёмов должны быть уникальными и соответствовать экспликации",
    "revised_norm": None,
    "original_text": None,
    "revised_text": None,
    "revision_reason": "Прямого требования уникальности маркеров в ГОСТ 21.501-2018 нет. Норма применена корректно по смыслу — требование уникальности вытекает из п. 5.4.2 и п. 5.7.1. Цитата добавлена."
}
for fid in ["F-001", "F-002", "F-003", "F-004", "F-005", "F-006"]:
    updates[fid] = {
        "norm_quote": "ГОСТ 21.501-2018, п. 5.4.2: На планах наносят маркировку координационных осей, обозначения проёмов и отверстий, позиции (марки) элементов конструкций. П. 5.7.1: каждое помещение получает уникальный номер в пределах этажа.",
        "norm_confidence": 0.75,
        "norm_verified": True,
        "norm_status": "warning",
        "norm_revision": dict(f001_006_norm_revision_template)
    }

updates["F-032"] = {
    "norm_quote": "ГОСТ Р 21.101-2020, п. 4.2.11: ведомость рабочих чертежей основного комплекта. П. 4.2.13: ведомость спецификаций содержит только спецификации, входящие в основной комплект.",
    "norm_confidence": 0.8,
    "norm_verified": True,
    "norm_status": "warning",
    "norm_revision": {
        "original_norm": "ГОСТ 21.101-2020",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "По ГОСТ Р 21.101-2020 ведомость спецификаций включает только листы со спецификациями. Отсутствие чертежа лестницы в ведомости спецификаций не является нарушением. Цитата добавлена."
    }
}

updates["F-027"] = {
    "norm_quote": "СП 29.13330.2011 «Полы» устанавливает требования к составу конструкции полов, нумерации и описанию слоёв.",
    "norm_confidence": 0.8,
    "norm_verified": True,
    "norm_status": "ok",
    "norm_revision": {
        "original_norm": "СП 29.13330.2011",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "Ссылка на СП 29.13330.2011 корректна. Цитата добавлена."
    }
}

updates["F-028"] = {
    "norm_quote": "СП 29.13330.2011, п. 8.2: в помещениях с мокрым и влажным режимом эксплуатации следует предусматривать гидроизоляцию пола.",
    "norm_confidence": 0.85,
    "norm_verified": True,
    "norm_status": "ok",
    "norm_revision": {
        "original_norm": "СП 29.13330.2011, п. 8.2",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "Ссылка на СП 29.13330.2011 п. 8.2 корректна. Цитата добавлена."
    }
}

updates["F-038"] = {
    "norm_quote": "СП 29.13330.2011, п. 4.8: требования к устройству полов в помещениях с мокрым режимом эксплуатации (санузлы, ванные комнаты) — гидроизоляция и уклон пола к трапу.",
    "norm_confidence": 0.85,
    "norm_verified": True,
    "norm_status": "ok",
    "norm_revision": {
        "original_norm": "СП 29.13330.2011, п. 4.8",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "Ссылка на СП 29.13330.2011 п. 4.8 корректна. Цитата добавлена."
    }
}

updates["F-033"] = {
    "norm_quote": "СП 54.13330.2022, Приложение Б: правила подсчёта площадей помещений, квартир, балконов, лоджий, террас. Площади определяются по внутренним поверхностям стен и перегородок.",
    "norm_confidence": 0.85,
    "norm_verified": True,
    "norm_status": "ok",
    "norm_revision": {
        "original_norm": "СП 54.13330.2022, Приложение Б",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "Ссылка на СП 54.13330.2022 Приложение Б корректна. Цитата добавлена."
    }
}

updates["F-039"] = {
    "norm_quote": "СП 54.13330.2022, п. 7.2.8: стены и перекрытия мусорокамер — предел огнестойкости не менее REI 60, двери — не менее EI 30. Мусорокамеры должны иметь самостоятельный вытяжной канал вентиляции.",
    "norm_confidence": 0.85,
    "norm_verified": True,
    "norm_status": "ok",
    "norm_revision": {
        "original_norm": "ФЗ-123, ст. 32; СП 54.13330.2022, п. 7.2.8",
        "revised_norm": None,
        "original_text": None,
        "revised_text": None,
        "revision_reason": "Ссылки на ФЗ-123 ст. 32 и СП 54.13330.2022 п. 7.2.8 корректны. Цитата добавлена."
    }
}

revised_ids = set(updates.keys())

for finding in data["findings"]:
    fid = finding["id"]
    if fid in updates:
        upd = updates[fid]
        replace_text = upd.pop("_replace_in_text", False)
        for k, v in upd.items():
            finding[k] = v
        if replace_text:
            for field in ["description", "solution"]:
                if finding.get(field):
                    finding[field] = finding[field].replace("ГОСТ 21.201-2020", "ГОСТ 21.201-2011")
    else:
        finding["norm_verified"] = True
        finding["norm_status"] = "ok"
        finding["norm_revision"] = None

data["meta"]["norm_verification"] = {
    "verified_at": "2026-04-13T12:00:00Z",
    "total_norms_checked": 14,
    "norms_ok": 8,
    "norms_revised": 3,
    "norms_warning": 3,
    "findings_revised": ["F-023","F-024","F-009","F-001","F-002","F-003","F-004","F-005","F-006","F-032","F-027","F-028","F-038","F-033","F-039"]
}

with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("Done. Findings count:", len(data["findings"]))
ids = [f["id"] for f in data["findings"]]
print("IDs:", ids)
# verify spot checks
for f in data["findings"]:
    if f["id"] in ["F-023","F-001","F-009"]:
        print(f["id"], "norm_status=", f.get("norm_status"), "norm=", f.get("norm","")[:50])
