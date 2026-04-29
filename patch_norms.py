#!/usr/bin/env python3
"""Patch 03_findings.json with norm verification results."""
import json
import copy
from datetime import datetime

FINDINGS_PATH = "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.2-К2 (Изм.1).pdf/_output/03_findings.json"

with open(FINDINGS_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

# Norm revision decisions keyed by finding ID
# norm_status: "ok" | "revised" | "warning"
REVISIONS = {
    "F-079": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "ГОСТ 25129-82 (действует); ГОСТ 9.402-2004 (действует)",
            "revised_norm": "ГОСТ 25129-2020 (действует); ГОСТ 9.402-2004 (действует)",
            "original_text": None,
            "revised_text": None,
            "revision_reason": "ГОСТ 25129-82 заменён на ГОСТ 25129-2020 (статус replaced подтверждён в Norms-main; ГОСТ 25129-2020 — действующий). Ссылка в тексте замечания и решения обновлена."
        }
    },
    "F-020": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3 и 10.4",
            "revised_norm": None,
            "original_text": "При наличии отверстий, вырезов и местных ослаблений железобетонных элементов следует предусматривать дополнительное армирование по расчёту и конструктивным требованиям",
            "revised_text": None,
            "revision_reason": "Цитата пп. 10.3 и 10.4 СП 63.13330.2018 не подтверждена. П. 10.3 является заголовком раздела без нормативного текста; п. 10.4 посвящён колоннам, стенам и узлам. П. 10.3.7 применяется только к бетонным (не железобетонным) конструкциям. Конкретный пункт о дополнительном армировании у отверстий в железобетонных плитах через MCP не найден."
        }
    },
    "F-022": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 5.1.1, 10.3",
            "revised_norm": None,
            "original_text": "Конструктивные решения железобетонных элементов должны обеспечивать несущую способность и пригодность к нормальной эксплуатации с учётом отверстий, закладных и ослаблений сечения",
            "revised_text": None,
            "revision_reason": "П. 5.1.1 СП 63.13330.2018 посвящён методике расчётов по предельным состояниям и не содержит требований к конструктивным решениям с учётом отверстий и ослаблений. П. 10.3 является заголовком раздела без нормативного текста. Конкретный пункт с заявленной нормой через MCP не найден."
        }
    },
    "F-035": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.25",
            "original_text": "Анкерование арматуры должно обеспечивать восприятие расчётных усилий, а длина анкеровки должна приниматься не менее требуемой",
            "revised_text": "Требуемую расчетную длину анкеровки арматуры с учетом конструктивного решения элемента в зоне анкеровки определяют по формуле l_an = α₁ · l₀,an · A_s,cal / A_s,ef; в любом случае фактическую длину анкеровки принимают не менее 15d_s и 200 мм",
            "revision_reason": "П. 10.3 является заголовком раздела без нормативного текста. Ссылка уточнена до п. 10.3.25 (расчётная длина анкеровки), текст подтверждён через Norms-main MCP."
        }
    },
    "F-037": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3.30",
            "revised_norm": None,
            "original_text": "Стыкуемые внахлёстку стержни следует располагать вплотную друг к другу или на расстоянии в свету между ними не более 4d и не более 50 мм",
            "revised_text": None,
            "revision_reason": "Заявленное требование об ограничении расстояния в свету «не более 4d и не более 50 мм» в тексте п. 10.3.30 СП 63.13330.2018 (подтверждён через MCP) отсутствует. П. 10.3.30 устанавливает ограничение диаметра стержней (≤40 мм) и формулу длины нахлёстки. Пп. 10.3.29–10.3.32 проверены — требование не найдено."
        }
    },
    "F-040": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 (действует), п. 5.3; СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": None,
            "original_text": None,
            "revised_text": None,
            "revision_reason": "norm_quote отсутствует (null); верификация цитаты невозможна. П. 10.3 СП 63.13330.2018 является заголовком раздела без нормативного текста. Ссылка на ГОСТ 21.501-2018 (действует) остаётся в силе как основной норматив по оформлению чертежей."
        }
    },
    "F-041": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3.25, 10.3.26",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.25, 10.3.26",
            "original_text": "Длина анкеровки рабочей арматуры должна обеспечивать восприятие расчётных усилий; минимальные длины принимаются по расчёту и конструктивным требованиям",
            "revised_text": "Требуемую расчетную длину анкеровки арматуры с учетом конструктивного решения элемента в зоне анкеровки определяют по формуле l_an = α₁ · l₀,an · A_s,cal / A_s,ef; в любом случае фактическую длину анкеровки принимают не менее 15d_s и 200 мм",
            "revision_reason": "Исходная цитата являлась обобщающим парафразом пп. 10.3.25–10.3.26. Ссылка на норму корректна; цитата уточнена по фактическому тексту п. 10.3.25, подтверждённому через Norms-main MCP."
        }
    },
    "F-042": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "ГОСТ 21.501-2018 (действует); СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "ГОСТ 21.501-2018 (действует)",
            "original_text": "Рабочие чертежи КЖ должны содержать данные, необходимые для изготовления и монтажа изделий и арматурных элементов",
            "revised_text": None,
            "revision_reason": "П. 10.3 СП 63.13330.2018 является заголовком раздела без нормативного текста; требования к составу рабочих чертежей КЖ регулируются ГОСТ 21.501-2018, а не п. 10.3. Ссылка на СП 63.13330.2018, п. 10.3 удалена как некорректная."
        }
    },
    "F-043": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.25",
            "original_text": "Анкеровка арматуры должна обеспечиваться расчётной длиной анкеровки, указываемой/обеспечиваемой в рабочих чертежах узлов армирования",
            "revised_text": "Требуемую расчетную длину анкеровки арматуры с учетом конструктивного решения элемента в зоне анкеровки определяют по формуле l_an = α₁ · l₀,an · A_s,cal / A_s,ef; в любом случае фактическую длину анкеровки принимают не менее 15d_s и 200 мм",
            "revision_reason": "П. 10.3 является заголовком раздела без нормативного текста. Ссылка уточнена до п. 10.3.25 (расчётная длина анкеровки); текст подтверждён через Norms-main MCP."
        }
    },
    "F-044": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.1",
            "original_text": "Толщина защитного слоя бетона должна назначаться не менее значений, обеспечивающих совместную работу арматуры с бетоном, анкеровку арматуры и требуемую огнестойкость",
            "revised_text": "Защитный слой бетона должен обеспечивать: совместную работу арматуры с бетоном; анкеровку арматуры в бетоне и возможность устройства стыков арматурных элементов; сохранность арматуры от воздействий окружающей среды (в том числе агрессивных); огнестойкость конструкций",
            "revision_reason": "П. 10.3 является заголовком раздела без нормативного текста. Ссылка уточнена до п. 10.3.1; фактический текст подтверждён через Norms-main MCP."
        }
    },
    "F-047": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3.7",
            "revised_norm": None,
            "original_text": "Вокруг отверстий в монолитных плитах перекрытия следует устанавливать дополнительное армирование, обеспечивающее восприятие усилий, возникающих в зоне обрамления",
            "revised_text": None,
            "revision_reason": "П. 10.3.7 СП 63.13330.2018 устанавливает конструктивное армирование для бетонных (не железобетонных) конструкций в зонах резкого изменения сечения и у проёмов в стенах. Заявленное требование об обрамляющем армировании вокруг отверстий в железобетонных монолитных плитах в данном пункте отсутствует. Конкретный пункт через MCP не найден."
        }
    },
    "F-048": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "ГОСТ 21.602-2016 (действует); СП 63.13330.2018 (действует), п. 10.4.6",
            "revised_norm": None,
            "original_text": "Отверстия, вырезы и ослабления в железобетонных элементах должны учитываться в проекте с назначением необходимого дополнительного армирования",
            "revised_text": None,
            "revision_reason": "П. 10.4.6 СП 63.13330.2018 в Norms-main содержит только заглушку на английском языке; полный русскоязычный текст пункта недоступен. Верификация цитаты невозможна."
        }
    },
    "F-051": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 5.1.1",
            "revised_norm": None,
            "original_text": "Конструктивные решения железобетонных элементов должны приниматься на основании расчёта и конструирования с учётом действительных условий работы конструкции",
            "revised_text": None,
            "revision_reason": "П. 5.1.1 СП 63.13330.2018 посвящён методике расчётов по предельным состояниям (прочность, устойчивость, трещиностойкость, деформативность) и не содержит заявленного требования к конструктивным решениям с учётом действительных условий работы."
        }
    },
    "F-055": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.1",
            "original_text": "Проектом должны быть назначены размеры защитного слоя бетона для рабочей арматуры",
            "revised_text": "Защитный слой бетона должен обеспечивать: совместную работу арматуры с бетоном; анкеровку арматуры в бетоне и возможность устройства стыков арматурных элементов; сохранность арматуры от воздействий окружающей среды (в том числе агрессивных); огнестойкость конструкций",
            "revision_reason": "П. 10.3 является заголовком раздела без нормативного текста. Ссылка уточнена до п. 10.3.1; текст подтверждён через Norms-main MCP."
        }
    },
    "F-061": {
        "norm_verified": True,
        "norm_status": "revised",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), п. 10.3",
            "revised_norm": "СП 63.13330.2018 (действует), п. 10.3.25",
            "original_text": "Длины анкеровки арматуры должны назначаться расчётом и/или по нормативным требованиям",
            "revised_text": "Требуемую расчетную длину анкеровки арматуры с учетом конструктивного решения элемента в зоне анкеровки определяют по формуле l_an = α₁ · l₀,an · A_s,cal / A_s,ef; в любом случае фактическую длину анкеровки принимают не менее 15d_s и 200 мм",
            "revision_reason": "П. 10.3 является заголовком раздела без нормативного текста. Ссылка уточнена до п. 10.3.25 (расчётная длина анкеровки); текст подтверждён через Norms-main MCP."
        }
    },
    "F-066": {
        "norm_verified": False,
        "norm_status": "warning",
        "norm_revision": {
            "original_norm": "СП 63.13330.2018 (действует), раздел 10",
            "revised_norm": None,
            "original_text": "Анкеровка арматуры должна обеспечиваться расчётной длиной анкеровки, указываемой/обеспечиваемой в рабочих чертежах узлов армирования",
            "revised_text": None,
            "revision_reason": "Ссылка на раздел 10 СП 63.13330.2018 в целом не подтверждена конкретным пунктом. Цитата о длине анкеровки применительно к рабочим чертежам узлов армирования в разделе 10 через MCP не найдена."
        }
    },
}

revised_ids = [fid for fid, rev in REVISIONS.items() if rev["norm_status"] in ("revised", "warning")]

# Process findings
updated_findings = []
for finding in data["findings"]:
    fid = finding["id"]
    f = copy.deepcopy(finding)

    if fid in REVISIONS:
        rev = REVISIONS[fid]
        f["norm_verified"] = rev["norm_verified"]
        f["norm_status"] = rev["norm_status"]
        f["norm_revision"] = rev["norm_revision"]

        # For F-079: also update the norm field in finding itself
        if fid == "F-079":
            f["norm"] = "ГОСТ 25129-2020 (действует); ГОСТ 9.402-2004 (действует)"
            f["solution"] = f.get("solution", "").replace("ГОСТ 25129-82", "ГОСТ 25129-2020")
        # For revised norm references: update the norm field
        if fid == "F-035":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.25"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
        elif fid == "F-041":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.25, 10.3.26"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
        elif fid == "F-042":
            f["norm"] = "ГОСТ 21.501-2018 (действует)"
        elif fid == "F-043":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.25"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
        elif fid == "F-044":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.1"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
        elif fid == "F-055":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.1"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
        elif fid == "F-061":
            f["norm"] = "СП 63.13330.2018 (действует), п. 10.3.25"
            f["norm_quote"] = rev["norm_revision"]["revised_text"]
    else:
        f["norm_verified"] = True
        f["norm_status"] = "ok"
        f["norm_revision"] = None

    updated_findings.append(f)

# Build output
now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
norms_checked = len(REVISIONS)
norms_ok = sum(1 for v in REVISIONS.values() if v["norm_status"] == "ok")
norms_revised = sum(1 for v in REVISIONS.values() if v["norm_status"] in ("revised", "warning"))

output = {
    "meta": {
        **data["meta"],
        "norm_verification": {
            "verified_at": now_iso,
            "total_norms_checked": norms_checked,
            "norms_ok": norms_ok,
            "norms_revised": norms_revised,
            "findings_revised": [fid for fid, rev in REVISIONS.items() if rev["norm_status"] == "revised"],
            "findings_warning": [fid for fid, rev in REVISIONS.items() if rev["norm_status"] == "warning"],
            "source": "norms_main_mcp"
        }
    },
    "findings": updated_findings,
    "removed_findings": data.get("removed_findings", [])
}

with open(FINDINGS_PATH, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"Done. Processed {len(updated_findings)} findings.")
print(f"Revised: {[fid for fid, rev in REVISIONS.items() if rev['norm_status'] == 'revised']}")
print(f"Warning: {[fid for fid, rev in REVISIONS.items() if rev['norm_status'] == 'warning']}")
