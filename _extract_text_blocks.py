import json

TARGET_TEXT_IDS = [
    "96CM-AMP7-MG9",
    "44PN-PFU9-EGT",
    "QKRN-3VYC-M9J",
    "79XW-3YTR-LUP",
    "4L6N-TVVW-NYW",
    "7CKU-VPHA-TKY",
    "97KK-NRUA-NTX",
    "GUDF-FVGR-7TL",
]

graph_path = "D:/Отдел Системного Анализа/1. Audit Manager/projects/VK/133_23-ГК-ВК1/_output/document_graph.json"
with open(graph_path, 'r', encoding='utf-8') as f:
    graph_data = json.load(f)

pages = graph_data.get("pages", [])

text_results = {}
for page in pages:
    page_num = page.get("page")
    text_blocks = page.get("text_blocks", [])
    for tb in text_blocks:
        tid = tb.get("id") or tb.get("block_id")
        if tid in TARGET_TEXT_IDS:
            text_results[tid] = {
                "text": tb.get("text", ""),
                "page": page_num,
                "exists": True
            }

result = {}
for tid in TARGET_TEXT_IDS:
    if tid in text_results:
        result[tid] = text_results[tid]
    else:
        result[tid] = {"text": None, "page": None, "exists": False}

print(json.dumps(result, ensure_ascii=False, indent=2))
