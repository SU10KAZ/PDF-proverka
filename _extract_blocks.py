import json

TARGET_IMAGE_IDS = [
    "XVX6-4CH3-J9C",
    "4GCV-CJRJ-GGN",
    "4RK4-KLWF-M3L",
    "7UDM-YDHA-N3G",
    "9RFT-Y7EV-CNV",
    "4QU9-UYEX-QAF",
    "6XRH-WJJ7-6ET",
    "6XKR-EAYA-W6V",
    "76N3-UAGD-J66",
    "499D-WQHC-VYK",
]

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

# FILE 1: 02_blocks_analysis.json
blocks_path = "D:/Отедел Системного Анализа/1. Audit Manager/projects/VK/133_23-ГК-ВК1/_output/02_blocks_analysis.json"
with open(blocks_path, 'r', encoding='utf-8') as f:
    blocks_data = json.load(f)

block_analyses = blocks_data.get("block_analyses", [])
print(f"Total block_analyses: {len(block_analyses)}")
if block_analyses:
    sample = block_analyses[0]
    print(f"Sample block keys: {list(sample.keys())}")

image_results = {}
for block in block_analyses:
    bid = block.get("block_id") or block.get("id")
    if bid in TARGET_IMAGE_IDS:
        image_results[bid] = {
            "summary": block.get("summary", block.get("analysis_summary", "N/A")),
            "key_values_read": block.get("key_values_read", block.get("key_values", [])),
            "page": block.get("page"),
            "sheet": block.get("sheet", block.get("sheet_no")),
        }

print(f"\nFound image blocks: {list(image_results.keys())}")
missing_img = [bid for bid in TARGET_IMAGE_IDS if bid not in image_results]
print(f"Missing image blocks: {missing_img}")

# FILE 2: document_graph.json
graph_path = "D:/Отедел Системного Анализа/1. Audit Manager/projects/VK/133_23-ГК-ВК1/_output/document_graph.json"
with open(graph_path, 'r', encoding='utf-8') as f:
    graph_data = json.load(f)

pages = graph_data.get("pages", [])
print(f"\nTotal pages in graph: {len(pages)}")
if pages:
    print(f"Sample page keys: {list(pages[0].keys())}")

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

print(f"Found text blocks: {list(text_results.keys())}")
missing_txt = [tid for tid in TARGET_TEXT_IDS if tid not in text_results]
print(f"Missing text blocks: {missing_txt}")

# Output final result
result = {
    "image_blocks": image_results,
    "text_blocks": {}
}

for tid in TARGET_TEXT_IDS:
    if tid in text_results:
        result["text_blocks"][tid] = text_results[tid]
    else:
        result["text_blocks"][tid] = {"text": None, "page": None, "exists": False}

print("\n\n=== FINAL JSON RESULT ===")
print(json.dumps(result, ensure_ascii=False, indent=2))
