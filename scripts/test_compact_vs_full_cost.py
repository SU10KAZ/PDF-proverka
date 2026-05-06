"""
Тест: сравнение стоимости compact (50 DPI, min 500px) vs full (100 DPI, min 800px)
для одних и тех же блоков, отправленных через OpenRouter GPT.

Использует 4 выбранных блока (2 medium + 2 large) из проекта 133_23-ГК-ЭМ1.
Отправляет 5 батчей на каждый профиль (с одним и тем же промптом) и сравнивает
количество токенов и итоговую стоимость.
"""

import base64
import json
import os
import sys
import time
import tempfile
from pathlib import Path

# ── пути ──────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

PROJECT_DIR  = ROOT / "projects" / '213. Мосфильмовская 31А "King&Sons"' / "EOM" / "133_23-ГК-ЭМ1"
PDF_PATH     = PROJECT_DIR / "document.pdf"
INDEX_PATH   = PROJECT_DIR / "_output" / "blocks" / "index.json"
RESULT_JSON  = PROJECT_DIR / "document_result.json"

SELECTED_BLOCK_IDS = [
    "A7NU-R6LR-UVQ",  # medium, 119 KB @ 1500px, page 11
    "E7YU-JC3U-QAR",  # medium, 127 KB @ 1500px, page 12
    "7DTQ-9PUJ-XNE",  # large,  970 KB @ 2596px,  page 8
    "9MJT-RRPM-PEJ",  # large, 1169 KB @ 2595px,  page 13
]

PROFILES = {
    "compact_50dpi_500px": {"dpi": 50,  "min_long_side": 500},
    "mid_150dpi_800px":    {"dpi": 150, "min_long_side": 800},
    "full_100dpi_800px":   {"dpi": 100, "min_long_side": 800},
}

BATCHES_PER_PROFILE = 5
MODEL = "openai/gpt-5.4"
# Цена GPT-5.4 на OpenRouter ($/1M tokens)
PRICE_INPUT  = 2.50
PRICE_OUTPUT = 10.00

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

SYSTEM_PROMPT = "Ты — эксперт по проектной документации МКД."
USER_PROMPT = (
    "Посмотри на чертёж и кратко (2-3 предложения) опиши, "
    "что на нём изображено. Укажи раздел проектирования."
)

# ─── загрузка размеров страниц из result.json ─────────────────────────────────

def load_page_dims(result_json: Path) -> dict[int, tuple[int, int]]:
    data = json.loads(result_json.read_text(encoding="utf-8"))
    dims = {}
    for pg in data.get("pages", []):
        pn = pg.get("page_number") or pg.get("page_num") or pg.get("page")
        if pn is not None:
            dims[int(pn)] = (int(pg["width"]), int(pg["height"]))
    return dims


# ─── рендер блока из PDF через crop_from_pdf ──────────────────────────────────

def render_block_to_png(
    pdf_path: Path,
    page_num: int,       # 1-based (как в index.json)
    coords_px: list,
    page_width: int,
    page_height: int,
    dpi: int,
    min_long_side: int,
    out_path: Path,
) -> dict:
    from blocks import crop_from_pdf
    w, h = crop_from_pdf(
        pdf_path, page_num, coords_px,
        page_width, page_height,
        out_path,
        dpi=dpi, min_long_side=min_long_side,
    )
    size_kb = out_path.stat().st_size / 1024
    return {"w": w, "h": h, "size_kb": round(size_kb, 1)}


def render_blocks_for_profile(
    profile_name: str,
    profile: dict,
    blocks_meta: list[dict],
    page_dims: dict[int, tuple[int, int]],
    tmp_dir: Path,
) -> list[dict]:
    prof_dir = tmp_dir / profile_name
    prof_dir.mkdir(exist_ok=True)
    rendered = []
    for bm in blocks_meta:
        bid    = bm["block_id"]
        page   = bm["page"]   # 1-based (как в crop_from_pdf)
        pw, ph = page_dims.get(page, (0, 0))
        if pw == 0 or ph == 0:
            print(f"  [{profile_name}] {bid}: нет размеров страницы {page} — пропуск")
            continue
        out  = prof_dir / f"block_{bid}.png"
        info = render_block_to_png(
            PDF_PATH, page, bm["crop_px"],
            pw, ph,
            profile["dpi"], profile["min_long_side"],
            out,
        )
        rendered.append({"block_id": bid, "path": out, **info})
        print(f"  [{profile_name}] {bid}: {info['w']}x{info['h']}px, {info['size_kb']:.0f} KB")
    return rendered


# ─── OpenRouter вызов ─────────────────────────────────────────────────────────

def call_openrouter(images: list[Path]) -> dict:
    import urllib.request, urllib.error

    content = [{"type": "text", "text": USER_PROMPT}]
    for img_path in images:
        b64 = base64.b64encode(img_path.read_bytes()).decode()
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        })

    payload = json.dumps({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": content},
        ],
        "max_tokens": 300,
    }).encode()

    req = urllib.request.Request(
        f"{OPENROUTER_BASE_URL}/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8080",
            "X-Title": "BIM Audit Cost Test",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return {"error": f"HTTP {e.code}: {body[:300]}"}

    usage = data.get("usage", {})
    return {
        "prompt_tokens":     usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
        "total_tokens":      usage.get("total_tokens", 0),
        "model":             data.get("model", ""),
    }


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    if not OPENROUTER_API_KEY:
        print("[ERROR] OPENROUTER_API_KEY не задан")
        sys.exit(1)

    # Загрузить метаданные выбранных блоков из index.json
    idx = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    all_blocks = {b["block_id"]: b for b in idx["blocks"]}
    blocks_meta = []
    for bid in SELECTED_BLOCK_IDS:
        bm = all_blocks.get(bid)
        if not bm:
            print(f"[WARN] block {bid} не найден в index.json")
            continue
        if "crop_px" not in bm:
            print(f"[WARN] block {bid} не имеет crop_px — пропускаем")
            continue
        blocks_meta.append(bm)

    if not blocks_meta:
        print("[ERROR] Нет блоков с crop_px — нужно проверить index.json")
        sys.exit(1)

    print(f"\nБлоков для теста: {len(blocks_meta)}")
    for bm in blocks_meta:
        print(f"  {bm['block_id']} | page={bm['page']} | crop_px={bm.get('crop_px')}")

    page_dims = load_page_dims(RESULT_JSON)
    print(f"Размеры страниц загружены: {len(page_dims)} страниц\n")

    with tempfile.TemporaryDirectory(prefix="cost_test_") as tmpdir:
        tmp = Path(tmpdir)
        results = {}

        # Рендер блоков в обоих профилях
        rendered_by_profile = {}
        for pname, pconf in PROFILES.items():
            print(f"=== Рендер профиль: {pname} (dpi={pconf['dpi']}, min={pconf['min_long_side']}) ===")
            rendered_by_profile[pname] = render_blocks_for_profile(pname, pconf, blocks_meta, page_dims, tmp)

        # Отправка батчей
        for pname, rendered in rendered_by_profile.items():
            print(f"\n=== Отправка {BATCHES_PER_PROFILE} батчей [{pname}] → {MODEL} ===")
            images = [r["path"] for r in rendered]
            total_sizes_kb = sum(r["size_kb"] for r in rendered)
            print(f"  Изображений в батче: {len(images)}, суммарно: {total_sizes_kb:.0f} KB")

            batch_results = []
            for i in range(BATCHES_PER_PROFILE):
                print(f"  Батч {i+1}/{BATCHES_PER_PROFILE}...", end=" ", flush=True)
                t0 = time.time()
                res = call_openrouter(images)
                elapsed = time.time() - t0
                if "error" in res:
                    print(f"ОШИБКА: {res['error']}")
                else:
                    cost = (res["prompt_tokens"] * PRICE_INPUT + res["completion_tokens"] * PRICE_OUTPUT) / 1_000_000
                    print(f"in={res['prompt_tokens']} out={res['completion_tokens']} | ${cost:.5f} | {elapsed:.1f}s")
                batch_results.append(res)
                if i < BATCHES_PER_PROFILE - 1:
                    time.sleep(1.5)

            results[pname] = {
                "rendered":      rendered,
                "total_size_kb": total_sizes_kb,
                "batches":       batch_results,
            }

    # ── Итоги ──────────────────────────────────────────────────────────────────
    print("\n" + "═" * 65)
    print("ИТОГИ СРАВНЕНИЯ")
    print("═" * 65)

    summary = {}
    for pname, data in results.items():
        ok_batches = [b for b in data["batches"] if "error" not in b]
        if not ok_batches:
            print(f"\n[{pname}] — все батчи с ошибками")
            continue
        avg_in  = sum(b["prompt_tokens"]     for b in ok_batches) / len(ok_batches)
        avg_out = sum(b["completion_tokens"] for b in ok_batches) / len(ok_batches)
        avg_cost = (avg_in * PRICE_INPUT + avg_out * PRICE_OUTPUT) / 1_000_000

        print(f"\n[{pname}]")
        print(f"  Файлы блоков:      ", end="")
        for r in data["rendered"]:
            print(f"{r['block_id']}={r['size_kb']:.0f}KB({r['w']}x{r['h']})", end="  ")
        print()
        print(f"  Суммарно KB:        {data['total_size_kb']:.0f} KB")
        print(f"  Avg prompt tokens:  {avg_in:.0f}")
        print(f"  Avg output tokens:  {avg_out:.0f}")
        print(f"  Avg cost/batch:     ${avg_cost:.5f}")
        print(f"  Батчей OK:          {len(ok_batches)}/{BATCHES_PER_PROFILE}")
        summary[pname] = {"avg_in": avg_in, "avg_out": avg_out, "avg_cost": avg_cost, "size_kb": data["total_size_kb"]}

    if len(summary) >= 2:
        pnames = list(summary.keys())
        # базовый профиль — первый (самый дешёвый)
        base_name = pnames[0]
        base = summary[base_name]
        print(f"\n{'─'*65}")
        print(f"{'Профиль':<25} {'Размер':>8} {'Tokens':>8} {'Cost/btch':>10} {'vs 50dpi':>9}")
        print(f"{'─'*25} {'─'*8} {'─'*8} {'─'*10} {'─'*9}")
        for pname in pnames:
            s = summary[pname]
            ratio_cost = s["avg_cost"] / base["avg_cost"] if base["avg_cost"] else 1
            marker = "  ← base" if pname == base_name else f"  +{(ratio_cost-1)*100:.0f}%" if ratio_cost > 1 else f"  -{(1-ratio_cost)*100:.0f}%"
            print(f"  {pname:<23} {s['size_kb']:>6.0f}K {s['avg_in']:>8.0f} ${s['avg_cost']:>9.5f}{marker}")
        print()
        # дельта между крайними
        cheapest = summary[pnames[0]]
        priciest = summary[pnames[-1]]
        abs_delta = priciest["avg_cost"] - cheapest["avg_cost"]
        print(f"Разница {pnames[0]} → {pnames[-1]}: +${abs_delta:.5f}/батч (+{abs_delta/cheapest['avg_cost']*100:.0f}%)")


if __name__ == "__main__":
    main()
