"""
Тест: сравнение читаемости мелкого текста на планах при 100 DPI vs 300 DPI
через локальную Qwen модель (qwen3.6-35b).

20 блоков с наибольшим ocr_text_len из КЖ5.22 (конструктивные планы).
Каждый блок рендерится в двух профилях, отправляется в Qwen с просьбой
перечислить все читаемые маркировки/размеры/текстовые метки.

Итог: сравниваем, сколько текста Qwen извлекает при 100 vs 300 DPI.
"""

import base64
import json
import os
import sys
import time
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# ── проект ─────────────────────────────────────────────────────────────────────
PROJECT_DIR = ROOT / "projects" / "214. Alia (ASTERUS)" / "KJ" / "13АВ-РД-КЖ5.22-28.1-К1.pdf"
PDF_PATH    = PROJECT_DIR / "13АВ-РД-КЖ5.22-28.1-К1.pdf"
INDEX_PATH  = PROJECT_DIR / "_output" / "blocks" / "index.json"
RESULT_JSON = PROJECT_DIR / "13АВ-РД-КЖ5.22-28.1-К1_result.json"

# ── профили ────────────────────────────────────────────────────────────────────
PROFILES = {
    "dpi100_min800": {"dpi": 100, "min_long_side": 800},
    "dpi300_min800": {"dpi": 300, "min_long_side": 800},
}
N_BLOCKS = 20   # топ-20 по ocr_text_len

# ── Qwen endpoint ──────────────────────────────────────────────────────────────
from webapp.config import CHANDRA_BASE_URL, CHANDRA_BASIC_USER, CHANDRA_BASIC_PASS
QWEN_URL   = f"{CHANDRA_BASE_URL}/v1/chat/completions"
QWEN_MODEL = "qwen/qwen3.6-35b-a3b"

# ── промпт ─────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = "Ты — эксперт по чтению технических чертежей и конструктивных планов."

USER_PROMPT = """\
Посмотри на фрагмент конструктивного чертежа.

Задача: извлечь ВСЕ читаемые текстовые элементы — маркировки, позиции, \
размерные надписи, метки осей, примечания, шрифтовые метки.

Ответь в формате:
ЧИТАЕМО: <список через ; — всё что смог прочесть>
НЕЧИТАЕМО: <что видно, но текст размыт/нечёткий>
ИТОГ: <кол-во читаемых элементов> элементов прочитано из примерно <оценка всего> видимых
"""


# ─── helpers ──────────────────────────────────────────────────────────────────

def load_page_dims(result_json: Path) -> dict[int, tuple[int, int]]:
    data = json.loads(result_json.read_text(encoding="utf-8"))
    dims = {}
    for pg in data.get("pages", []):
        pn = pg.get("page_number") or pg.get("page_num") or pg.get("page")
        if pn is not None:
            dims[int(pn)] = (int(pg["width"]), int(pg["height"]))
    return dims


def render_block(
    pdf_path: Path,
    page_num: int,       # 1-based
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


def call_qwen(image_path: Path) -> dict:
    import urllib.request, urllib.error
    import base64 as b64

    img_b64 = b64.b64encode(image_path.read_bytes()).decode()
    payload = json.dumps({
        "model": QWEN_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "image_url",
                 "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
                {"type": "text", "text": USER_PROMPT},
            ]},
        ],
        "max_tokens": 1500,
        "temperature": 0.1,
    }).encode()

    creds = base64.b64encode(f"{CHANDRA_BASIC_USER}:{CHANDRA_BASIC_PASS}".encode()).decode()
    req = urllib.request.Request(
        QWEN_URL,
        data=payload,
        headers={
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        return {"error": f"HTTP {e.code}: {body[:200]}"}
    except Exception as e:
        return {"error": str(e)[:200]}

    msg = data.get("choices", [{}])[0].get("message", {})
    # Qwen3 thinking mode: финальный ответ в content, размышления в reasoning_content.
    # Если content пустой (токены ушли на thinking) — берём reasoning_content.
    content = msg.get("content", "") or msg.get("reasoning_content", "")
    usage = data.get("usage", {})
    return {
        "text": content,
        "prompt_tokens": usage.get("prompt_tokens", 0),
        "completion_tokens": usage.get("completion_tokens", 0),
    }


def extract_readable_count(text: str) -> int:
    """Извлечь число прочитанных элементов из строки ИТОГ."""
    import re
    m = re.search(r"ИТОГ[:\s]*(\d+)", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # fallback: считаем точки с запятой в ЧИТАЕМО
    m2 = re.search(r"ЧИТАЕМО[:\s]*(.+?)(?:\n|НЕЧИТАЕМО|ИТОГ|$)", text, re.DOTALL | re.IGNORECASE)
    if m2:
        items = [x.strip() for x in m2.group(1).split(";") if x.strip() and x.strip() != "—"]
        return len(items)
    return 0


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    if not CHANDRA_BASE_URL:
        print("[ERROR] CHANDRA_BASE_URL не задан в .env")
        sys.exit(1)

    # Загрузка метаданных блоков
    idx = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    all_blocks = {b["block_id"]: b for b in idx["blocks"]}
    blocks_sorted = sorted(
        (b for b in idx["blocks"] if b.get("crop_px")),
        key=lambda b: b.get("ocr_text_len", 0),
        reverse=True,
    )
    selected = blocks_sorted[:N_BLOCKS]

    print(f"Проект: {PROJECT_DIR.name}")
    print(f"Блоков для теста: {len(selected)}")
    print(f"Qwen endpoint: {QWEN_URL}")
    print()

    page_dims = load_page_dims(RESULT_JSON)

    results = {}  # profile_name → list of per-block results

    with tempfile.TemporaryDirectory(prefix="qwen_dpi_test_") as tmpdir:
        tmp = Path(tmpdir)

        for pname, pconf in PROFILES.items():
            dpi      = pconf["dpi"]
            min_side = pconf["min_long_side"]
            prof_dir = tmp / pname
            prof_dir.mkdir()
            print(f"{'═'*60}")
            print(f"ПРОФИЛЬ: {pname}  (dpi={dpi}, min_long_side={min_side})")
            print(f"{'═'*60}")

            block_results = []

            for i, bm in enumerate(selected, 1):
                bid  = bm["block_id"]
                page = bm["page"]
                pw, ph = page_dims.get(page, (0, 0))
                if pw == 0 or ph == 0:
                    print(f"  [{i:2d}] {bid}: нет dims для стр.{page} — пропуск")
                    continue

                out_png = prof_dir / f"block_{bid}.png"
                try:
                    info = render_block(
                        PDF_PATH, page, bm["crop_px"],
                        pw, ph, dpi, min_side, out_png,
                    )
                except Exception as e:
                    print(f"  [{i:2d}] {bid}: ошибка рендера: {e}")
                    continue

                print(f"  [{i:2d}] {bid}: {info['w']}x{info['h']}px, {info['size_kb']:.0f}KB | отправляю Qwen...", end=" ", flush=True)
                t0 = time.time()
                res = call_qwen(out_png)
                elapsed = time.time() - t0

                if "error" in res:
                    print(f"ОШИБКА: {res['error']}")
                    block_results.append({"block_id": bid, "error": res["error"], **info})
                    continue

                readable = extract_readable_count(res["text"])
                print(f"OK ({elapsed:.0f}s) | читаемых: {readable} | in={res['prompt_tokens']} out={res['completion_tokens']}")

                block_results.append({
                    "block_id": bid,
                    "ocr_text_len": bm.get("ocr_text_len", 0),
                    "readable_count": readable,
                    "render": info,
                    "usage": {
                        "prompt": res["prompt_tokens"],
                        "completion": res["completion_tokens"],
                    },
                    "response": res["text"],
                })

                time.sleep(0.5)

            results[pname] = block_results

    # ── Сравнение ───────────────────────────────────────────────────────────────
    print(f"\n{'═'*65}")
    print("ИТОГИ: 100 DPI vs 300 DPI — читаемость текста Qwen")
    print(f"{'═'*65}")

    # Таблица по блокам
    print(f"\n{'block_id':<22} {'ocr_len':>7} | {'100dpi':>8} {'sz100':>6} | {'300dpi':>8} {'sz300':>6} | {'delta':>6}")
    print(f"{'─'*22} {'─'*7} | {'─'*8} {'─'*6} | {'─'*8} {'─'*6} | {'─'*6}")

    p0_name, p1_name = list(PROFILES.keys())
    by_id_0 = {r["block_id"]: r for r in results.get(p0_name, []) if "error" not in r}
    by_id_1 = {r["block_id"]: r for r in results.get(p1_name, []) if "error" not in r}

    total_0 = total_1 = 0
    total_sz_0 = total_sz_1 = 0
    compared = 0

    for bm in selected:
        bid = bm["block_id"]
        r0  = by_id_0.get(bid)
        r1  = by_id_1.get(bid)
        if not r0 or not r1:
            continue
        rc0 = r0["readable_count"]
        rc1 = r1["readable_count"]
        sz0 = r0["render"]["size_kb"]
        sz1 = r1["render"]["size_kb"]
        delta = rc1 - rc0
        sign  = "+" if delta > 0 else ""
        print(f"{bid:<22} {bm.get('ocr_text_len',0):>7} | {rc0:>8} {sz0:>5.0f}K | {rc1:>8} {sz1:>5.0f}K | {sign}{delta:>5}")
        total_0 += rc0
        total_1 += rc1
        total_sz_0 += sz0
        total_sz_1 += sz1
        compared += 1

    if compared == 0:
        print("Нет данных для сравнения.")
        return

    print(f"{'─'*22} {'─'*7} | {'─'*8} {'─'*6} | {'─'*8} {'─'*6} | {'─'*6}")
    delta_total = total_1 - total_0
    sign = "+" if delta_total > 0 else ""
    print(f"{'ИТОГО':>30} | {total_0:>8} {total_sz_0:>5.0f}K | {total_1:>8} {total_sz_1:>5.0f}K | {sign}{delta_total:>5}")
    print()
    print(f"Блоков сравнено:   {compared}")
    print(f"Файлы 100 DPI:     {total_sz_0:.0f} KB суммарно")
    print(f"Файлы 300 DPI:     {total_sz_1:.0f} KB суммарно  ({total_sz_1/total_sz_0:.1f}× больше)")
    print(f"Читаемых 100 DPI:  {total_0} элементов")
    print(f"Читаемых 300 DPI:  {total_1} элементов  ({total_1/total_0:.2f}× от 100dpi)" if total_0 else "")

    if total_1 > total_0 * 1.15:
        print(f"\nВывод: 300 DPI даёт значимо больше читаемого текста (+{total_1-total_0} элементов, +{(total_1/total_0-1)*100:.0f}%)")
    elif total_1 > total_0 * 1.05:
        print(f"\nВывод: 300 DPI чуть лучше (+{(total_1/total_0-1)*100:.0f}%), но разница небольшая")
    else:
        print(f"\nВывод: 100 DPI достаточно — 300 DPI не даёт значимого улучшения")

    # Сохраняем детализированный JSON
    out_json = ROOT / "scripts" / "test_qwen_dpi_results.json"
    out_json.write_text(
        json.dumps({"profiles": list(PROFILES.keys()), "results": results}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nДетали → {out_json}")


if __name__ == "__main__":
    main()
