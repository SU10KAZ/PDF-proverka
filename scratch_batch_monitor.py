"""Монитор батча из 4 проектов (live). Логирует стадии/падения, выходит когда батч завершён."""
import time
import json
import urllib.request
import urllib.error

import backend.app.core.config  # load_dotenv
from backend.app.core import portal_auth

s = portal_auth.get_settings()
C = f"{s.cookie_name}={portal_auth.issue_token('andrey', s)}"


def g(path):
    r = urllib.request.Request("http://127.0.0.1:8081" + path)
    r.add_header("Cookie", C)
    try:
        with urllib.request.urlopen(r, timeout=25) as x:
            return json.loads(x.read().decode())
    except Exception as e:
        return {"_err": str(e)}


def now():
    return time.strftime("%H:%M:%S")


prev = {}
prev_stage = {}
start = time.time()
for _ in range(120):  # ~4ч при шаге 120с
    b = g("/api/audit/batch/status")
    if b.get("_err"):
        print(f"[{now()}] batch status err: {b['_err']}", flush=True)
        time.sleep(120)
        continue
    active = b.get("active")
    items = (b.get("queue") or {}).get("items", [])
    # статусы проектов
    for it in items:
        pid = it["project_id"]
        st = it.get("status")
        if prev.get(pid) != st:
            print(f"[{now()}] {pid}: {prev.get(pid)} → {st}"
                  + (f"  ERR={it.get('error')}" if it.get("error") else ""), flush=True)
            prev[pid] = st
    # текущая стадия running-проекта
    live = g("/api/audit/live-status").get("running", {})
    for pid, info in live.items():
        if pid == "__BATCH__":
            continue
        stg = f"{info.get('stage')}/{info.get('status')}"
        if prev_stage.get(pid) != stg:
            print(f"[{now()}]   стадия {pid}: {stg}", flush=True)
            prev_stage[pid] = stg
    if not active:
        print(f"[{now()}] === БАТЧ ЗАВЕРШЁН ===", flush=True)
        for it in items:
            print(f"   {it['project_id']}: {it.get('status')}"
                  + (f"  ERR={it.get('error')}" if it.get("error") else ""), flush=True)
        break
    time.sleep(120)
else:
    print(f"[{now()}] === ПРЕДОХРАНИТЕЛЬ 4ч — монитор вышел, батч ещё активен ===", flush=True)
