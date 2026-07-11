"""Мониторинг аудита AR7 V2 через live-API до завершения."""
import time
import json
import urllib.request
import urllib.error
import urllib.parse

import backend.app.core.config  # load_dotenv
from backend.app.core import portal_auth

s = portal_auth.get_settings()
C = f"{s.cookie_name}={portal_auth.issue_token('andrey', s)}"
PID = urllib.parse.quote("AR/133-23-ГК-АР7", safe="")


def get(path):
    r = urllib.request.Request("http://127.0.0.1:8081" + path, method="GET")
    r.add_header("Cookie", C)
    try:
        with urllib.request.urlopen(r, timeout=30) as x:
            return json.loads(x.read().decode())
    except urllib.error.HTTPError as e:
        return {"_http": e.code, "_body": e.read().decode()[:200]}
    except Exception as e:
        return {"_err": str(e)}


DONE = {"completed", "failed", "cancelled", "error"}
last = None
for i in range(400):  # до ~3.3 ч
    st = get(f"/api/audit/{PID}/status?version_id=v002")
    stage = st.get("stage")
    status = str(st.get("status", "")).lower()
    line = f"[{time.strftime('%H:%M:%S')}] stage={stage} status={status} cost=${st.get('cost_usd',0):.3f}"
    if line != last:
        print(line, flush=True)
        last = line
    if status in DONE:
        print(f"=== FINISHED: {status} ===", flush=True)
        print(json.dumps(st, ensure_ascii=False)[:600], flush=True)
        break
    time.sleep(30)
