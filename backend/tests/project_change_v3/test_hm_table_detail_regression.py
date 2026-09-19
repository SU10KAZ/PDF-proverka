"""Regression (17e83cec): the Human Mapping TABLE detail panel crashed on string tables.

The detail renderer is taken from the page the server actually serves and run
in node against every TABLE block of both sealed fixtures (A/B) plus the table
shapes the generic V3 builder can emit.  Zero model calls.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import human_mapping

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is required to run the served renderer")

HARNESS = r"""
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const assetUrl=u=>u;
%s
const cases=JSON.parse(require('fs').readFileSync(0,'utf8'));
const out=[];
for(const c of cases){
  try{const html=showDetail(c.side,c.block.id,{old_blocks:c.side==='OLD'?[c.block]:[],new_blocks:c.side==='NEW'?[c.block]:[],pages:{OLD:[],NEW:[]}});
      out.push({id:c.block.id,ok:true,table:html.includes('<table>'),pre:html.includes('<pre>'),html:html.length});}
  catch(e){out.push({id:c.block.id,ok:false,error:String(e)});}
}
process.stdout.write(JSON.stringify(out));
"""


@pytest.fixture(scope="module")
def client():
    app = FastAPI()
    app.include_router(human_mapping.router)
    app.include_router(human_mapping.api_router)
    return TestClient(app)


def served_renderer(client) -> str:
    page = client.get("/human-mapping/?object=4f3e5916&pair=A").text
    functions = [re.search(rf"function {name}\(.*?\n", page).group(0) for name in ("tableHtml", "findBlock", "showDetail")]
    return "\n".join(f.rstrip("\n") for f in functions)


def render(client, cases):
    script = HARNESS % served_renderer(client)
    run = subprocess.run([shutil.which("node"), "-e", script], input=json.dumps(cases, ensure_ascii=False),
                         capture_output=True, text=True, check=True)
    return json.loads(run.stdout)


@pytest.mark.parametrize("letter", ["A", "B"])
def test_every_sealed_table_block_renders(client, letter):
    data = client.get(f"/api/human-mapping/objects/4f3e5916/comparisons/{letter}/ui-data").json()
    cases = [{"side": side, "block": block}
             for region in data["regions"] for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks"))
             for block in region[key] if block["type"] == "TABLE"]
    assert cases, "the sealed fixture has TABLE blocks"
    assert any(isinstance(t, str) for c in cases for t in c["block"].get("tables") or [])
    results = render(client, cases)
    assert all(r["ok"] for r in results), [r for r in results if not r["ok"]][:3]
    assert all(r["table"] or r["pre"] for r in results)


def test_table_shapes_of_the_generic_builder(client):
    base = {"type": "TABLE", "page": 1, "bbox": [0.1, 0.1, 0.5, 0.5]}
    shapes = {
        "markdown_string": ["| Поз. | Расход |\n|---|---|\n| П1 | 1200 м3/ч |"],
        "array_rows": [[["Поз.", "Расход"], ["П1", "1200 м3/ч"]]],
        "plain_text": ["без разметки таблицы"],
        "empty_list": [],
        "missing": None,
    }
    cases = [{"side": "NEW", "block": {**base, "id": name, **({"tables": tables} if tables is not None else {})}}
             for name, tables in shapes.items()]
    results = {r["id"]: r for r in render(client, cases)}
    assert all(r["ok"] for r in results.values()), results
    assert results["markdown_string"]["table"] and results["array_rows"]["table"]
    assert results["plain_text"]["pre"] and not results["plain_text"]["table"]
