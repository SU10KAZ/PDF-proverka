import json
from pathlib import Path
import pytest
from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import (
 ALL_REMAINING_PROFILES,build_remaining_graph,evaluate_remaining_gate,render_remaining_markdown)
from backend.app.pipeline.stages.block_grounding.profiled_graph_localization import ru_profile
ROOT=Path(__file__).resolve().parents[1];SS=ROOT/"experiments"/"блоки разных дисциплин"/"СС";M=SS/"ALIA_REMAINING_CORPUS.json"
def cases():return json.loads(M.read_text()) if M.exists() else []
@pytest.fixture(scope="module")
def graphs():return {c["block_id"]:build_remaining_graph(SS/c["output"],block_id=c["block_id"]) for c in cases()}
@pytest.mark.parametrize("case",cases(),ids=lambda c:c["block_id"])
def test_remaining_corpus(case,graphs):
 g=graphs[case["block_id"]];assert g and g["profile_id"] in ALL_REMAINING_PROFILES
 assert g["validation"]["nodes_total"]>0 and evaluate_remaining_gate(g)["use"]
 description=render_remaining_markdown(g)
 assert ru_profile(g["profile_id"]) in description and g["profile_id"] not in description
def test_all_six_families_present(graphs):
 assert {g["profile_id"] for g in graphs.values()}==set(ALL_REMAINING_PROFILES)
 assert len(graphs)==19

def test_plan_graph_has_distinct_axes_routes_and_equipment_ids(graphs):
 for bid in ("4EVJ-MYPD-7P7","9KLR-W3LY-EAA","TQVW-YHVA-NDM"):
  g=graphs[bid];v=g["validation"]
  assert v["axes_total"]>0 and v["route_components_total"]>0 and v["route_segments_total"]>0
  node_ids=[n["id"] for n in g["nodes"]]
  axis_ids=[a["id"] for a in g["grid"]["axes"]]
  assert len(node_ids)==len(set(node_ids)) and not set(node_ids)&set(axis_ids)
  assert evaluate_remaining_gate(g)["readiness"]=="complete"

def test_wiring_reports_only_path_pairs_and_strict_completeness(graphs):
 aps=graphs["6FTW-WUQ7-MUY"]
 assert aps["validation"]["confirmed_connections"]>=10
 assert all(e["edge_state"]=="path_confirmed" for e in aps["edges"])
 gate=evaluate_remaining_gate(aps)
 assert gate["use"] is True and gate["complete"] is True

def test_principle_graph_uses_ports_paths_and_hypernets_without_fake_pairs(graphs):
 for bid in ("9WRW-TQYG-JG6","6LCH-PCEN-6HU"):
  g=graphs[bid];v=g["validation"]
  assert v["apparatus_attach_rate"]>=.7 and v["port_attach_rate"]>=.8
  assert v["confirmed_pair_circuits"]>=2 and v["multi_apparatus_circuits"]>=1
  assert all(edge["edge_state"]=="path_confirmed" for edge in g["edges"])
  pair_networks={network["id"] for network in g["networks"] if network["path_state"]=="confirmed_pair"}
  assert {edge["network_id"] for edge in g["edges"]}<=pair_networks
  assert evaluate_remaining_gate(g)["complete"] is True

def test_all_remaining_profiles_pass_strict_completeness(graphs):
 assert all(evaluate_remaining_gate(graph)["complete"] for graph in graphs.values())
