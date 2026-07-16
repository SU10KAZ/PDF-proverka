from backend.app.pipeline.stages.block_grounding.profiled_graph_localization import (
    package_display,
    ru_node_type,
    ru_profile,
    ru_state,
)
from backend.app.pipeline.stages.block_grounding.alia_scheme_geometry import (
    render_alia_scheme_markdown,
)
from backend.app.pipeline.stages.block_grounding.alia_remaining_geometry import (
    render_remaining_markdown,
)


def test_equipotential_graph_is_localized_without_changing_machine_codes():
    package = {
        "profile_id": "equipotential_scheme",
        "source_kind": "structured_electrical",
        "graph": {
            "nodes": [
                {"id": "node-1", "label": "ГРЩ", "node_type": "panel", "field_state": "present"},
                {"id": "node-2", "label": "PE", "node_type": "bonding_target", "field_state": "present"},
            ],
            "networks": [{
                "id": "network-1", "label": "PE", "network_type": "equipotential_bonding",
                "path_state": "same_cad_component", "endpoint_ids": ["node-1", "node-2"],
            }],
            "edges": [{
                "from": "node-1", "to": "node-2", "edge_type": "grounding_or_bonding",
                "edge_state": "nearest_geometry",
            }],
        },
    }

    display = package_display(package)

    assert display["profile_title"] == "Система уравнивания потенциалов"
    assert display["source_title"] == "структурированный граф ЭОМ"
    assert display["nodes"][0]["type_title"] == "щит или панель"
    assert display["nodes"][0]["state_title"] == "извлечено из текстового слоя PDF"
    assert display["nodes"][0]["needs_review"] is False
    assert display["nodes_review_total"] == 0
    assert display["networks"][0]["type_title"] == "связь уравнивания потенциалов"
    assert display["edges"][0]["from_label"] == "ГРЩ"
    assert display["edges"][0]["to_label"] == "PE"
    assert display["edges"][0]["type_title"] == "заземление или уравнивание потенциалов"
    # Машинный контракт графа не локализуется и не мутируется.
    assert package["graph"]["nodes"][0]["node_type"] == "panel"


def test_only_non_routine_node_provenance_is_marked_for_review():
    package = {
        "profile_id": "architecture_node_detail",
        "source_kind": "structured_architecture",
        "graph": {
            "nodes": [
                {"id": "exact", "label": "Дверь", "node_type": "door", "field_state": "present"},
                {"id": "weak", "label": "Контур", "node_type": "opening", "field_state": "geometry_only"},
            ],
            "containers": [], "networks": [], "edges": [],
        },
    }

    display = package_display(package)

    assert display["nodes"][0]["needs_review"] is False
    assert display["nodes"][1]["needs_review"] is True
    assert display["nodes"][1]["state_title"] == "подтверждено только геометрией"
    assert display["nodes_review_total"] == 1


def test_unknown_machine_values_have_russian_safe_fallbacks():
    assert ru_profile("future_profile") == "Графический блок по предметному профилю"
    assert ru_node_type("future_node") == "элемент схемы"
    assert ru_state("future_state") == "состояние подтверждено структурой блока"


def test_chandra_block_title_is_visible_without_replacing_machine_profile():
    package = {
        "profile_id": "ar_floor_wall_junction_detail",
        "source_kind": "structured_architecture",
        "classification": {
            "source": "chandra_md",
            "block_title": "Схемы гидроизоляции и примыкания плавающего пола.",
        },
        "graph": {"nodes": [], "containers": [], "networks": [], "edges": []},
    }

    display = package_display(package)

    assert display["profile_title"] == "Узел примыкания пола и стены"
    assert display["block_title"] == "Схемы гидроизоляции и примыкания плавающего пола."
    assert display["classification_source"] == "chandra_md"


def test_alia_markdown_hides_node_network_and_state_codes():
    graph = {
        "profile_id": "external_terminal_wiring",
        "source": {"pdf_file": "схема.pdf"},
        "nodes": [{"id": "n1", "label": "ШАУВ", "node_type": "control_cabinet"}],
        "containers": [],
        "networks": [{
            "network_type": "external_wiring", "label": "ШАУВ external wiring",
            "path_state": "column_and_terminal_labels", "endpoint_ids": ["n1"],
        }],
        "edges": [],
        "validation": {"nodes_total": 1, "containers_total": 0, "networks_total": 1, "edges_total": 0},
        "readiness": {"status": "topology_partial", "reasons": []},
        "warnings": [],
    }

    markdown = render_alia_scheme_markdown(graph)

    assert "Схема внешних клеммных подключений" in markdown
    assert "шкаф управления" in markdown
    assert "внешние подключения" in markdown
    assert "подтверждено колонкой и подписями клемм" in markdown
    for machine_code in ("external_terminal_wiring", "control_cabinet", "external_wiring",
                         "column_and_terminal_labels", "topology_partial"):
        assert machine_code not in markdown


def test_remaining_markdown_uses_russian_profile_subtype_and_node_names():
    graph = {
        "profile_id": "installation_assembly",
        "nodes": [{"id": "n1", "label": "Кронштейн", "node_type": "assembly_part"}],
        "containers": [], "networks": [], "edges": [], "warnings": [],
        "validation": {"subtype": "assembly", "nodes_total": 1, "containers_total": 0, "line_segments": 4},
        "readiness": {"status": "complete", "reasons": []},
    }

    markdown = render_remaining_markdown(graph)

    assert "Монтажный узел слаботочных систем" in markdown
    assert "монтажная сборка" in markdown
    assert "элемент сборки" in markdown
    assert "installation_assembly" not in markdown
    assert "assembly_part" not in markdown
