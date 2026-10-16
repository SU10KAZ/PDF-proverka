from backend.app.services.version_comparison.store import _name_hint, _suggest_label


def test_version_labels_start_at_v1_and_preserve_revision_and_date():
    for number in range(12):
        label = _suggest_label({'detected': {'file_number': number, 'izm_max': 7,
                                             'release_dates': ['02.07.2026']}})
        assert label == f'V{number + 1} Изм.7 02.07.2026'


def test_pdf_without_index_is_v1_and_suffix_counts_from_v2():
    for suffix, expected in [("", "V1"), (" (0)", "V1"), (" (1)", "V2"), (" (2)", "V3")]:
        name = f"133_23-ГК-АК{suffix}.pdf"
        detected = _name_hint(name)
        assert _suggest_label({"file_name": name, "detected": detected}) == expected
        detected.update(izm_max=3, release_dates=["04.02.2026"])
        assert _suggest_label({"file_name": name, "detected": detected}) == f"{expected} Изм.3 04.02.2026"
