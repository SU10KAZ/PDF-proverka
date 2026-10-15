from backend.app.services.version_comparison.store import _suggest_label


def test_version_labels_start_at_v1_and_preserve_revision_and_date():
    for number in range(12):
        label = _suggest_label({'detected': {'file_number': number, 'izm_max': 7,
                                             'release_dates': ['02.07.2026']}})
        assert label == f'V{number + 1} Изм.7 02.07.2026'
