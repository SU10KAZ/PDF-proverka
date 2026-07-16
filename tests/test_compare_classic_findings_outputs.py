import json

from backend.scripts.compare_classic_findings_outputs import compare


def _write_findings(path, findings):
    path.write_text(json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8")


def test_compare_allows_grouped_qf_finding_to_cover_split_baseline(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"

    _write_findings(
        baseline_path,
        [
            {
                "id": "F-051",
                "description": (
                    "Для линий QF3.29, QF3.30 и QF4.14 ток однофазного КЗ 0.142 кА "
                    "равен 8.9 In для автоматов C16; мгновенное отключение не гарантировано."
                ),
            },
            {
                "id": "F-052",
                "description": (
                    "Для линии QF4.11 ток КЗ 0.089 кА равен 5.6 In, а для QF4.12 "
                    "0.12 кА равен 7.5 In; автоматы C16 не подтверждают отключение."
                ),
            },
        ],
    )
    _write_findings(
        candidate_path,
        [
            {
                "id": "F-013",
                "problem": "На нескольких линиях ВРУ-К1.2 ток КЗ недостаточен.",
                "description": (
                    "Для QF3.29, QF3.30 и QF4.14 ток КЗ 0.142 кА равен 8.9 In. "
                    "Для QF4.11 ток КЗ 0.089 кА равен 5.6 In, для QF4.12 0.12 кА "
                    "равен 7.5 In. Во всех случаях автомат C16 не подтверждает "
                    "гарантированное мгновенное отключение."
                ),
            }
        ],
    )

    report = compare(baseline_path, candidate_path, threshold=0.38)

    assert report["matched"] == 2
    assert report["unique_candidate_matches"] == 1
    assert report["candidate_reused_matches"] == 1
    assert report["candidate_recall_vs_baseline"] == 1.0
    assert report["candidate_precision_vs_baseline"] == 1.0


def test_compare_does_not_reuse_candidate_without_shared_technical_refs(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"

    _write_findings(
        baseline_path,
        [
            {
                "id": "F-001",
                "description": "Анкеры БСР М10х100 заданы без производителя и несущей способности.",
            },
            {
                "id": "F-002",
                "description": "Ведомость ссылочных документов не содержит все использованные ГОСТ и СП.",
            },
        ],
    )
    _write_findings(
        candidate_path,
        [
            {
                "id": "F-010",
                "description": (
                    "Ведомость ссылочных документов неполная: не отражены СП 28.13330.2017, "
                    "СП 70.13330.2012, ГОСТ 7798-70 и другие нормативные документы."
                ),
            }
        ],
    )

    report = compare(baseline_path, candidate_path, threshold=0.22)

    assert report["matched"] == 1
    assert report["unique_candidate_matches"] == 1
    assert report["candidate_reused_matches"] == 0


def test_compare_rejects_dimension_finding_without_shared_measurements(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"

    _write_findings(
        baseline_path,
        [
            {
                "id": "F-028",
                "description": (
                    "На схеме установки оборудования на калитке показано расстояние стойки "
                    "800-850 мм, хотя примечание требует не менее 1 м от калитки."
                ),
                "norm": "ГОСТ Р 21.101-2020, п. 5.1.1",
            }
        ],
    )
    _write_findings(
        candidate_path,
        [
            {
                "id": "F-006",
                "description": (
                    "В проекте термин «считыватель» местами ошибочно заменен на «счетчик». "
                    "На схеме установки оборудования на калитке использовано некорректное "
                    "наименование стойки для счетчика."
                ),
                "norm": "ГОСТ Р 21.101-2020, п. 5.1.1",
            }
        ],
    )

    report = compare(baseline_path, candidate_path, threshold=0.22)

    assert report["matched"] == 0
