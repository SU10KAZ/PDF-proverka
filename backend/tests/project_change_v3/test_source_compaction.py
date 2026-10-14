"""Exact input equivalence and request savings, with synthetic sources only."""
from copy import deepcopy
import json

from backend.app.services.project_change_v3 import source_compaction as sc
from backend.app.services.project_change_v3.provider import build_codex_payload


def test_dictionary_keeps_order_counts_whitespace_and_cross_field_references():
    line = '  Площадь 1,90 м²\t🙂\r\n'
    shared = 'Точная таблица\t1,80\r\n' * 200
    data = {'pages': [{'native_page_text': line * 1000 + '\nКонец без перевода строки',
                       'blocks': [{'text': shared}, {'text': shared}]}]}
    before = deepcopy(data)
    encoded, receipt = sc.compact(data)
    assert sc.decode(encoded) == data == before
    assert receipt['roundtrip_verified'] is True
    assert receipt['encoded_source_bytes'] < receipt['raw_source_bytes'] / 5
    assert encoded['strings']
    assert sc.LINES in encoded['data']['pages'][0]['native_page_text']
    wire_receipt = {}
    payload, images, labels = build_codex_payload('prompt', data, [], compaction_receipt=wire_receipt)
    assert sc.decode(json.loads(payload.split('\nSOURCE DATA:\n')[1])) == data
    assert wire_receipt == receipt and images == labels == []


def test_unique_large_text_and_small_inputs_keep_original_wire_format():
    for text in ('small', ''.join(f'unique source line {i}\n' for i in range(1000))):
        data = {'pages': [{'native_page_text': text}]}
        encoded, receipt = sc.compact(data)
        assert encoded is data and receipt == {}


def test_reserved_marker_in_original_data_disables_encoding():
    for key in (sc.REF, sc.LINES, sc.SEGMENTS):
        data = {'pages': [{'native_page_text': 'abc\n' * 5000, 'blocks': [{key: 'literal'}]}]}
        assert sc.compact(data) == (data, {})


def test_alternating_repeated_lines_and_empty_final_line_roundtrip():
    data = {'pages': [{'native_page_text': (' A ' * 100 + '\r\n' + ' B ' * 100 + '\n') * 100}]}
    encoded, receipt = sc.compact(data)
    assert receipt and sc.decode(encoded) == data


def test_unique_pdf_lines_with_variable_layout_padding_are_compact_and_exact():
    text = ''.join(f'Помещение {i}' + ' ' * (400 + i) + f'Площадь {i},90 м²\r\n'
                   for i in range(200)) + '\t' * 500 + 'Конец🙂'
    data = {'pages': [{'native_page_text': text, 'blocks': [{'structured_md': 'без изменений'}]}]}
    before = deepcopy(data)
    encoded, receipt = sc.compact(data)
    assert sc.SEGMENTS in encoded['data']['pages'][0]['native_page_text']
    assert receipt['encoded_source_bytes'] < receipt['raw_source_bytes'] / 5
    assert sc.decode(encoded) == data == before
    wire, _, _ = build_codex_payload('prompt', data, [])
    assert sc.decode(json.loads(wire.split('\nSOURCE DATA:\n')[1])) == data


def test_padding_at_text_boundaries_and_dictionary_refs_in_segments_roundtrip():
    shared = ('Уникальный длинный фрагмент с числами 1,80 и 1,90. ' * 10).rstrip()
    text = ' ' * 9000 + shared + '\t' * 5000 + shared + ' ' * 1000
    data = {'pages': [{'native_page_text': text}]}
    encoded, receipt = sc.compact(data)
    assert encoded['strings'] and receipt
    assert sc.decode(encoded) == data


def test_ordinary_whitespace_in_unique_text_keeps_original_representation():
    text = ''.join(f'Строка {i}:  1,80\t1,90\n' for i in range(500))
    data = {'pages': [{'native_page_text': text}]}
    assert sc.compact(data) == (data, {})
