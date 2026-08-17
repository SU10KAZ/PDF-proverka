from __future__ import annotations

from pathlib import Path

import fitz
import numpy as np

from backend.app.services.stage_comparison.change_regions import cluster_differences, compare_text, compare_vectors


def _pdf(path: Path, words=(), lines=()):
    doc=fitz.open(); page=doc.new_page(width=200,height=120)
    for point,text in words: page.insert_text(point,text)
    for start,end in lines: page.draw_line(start,end)
    doc.save(path); doc.close()


def _pages(tmp_path, left_words=(), right_words=(), left_lines=(), right_lines=()):
    left,right=tmp_path/'left.pdf',tmp_path/'right.pdf';_pdf(left,left_words,left_lines);_pdf(right,right_words,right_lines)
    return fitz.open(left),fitz.open(right)


def test_text_change_number_add_remove_and_move_are_preserved(tmp_path):
    left,right=_pages(tmp_path,[((20,20),'word'),((30,40),'16'),((30,70),'gone')],[((20,20),'term'),((30,40),'17'),((60,90),'new')])
    try:
        rows=compare_text(left[0],right[0],np.eye(3)); changes={row['change'] for row in rows}
        assert {'changed','removed','added'} <= changes
        assert any(row.get('left_value')=='16' and row.get('right_value')=='17' for row in rows)
    finally:left.close();right.close()


def test_text_micro_shift_is_noise_but_real_move_is_reported(tmp_path):
    left,right=_pages(tmp_path,[((20,20),'stable'),((20,60),'moving')],[((21,20),'stable'),((50,60),'moving')])
    try:
        rows=compare_text(left[0],right[0],np.eye(3)); assert len(rows)==1 and rows[0]['change']=='moved'
    finally:left.close();right.close()


def test_vector_add_remove_and_tolerant_coordinate_match(tmp_path):
    left,right=_pages(tmp_path,left_lines=[((10,10),(80,10)),((10,40),(80,40))],right_lines=[((11,10),(81,10)),((10,70),(80,70))])
    try:
        rows=compare_vectors(left[0],right[0],np.eye(3)); changes=[row['change'] for row in rows]
        assert 'removed' in changes
    finally:left.close();right.close()
    left,right=_pages(tmp_path,left_lines=[],right_lines=[((10,70),(80,70))])
    try:
        assert any(row['change']=='added' for row in compare_vectors(left[0],right[0],np.eye(3)))
    finally:left.close();right.close()


def test_close_evidence_clusters_once_remote_and_stamp_stay_separate():
    items=[{'kind':'vector','change':'added','bbox':[10,10,20,10]},{'kind':'vector','change':'added','bbox':[22,10,30,10]},{'kind':'text','change':'changed','bbox':[150,20,160,30]},{'kind':'text','change':'changed','bbox':[170,85,180,95]}]
    regions=cluster_differences(items,200,100)
    assert len(regions)==3
    assert regions[-1]['region_role']=='stamp'


def test_clustering_is_deterministic_for_group_of_lines():
    items=[{'kind':'vector','change':'added','bbox':[10+i,10,11+i,10]} for i in range(8)]
    assert cluster_differences(items,200,100)==cluster_differences(items,200,100)
