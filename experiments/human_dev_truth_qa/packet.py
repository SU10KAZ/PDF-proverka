"""Fixed content-based selection and an explicit allowlist for the blind browser."""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
import re

from experiments.foundation_dev_annotation.packet import is_content
from experiments.foundation_dev_annotation.store import encoded
from .audit import ORIGINAL_SHA

NAMESPACE = "HUMAN_DEV_TRUTH_WAVE1_QA"
STATE_DIR = Path("/home/coder/auditmanager/corpus-audits/20260913_human_dev_truth_qa")

# Private audit rationale: never served in the blind UI. Neither model predictions
# nor model agreement are selection inputs. Human labels only ensure both labels
# are represented and identify analogous source structures answered differently.
SELECTION = {
    "sfv3dev_34620f39058fd8a1a964a6d5": "Титульные листы: переход страниц 3–4; сопоставление с другим вводным переходом.",
    "sfv3dev_375ff5c6aa84053580d3072b": "Титульный лист и вводные ведомости на страницах 4–5.",
    "sfv3dev_4c0637ea3f4c30d192b6979c": "Похожие примечания на страницах 28 и 39; длинный промежуток; пара с 40–46 того же документа.",
    "sfv3dev_4d52b7a289095be6915bcdc1": "Похожие примечания на страницах 40 и 46; парный случай с отличающимся человеческим ответом.",
    "sfv3dev_6bfe2ba926ca70a4bea26012": "Спецификация разных групп оборудования и комплектов, повторная нумерация позиций, страницы 28–29.",
    "sfv3dev_fbd71a470c4412f1dc15edab": "Соседние табличные листы: кабельные связи и спецификация оборудования, страницы 23–24.",
    "sfv3dev_b10fab980ec4c3113ff16b8d": "Табличные требования на страницах 126–127: меняются заголовки и число колонок.",
    "sfv3dev_f8e6f1bc79973bbae5ee50db": "Крепёж и описание конструкций на страницах 60–61: меняются заголовки и контекст.",
    "sfv3dev_0dcd836c40dcd609c46c08cd": "Таблица оборудования и табличное описание графика на одной физической странице 46.",
    "sfv3dev_1dd8d73fe302cff8d05630fa": "Принадлежность таблицы между разделами с повторяющимся номером 13 на странице 29.",
}


class QAPacket:
    def __init__(self, source, truth):
        self.source, self.truth = source, truth
        self.original = {c["case_id"]: c for c in truth["cases"]}
        presentations = {c["case_id"]: c for c in source.presentations}
        order = sorted(SELECTION, key=lambda cid: hashlib.sha256(("blind-order-v1:" + cid).encode()).hexdigest())
        self.mapping = {f"qa_{i:02d}": cid for i, cid in enumerate(order, 1)}
        self.presentations, self.sources = [], {}
        self.page_text = {}
        for qa_id, case_id in self.mapping.items():
            c = presentations[case_id]
            panels = []
            for panel in c["panels"]:
                source_id = panel["pdf_url"].split("/")[-1].split("#")[0]
                self.sources[source_id] = source.sources[source_id]
                panels.append({key: deepcopy(panel[key]) for key in
                               ("title", "page", "pdf_url", "image_url", "context", "full_context", "preceding_heading")})
                panels[-1].update(source_id=source_id, page_count=source.sources[source_id]["pages"])
            self.presentations.append({"case_id": qa_id, "question": c["question"], "document": c["document"], "panels": panels})
        docs = json.loads((source.root / "dev_documents.json").read_text())
        for doc in docs:
            sid = "src_" + doc["artifacts"]["pdf"]["sha256"]
            if sid not in self.sources:
                continue
            chunks = re.split(r"^## Page (\d+)\s*$", Path(doc["artifacts"]["work_md"]["path"]).read_text(), flags=re.M)
            self.page_text[sid] = {int(n): [line for line in body.splitlines() if is_content(line)]
                                   for n, body in zip(chunks[1::2], chunks[2::2])}
        self.public_packet = {"namespace": NAMESPACE, "cases": self.presentations}
        self.sha256 = hashlib.sha256(encoded(self.public_packet)).hexdigest()
        self.selection = {"namespace": NAMESPACE, "original_sha256": ORIGINAL_SHA,
                          "packet_sha256": source.sha256, "blind_packet_sha256": self.sha256,
                          "prediction_inputs": [], "selection_basis": "Source content, human label coverage and analogous human cases only",
                          "cases": [{"qa_id": qid, "case_id": cid, "reason": SELECTION[cid],
                                     "kind": self.original[cid]["kind"], "original_answer": self.original[cid]["human_answer"],
                                     "source_anchors": self.original[cid]["source_anchors"]} for qid, cid in self.mapping.items()]}

    def page_image(self, source_id, page):
        if source_id not in self.sources:
            raise KeyError(source_id)
        return self.source.page_image(source_id, page)
