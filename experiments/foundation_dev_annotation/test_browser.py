"""Real Chromium tests; all answers go to disposable synthetic registries."""
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import json
import os
import unittest

from .packet import NAMESPACE, Packet
from .server import AnnotationServer
from .store import Store
from .test_annotation import answer

try:
    from playwright.sync_api import sync_playwright, expect
except ImportError:
    sync_playwright = None


@unittest.skipUnless(sync_playwright, "Install Playwright in the test environment")
class BrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packet = Packet()
        cls.playwright = sync_playwright().start()
        executable = os.environ.get("WAVE1_CHROMIUM")
        cls.browser = cls.playwright.chromium.launch(headless=True, **({"executable_path": executable} if executable else {}))

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.tmp = TemporaryDirectory(prefix="wave1-browser-synthetic-")
        self.directory = Path(self.tmp.name) / NAMESPACE
        self.store = Store(self.packet, self.directory)
        self.start_server()
        self.context = self.browser.new_context(viewport={"width": 1440, "height": 1050})
        self.page = self.context.new_page()
        self.errors = []
        self.page.on("pageerror", lambda error: self.errors.append(str(error)))
        self.page.goto(self.base)
        expect(self.page.locator("#task")).to_be_visible()

    def start_server(self, port=0):
        self.server = AnnotationServer(self.packet, self.store, port=port)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def stop_server(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def tearDown(self):
        self.context.close()
        self.stop_server()
        self.tmp.cleanup()
        self.assertEqual(self.errors, [])

    def screenshot(self, name):
        if folder := os.environ.get("WAVE1_SCREENSHOTS"):
            path = Path(folder); path.mkdir(parents=True, exist_ok=True)
            self.page.screenshot(path=str(path / (name + ".png")), full_page=True)

    def select_case(self, case):
        self.page.locator("#case-select").select_option(case["case_id"])
        expect(self.page.locator("#case-select")).to_have_value(case["case_id"])

    def save(self, value):
        self.page.locator(f'#answers button[data-answer="{value}"]').click()
        self.page.locator("#save").click()
        expect(self.page.locator("#status")).to_have_text("Ответ сохранён")

    def test_all_nine_semantic_choices_simple_ui_pdf_and_mobile(self):
        self.screenshot("section")
        visible = self.page.locator("body").inner_text()
        self.assertNotRegex(visible, r"\b(?:SAME|NEW|REVIEW|PROVEN|DEFAULT|T[1-7]|S[1-5]|BoundaryDecision|sfv3dev_\w+)\b")
        self.assertNotIn("22", self.page.locator("#progress").inner_text())
        self.assertEqual(self.page.locator("#case-select option").count(), 104)
        self.assertEqual(self.page.locator(".anchor-preview").count(), 2)
        expected_questions = {"SECTION": "Это один и тот же смысловой раздел?", "TABLE": "Это одна и та же таблица, которая продолжается?",
                              "OWNER": "Этот фрагмент относится к показанному разделу или таблице?"}
        for kind in ("SECTION", "TABLE", "OWNER"):
            case = next(c for c in self.packet.cases if c["kind"] == kind)
            for revision, value in enumerate(("YES", "NO", "UNSURE"), 1):
                self.select_case(case)
                expect(self.page.locator("#question")).to_have_text(expected_questions[kind])
                if revision == 1:
                    self.screenshot(kind.lower())
                self.save(value)
                record = self.store.state()["records"][case["case_id"]]
                self.assertEqual(record["human_answer"], value)
                self.assertEqual(record["revision"], revision)
                self.assertEqual(self.page.locator('#answers button[aria-pressed="true"]').count(), 0)
                expect(self.page.locator("#save")).to_be_disabled()
        link = self.page.get_by_role("link", name="Открыть PDF", exact=True).first
        self.assertRegex(link.get_attribute("href"), r"^/pdf/src_[0-9a-f]{64}#page=\d+$")
        with self.page.expect_popup() as popup_info:
            link.click()
        popup = popup_info.value
        popup.wait_for_load_state("domcontentloaded")
        self.assertIn("#page=", popup.url)
        popup.close()
        self.page.get_by_text("Посмотреть страницу PDF", exact=True).first.click()
        image = self.page.locator(".panel img").first
        expect(image).to_be_visible()
        self.page.wait_for_function("document.querySelector('.panel img').naturalWidth > 0")
        self.page.set_viewport_size({"width": 390, "height": 844})
        self.assertTrue(self.page.evaluate("document.documentElement.scrollWidth <= innerWidth"))
        self.screenshot("mobile")

    def test_broken_reasons_unsure_separation_and_draft_reload(self):
        self.page.locator('#answers button[data-answer="NO"]').click()
        self.page.reload()
        expect(self.page.locator('#answers button[data-answer="NO"]')).to_have_attribute("aria-pressed", "true")
        self.assertEqual(self.store.state()["progress"]["answered"], 0)
        for reason in ("WRONG_FRAGMENTS", "WRONG_SOURCE", "MISSING_FRAGMENT", "OTHER"):
            self.select_case(self.packet.cases[0])
            self.page.locator("#problem").click()
            expect(self.page.locator("#save")).to_be_disabled()
            self.page.locator("#problem-reason").select_option(reason)
            self.page.locator("#note").fill("Synthetic problem report")
            self.page.locator("#save").click()
            expect(self.page.locator("#status")).to_have_text("Ответ сохранён")
            saved = self.store.state()["records"][self.packet.cases[0]["case_id"]]
            self.assertEqual(saved["problem_reason"], reason)
            self.assertIsNone(saved["human_answer"])
            self.assertIsNone(saved["mapped_answer"])
        self.save("UNSURE")
        expect(self.page.locator("#progress")).to_contain_text("Не уверен: 1")
        expect(self.page.locator("#progress")).to_contain_text("Проблема с примером: 1")

    def test_browser_close_and_service_restart_resume_without_local_storage(self):
        self.save("YES")
        self.context.close()
        port = self.server.server_port
        self.stop_server()
        self.store = Store(self.packet, self.directory)
        self.start_server(port)
        # Entirely new browser context: no localStorage/cookies to recover from.
        self.context = self.browser.new_context()
        self.page = self.context.new_page()
        self.page.goto(self.base)
        expect(self.page.locator("#progress")).to_contain_text("Ответов: 1 / 104")
        expect(self.page.locator("#case-select")).to_have_value(self.packet.cases[1]["case_id"])
        self.save("NO")
        self.assertEqual(len(json.loads(self.store.export())["revision_history"]), 2)

    def test_failed_request_retains_answer_and_reconciles_after_reload(self):
        self.page.route("**/api/answers", lambda route: route.abort())
        self.page.route("**/api/submissions/**", lambda route: route.abort())
        self.page.locator('#answers button[data-answer="YES"]').click()
        self.page.locator("#save").click()
        expect(self.page.locator("#status")).to_contain_text("Ответ пока не подтверждён")
        expect(self.page.locator('#answers button[data-answer="YES"]')).to_have_attribute("aria-pressed", "true")
        expect(self.page.locator("#save")).to_be_disabled()
        self.assertEqual(self.store.state()["progress"]["answered"], 0)
        self.page.reload()
        expect(self.page.locator('#answers button[data-answer="YES"]')).to_have_attribute("aria-pressed", "true")
        self.page.unroute("**/api/answers")
        self.page.unroute("**/api/submissions/**")
        expect(self.page.locator("#status")).to_have_text("Ответ сохранён", timeout=20000)
        self.assertEqual(len(json.loads(self.store.export())["revision_history"]), 1)
        self.assertEqual(self.page.evaluate("Object.keys(localStorage).filter(k=>k.includes(':draft:')).length"), 0)

    def test_lost_ack_duplicate_click_and_restarted_server_token(self):
        def lose_ack(route):
            route.fetch()  # Server commits, browser receives a simulated network failure.
            route.abort()
        self.page.route("**/api/answers", lose_ack)
        self.page.route("**/api/submissions/**", lambda route: route.abort())
        self.page.locator('#answers button[data-answer="UNSURE"]').click()
        self.page.locator("#save").evaluate("button => { button.click(); button.click(); }")
        expect(self.page.locator("#status")).to_contain_text("Ответ пока не подтверждён")
        self.assertEqual(self.store.state()["progress"]["answered"], 1)
        port = self.server.server_port
        self.stop_server()
        self.store = Store(self.packet, self.directory)
        self.start_server(port)
        self.page.unroute("**/api/answers")
        self.page.unroute("**/api/submissions/**")
        expect(self.page.locator("#status")).to_have_text("Ответ сохранён", timeout=20000)
        self.assertEqual(len(json.loads(self.store.export())["revision_history"]), 1)
        self.save("YES")  # Reconciliation refreshed the restarted server's token.

    def test_stale_tab_requires_explicit_reselection(self):
        self.page.locator('#answers button[data-answer="NO"]').click()
        self.store.save(answer(self.packet, value="YES"))
        self.page.locator("#save").click()
        expect(self.page.locator("#status")).to_contain_text("другом окне")
        expect(self.page.locator("#save")).to_be_disabled()
        self.assertEqual(self.store.state()["records"][self.packet.cases[0]["case_id"]]["human_answer"], "YES")
        self.save("NO")
        self.assertEqual(self.store.state()["records"][self.packet.cases[0]["case_id"]]["revision"], 2)

    def test_completion_download_freeze_and_stop(self):
        for c in self.packet.cases[:-1]:
            self.store.save(answer(self.packet, c))
        self.page.reload()
        expect(self.page.locator("#progress")).to_contain_text("Ответов: 103 / 104")
        with self.page.expect_download() as download:
            self.save("UNSURE")
        self.assertEqual(download.value.suggested_filename, "DEV_HUMAN_TRUTH_WAVE1.json")
        exported = json.loads(Path(download.value.path()).read_text())
        self.assertTrue(exported["frozen"])
        self.assertEqual(len(exported["cases"]), 104)
        expect(self.page.locator("#complete")).to_be_visible()
        expect(self.page.locator("#task")).to_be_hidden()
        self.assertTrue(self.store.state()["frozen"])


if __name__ == "__main__":
    unittest.main()
