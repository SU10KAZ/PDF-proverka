"""Real Chromium QA flow; writes only to disposable synthetic registries."""
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Thread
import json
import os
import unittest

from .audit import audit
from .packet import QAPacket
from .server import Server
from .store import Store, QA_NAME, FINAL_NAME

try:
    from playwright.sync_api import sync_playwright, expect
except ImportError:
    sync_playwright = None


@unittest.skipUnless(sync_playwright, "Playwright is required; skip is not a browser pass")
class BrowserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source,truth,cls.report=audit()
        cls.packet=QAPacket(source,truth)
        cls.playwright=sync_playwright().start()
        executable=os.environ.get("QA_CHROMIUM","/opt/google/chrome/chrome")
        cls.browser=cls.playwright.chromium.launch(headless=True,executable_path=executable)

    @classmethod
    def tearDownClass(cls):
        cls.browser.close();cls.playwright.stop()

    def setUp(self):
        self.tmp=TemporaryDirectory(prefix="human-qa-browser-synthetic-")
        self.store=Store(self.packet,self.tmp.name,self.report)
        self.server=Server(self.packet,self.store,port=0)
        self.thread=Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.base=f"http://127.0.0.1:{self.server.server_port}"
        self.context=self.browser.new_context(viewport={"width":1440,"height":1000})
        self.page=self.context.new_page();self.errors=[]
        self.page.on("pageerror",lambda error:self.errors.append(str(error)))
        self.page.goto(self.base);expect(self.page.locator("#task")).to_be_visible()

    def tearDown(self):
        self.context.close();self.server.shutdown();self.server.server_close();self.thread.join();self.tmp.cleanup()
        self.assertEqual(self.errors,[])

    def save(self, answer):
        self.page.locator(f'[data-answer="{answer}"]').click();self.page.locator("#save").click()
        expect(self.page.locator("#status")).to_have_text("Ответ сохранён")

    def screenshot(self,name):
        if directory:=os.environ.get("QA_SCREENSHOTS"):
            path=Path(directory);path.mkdir(parents=True,exist_ok=True)
            self.page.screenshot(path=str(path/(name+".png")),full_page=True)

    def test_full_blind_flow_pdf_navigation_disagreement_only_and_human_freeze(self):
        self.assertEqual(self.page.locator("#case-select option").count(),10)
        self.assertEqual(self.page.locator("#answers button").all_text_contents(),["Да","Нет","Не могу определить","Проблема с примером"])
        self.assertNotRegex(self.page.locator("body").inner_text(),r"\b(?:SAME|NEW|REVIEW|SECTION|TABLE|OWNER|T[1-7]|S[1-5])\b")
        expect(self.page.locator("#comparison")).to_be_hidden()
        expect(self.page.locator("#summary")).to_be_hidden()
        self.screenshot("blind-desktop")
        with self.page.expect_response(lambda r: "/page/" in r.url and r.url.endswith(".png")) as loaded:
            self.page.get_by_text("PDF и соседние страницы",exact=True).first.click()
        self.assertEqual(loaded.value.status,200)
        expect(self.page.locator(".panel img").first).to_have_js_property("complete",True)
        self.assertGreater(self.page.locator(".panel img").first.evaluate("img => img.naturalWidth"),0)
        navigation=self.page.locator(".navigation").first
        page_no=int(navigation.locator("input").input_value())
        navigation.get_by_role("button",name="→",exact=True).click()
        expect(navigation.locator("input")).to_have_value(str(page_no+1))
        self.assertIn(f"#page={page_no+1}",self.page.get_by_role("link",name="Открыть PDF").first.get_attribute("href"))
        self.page.set_viewport_size({"width":390,"height":844})
        self.assertTrue(self.page.evaluate("document.documentElement.scrollWidth<=innerWidth"))
        self.screenshot("blind-mobile")
        self.page.set_viewport_size({"width":1440,"height":1000})
        ids=list(self.packet.mapping); differing=ids[0]
        for i,qid in enumerate(ids):
            self.page.locator("#case-select").select_option(qid)
            original=self.packet.original[self.packet.mapping[qid]]["human_answer"]
            answer=("NO" if original=="YES" else "YES") if qid==differing else original
            self.save(answer)
            if i<9:
                expect(self.page.locator("#summary")).to_be_hidden()
                self.assertNotIn("Первоначальный ответ",self.page.locator("body").inner_text())
        expect(self.page.locator("#summary")).to_contain_text("Совпадений: 9. Расхождений: 1.")
        self.assertEqual(self.page.locator("#case-select option").count(),1)
        expect(self.page.locator("#comparison")).to_be_visible()
        expect(self.page.locator("#freeze")).to_be_hidden()
        self.screenshot("disagreement-synthetic")
        self.save("YES")
        expect(self.page.locator("#freeze")).to_be_visible()
        self.assertFalse((Path(self.tmp.name)/FINAL_NAME).exists())
        qa=(Path(self.tmp.name)/QA_NAME).read_bytes()
        self.page.locator("#freeze").click()
        expect(self.page.locator("#download-final")).to_be_visible()
        expect(self.page.locator("#task")).to_be_hidden()
        self.assertEqual((Path(self.tmp.name)/QA_NAME).read_bytes(),qa)
        self.assertTrue(json.loads((Path(self.tmp.name)/FINAL_NAME).read_bytes())["frozen"])

    def test_lost_acknowledgement_reload_and_duplicate_recovery(self):
        first=list(self.packet.mapping)[0]
        self.page.locator('[data-answer="NO"]').click()
        self.page.reload()
        expect(self.page.locator('[data-answer="NO"]')).to_have_attribute("aria-pressed","true")
        def lost(route):
            route.fetch();route.abort()
        self.page.route("**/api/answers",lost)
        self.page.locator("#save").click()
        expect(self.page.locator("#status")).to_contain_text("Сохранение не подтверждено")
        self.assertEqual(self.store.state()["answered"],1)
        self.page.unroute("**/api/answers",lost)
        self.page.reload()
        expect(self.page.locator("#status")).to_have_text("Ответ сохранён")
        self.assertEqual(self.store.state()["answered"],1)
        self.assertEqual(self.store.state()["records"][first]["revision"],1)
        expect(self.page.locator("#case-select")).not_to_have_value(first)


if __name__=="__main__":
    unittest.main()
