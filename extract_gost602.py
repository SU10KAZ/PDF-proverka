import fitz, re

pdf_path = r'C:\Users\uzun.a.i\.claude\projects\D----------------------------1--Calude-code\5e2f784b-3db2-4dfe-96b2-3824b08bc734\tool-results\webfetch-1774110856931-m114p0.pdf'
doc = fitz.open(pdf_path)

full_text = ''
for i, page in enumerate(doc):
    full_text += f'\n===PAGE {i+1}===\n' + page.get_text()

print("Total chars:", len(full_text))
print("=== ALL SECTION HEADINGS ===")
# Find section headings
for m in re.finditer(r'\n\s*(\d+\.\d+(?:\.\d+)?)\s+([А-ЯЁа-яё].{5,60})', full_text):
    print(m.group(0).strip()[:100])

print("\n=== SECTION 5.4 ===")
m = re.search(r'5\.4.{0,1000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:800])
else:
    print("NOT FOUND")

print("\n=== SECTIONS 4.5-4.8 ===")
m = re.search(r'4\.5.{0,1500}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1200])
else:
    print("NOT FOUND")

print("\n=== SECTIONS 5.x overview ===")
m = re.search(r'5\s+[А-ЯЁ].{0,3000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:2000])
