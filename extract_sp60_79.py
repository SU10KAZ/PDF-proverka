import fitz, re

pdf_path = r'C:\Users\uzun.a.i\.claude\projects\D----------------------------1--Calude-code\5e2f784b-3db2-4dfe-96b2-3824b08bc734\tool-results\webfetch-1774111084362-bky1yh.pdf'
doc = fitz.open(pdf_path)

full_text = ''
for page in doc:
    full_text += page.get_text() + '\n'

print("=== SECTION 7.9 FULL (up to 7.10) ===")
m = re.search(r'7\.9\s+Оборудование(.+?)7\.10\s', full_text, re.DOTALL)
if m:
    print(m.group(0)[:3000])
else:
    print("NOT FOUND, showing 7.9 content:")
    m = re.search(r'7\.9\s+Оборудование.{0,3000}', full_text, re.DOTALL)
    if m:
        print(m.group(0)[:3000])

print("\n=== LOOKING FOR FAN WORKING POINT REQUIREMENTS ===")
# Look for "рабочей зоне" or "рабочей точке" in context of fans
for m in re.finditer(r'.{200}(рабочей зоне|рабочей точке|рабочей характеристике|запас.{0,20}давлен).{200}', full_text, re.DOTALL):
    print("MATCH:", m.group(0)[:400])
    print("---")

print("\n=== SECTION 7.10 ===")
m = re.search(r'7\.10\s+.{0,2000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1500])

print("\n=== SECTION 7.11.3 AND 7.11.4-7.11.6 ===")
for num in ['7.11.3', '7.11.4', '7.11.5', '7.11.6']:
    m = re.search(rf'{re.escape(num)}.{{0,500}}', full_text, re.DOTALL)
    if m:
        print(f'--- {num} ---')
        print(m.group(0)[:400])
