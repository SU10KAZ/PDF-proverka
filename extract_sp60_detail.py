import fitz, re

pdf_path = r'C:\Users\uzun.a.i\.claude\projects\D----------------------------1--Calude-code\5e2f784b-3db2-4dfe-96b2-3824b08bc734\tool-results\webfetch-1774111084362-bky1yh.pdf'
doc = fitz.open(pdf_path)

full_text = ''
for page in doc:
    full_text += page.get_text() + '\n'

# Find section 7.11 main text
print("=== SECTION 7.11 HEADING AND BEGINNING ===")
m = re.search(r'7\.11\s+Воздуховоды.{0,2000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1500])

print("\n=== SECTION 7.11.9 FULL TEXT ===")
m = re.search(r'7\.11\.9.{0,1500}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1000])

print("\n=== SECTION 7.9 EQUIPMENT FANS ===")
m = re.search(r'7\.9\s+Оборудование.{0,2000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1500])

print("\n=== SECTION 7.9.1 FULL ===")
m = re.search(r'7\.9\.1.{0,1500}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1000])

print("\n=== 7.3.4 FULL ===")
m = re.search(r'7\.3\.4.{0,1000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:800])

print("\n=== 7.1 GENERAL PROVISIONS (first 1000 chars) ===")
m = re.search(r'7\.1\s+Общие положения.{0,2000}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:1200])

print("\n=== 8.11 FULL ===")
m = re.search(r'8\.11.{0,800}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:800])

print("\n=== 7.11.8 FULL ===")
m = re.search(r'7\.11\.8.{0,600}', full_text, re.DOTALL)
if m:
    print(m.group(0)[:600])
