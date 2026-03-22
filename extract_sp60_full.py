import fitz

pdf_path = r'C:\Users\uzun.a.i\.claude\projects\D----------------------------1--Calude-code\5e2f784b-3db2-4dfe-96b2-3824b08bc734\tool-results\webfetch-1774111084362-bky1yh.pdf'
doc = fitz.open(pdf_path)

# Extract all text
full_text = ''
for page in doc:
    full_text += page.get_text() + '\n'

# Find sections
import re

def find_section(text, section_num):
    pattern = rf'\b{re.escape(section_num)}\b'
    matches = list(re.finditer(pattern, text))
    results = []
    for m in matches:
        start = max(0, m.start() - 50)
        end = min(len(text), m.end() + 800)
        results.append(text[start:end])
    return results

sections = ['7.1.1', '7.1 ', '7.3.4', '7.11.1', '7.11.9', '8.11', '7.7', '7.8']
for s in sections:
    print(f'\n{"="*60}')
    print(f'SECTION {s}')
    print('='*60)
    results = find_section(full_text, s)
    for i, r in enumerate(results[:3]):
        print(f'--- Match {i+1} ---')
        print(r[:500])
        print()
