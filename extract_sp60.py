import fitz
import sys

pdf_path = r'C:\Users\uzun.a.i\.claude\projects\D----------------------------1--Calude-code\5e2f784b-3db2-4dfe-96b2-3824b08bc734\tool-results\webfetch-1774111084362-bky1yh.pdf'
doc = fitz.open(pdf_path)
print('Pages:', doc.page_count)

targets = ['7.11', '7.3.4', '7.1.1', '7.1 ', '8.11', 'воздуховод', 'вентилятор', 'клапан']
for i, page in enumerate(doc):
    text = page.get_text()
    lines = text.split('\n')
    for j, line in enumerate(lines):
        if any(t in line for t in targets):
            start = max(0, j-1)
            end = min(len(lines), j+10)
            context = '\n'.join(lines[start:end])
            print(f'=== Page {i+1}, line {j} ===')
            print(context)
            print('---')
