import json
with open(r'D:\Отедел Системного Анализа\1. Calude code\projects\АИ\133-23-ГК-АИ2\_output\02_blocks_analysis.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

all_block_ids = set()
blocks_by_page = {}
block_info = {}
for block in data['block_analyses']:
    bid = block['block_id']
    page = block.get('page', 0)
    all_block_ids.add(bid)
    if page not in blocks_by_page:
        blocks_by_page[page] = []
    blocks_by_page[page].append(bid)
    block_info[bid] = {
        'page': page,
        'sheet': block.get('sheet', ''),
        'label': block.get('label', ''),
        'summary': block.get('summary', '')[:100]
    }

print(f'Total blocks: {len(all_block_ids)}')

phantoms = ['9XJD-DWQC-WMH', '4X7E-JEPF-9DD', 'P3CM-VNLW-F7U']
for p in phantoms:
    status = "EXISTS" if p in all_block_ids else "PHANTOM"
    print(f'{p}: {status}')

print('\nBlocks on page 27:')
for bid in blocks_by_page.get(27, []):
    info = block_info[bid]
    print(f'  {bid}: sheet={info["sheet"]}, label={info["label"][:40]}, summary={info["summary"][:60]}')

print('\nBlocks on page 10:')
for bid in blocks_by_page.get(10, []):
    info = block_info[bid]
    print(f'  {bid}: sheet={info["sheet"]}, label={info["label"][:40]}, summary={info["summary"][:60]}')

print('\nBlocks on page 4:')
for bid in blocks_by_page.get(4, []):
    info = block_info[bid]
    print(f'  {bid}: sheet={info["sheet"]}, label={info["label"][:40]}, summary={info["summary"][:60]}')

print('\nBlocks on page 30 (for F-103, block 9K3R-DW3E-VWD):')
for bid in blocks_by_page.get(30, []):
    info = block_info[bid]
    print(f'  {bid}: sheet={info["sheet"]}, label={info["label"][:40]}, summary={info["summary"][:60]}')
