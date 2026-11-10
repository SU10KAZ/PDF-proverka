"""Literal prepared-MD graphic stamps, separate from generated graphic summaries."""
from collections import Counter
import re


def graphic_stamp_metadata(md, block_ids):
    headers = list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$', md, re.M | re.I))
    counts = Counter(h.group(2) for h in headers)
    wanted = set(block_ids)
    result = []
    for i, h in enumerate(headers):
        bid = h.group(2)
        if bid not in wanted or counts[bid] != 1 or h.group(1).upper() not in ('IMAGE', 'GRAPHIC'):
            continue
        body = md[h.end():headers[i+1].start() if i+1 < len(headers) else len(md)]
        body = re.split(r'^##[ \t]+(?:Page|СТРАНИЦА)\b', body, maxsplit=1, flags=re.M | re.I)[0]
        # Only the explicit metadata line; never the model's drawing description.
        stamps = re.findall(r'^> \*\*Stamp:\*\*[^\n]*$', body, re.M)
        if len(stamps) == 1:
            result.append({'block_id': bid, 'stamp': stamps[0]})
    return result
