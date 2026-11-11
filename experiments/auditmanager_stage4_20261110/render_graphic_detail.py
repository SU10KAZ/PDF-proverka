"""Compatibility CLI for the shared GRAPHIC-only renderer."""
import argparse
import json
from backend.app.pipeline.stages.prepare.graphic_detail_render import render

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ('pdf-path', 'blocks-path', 'block-id', 'expected-pdf-sha256', 'output-dir'):
        parser.add_argument('--'+arg, required=True)
    parser.add_argument('--columns', type=int, default=4)
    parser.add_argument('--rows', type=int, default=1)
    parser.add_argument('--scale', type=float, default=2.0)
    parser.add_argument('--overlap', type=float, default=0.025)
    parser.add_argument('--padding', type=float, default=0.0)
    result = render(**vars(parser.parse_args()))
    print(json.dumps({'tiles': len(result['tiles']), 'semantic_verified': False}))
