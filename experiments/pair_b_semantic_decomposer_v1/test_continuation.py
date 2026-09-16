import ast
from pathlib import Path
import unittest


class TimeoutOnlyChange(unittest.TestCase):
    def test_call_has_only_the_deadline_change(self):
        here = Path(__file__).parent
        def body(name):
            tree = ast.parse((here / name).read_text())
            return next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'call')
        old, new = body('recovery.py'), body('continue_decomposition.py')
        edited = 0
        for node in ast.walk(old):
            if isinstance(node, ast.keyword) and node.arg == 'timeout':
                self.assertEqual(node.value.value, 900)
                node.value.value = 3600
                edited += 1
        self.assertEqual(edited, 1)
        self.assertEqual(ast.dump(old, include_attributes=False), ast.dump(new, include_attributes=False))


if __name__ == '__main__':
    unittest.main()
