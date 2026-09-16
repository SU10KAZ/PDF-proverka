import unittest

from .consolidated_local_comparison_v2 import alias_entries, alias_lookup, validate_and_expand


class AliasContractTest(unittest.TestCase):
    def setUp(self):
        self.group = {"atomic_candidate_ids": ["bundle_one/c041", "bundle_two/c041", "bundle_three/C027"]}
        self.entries = alias_entries(self.group)
        self.valid = {"atomic_results": [
            {"atomic_alias": x["atomic_alias"], "verdict": "SUPPORTED", "reason": "test"} for x in self.entries]}

    def rejected(self, value):
        with self.assertRaises(ValueError): validate_and_expand(value, self.entries)

    def test_stable_aliases(self):
        self.assertEqual(alias_entries(self.group), self.entries)
        self.assertEqual([x["atomic_alias"] for x in self.entries], ["A01", "A02", "A03"])

    def test_exact_expansion(self):
        rows = validate_and_expand(self.valid, self.entries)["atomic_member_results"]
        self.assertEqual([x["candidate_id"] for x in rows], self.group["atomic_candidate_ids"])

    def test_one_to_one_lookup(self):
        self.assertEqual(alias_lookup(self.entries)["A02"], "bundle_two/c041")

    def test_short_id_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"]]}
        value["atomic_results"][0]["atomic_alias"] = "c041"; self.rejected(value)

    def test_unknown_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"]]}
        value["atomic_results"][0]["atomic_alias"] = "A99"; self.rejected(value)

    def test_duplicate_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"]]}
        value["atomic_results"].append(dict(value["atomic_results"][0])); self.rejected(value)

    def test_missing_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"][:-1]]}; self.rejected(value)

    def test_case_fuzzy_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"]]}
        value["atomic_results"][0]["atomic_alias"] = "a01"; self.rejected(value)

    def test_invented_rejected(self):
        value = {"atomic_results": [dict(x) for x in self.valid["atomic_results"]]}
        value["atomic_results"].append({"atomic_alias": "A88", "verdict": "SUPPORTED", "reason": "test"}); self.rejected(value)

    def test_duplicate_full_ids_rejected(self):
        with self.assertRaises(ValueError): alias_lookup([
            {"atomic_alias": "A01", "atomic_candidate_id": "x"},
            {"atomic_alias": "A02", "atomic_candidate_id": "x"}])


if __name__ == "__main__": unittest.main()
