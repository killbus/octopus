import contextlib
import io
import unittest

from scripts import updatePrice


class CollectEntriesTest(unittest.TestCase):
    def test_model_ids_are_case_insensitively_deduplicated(self):
        raw_price = {
            "openai": {
                "models": {
                    "first": {"id": "GLM-5.2", "cost": {"input": 1, "output": 2}},
                }
            },
            "zhipuai": {
                "models": {
                    "second": {
                        "id": "glm-5.2",
                        "cost": {"input": 9, "output": 10},
                    },
                }
            },
        }

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            entries = updatePrice.collect_entries(raw_price)

        self.assertEqual(["glm-5.2"], list(entries))
        self.assertIn("Input: 1, Output: 2", entries["glm-5.2"])
        self.assertIn("Duplicate model 'glm-5.2'", output.getvalue())

    def test_real_model_wins_over_an_earlier_alias(self):
        raw_price = {
            "anthropic": {
                "models": {
                    "alias-source": {
                        "id": "claude-opus-4-5",
                        "cost": {"input": 1},
                    },
                    "real-model": {
                        "id": "claude-4.5-opus",
                        "cost": {"input": 7},
                    },
                }
            }
        }

        entries = updatePrice.collect_entries(raw_price)

        self.assertIn("Input: 7", entries["claude-4.5-opus"])


if __name__ == "__main__":
    unittest.main()
