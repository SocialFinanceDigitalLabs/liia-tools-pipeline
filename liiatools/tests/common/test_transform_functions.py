import unittest

import numpy as np
import pandas as pd
import hashlib
import hmac

from liiatools.common._transform_functions import to_integer
from liiatools.common._transform_functions import hmac_column_sha256
from liiatools.common.data import ColumnConfig


class TestToInteger(unittest.TestCase):
    def setUp(self):
        self.column_config = ColumnConfig(id="test_column", type="string")
        self.metadata = {"metadata": "test"}

    def test_to_integer_with_valid_number(self):
        row = pd.Series({"test_column": " 123.0"})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, 123)

    def test_to_integer_with_valid_int(self):
        row = pd.Series({"test_column": 123})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, 123)

    def test_to_integer_with_valid_string(self):
        row = pd.Series({"test_column": " abc123"})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, "ABC123")

    def test_to_integer_with_nan(self):
        row = pd.Series({"test_column": np.nan})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, "")

    def test_to_integer_with_na(self):
        row = pd.Series({"test_column": pd.NA})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, "")

    def test_to_integer_with_empty_value(self):
        row = pd.Series({"test_column": None})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, "")


class TestHmacColumnSha256(unittest.TestCase):

    def setUp(self):
        self.column_config = ColumnConfig(id="child_id", type="string")
        self.key_a = "test-secret-key-a"
        self.key_b = "test-secret-key-b"

    def _row(self, value):
        return pd.Series({"child_id": value})

    def test_same_input_same_key_gives_same_hash(self):
        """Same value + same key must always produce the same output (linking depends on this)"""
        row = self._row("ABC123XYZ_501")

        result_1 = hmac_column_sha256(row, self.column_config, self.key_a)
        result_2 = hmac_column_sha256(row, self.column_config, self.key_a)

        self.assertEqual(result_1, result_2)

    def test_different_key_gives_different_hash(self):
        """Changing the key must change the output - proves HMAC key is actually used"""
        row = self._row("ABC123XYZ_501")

        result_with_key_a = hmac_column_sha256(row, self.column_config, self.key_a)
        result_with_key_b = hmac_column_sha256(row, self.column_config, self.key_b)

        self.assertNotEqual(result_with_key_a, result_with_key_b)

    def test_different_values_give_different_hashes(self):
        """Different identifiers (same key) must produce different hashes"""
        row_1 = self._row("ABC123XYZ_501")
        row_2 = self._row("DEF456XYZ_501")

        result_1 = hmac_column_sha256(row_1, self.column_config, self.key_a)
        result_2 = hmac_column_sha256(row_2, self.column_config, self.key_a)

        self.assertNotEqual(result_1, result_2)

    def test_empty_string_value_returned_unchanged(self):
        """An empty identifier should be returned as-is, not hashed"""
        row = self._row("")

        result = hmac_column_sha256(row, self.column_config, self.key_a)

        self.assertEqual(result, "")

    def test_none_value_returned_unchanged(self):
        row = self._row(None)

        result = hmac_column_sha256(row, self.column_config, self.key_a)

        self.assertIsNone(result)

    # --- 7. Output format check --------------------------------------------

    def test_output_is_64_character_hex_string(self):
        """SHA-256 digests are always 64 hex characters, regardless of input length"""
        row = self._row("A")

        result = hmac_column_sha256(row, self.column_config, self.key_a)

        self.assertEqual(len(result), 64)
        int(result, 16)  # raises ValueError if not valid hex

    def test_suffixed_identifiers_from_different_las_produce_different_hashes(self):
        """
        Mirrors the manual test from 18 Sept: same raw identifier, but suffixed
        with different LA codes upstream (by add_la_suffix), must hash differently
        """
        row_la_1 = self._row("ABC123XYZ_501")   # LA code 501
        row_la_2 = self._row("ABC123XYZ_845")   # LA code 845 - same raw id, different LA

        result_1 = hmac_column_sha256(row_la_1, self.column_config, self.key_a)
        result_2 = hmac_column_sha256(row_la_2, self.column_config, self.key_a)

        self.assertNotEqual(result_1, result_2)
