import unittest

import numpy as np
import pandas as pd

from liiatools.common._transform_functions import to_integer
from liiatools.common.data import ColumnConfig


class TestToInteger(unittest.TestCase):
    def setUp(self):
        self.column_config = ColumnConfig(id="test_column", type="integer")
        self.metadata = {"metadata": "test"}

    def test_to_integer_with_valid_number(self):
        row = pd.Series({"test_column": "123.0"})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, 123)

    def test_to_integer_with_invalid_number(self):
        row = pd.Series({"test_column": "abc"})
        result = to_integer(row, self.column_config, self.metadata)
        self.assertEqual(result, "abc")

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
