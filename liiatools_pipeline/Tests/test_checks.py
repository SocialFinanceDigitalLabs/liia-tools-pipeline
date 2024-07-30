import unittest
from datetime import datetime
from liiatools.common.checks import check_year, check_la, check_year_within_range, check_la_signature


class TestUtils(unittest.TestCase):

    def test_check_year(self):
        self.assertEqual(check_year('report_2022_summary.csv'), '2022')
        self.assertEqual(check_year('report_14032021_summary.csv'), '2021')
        self.assertEqual(check_year('report_2017-18_summary.csv'), '2018')
        self.assertEqual(check_year('report_201819_summary.csv'), '2019')
        self.assertEqual(check_year('report_2019/20_summary.csv'), '2020')
        self.assertEqual(check_year('report_1920_summary.csv'), '2020')
        self.assertEqual(check_year('report_21/22_summary.csv'), '2022')
        self.assertEqual(check_year('report_21-22_summary.csv'), '2022')
        with self.assertRaises(ValueError):
            check_year('report_no_year_summary.csv')

    def test_check_la(self):
        self.assertEqual(check_la('822_2023_header.csv'), '822')
        self.assertEqual(check_la('935_2023_episodes.csv'), '935')
        with self.assertRaises(ValueError):
            check_la('no_la_code_2023.csv')

    def test_check_year_within_range(self):
        self.assertTrue(check_year_within_range('2022', 5, 1, datetime(2023, 7, 29)))
        self.assertTrue(check_year_within_range('2023', 5, 1, datetime(2023, 7, 29)))
        self.assertFalse(check_year_within_range('2016', 5, 1, datetime(2023, 7, 29)))
        self.assertTrue(check_year_within_range('2024', 5, 1, datetime(2023, 7, 29)))

    def test_check_la_signature(self):
        pipeline_config = {
            '822': {'PAN': 'Yes', 'SUFFICIENCY': 'No'},
            '935': {'PAN': 'No', 'SUFFICIENCY': 'Yes'},
            '123': {'PAN': 'Yes', 'SUFFICIENCY': 'Yes'}
        }

        self.assertEqual(check_la_signature(pipeline_config, 'PAN'), ['822', '123'])
        self.assertEqual(check_la_signature(pipeline_config, 'SUFFICIENCY'), ['935', '123'])
        self.assertEqual(check_la_signature(pipeline_config, 'NON_EXISTENT_REPORT'), [])


if __name__ == '__main__':
    unittest.main()
