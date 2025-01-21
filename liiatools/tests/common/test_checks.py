import unittest
from datetime import datetime

from liiatools.common.checks import (
    check_la,
    check_la_signature,
    check_month,
    check_year,
    check_year_within_range,
)


def test_check_year():
    assert check_year("2020 SHOULD BE PLACED FOR ADOPTION Version 12") == "2020"
    assert check_year("19/20 adoption version 11") == "2020"
    assert check_year("2018/19 adoption version 11") == "2019"
    assert check_year("file_2022_ad1") == "2022"
    assert check_year("file_14032021") == "2021"
    assert check_year("file_20032021") == "2021"
    assert check_year("file_2017-18") == "2018"
    assert check_year("file_201819") == "2019"
    assert check_year("file_1920") == "2020"
    assert check_year("file_21/22") == "2022"
    assert check_year("file_version_12_18/19") == "2019"
    assert check_year("file_version_1_18/19_final") == "2019"
    assert check_year("file_version_1_1819") == "2019"


class TestCheckYear(unittest.TestCase):
    def test_check_year(self):
        with self.assertRaises(ValueError):
            check_year("file_no_year.csv")

    def test_check_year_2(self):
        with self.assertRaises(ValueError):
            check_year("1811.csv")


def test_check_month():
    assert check_month("annex_a_jan_2024") == "jan"
    assert check_month("annex_a_FEB_2022") == "feb"
    assert check_month("annex_a_Mar_2021") == "mar"
    assert check_month("annex_a_april_2030") == "apr"


class TestCheckMonth(unittest.TestCase):
    def test_check_month(self):
        with self.assertRaises(ValueError):
            check_month("file_no_month.csv")

    def test_check_month_2(self):
        with self.assertRaises(ValueError):
            check_month("file_apeel_2012.csv")


def test_check_la():
    assert check_la("Fons-a821f-Cambridgeshire-873") == "873"
    assert check_la("Fons-04cd3-Thurrock-883") == "883"
    assert check_la("Fons-0fg93-Hackney-HAC") == "HAC"


class TestCheckLA(unittest.TestCase):
    def test_check_la(self):
        with self.assertRaises(ValueError):
            check_la("file_no_la.csv")

    def test_check_la_2(self):
        with self.assertRaises(ValueError):
            check_la("SSDA903_2020_episodes.csv")


def test_check_year_within_range():
    assert check_year_within_range(2016, 6, 6, datetime(2023, 5, 31)) is False
    assert check_year_within_range(2023, 6, 6, datetime(2023, 5, 31)) is True
    assert check_year_within_range(2024, 6, 6, datetime(2023, 5, 31)) is False
    assert check_year_within_range(2024, 6, 6, datetime(2023, 6, 1)) is True
    assert check_year_within_range(2013, 10, 2, datetime(2023, 1, 31)) is True


def test_check_la_signature():
    pipeline_config = {
        "BAR": {"PAN": "Yes", "SUFFICIENCY": "Yes"},
        "BEX": {"PAN": "Yes", "SUFFICIENCY": "No"},
    }

    assert check_la_signature(pipeline_config, "PAN") == ["BAR", "BEX"]
    assert check_la_signature(pipeline_config, "SUFFICIENCY") == ["BAR"]
    assert check_la_signature(pipeline_config, None) == []
    assert check_la_signature(pipeline_config, "") == []
