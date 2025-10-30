import re
from datetime import datetime

from liiatools.common.reference import authorities


def check_year(filename):
    """
    Check a filename to see if it contains a year, if it does, return that year
    Expected year formats within string:
        2022
        14032021
        2017-18
        201819
        2019/20
        1920
        21/22
        21-22

    :param filename: Filename that probably contains a year
    :return: Year within the string
    :raises ValueError: If no year is found
    """
    match = re.search(r"(20)(\d{2})(.{0,3}\d{2})*", filename)
    if match:
        try:
            if len(match.group(3)) == 2:
                year = match.group(1) + match.group(3)
                return year
            if len(match.group(3)) == 3:
                year = match.group(1) + match.group(3)[-2:]
                return year
            if len(match.group(3)) == 4:
                year = match.group(3)
                return year
            if len(match.group(3)) == 5:
                year = match.group(3)[-4:]
                return year
        except TypeError:
            year = match.group(1) + match.group(2)
            return year

    fy_match = re.search(r"(\d{2})(.{0,3}\d{2})(.*)(\d*)", filename)
    if fy_match:
        if (
            len(fy_match.group(2)) == 2
            and int(fy_match.group(2)) == int(fy_match.group(1)) + 1
        ):
            year = "20" + fy_match.group(2)
            return year
        if (
            len(fy_match.group(2)) == 3
            and int(fy_match.group(2)[-2:]) == int(fy_match.group(1)) + 1
        ):
            year = "20" + fy_match.group(2)[-2:]
            return year
        if int(fy_match.group(3)[1:3]) == int(fy_match.group(2)[-2:]) + 1:
            year = "20" + fy_match.group(3)[1:3]
            return year
        if int(fy_match.group(2)[-2:]) == int(fy_match.group(2)[-4:-2]) + 1:
            year = "20" + fy_match.group(2)[-2:]
            return year

    raise ValueError


def check_month(filename):
    """
    Check a filename to see if it contains a month, if it does, return that month
    Expected month formats within string:
        jan
        january
        feb
        february

    :param filename: Filename that probably contains a month
    :return: Month within the string
    :raises ValueError: If no month is found
    """
    match = re.search(
        r"jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec", filename, re.IGNORECASE
    )
    if match:
        return match.group(0).lower()

    raise ValueError


def check_la(directory):
    """
    Check a directory to see if it contains the three-digit code associated with an LA, if it does, return that code
    Expected directory formats:
        Fons-a821f-Cambridgeshire-873
        Fons-04cd3-Thurrock-883
        Fons-0fg93-Hackney-HAC

    :param directory: Directory that contains an LA code
    :return: An LA code within the string
    :raises ValueError: If no LA is found
    """
    for pattern in authorities.codes:
        match = re.search(f"{pattern}$", directory)
        if match:
            return match.group(0)

    raise ValueError


def check_year_within_range(
    year, retention_period, new_year_start_month=1, as_at_date=datetime.now()
):
    """
    Check that year is within permitted range of data retention policy
    The check is made with reference to the as_at_date which will normally be the current date

    :param year: The year to check
    :param retention_period: The number of years to go back
    :param new_year_start_month: The month which signifies start of a new year for data retention policy
    :param as_at_date: The reference date against which we are checking the valid range
    :return: True if year is within range, False otherwise
    """

    year_to_check = int(year)
    current_year = as_at_date.year
    current_month = as_at_date.month
    if current_month < new_year_start_month:
        earliest_allowed_year = current_year - retention_period
        latest_allowed_year = current_year
    else:
        earliest_allowed_year = (
            current_year - retention_period + 1
        )  # roll forward one year
        latest_allowed_year = current_year + 1

    return earliest_allowed_year <= year_to_check <= latest_allowed_year


def check_la_signature(pipeline_config, report):
    """
    Check the LAs that have signed the data sharing agreement according to the config

    :param pipeline_config: The pipeline config object with the la signatures
    :param report: The report to check for signature status (e.g., PAN, SUFFICIENCY)
    :return: A list of LAs that have signed the data sharing agreement
    """
    signed_las = []

    for la_code, signature_status in pipeline_config.items():
        try:
            if signature_status[report] == "Yes":
                signed_las.append(la_code)
        except KeyError:
            continue

    return signed_las


def check_identifier(filename):
    """
    Check a filename to see if it contains an identifier, if it does, return that identifier
    Expected identifier formats within string:
        123456_0_5_2024_jan
        CANS_123456_0_5_2024_feb
        123456_6_21_2024_jan

    :param filename: Filename that probably contains an identifier
    :return: Identifier within the string
    :raises ValueError: If no identifier is found
    """
    match = re.search(r"([[a-zA-Z0-9]*)_\d_\d{1,2}_\d{4}_(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", filename)
    if match:
        if str.lower(match.group(1)) != "cans":
            return match.group(1)

    raise ValueError
